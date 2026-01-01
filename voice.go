// Discordgo - Discord bindings for Go
// Available at https://github.com/bwmarrin/discordgo

// Copyright 2015-2016 Bruce Marriner <bruce@sqls.net>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains code related to Discord voice support

package discordgo

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ------------------------------------------------------------------------------------------------
// Code related to both VoiceConnection Websocket and UDP connections.
// ------------------------------------------------------------------------------------------------

// voiceState holds all mutable state owned by the owner goroutine.
// Only the owner goroutine may access these fields directly.
type voiceState struct {
	ready        bool
	speaking     bool
	deaf         bool
	mute         bool
	reconnecting bool

	// channelID is the current voice channel - mutable, updated on channel changes
	channelID string

	wsConn  *websocket.Conn
	wsMutex sync.Mutex // Serialize websocket writes
	udpConn *net.UDPConn

	aead         cipher.AEAD
	nonceCounter uint32

	sessionID string
	token     string
	endpoint  string

	op2 voiceOP2
	op4 voiceOP4

	close chan struct{}

	voiceSpeakingUpdateHandlers []VoiceSpeakingUpdateHandler
}

// voiceCmd represents a command to be executed by the owner goroutine.
type voiceCmd struct {
	fn   func(s *voiceState)
	done chan struct{}
}

// A VoiceConnection struct holds all the data and functions related to a Discord Voice Connection.
type VoiceConnection struct {
	// Command channel - all state operations go through here
	cmds   chan voiceCmd
	ctx    context.Context
	cancel context.CancelFunc

	// Thread-safe audio channels (Go channels are safe for concurrent use)
	OpusSend chan []byte  // Chan for sending opus audio
	OpusRecv chan *Packet // Chan for receiving opus audio

	// Immutable metadata (set once during creation, read-only after)
	Debug    bool // If true, print extra logging -- DEPRECATED
	LogLevel int
	UserID   string
	GuildID  string
	session  *Session
}

// VoiceSpeakingUpdateHandler type provides a function definition for the
// VoiceSpeakingUpdate event
type VoiceSpeakingUpdateHandler func(vc *VoiceConnection, vs *VoiceSpeakingUpdate)

// runOwner is the single owner of all voiceState.
// It processes commands sequentially, ensuring no concurrent access to mutable state.
func (v *VoiceConnection) runOwner(state *voiceState) {
	defer v.cleanup(state)

	for {
		select {
		case cmd := <-v.cmds:
			cmd.fn(state)
			if cmd.done != nil {
				close(cmd.done)
			}
		case <-v.ctx.Done():
			// Drain any pending commands to unblock waiting goroutines
			// This prevents goroutine leaks from DoVoice() callers stuck on <-done
			for {
				select {
				case cmd := <-v.cmds:
					// Don't execute the function since we're shutting down,
					// just close done to unblock the caller
					if cmd.done != nil {
						close(cmd.done)
					}
				default:
					return
				}
			}
		}
	}
}

// DoVoice executes fn with exclusive access to voiceState.
// Blocks until fn completes. Returns false if connection is closed.
//
// The three-level select handles these cases:
//   1. Outer select: Try to send command, or bail immediately if context is done
//   2. Middle select: Wait for command completion, or detect context cancellation
//   3. Inner select: Grace period for runOwner's drain loop to close done channel
//
// The 100ms timeout is defense-in-depth - the drain loop in runOwner should
// always close done, but this prevents infinite blocking if something goes wrong.
//
// Note: Deferred DoVoice calls will silently return false if context is cancelled.
// Callers using defer should ensure this is acceptable (e.g., state will be cleaned
// up by cleanup() anyway, or the VoiceConnection is being replaced).
func (v *VoiceConnection) DoVoice(fn func(s *voiceState)) bool {
	if v.cmds == nil {
		return false
	}
	done := make(chan struct{})
	select {
	case v.cmds <- voiceCmd{fn: fn, done: done}:
		// Command accepted - wait for runOwner to execute it
		select {
		case <-done:
			return true
		case <-v.ctx.Done():
			// Context cancelled while waiting - runOwner's drain loop should close done
			select {
			case <-done:
				return true
			case <-time.After(100 * time.Millisecond):
				return false
			}
		}
	case <-v.ctx.Done():
		return false
	}
}

// DoVoiceAsync sends a command without waiting for completion.
func (v *VoiceConnection) DoVoiceAsync(fn func(s *voiceState)) {
	if v.cmds == nil {
		return
	}
	select {
	case v.cmds <- voiceCmd{fn: fn}:
	case <-v.ctx.Done():
	}
}

// cleanup handles graceful shutdown of voice connection resources
func (v *VoiceConnection) cleanup(state *voiceState) {
	v.log(LogInformational, "cleaning up voice connection")

	state.ready = false
	state.speaking = false

	// Close signal channel to stop all goroutines
	if state.close != nil {
		close(state.close)
		state.close = nil
	}

	// Close UDP connection
	if state.udpConn != nil {
		v.log(LogInformational, "closing udp")
		if err := state.udpConn.Close(); err != nil {
			v.log(LogError, "error closing udp connection, %s", err)
		}
		state.udpConn = nil
	}

	// Close WebSocket connection
	if state.wsConn != nil {
		v.log(LogInformational, "sending close frame")

		state.wsMutex.Lock()
		err := state.wsConn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		state.wsMutex.Unlock()
		if err != nil {
			v.log(LogError, "error closing websocket, %s", err)
		}

		time.Sleep(1 * time.Second)

		v.log(LogInformational, "closing websocket")
		if err := state.wsConn.Close(); err != nil {
			v.log(LogError, "error closing websocket, %s", err)
		}
		state.wsConn = nil
	}

	v.log(LogInformational, "Voice connection cleaned up")
}

// Ready returns the current ready state (thread-safe read via command).
func (v *VoiceConnection) Ready() bool {
	var ready bool
	v.DoVoice(func(s *voiceState) {
		ready = s.ready
	})
	return ready
}

// GetChannelID returns the current channel ID (thread-safe read via command).
func (v *VoiceConnection) GetChannelID() string {
	var channelID string
	v.DoVoice(func(s *voiceState) {
		channelID = s.channelID
	})
	return channelID
}

// Speaking sends a speaking notification to Discord over the voice websocket.
// This must be sent as true prior to sending audio and should be set to false
// once finished sending audio.
// b : Send true if speaking, false if not.
func (v *VoiceConnection) Speaking(b bool) (err error) {
	v.log(LogDebug, "called (%t)", b)

	type voiceSpeakingData struct {
		Speaking bool `json:"speaking"`
		Delay    int  `json:"delay"`
	}

	type voiceSpeakingOp struct {
		Op   int               `json:"op"` // Always 5
		Data voiceSpeakingData `json:"d"`
	}

	v.DoVoice(func(s *voiceState) {
		if s.wsConn == nil {
			err = fmt.Errorf("no VoiceConnection websocket")
			return
		}

		data := voiceSpeakingOp{5, voiceSpeakingData{b, 0}}
		s.wsMutex.Lock()
		err = s.wsConn.WriteJSON(data)
		s.wsMutex.Unlock()

		if err != nil {
			s.speaking = false
			v.log(LogError, "Speaking() write json error, %s", err)
			return
		}

		s.speaking = b
	})

	return
}

// ChangeChannel sends Discord a request to change channels within a Guild
// !!! NOTE !!! This function may be removed in favour of just using ChannelVoiceJoin
func (v *VoiceConnection) ChangeChannel(channelID string, mute, deaf bool) (err error) {
	v.log(LogInformational, "called")

	data := voiceChannelJoinOp{4, voiceChannelJoinData{&v.GuildID, &channelID, mute, deaf}}
	v.session.RLock()
	wsConn := v.session.wsConn
	v.session.RUnlock()
	if wsConn == nil {
		return fmt.Errorf("websocket connection not available")
	}
	v.session.wsMutex.Lock()
	err = wsConn.WriteJSON(data)
	v.session.wsMutex.Unlock()
	if err != nil {
		return
	}

	// Update mutable state via command
	v.DoVoice(func(s *voiceState) {
		s.channelID = channelID
		s.deaf = deaf
		s.mute = mute
		s.speaking = false
	})

	return
}

// Disconnect disconnects from this voice channel and closes the websocket
// and udp connections to Discord.
func (v *VoiceConnection) Disconnect() (err error) {
	// Send disconnect packet via command
	v.DoVoice(func(s *voiceState) {
		if s.sessionID != "" {
			data := voiceChannelJoinOp{4, voiceChannelJoinData{&v.GuildID, nil, true, true}}
			v.session.RLock()
			wsConn := v.session.wsConn
			v.session.RUnlock()
			if wsConn != nil {
				v.session.wsMutex.Lock()
				err = wsConn.WriteJSON(data)
				v.session.wsMutex.Unlock()
			}
			s.sessionID = ""
		}
	})

	// Cancel context to trigger cleanup
	v.cancel()

	v.log(LogInformational, "Deleting VoiceConnection %s", v.GuildID)

	v.session.Lock()
	delete(v.session.VoiceConnections, v.GuildID)
	v.session.Unlock()

	return
}

// Close closes the voice ws and udp connections
func (v *VoiceConnection) Close() {
	v.log(LogInformational, "Close called")
	v.cancel()

	// Remove from session's VoiceConnections map
	if v.session != nil {
		v.session.Lock()
		delete(v.session.VoiceConnections, v.GuildID)
		v.session.Unlock()
	}
}

// AddHandler adds a Handler for VoiceSpeakingUpdate events.
func (v *VoiceConnection) AddHandler(h VoiceSpeakingUpdateHandler) {
	v.DoVoice(func(s *voiceState) {
		s.voiceSpeakingUpdateHandlers = append(s.voiceSpeakingUpdateHandlers, h)
	})
}

// VoiceSpeakingUpdate is a struct for a VoiceSpeakingUpdate event.
type VoiceSpeakingUpdate struct {
	UserID   string `json:"user_id"`
	SSRC     int    `json:"ssrc"`
	Speaking bool   `json:"speaking"`
}

// ------------------------------------------------------------------------------------------------
// Unexported Internal Functions Below.
// ------------------------------------------------------------------------------------------------

// A voiceOP4 stores the data for the voice operation 4 websocket event
// which provides us with the NaCl SecretBox encryption key
type voiceOP4 struct {
	SecretKey [32]byte `json:"secret_key"`
	Mode      string   `json:"mode"`
}

// A voiceOP2 stores the data for the voice operation 2 websocket event
// which is sort of like the voice READY packet
type voiceOP2 struct {
	SSRC              uint32        `json:"ssrc"`
	Port              int           `json:"port"`
	Modes             []string      `json:"modes"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	IP                string        `json:"ip"`
}

// WaitUntilConnected waits for the Voice Connection to
// become ready, if it does not become ready it returns an err
func (v *VoiceConnection) waitUntilConnected() error {
	v.log(LogInformational, "called")

	i := 0
	for {
		if v.Ready() {
			return nil
		}

		if i > 10 {
			return fmt.Errorf("timeout waiting for voice")
		}

		time.Sleep(1 * time.Second)
		i++
	}
}

// open opens a voice connection. This should be called
// after VoiceChannelJoin is used and the data VOICE websocket events
// are captured.
func (v *VoiceConnection) open() (err error) {
	v.log(LogInformational, "called")

	v.DoVoice(func(s *voiceState) {
		// Don't open a websocket if one is already open
		if s.wsConn != nil {
			v.log(LogWarning, "refusing to overwrite non-nil websocket")
			return
		}

		// Wait for the SessionID
		i := 0
		for s.sessionID == "" {
			if i > 20 {
				err = fmt.Errorf("did not receive voice Session ID in time")
				return
			}
			// Temporarily release to allow sessionID to be populated
			// We're inside DoVoice, so we need to check via polling
			time.Sleep(50 * time.Millisecond)
			i++
		}

		// Connect to VoiceConnection Websocket
		vg := "wss://" + strings.TrimSuffix(s.endpoint, ":80")
		v.log(LogInformational, "connecting to voice endpoint %s", vg)
		s.wsConn, _, err = v.session.Dialer.Dial(vg, nil)
		if err != nil {
			v.log(LogWarning, "error connecting to voice endpoint %s, %s", vg, err)
			return
		}

		type voiceHandshakeData struct {
			ServerID  string `json:"server_id"`
			UserID    string `json:"user_id"`
			SessionID string `json:"session_id"`
			Token     string `json:"token"`
		}
		type voiceHandshakeOp struct {
			Op   int                `json:"op"` // Always 0
			Data voiceHandshakeData `json:"d"`
		}
		data := voiceHandshakeOp{0, voiceHandshakeData{v.GuildID, v.UserID, s.sessionID, s.token}}

		s.wsMutex.Lock()
		err = s.wsConn.WriteJSON(data)
		s.wsMutex.Unlock()
		if err != nil {
			v.log(LogWarning, "error sending init packet, %s", err)
			return
		}

		s.close = make(chan struct{})

		// Start websocket listener - runs outside command queue
		wsConn := s.wsConn
		closeChan := s.close
		go v.wsListen(wsConn, closeChan)
	})

	return
}

// wsListen listens on the voice websocket for messages and passes them
// to the voice event handler. This is automatically called by the open func.
func (v *VoiceConnection) wsListen(wsConn *websocket.Conn, close <-chan struct{}) {
	v.log(LogInformational, "wsListen called")

	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			// 4014 indicates a manual disconnection by someone in the guild
			if websocket.IsCloseError(err, 4014) {
				v.log(LogInformational, "received 4014 manual disconnection")
				v.handle4014Disconnect(wsConn)
				return
			}

			// Check if this is still the active connection
			var sameConnection bool
			v.DoVoice(func(s *voiceState) {
				sameConnection = s.wsConn == wsConn
			})

			if sameConnection {
				v.log(LogError, "voice websocket closed unexpectedly, %s", err)
				go v.reconnect()
			}
			return
		}

		// Pass received message to voice event handler
		select {
		case <-close:
			return
		case <-v.ctx.Done():
			return
		default:
			go v.onEvent(message)
		}
	}
}

// handle4014Disconnect handles the 4014 manual disconnection case
func (v *VoiceConnection) handle4014Disconnect(wsConn *websocket.Conn) {
	// Abandon the voice WS connection
	v.DoVoice(func(s *voiceState) {
		s.wsConn = nil
	})

	// Wait for VOICE_SERVER_UPDATE
	for i := 0; i < 5; i++ {
		<-time.After(1 * time.Second)

		var reconnected bool
		v.DoVoice(func(s *voiceState) {
			reconnected = s.wsConn != nil
		})
		if reconnected {
			v.log(LogInformational, "successfully reconnected after 4014 manual disconnection")
			return
		}
	}

	// When VOICE_SERVER_UPDATE is not received, disconnect
	v.log(LogInformational, "disconnect due to 4014 manual disconnection")

	v.session.Lock()
	delete(v.session.VoiceConnections, v.GuildID)
	v.session.Unlock()

	v.Close()
}

// onEvent handles any voice websocket events. This is only called by the
// wsListen() function.
func (v *VoiceConnection) onEvent(message []byte) {
	v.log(LogDebug, "received: %s", string(message))

	var e Event
	if err := json.Unmarshal(message, &e); err != nil {
		v.log(LogError, "unmarshall error, %s", err)
		return
	}

	switch e.Operation {

	case 2: // READY
		v.handleOP2Ready(e.RawData)

	case 3: // HEARTBEAT response
		// TODO: maybe implement latency tracking
		return

	case 4: // UDP encryption secret key
		v.handleOP4SecretKey(e.RawData)

	case 5: // Speaking update
		v.handleOP5Speaking(e.RawData)

	default:
		v.log(LogDebug, "unknown voice operation, %d, %s", e.Operation, string(e.RawData))
	}
}

// handleOP2Ready handles the voice READY event
func (v *VoiceConnection) handleOP2Ready(rawData json.RawMessage) {
	var op2 voiceOP2
	if err := json.Unmarshal(rawData, &op2); err != nil {
		v.log(LogError, "OP2 unmarshall error, %s, %s", err, string(rawData))
		return
	}

	// Store op2 and start goroutines via command
	v.DoVoice(func(s *voiceState) {
		s.op2 = op2

		// Start the voice websocket heartbeat
		wsConn := s.wsConn
		closeChan := s.close
		heartbeatInterval := s.op2.HeartbeatInterval
		go v.wsHeartbeat(wsConn, closeChan, heartbeatInterval)

		// Start the UDP connection
		err := v.udpOpenInternal(s)
		if err != nil {
			v.log(LogError, "error opening udp connection, %s", err)
			return
		}

		// Start the opusSender
		if v.OpusSend == nil {
			v.OpusSend = make(chan []byte, 2)
		}
		udpConn := s.udpConn
		go v.opusSender(udpConn, closeChan, v.OpusSend, 48000, 960)

		// Start the opusReceiver if not deaf
		if !s.deaf {
			if v.OpusRecv == nil {
				v.OpusRecv = make(chan *Packet, 2)
			}
			go v.opusReceiver(udpConn, closeChan, v.OpusRecv)
		}
	})
}

// handleOP4SecretKey handles the encryption secret key event
func (v *VoiceConnection) handleOP4SecretKey(rawData json.RawMessage) {
	v.DoVoice(func(s *voiceState) {
		s.op4 = voiceOP4{}
		if err := json.Unmarshal(rawData, &s.op4); err != nil {
			v.log(LogError, "OP4 unmarshall error, %s, %s", err, string(rawData))
			return
		}

		block, _ := aes.NewCipher(s.op4.SecretKey[:])
		s.aead, _ = cipher.NewGCM(block)
	})
}

// handleOP5Speaking handles speaking update events
func (v *VoiceConnection) handleOP5Speaking(rawData json.RawMessage) {
	var handlers []VoiceSpeakingUpdateHandler
	v.DoVoice(func(s *voiceState) {
		handlers = s.voiceSpeakingUpdateHandlers
	})

	if len(handlers) == 0 {
		return
	}

	voiceSpeakingUpdate := &VoiceSpeakingUpdate{}
	if err := json.Unmarshal(rawData, voiceSpeakingUpdate); err != nil {
		v.log(LogError, "OP5 unmarshall error, %s, %s", err, string(rawData))
		return
	}

	for _, h := range handlers {
		h(v, voiceSpeakingUpdate)
	}
}

type voiceHeartbeatOp struct {
	Op   int `json:"op"` // Always 3
	Data int `json:"d"`
}

// wsHeartbeat sends regular heartbeats to voice Discord so it knows the client
// is still connected. If you do not send these heartbeats Discord will
// disconnect the websocket connection after a few seconds.
func (v *VoiceConnection) wsHeartbeat(wsConn *websocket.Conn, close <-chan struct{}, i time.Duration) {
	if close == nil || wsConn == nil {
		return
	}

	ticker := time.NewTicker(i * time.Millisecond)
	defer ticker.Stop()

	for {
		v.log(LogDebug, "sending heartbeat packet")

		// Send heartbeat via command to ensure thread-safe wsConn access
		var err error
		var connectionChanged bool
		ok := v.DoVoice(func(s *voiceState) {
			if s.wsConn != wsConn {
				connectionChanged = true
				return
			}
			s.wsMutex.Lock()
			err = wsConn.WriteJSON(voiceHeartbeatOp{3, int(time.Now().Unix())})
			s.wsMutex.Unlock()
		})

		// Exit if context cancelled, connection changed, or error occurred
		if !ok || connectionChanged {
			return
		}
		if err != nil {
			v.log(LogError, "error sending heartbeat, %s", err)
			return
		}

		select {
		case <-ticker.C:
			// continue loop
		case <-close:
			return
		case <-v.ctx.Done():
			return
		}
	}
}

// ------------------------------------------------------------------------------------------------
// Code related to the VoiceConnection UDP connection
// ------------------------------------------------------------------------------------------------

type voiceUDPData struct {
	Address string `json:"address"`
	Port    uint16 `json:"port"`
	Mode    string `json:"mode"`
}

type voiceUDPD struct {
	Protocol string       `json:"protocol"`
	Data     voiceUDPData `json:"data"`
}

type voiceUDPOp struct {
	Op   int       `json:"op"`
	Data voiceUDPD `json:"d"`
}

// udpOpenInternal opens a UDP connection to the voice server.
// MUST be called from within DoVoice.
func (v *VoiceConnection) udpOpenInternal(s *voiceState) (err error) {
	if s.wsConn == nil {
		return fmt.Errorf("nil voice websocket")
	}

	if s.udpConn != nil {
		return fmt.Errorf("udp connection already open")
	}

	if s.close == nil {
		return fmt.Errorf("nil close channel")
	}

	if s.endpoint == "" {
		return fmt.Errorf("empty endpoint")
	}

	host := s.op2.IP + ":" + strconv.Itoa(s.op2.Port)
	addr, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		v.log(LogWarning, "error resolving udp host %s, %s", host, err)
		return
	}

	v.log(LogInformational, "connecting to udp addr %s", addr.String())
	s.udpConn, err = net.DialUDP("udp", nil, addr)
	if err != nil {
		v.log(LogWarning, "error connecting to udp addr %s, %s", addr.String(), err)
		return
	}

	// Create IP discovery packet
	sb := make([]byte, 74)
	binary.BigEndian.PutUint16(sb, 1)
	binary.BigEndian.PutUint16(sb[2:], 70)
	binary.BigEndian.PutUint32(sb[4:], s.op2.SSRC)

	_, err = s.udpConn.Write(sb)
	if err != nil {
		v.log(LogWarning, "udp write error to %s, %s", addr.String(), err)
		return
	}

	// Receive response
	rb := make([]byte, 74)
	rlen, _, err := s.udpConn.ReadFromUDP(rb)
	if err != nil {
		v.log(LogWarning, "udp read error, %s, %s", addr.String(), err)
		return
	}

	if rlen < 74 {
		v.log(LogWarning, "received udp packet too small")
		return fmt.Errorf("received udp packet too small")
	}

	// Parse IP
	var ip string
	for i := 8; i < len(rb)-2; i++ {
		if rb[i] == 0 {
			break
		}
		ip += string(rb[i])
	}

	// Parse port
	port := binary.BigEndian.Uint16(rb[len(rb)-2:])

	// Send mode selection
	data := voiceUDPOp{1, voiceUDPD{"udp", voiceUDPData{ip, port, "aead_aes256_gcm_rtpsize"}}}

	s.wsMutex.Lock()
	err = s.wsConn.WriteJSON(data)
	s.wsMutex.Unlock()
	if err != nil {
		v.log(LogWarning, "udp write error, %#v, %s", data, err)
		return
	}

	// Start UDP keepalive
	udpConn := s.udpConn
	closeChan := s.close
	go v.udpKeepAlive(udpConn, closeChan, 5*time.Second)

	return
}

// udpKeepAlive sends a udp packet to keep the udp connection open
func (v *VoiceConnection) udpKeepAlive(udpConn *net.UDPConn, close <-chan struct{}, i time.Duration) {
	if udpConn == nil || close == nil {
		return
	}

	var sequence uint64
	packet := make([]byte, 8)

	ticker := time.NewTicker(i)
	defer ticker.Stop()

	for {
		binary.LittleEndian.PutUint64(packet, sequence)
		sequence++

		_, err := udpConn.Write(packet)
		if err != nil {
			v.log(LogError, "udp keepalive write error, %s", err)
			v.reconnect()
			return
		}

		select {
		case <-ticker.C:
		case <-close:
			return
		case <-v.ctx.Done():
			return
		}
	}
}

// opusSender will listen on the given channel and send any
// pre-encoded opus audio to Discord.
func (v *VoiceConnection) opusSender(udpConn *net.UDPConn, close <-chan struct{}, opus <-chan []byte, rate, size int) {
	if udpConn == nil || close == nil {
		return
	}

	// Signal ready at start
	v.DoVoice(func(s *voiceState) {
		s.ready = true
	})
	// Note: deferred DoVoice may silently fail if context is cancelled during shutdown.
	// This is acceptable since cleanup() will set ready=false anyway.
	defer v.DoVoice(func(s *voiceState) {
		s.ready = false
	})

	var sequence uint16
	var timestamp uint32
	var recvbuf []byte
	var ok bool
	udpHeader := make([]byte, 12)
	nonce := make([]byte, 12)

	// Build static parts of UDP header
	udpHeader[0] = 0x80
	udpHeader[1] = 0x78

	// Get SSRC once at start
	var ssrc uint32
	v.DoVoice(func(s *voiceState) {
		ssrc = s.op2.SSRC
	})
	binary.BigEndian.PutUint32(udpHeader[8:], ssrc)

	ticker := time.NewTicker(time.Millisecond * time.Duration(size/(rate/1000)))
	defer ticker.Stop()

	for {
		select {
		case <-close:
			return
		case <-v.ctx.Done():
			return
		case recvbuf, ok = <-opus:
			if !ok {
				return
			}
		}

		// All encryption and sending via command for thread-safety
		var sendErr error
		v.DoVoice(func(s *voiceState) {
			if s.udpConn != udpConn || s.aead == nil {
				return
			}

			// Auto-speak if needed
			if !s.speaking {
				s.wsMutex.Lock()
				if s.wsConn != nil {
					type voiceSpeakingData struct {
						Speaking bool `json:"speaking"`
						Delay    int  `json:"delay"`
					}
					type voiceSpeakingOp struct {
						Op   int               `json:"op"`
						Data voiceSpeakingData `json:"d"`
					}
					s.wsConn.WriteJSON(voiceSpeakingOp{5, voiceSpeakingData{true, 0}})
				}
				s.wsMutex.Unlock()
				s.speaking = true
			}

			// Build header
			binary.BigEndian.PutUint16(udpHeader[2:], sequence)
			binary.BigEndian.PutUint32(udpHeader[4:], timestamp)

			// Encrypt
			binary.LittleEndian.PutUint32(nonce[:4], s.nonceCounter)
			s.nonceCounter++

			sendbuf := s.aead.Seal(nil, nonce, recvbuf, udpHeader)
			sendbuf = append(sendbuf, nonce[:4]...)
			sendbuf = append(udpHeader, sendbuf...)

			_, sendErr = s.udpConn.Write(sendbuf)
		})

		if sendErr != nil {
			v.log(LogError, "udp write error, %s", sendErr)
			return
		}

		// Wait for ticker
		select {
		case <-close:
			return
		case <-v.ctx.Done():
			return
		case <-ticker.C:
		}

		// Increment sequence and timestamp
		if sequence == 0xFFFF {
			sequence = 0
		} else {
			sequence++
		}

		if timestamp+uint32(size) >= 0xFFFFFFFF {
			timestamp = 0
		} else {
			timestamp += uint32(size)
		}
	}
}

// A Packet contains the headers and content of a received voice packet.
type Packet struct {
	SSRC      uint32
	Sequence  uint16
	Timestamp uint32
	Type      []byte
	Opus      []byte
	PCM       []int16
}

// opusReceiver listens on the UDP socket for incoming packets
// and sends them across the given channel
func (v *VoiceConnection) opusReceiver(udpConn *net.UDPConn, close <-chan struct{}, c chan *Packet) {
	if udpConn == nil || close == nil {
		return
	}

	recvbuf := make([]byte, 2048)
	var nonce [12]byte

	for {
		rlen, err := udpConn.Read(recvbuf)
		if err != nil {
			// Check if this is still the active connection
			var sameConnection bool
			v.DoVoice(func(s *voiceState) {
				sameConnection = s.udpConn == udpConn
			})

			if sameConnection {
				v.log(LogError, "udp read error, %s", err)
				go v.reconnect()
			}
			return
		}

		select {
		case <-close:
			return
		case <-v.ctx.Done():
			return
		default:
		}

		// Skip non-RTP packets
		if rlen < 12 || (recvbuf[0]&0xC0) != 0x80 {
			continue
		}

		// Build packet
		p := Packet{}
		p.Type = recvbuf[0:2]
		p.Sequence = binary.BigEndian.Uint16(recvbuf[2:4])
		p.Timestamp = binary.BigEndian.Uint32(recvbuf[4:8])
		p.SSRC = binary.BigEndian.Uint32(recvbuf[8:12])

		// RTP header parsing
		cc := int(recvbuf[0] & 0x0F)
		hasExt := (recvbuf[0] & 0x10) != 0

		baseHeaderLen := 12 + (4 * cc)
		if rlen < baseHeaderLen {
			continue
		}

		aadLen := baseHeaderLen
		extPayloadBytes := 0
		if hasExt {
			if rlen < baseHeaderLen+4 {
				continue
			}
			extLenWords := int(binary.BigEndian.Uint16(recvbuf[baseHeaderLen+2 : baseHeaderLen+4]))
			extPayloadBytes = extLenWords * 4
			aadLen = baseHeaderLen + 4
		}

		if rlen < aadLen+4 {
			continue
		}

		// Decrypt via command for thread-safe aead access
		payload := recvbuf[aadLen:rlen]
		if len(payload) < 4 {
			continue
		}
		nonceCounter := payload[len(payload)-4:]
		cipherTextPayload := payload[:len(payload)-4]

		binary.LittleEndian.PutUint32(nonce[:4], binary.LittleEndian.Uint32(nonceCounter))

		var plain []byte
		var decryptErr error
		v.DoVoice(func(s *voiceState) {
			if s.aead == nil {
				decryptErr = fmt.Errorf("aead not initialized")
				return
			}
			plain, decryptErr = s.aead.Open(nil, nonce[:], cipherTextPayload, recvbuf[:aadLen])
		})

		if decryptErr != nil {
			continue
		}

		// Strip extension payload if present
		if extPayloadBytes > 0 {
			if len(plain) < extPayloadBytes {
				continue
			}
			plain = plain[extPayloadBytes:]
		}
		p.Opus = plain

		if c != nil {
			select {
			case c <- &p:
			case <-close:
				return
			case <-v.ctx.Done():
				return
			}
		}
	}
}

// reconnect will close down a voice connection then immediately try to
// reconnect to that session.
func (v *VoiceConnection) reconnect() {
	v.log(LogInformational, "reconnect called")

	// Check if already reconnecting
	var alreadyReconnecting bool
	v.DoVoice(func(s *voiceState) {
		if s.reconnecting {
			alreadyReconnecting = true
			return
		}
		s.reconnecting = true
	})

	if alreadyReconnecting {
		v.log(LogInformational, "already reconnecting, exiting")
		return
	}

	// Get mute/deaf/channelID state BEFORE closing - DoVoice fails after Close() cancels context
	var mute, deaf bool
	var channelID string
	v.DoVoice(func(s *voiceState) {
		mute = s.mute
		deaf = s.deaf
		channelID = s.channelID
	})

	// Close current connections (cancels context, making DoVoice no longer work)
	v.Close()

	// Note: deferred DoVoice to reset reconnecting flag will silently fail after Close(),
	// but this is fine since the old VoiceConnection is being replaced by a new one

	wait := time.Duration(1)
	for {
		<-time.After(wait * time.Second)
		wait *= 2
		if wait > 600 {
			wait = 600
		}

		v.session.RLock()
		dataReady := v.session.DataReady
		wsConnNil := v.session.wsConn == nil
		v.session.RUnlock()

		if !dataReady || wsConnNil {
			v.log(LogInformational, "cannot reconnect with unready session")
			continue
		}

		v.log(LogInformational, "trying to reconnect to channel %s", channelID)

		_, err := v.session.ChannelVoiceJoin(v.GuildID, channelID, mute, deaf)
		if err == nil {
			v.log(LogInformational, "successfully reconnected to channel %s", channelID)
			return
		}

		v.log(LogInformational, "error reconnecting to channel %s, %s", channelID, err)

		// Send disconnect packet to reset
		data := voiceChannelJoinOp{4, voiceChannelJoinData{&v.GuildID, nil, true, true}}
		v.session.RLock()
		wsConn := v.session.wsConn
		v.session.RUnlock()
		if wsConn != nil {
			v.session.wsMutex.Lock()
			err = wsConn.WriteJSON(data)
			v.session.wsMutex.Unlock()
			if err != nil {
				v.log(LogError, "error sending disconnect packet, %s", err)
			}
		} else {
			v.log(LogError, "cannot send disconnect packet, wsConn is nil")
		}
	}
}
