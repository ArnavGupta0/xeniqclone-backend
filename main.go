package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/neha/xeniqclone/backend/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// ======================================================
// In-memory provider state
// ======================================================

type ProviderState struct {
	ProviderID   string
	ProviderCode string
	Available    bool
	Latitude     float64
	Longitude    float64
}

// Provider stream registry for call delivery
type ProviderStream struct {
	ProviderId string
	Stream     pb.ConnectService_StreamIncomingCallsServer
}

// Session state tracking
type SessionState struct {
	SessionID  string
	CallID     string
	ProviderID string
	ConsumerID string
	Active     bool
	CreatedAt  time.Time
}

// Connection stream registry for signaling
type ConnectionStream struct {
	UserID string
	Stream pb.ConnectService_StreamConnectionServer
}

// Session stream readiness tracking
type SessionStreams struct {
	ProviderStream *ConnectionStream
	ConsumerStream *ConnectionStream
	SessionID      string
	mu             sync.RWMutex
}

var (
	providersByID   = make(map[string]*ProviderState)
	providersByCode = make(map[string]*ProviderState)
	providerStreams = make(map[string]*ProviderStream)
	stateMu         sync.RWMutex
	streamsMu       sync.RWMutex

	// NEW: Session and connection tracking
	activeSessions    = make(map[string]*SessionState)     // sessionID -> Session
	activeCallsByUser = make(map[string]string)            // userID -> sessionID
	connectionStreams = make(map[string]*ConnectionStream) // userID -> Stream
	sessionMu         sync.RWMutex
	connectionMu      sync.RWMutex

	// Session stream readiness tracking
	sessionStreams   = make(map[string]*SessionStreams) // sessionID -> SessionStreams
	sessionStreamsMu sync.RWMutex

	// Control stream registry for navigation commands
	controlStreams   = make(map[string]*ControlStreamPair) // callID -> ControlStreamPair
	controlStreamsMu sync.RWMutex

	// State deduplication for CameraState messages
	lastCameraState   = make(map[string]*pb.CameraState) // callID -> last state
	lastCameraStateMu sync.RWMutex
)

// ControlStreamPair holds consumer and provider control streams for a call
type ControlStreamPair struct {
	ConsumerStream pb.ControlService_StreamControlServer
	ProviderStream pb.ControlService_StreamControlServer
	CallID         string
	mu             sync.RWMutex
}

func generateProviderCode() string {
	return fmt.Sprintf("XNQ-%06d", rand.Intn(1000000))
}

// ======================================================
// gRPC Server
// ======================================================

type server struct {
	pb.UnimplementedProviderServiceServer
	pb.UnimplementedConnectServiceServer
	pb.UnimplementedControlServiceServer
}

// ======================================================
// ProviderService
// ======================================================

func (s *server) RegisterProvider(
	ctx context.Context,
	req *pb.RegisterProviderRequest,
) (*pb.RegisterProviderResponse, error) {

	stateMu.Lock()
	defer stateMu.Unlock()

	id := req.GetProviderId()
	lat := req.GetLatitude()
	lng := req.GetLongitude()

	// Re-register: return existing code
	if p, ok := providersByID[id]; ok {
		p.Latitude = lat
		p.Longitude = lng

		return &pb.RegisterProviderResponse{
			Success:      true,
			ProviderCode: p.ProviderCode,
		}, nil
	}

	code := generateProviderCode()

	p := &ProviderState{
		ProviderID:   id,
		ProviderCode: code,
		Available:    false,
		Latitude:     lat,
		Longitude:    lng,
	}

	providersByID[id] = p
	providersByCode[code] = p

	log.Printf("📝 Provider registered: ID=%s Code=%s", id, code)

	return &pb.RegisterProviderResponse{
		Success:      true,
		ProviderCode: code,
	}, nil
}

func (s *server) SetAvailability(
	ctx context.Context,
	req *pb.SetAvailabilityRequest,
) (*pb.SetAvailabilityResponse, error) {

	stateMu.Lock()
	defer stateMu.Unlock()

	p, ok := providersByID[req.GetProviderId()]
	if !ok {
		return &pb.SetAvailabilityResponse{Success: false}, nil
	}

	p.Available = req.GetIsAvailable()

	log.Printf("🔄 Availability: ID=%s Code=%s Available=%v",
		p.ProviderID, p.ProviderCode, p.Available)

	return &pb.SetAvailabilityResponse{
		Success:      true,
		ProviderCode: p.ProviderCode,
	}, nil
}

func (s *server) ListProviders(
	ctx context.Context,
	req *pb.ListProvidersRequest,
) (*pb.ListProvidersResponse, error) {

	stateMu.RLock()
	defer stateMu.RUnlock()

	var list []*pb.ProviderStatusEvent

	for _, p := range providersByID {
		if p.Available {
			list = append(list, &pb.ProviderStatusEvent{
				ProviderId:   p.ProviderID,
				ProviderCode: p.ProviderCode,
				IsAvailable:  true,
				Latitude:     p.Latitude,
				Longitude:    p.Longitude,
				Timestamp:    time.Now().Unix(),
			})
		}
	}

	return &pb.ListProvidersResponse{Providers: list}, nil
}

func (s *server) SearchProvider(
	ctx context.Context,
	req *pb.SearchRequest,
) (*pb.SearchResponse, error) {

	stateMu.RLock()
	defer stateMu.RUnlock()

	p, ok := providersByCode[req.GetProviderCode()]
	if !ok {
		return &pb.SearchResponse{
			Success: false,
			Message: "Provider not found",
		}, nil
	}

	log.Printf("🔍 Provider found: Code=%s ID=%s Available=%v",
		req.GetProviderCode(), p.ProviderID, p.Available)

	return &pb.SearchResponse{
		Success:     true,
		Message:     "Provider found",
		ProviderId:  p.ProviderID,
		IsAvailable: p.Available,
	}, nil
}

// ======================================================
// ConnectService
// ======================================================

func (s *server) RequestCall(
	ctx context.Context,
	req *pb.CallRequest,
) (*pb.CallResponse, error) {

	stateMu.RLock()
	defer stateMu.RUnlock()

	var p *ProviderState
	var ok bool

	if req.GetProviderCode() != "" {
		p, ok = providersByCode[req.GetProviderCode()]
	} else {
		p, ok = providersByID[req.GetProviderId()]
	}

	if !ok {
		return &pb.CallResponse{
			Success: false,
			Message: "Provider not found",
		}, nil
	}

	log.Printf(
		"📞 Call request: Consumer=%s → Provider=%s (Code=%s) Purpose=%s",
		req.GetConsumerId(),
		p.ProviderID,
		p.ProviderCode,
		req.GetPurpose(),
	)

	// Generate session ID immediately (OPTION A: Early Session ID)
	sessionID := fmt.Sprintf("session-%d", time.Now().UnixNano())

	// Create session state immediately so both consumer and provider use same ID
	sessionMu.Lock()
	activeSessions[sessionID] = &SessionState{
		SessionID:  sessionID,
		CallID:     sessionID, // Use sessionID as callID for compatibility
		ProviderID: p.ProviderID,
		ConsumerID: req.GetConsumerId(),
		Active:     false, // Not active until provider accepts
		CreatedAt:  time.Now(),
	}
	activeCallsByUser[req.GetConsumerId()] = sessionID
	sessionMu.Unlock()

	log.Printf("[SESSION] Created session: %s (Pending acceptance)", sessionID)

	// Send to provider stream if connected
	streamsMu.RLock()
	provStream, streamOk := providerStreams[p.ProviderID]
	streamsMu.RUnlock()

	if streamOk {
		event := &pb.IncomingCallEvent{
			CallId:       sessionID, // Use sessionID instead of callID
			ConsumerId:   req.GetConsumerId(),
			ConsumerName: req.GetConsumerName(),
			Purpose:      req.GetPurpose(),
		}

		if err := provStream.Stream.Send(event); err != nil {
			log.Printf("❌ Failed to send call to provider: %v", err)
		} else {
			log.Printf("✅ Call event sent to provider stream")
		}
	} else {
		log.Printf("⚠️ Provider %s has no active stream", p.ProviderID)
	}

	return &pb.CallResponse{
		Success: true,
		Message: "Call request sent",
		CallId:  sessionID, // Return sessionID to consumer
	}, nil
}

func (s *server) StreamIncomingCalls(
	req *pb.IncomingCallsRequest,
	stream pb.ConnectService_StreamIncomingCallsServer,
) error {
	pid := req.GetProviderId()
	log.Printf("📡 Provider stream connected: %s", pid)

	// Register stream
	streamsMu.Lock()
	providerStreams[pid] = &ProviderStream{
		ProviderId: pid,
		Stream:     stream,
	}
	streamsMu.Unlock()

	// Cleanup on disconnect
	defer func() {
		streamsMu.Lock()
		delete(providerStreams, pid)
		streamsMu.Unlock()
		log.Printf("📡 Provider stream disconnected: %s", pid)
	}()

	// Keep alive
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *server) AcceptCall(
	ctx context.Context,
	req *pb.AnswerCallRequest,
) (*pb.AnswerCallResponse, error) {
	sessionID := req.GetCallId() // This is now sessionID from RequestCall
	providerID := req.GetProviderId()

	log.Printf("✅ Call accepted: %s by provider %s", sessionID, providerID)

	// Update existing session to active state
	sessionMu.Lock()
	sess, ok := activeSessions[sessionID]
	if !ok {
		// Session doesn't exist - shouldn't happen, but handle gracefully
		log.Printf("⚠️ Session %s not found, creating new one", sessionID)
		sess = &SessionState{
			SessionID:  sessionID,
			CallID:     sessionID,
			ProviderID: providerID,
			ConsumerID: "",
			Active:     true,
			CreatedAt:  time.Now(),
		}
		activeSessions[sessionID] = sess
	} else {
		// Activate existing session
		sess.Active = true
	}
	activeCallsByUser[providerID] = sessionID
	sessionMu.Unlock()

	log.Printf("[SESSION] Session %s activated", sessionID)

	return &pb.AnswerCallResponse{
		Success:   true,
		SessionId: sessionID,
	}, nil
}

func (s *server) DeclineCall(
	ctx context.Context,
	req *pb.DeclineCallRequest,
) (*pb.DeclineCallResponse, error) {
	log.Printf("❌ Call declined: %s by %s (reason: %s)",
		req.GetCallId(), req.GetProviderId(), req.GetReason())

	return &pb.DeclineCallResponse{Success: true}, nil
}

func (s *server) StreamConnection(
	stream pb.ConnectService_StreamConnectionServer,
) error {
	log.Println("🕸️ StreamConnection established")

	var currentSessionID string
	var currentUserID string

	// FIX #1: Immediate stream registration to prevent race condition
	// Wait for first message to get session/user IDs, then register BEFORE processing
	firstEvent, err := stream.Recv()
	if err != nil {
		log.Printf("❌ [STREAM] Failed to receive first message: %v", err)
		return err
	}

	currentSessionID = firstEvent.GetCallId()
	currentUserID = firstEvent.GetSenderId()

	// Register stream immediately
	connectionMu.Lock()
	connectionStreams[currentUserID] = &ConnectionStream{
		UserID: currentUserID,
		Stream: stream,
	}
	connectionMu.Unlock()

	log.Printf("📡 [STREAM] Registered connection: User=%s Session=%s", currentUserID, currentSessionID)

	// Update session with consumer ID if needed
	sessionMu.Lock()
	if sess, ok := activeSessions[currentSessionID]; ok {
		if sess.ConsumerID == "" && currentUserID != sess.ProviderID {
			sess.ConsumerID = currentUserID
			activeCallsByUser[currentUserID] = currentSessionID
			log.Printf("[SESSION] Consumer joined: %s", currentUserID)
		}
	}
	sessionMu.Unlock()

	// Register in session streams tracker
	sessionStreamsMu.Lock()
	if _, exists := sessionStreams[currentSessionID]; !exists {
		sessionStreams[currentSessionID] = &SessionStreams{SessionID: currentSessionID}
	}
	sessStreams := sessionStreams[currentSessionID]

	// Determine role and register
	sessionMu.RLock()
	_, ok := activeSessions[currentSessionID]
	sessionMu.RUnlock()

	if !ok {
		// Session not created yet (StreamConnection arrived before AcceptCall)
		// This is normal - register stream temporarily without role assignment
		log.Printf("⏳ [STREAM] Session not created yet, registering stream temporarily (user=%s, session=%s)", currentUserID, currentSessionID)
		sessionStreamsMu.Unlock()
		// Barrier will block signaling until both streams exist
	} else {
		// Session exists - assign role based on stream identity prefix
		sessStreams.mu.Lock()
		if strings.HasPrefix(currentUserID, "provider-") {
			sessStreams.ProviderStream = connectionStreams[currentUserID]
			log.Printf("📡 [STREAM] Provider stream registered for session %s", currentSessionID)
		} else if strings.HasPrefix(currentUserID, "consumer-") {
			sessStreams.ConsumerStream = connectionStreams[currentUserID]
			log.Printf("📡 [STREAM] Consumer stream registered for session %s", currentSessionID)
		} else {
			log.Printf("⚠️ [STREAM] Unknown user type: %s", currentUserID)
		}
		sessStreams.mu.Unlock()
		sessionStreamsMu.Unlock()
	}

	// Process first message
	ctx := stream.Context()
	errChan := make(chan error, 1)

	if !processMessage(firstEvent, currentSessionID, ctx) {
		return nil // EndCall received on first message
	}

	// FIX #4: Context-aware receive loop
	go func() {
		for {
			// Check context before blocking on Recv
			select {
			case <-ctx.Done():
				log.Printf("[STREAM] Context cancelled for %s", currentUserID)
				errChan <- ctx.Err()
				return
			default:
			}

			event, err := stream.Recv()
			if err != nil {
				// FIX #5: Notify peer on disconnect
				log.Printf("[STREAM] Connection lost for %s: %v", currentUserID, err)
				notifyPeerAndCleanup(currentSessionID, currentUserID, "peer_disconnected")
				errChan <- err
				return
			}

			// Process message and check if EndCall received
			if !processMessage(event, currentSessionID, ctx) {
				// FIX #3: Signal main handler to close via io.EOF
				errChan <- io.EOF
				return
			}
		}
	}()

	// Cleanup on disconnect
	defer func() {
		if currentUserID != "" {
			connectionMu.Lock()
			delete(connectionStreams, currentUserID)
			connectionMu.Unlock()

			// Clean up session streams
			if currentSessionID != "" {
				cleanupSessionStream(currentSessionID, currentUserID)
			}

			log.Printf("📡 [STREAM] Connection closed: %s", currentUserID)
		}
	}()

	// Wait for error or context done
	select {
	case err := <-errChan:
		if err == io.EOF {
			log.Printf("[STREAM] EndCall processed, closing stream for %s", currentUserID)
			return nil
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processMessage handles WebRTC signaling message forwarding and EndCall events
// Returns false if EndCall was received (caller should terminate stream)
func processMessage(event *pb.ConnectionEvent, currentSessionID string, ctx context.Context) bool {
	senderID := event.GetSenderId()
	receiverID := event.GetReceiverId()

	// Handle EndCall - CLEANUP
	if event.GetEnd() != nil {
		log.Printf("📞 [CALL] EndCall received: Session=%s Sender=%s Reason=%s",
			currentSessionID, senderID, event.GetEnd().GetReason())

		// FIX #2: Delete session from memory (not just mark inactive)
		sessionMu.Lock()
		if sess, ok := activeSessions[currentSessionID]; ok {
			delete(activeCallsByUser, sess.ProviderID)
			delete(activeCallsByUser, sess.ConsumerID)
			delete(activeSessions, currentSessionID) // ← CRITICAL: Delete session
			log.Printf("[SESSION] Session %s deleted", currentSessionID)
		}
		sessionMu.Unlock()

		// Clean up control streams for this session
		cleanupControlStreams(currentSessionID)

		// Forward EndCall to peer
		connectionMu.RLock()
		if peerStream, ok := connectionStreams[receiverID]; ok {
			if err := peerStream.Stream.Send(event); err != nil {
				log.Printf("❌ Failed to forward EndCall: %v", err)
			} else {
				log.Printf("📤 [STREAM] EndCall forwarded to %s", receiverID)
			}
		}
		connectionMu.RUnlock()

		return false // Signal caller to close stream
	}

	log.Printf("📥 [STREAM] Event: %s → %s (Session: %s)", senderID, receiverID, currentSessionID)

	// Wait for both peers to be ready before forwarding
	// Use stream context to exit immediately on client disconnect
	if !waitForBothPeers(currentSessionID, receiverID, ctx) {
		log.Printf("⚠️ [STREAM] Timeout waiting for both peers (session=%s)", currentSessionID)
		return true // Don't forward, but continue processing
	}

	// Both peers ready - forward message
	connectionMu.RLock()
	receiverStream, found := connectionStreams[receiverID]
	connectionMu.RUnlock()

	if found {
		if err := receiverStream.Stream.Send(event); err != nil {
			log.Printf("❌ Failed to forward message: %v", err)
		} else {
			log.Printf("✅ [STREAM] Message forwarded to %s", receiverID)
		}
	} else {
		log.Printf("⚠️ [STREAM] Receiver %s disconnected during wait", receiverID)
	}

	return true // Continue processing
}

// notifyPeerAndCleanup sends EndCall event to peer and cleans up session
// FIX #5: Peer notification on unexpected disconnect
func notifyPeerAndCleanup(sessionID, disconnectedUserID, reason string) {
	if sessionID == "" || disconnectedUserID == "" {
		return
	}

	sessionMu.Lock()
	sess, ok := activeSessions[sessionID]
	if !ok {
		sessionMu.Unlock()
		return
	}

	// Determine peer ID
	peerID := sess.ConsumerID
	if disconnectedUserID == sess.ConsumerID {
		peerID = sess.ProviderID
	}

	// Clean up session
	delete(activeCallsByUser, sess.ProviderID)
	delete(activeCallsByUser, sess.ConsumerID)
	delete(activeSessions, sessionID)
	log.Printf("[SESSION] Session %s deleted (reason: %s)", sessionID, reason)
	sessionMu.Unlock()

	// Notify peer
	if peerID != "" {
		connectionMu.RLock()
		if peerStream, found := connectionStreams[peerID]; found {
			endEvent := &pb.ConnectionEvent{
				CallId:     sessionID,
				SenderId:   disconnectedUserID,
				ReceiverId: peerID,
				Payload: &pb.ConnectionEvent_End{
					End: &pb.EndCall{Reason: reason},
				},
			}
			if err := peerStream.Stream.Send(endEvent); err != nil {
				log.Printf("❌ Failed to notify peer %s: %v", peerID, err)
			} else {
				log.Printf("📤 [STREAM] Disconnect notification sent to %s", peerID)
			}
		}
		connectionMu.RUnlock()
	}
}

// waitForBothPeers blocks until both provider and consumer streams are registered
// Returns false if timeout or context cancelled
func waitForBothPeers(sessionID, receiverID string, ctx context.Context) bool {
	const maxWaitTime = 10 * time.Second
	const pollInterval = 20 * time.Millisecond

	start := time.Now()
	logged := false

	for {
		// Check timeout
		if time.Since(start) > maxWaitTime {
			log.Printf("⏰ [STREAM] Timeout waiting for peer %s (session=%s)", receiverID, sessionID)
			return false
		}

		// Check context (client disconnect)
		select {
		case <-ctx.Done():
			log.Printf("❌ [STREAM] Context cancelled while waiting (session=%s)", sessionID)
			return false
		default:
		}

		// Check if both peers are ready
		sessionStreamsMu.RLock()
		sessStreams, exists := sessionStreams[sessionID]
		sessionStreamsMu.RUnlock()

		if !exists {
			time.Sleep(pollInterval)
			continue
		}

		sessStreams.mu.RLock()
		bothReady := sessStreams.ProviderStream != nil && sessStreams.ConsumerStream != nil
		sessStreams.mu.RUnlock()

		if bothReady {
			if logged {
				log.Printf("📡 [STREAM] Both peers connected (session=%s)", sessionID)
			}
			return true
		}

		// Log once that we're waiting
		if !logged {
			log.Printf("⏳ [STREAM] Waiting for peer %s to connect (session=%s)", receiverID, sessionID)
			logged = true
		}

		time.Sleep(pollInterval)
	}
}

// cleanupSessionStream removes user stream from session tracker
func cleanupSessionStream(sessionID, userID string) {
	sessionStreamsMu.Lock()
	defer sessionStreamsMu.Unlock()

	if sessStreams, exists := sessionStreams[sessionID]; exists {
		sessStreams.mu.Lock()
		if sessStreams.ProviderStream != nil && sessStreams.ProviderStream.UserID == userID {
			sessStreams.ProviderStream = nil
		}
		if sessStreams.ConsumerStream != nil && sessStreams.ConsumerStream.UserID == userID {
			sessStreams.ConsumerStream = nil
		}

		// If both streams are gone, remove the session tracker
		if sessStreams.ProviderStream == nil && sessStreams.ConsumerStream == nil {
			delete(sessionStreams, sessionID)
			log.Printf("🗑️ [STREAM] Session streams tracker deleted (session=%s)", sessionID)
		}
		sessStreams.mu.Unlock()
	}
}

// ======================================================
// ControlService - Navigation Command Routing
// ======================================================

func (s *server) StreamControl(
	stream pb.ControlService_StreamControlServer,
) error {
	log.Println("🎮 [CONTROL] Stream connected, waiting for handshake...")

	// Wait for first message to determine role and call ID
	firstEvent, err := stream.Recv()
	if err != nil {
		log.Printf("❌ [CONTROL] Failed to receive first message: %v", err)
		return err
	}

	callID := firstEvent.GetCallId()
	senderID := firstEvent.GetSenderId()

	log.Printf("🎮 [CONTROL] Handshake received: call=%s sender=%s", callID, senderID)

	if callID == "" {
		log.Printf("❌ [CONTROL] Missing call_id in first message")
		return fmt.Errorf("call_id required")
	}

	// Determine role based on sender ID prefix
	isConsumer := strings.HasPrefix(senderID, "consumer-")
	isProvider := strings.HasPrefix(senderID, "provider-")

	if !isConsumer && !isProvider {
		log.Printf("⚠️ [CONTROL] Unknown role for sender: %s, defaulting to consumer", senderID)
		isConsumer = true
	}

	// Register stream in control registry
	controlStreamsMu.Lock()
	pair, exists := controlStreams[callID]
	if !exists {
		pair = &ControlStreamPair{CallID: callID}
		controlStreams[callID] = pair
	}
	controlStreamsMu.Unlock()

	// Assign stream based on role
	pair.mu.Lock()
	if isConsumer {
		pair.ConsumerStream = stream
		log.Printf("🎮 [CONTROL] Consumer stream registered for call: %s", callID)
	} else {
		pair.ProviderStream = stream
		log.Printf("🎮 [CONTROL] Provider stream registered for call: %s", callID)
	}
	pair.mu.Unlock()

	// Cleanup on disconnect
	defer func() {
		pair.mu.Lock()
		if isConsumer {
			pair.ConsumerStream = nil
		} else {
			pair.ProviderStream = nil
		}
		// If both streams gone, remove from registry
		if pair.ConsumerStream == nil && pair.ProviderStream == nil {
			controlStreamsMu.Lock()
			delete(controlStreams, callID)
			controlStreamsMu.Unlock()
			log.Printf("🗑️ [CONTROL] Control stream pair removed for call: %s", callID)
		}
		pair.mu.Unlock()
	}()

	errChan := make(chan error, 1)
	ctx := stream.Context()

	// Process first event if it contains a command
	if firstEvent.GetCommand() != nil {
		routeControlCommand(firstEvent, pair, isConsumer)
	}

	// Receive loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			event, err := stream.Recv()
			if err == io.EOF {
				errChan <- nil
				return
			}
			if err != nil {
				errChan <- err
				return
			}

			// Route command to peer
			routeControlCommand(event, pair, isConsumer)
		}
	}()

	// Wait for error or context done
	select {
	case err := <-errChan:
		if err != nil {
			log.Printf("🎮 [CONTROL] Stream ended for call %s: %v", callID, err)
		}
		return err
	case <-ctx.Done():
		log.Printf("🎮 [CONTROL] Context cancelled for call: %s", callID)
		return ctx.Err()
	}
}

// routeControlEvent forwards control events from consumer to provider
// Handles CameraState (with deduplication), GyroData, and legacy commands
func routeControlCommand(event *pb.ControlEvent, pair *ControlStreamPair, fromConsumer bool) {
	callID := event.GetCallId()

	// Determine event type for logging
	var eventType string

	// Handle CameraState with deduplication
	if event.GetState() != nil {
		state := event.GetState()
		eventType = "CameraState"

		// Check for duplicate state
		lastCameraStateMu.Lock()
		if lastState, ok := lastCameraState[callID]; ok {
			if statesEqual(lastState, state) {
				lastCameraStateMu.Unlock()
				log.Printf("⏭️ [CONTROL] Dropping duplicate state (call=%s)", callID)
				return // Drop duplicate
			}
		}
		lastCameraState[callID] = state
		lastCameraStateMu.Unlock()

		log.Printf("📐 [CONTROL] Routing state: zoom=%.2f anchor=(%.2f,%.2f) mode=%v (call=%s)",
			state.GetZoomLevel(), state.GetAnchorX(), state.GetAnchorY(), state.GetMode(), callID)
	} else if event.GetGyro() != nil {
		gyro := event.GetGyro()
		eventType = "GyroData"
		log.Printf("🌀 [CONTROL] Routing gyro: yaw=%.2f pitch=%.2f (call=%s)",
			gyro.GetYaw(), gyro.GetPitch(), callID)
	} else if event.GetCommand() != nil {
		eventType = event.GetCommand().GetType().String()
		log.Printf("🎮 [CONTROL] Routing command: %v (call=%s)", eventType, callID)
	} else {
		return // No payload
	}

	pair.mu.RLock()
	defer pair.mu.RUnlock()

	// Route consumer -> provider (primary flow)
	if fromConsumer && pair.ProviderStream != nil {
		if err := pair.ProviderStream.Send(event); err != nil {
			log.Printf("❌ [CONTROL] Failed to forward to provider: %v", err)
		} else {
			log.Printf("✅ [CONTROL] Forwarded to provider: %s", eventType)
		}
	} else if fromConsumer {
		log.Printf("⚠️ [CONTROL] Provider stream not available for call: %s", callID)
	}

	// Provider -> Consumer routing (for acknowledgments/state sync)
	if !fromConsumer && pair.ConsumerStream != nil {
		if err := pair.ConsumerStream.Send(event); err != nil {
			log.Printf("❌ [CONTROL] Failed to forward to consumer: %v", err)
		}
	}
}

// statesEqual checks if two CameraState messages are equivalent
func statesEqual(a, b *pb.CameraState) bool {
	if a == nil || b == nil {
		return a == b
	}
	// Use tolerance for float comparison
	const eps = 0.01
	return abs(a.GetZoomLevel()-b.GetZoomLevel()) < eps &&
		abs(a.GetAnchorX()-b.GetAnchorX()) < eps &&
		abs(a.GetAnchorY()-b.GetAnchorY()) < eps &&
		a.GetMode() == b.GetMode()
}

func abs(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}

// cleanupControlStreams removes control streams for a session (called from session cleanup)
func cleanupControlStreams(callID string) {
	controlStreamsMu.Lock()
	defer controlStreamsMu.Unlock()
	if _, exists := controlStreams[callID]; exists {
		delete(controlStreams, callID)
		log.Printf("🗑️ [CONTROL] Control streams cleaned up for call: %s", callID)
	}

	// Also clean up cached state
	lastCameraStateMu.Lock()
	delete(lastCameraState, callID)
	lastCameraStateMu.Unlock()
}

// ======================================================
// Interceptors
// ======================================================

func loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()

	// Panic Recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("🔥 [PANIC RECOVERED] in %s: %v\nStack: %s", info.FullMethod, r, string(debug.Stack()))
		}
	}()

	log.Printf("👉 [GRPC] Request: %s", info.FullMethod)

	// Call the handler
	resp, err := handler(ctx, req)

	duration := time.Since(start)
	if err != nil {
		log.Printf("❌ [GRPC] Error in %s (%v): %v", info.FullMethod, duration, err)
	} else {
		log.Printf("✅ [GRPC] Success in %s (%v)", info.FullMethod, duration)
	}

	return resp, err
}

// ======================================================
// Main
// ======================================================

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())

	// Force IPv4 for better local network discovery
	lis, err := net.Listen("tcp", "0.0.0.0:50051")

	if err != nil {
		log.Fatalf("❌ failed to listen: %v", err)
	}

	log.Println("🚀 gRPC SERVER LISTENING ON 0.0.0.0:50051")

	// Register Interceptor
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor),
	)

	srv := &server{}

	pb.RegisterProviderServiceServer(grpcServer, srv)
	pb.RegisterConnectServiceServer(grpcServer, srv)
	pb.RegisterControlServiceServer(grpcServer, srv)
	reflection.Register(grpcServer)

	go func() {
		log.Printf("✅ gRPC server listening on %s (IPv4)", lis.Addr().String())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("❌ Serve failed: %v", err)
		}
	}()

	// Graceful shutdown
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	log.Println("🛑 Shutting down server...")
	grpcServer.GracefulStop()
}
