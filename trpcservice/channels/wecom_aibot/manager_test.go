package wecom_aibot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestManagerAuthenticatesAndMapsDirectAndGroupCallbacks(t *testing.T) {
	connection := newTestConn()
	dispatcher := &testDispatcher{requests: make(chan gateway.DispatchRequest, 2)}
	dispatcher.events = func() <-chan gateway.DispatchEvent {
		result := make(chan gateway.DispatchEvent, 2)
		result <- gateway.DispatchEvent{Type: gateway.DispatchEventMessage, Text: "partial"}
		result <- gateway.DispatchEvent{Type: gateway.DispatchEventDone, Done: true}
		close(result)
		return result
	}
	manager, stop := startTestManager(t, connection, dispatcher, 4)
	defer stop()

	auth := readFrame(t, connection.writes)
	if auth.Cmd != cmdSubscribe {
		t.Fatalf("auth command = %q", auth.Cmd)
	}
	var credentials struct {
		BotID  string `json:"bot_id"`
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal(auth.Body, &credentials); err != nil || credentials.BotID != "bot-1" || credentials.Secret != "secret-1" {
		t.Fatalf("auth payload is invalid")
	}
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	connection.reads <- callbackFrame(t, "req-direct", "msg-direct", "", "user-1")
	direct := <-dispatcher.requests
	if direct.RequestID != "req-direct" || direct.Message.ConversationKind != channels.ConversationDirect || direct.Message.ExternalPeerID != "user-1" || direct.Message.ExternalMessageID != "msg-direct" {
		t.Fatalf("direct request = %+v", direct)
	}
	partial := readFrame(t, connection.writes)
	if partial.Cmd != cmdRespond || partial.Headers.ReqID != "req-direct" {
		t.Fatalf("direct reply = %+v", partial)
	}
	connection.reads <- ackFrame(t, "req-direct", 0)

	connection.reads <- callbackFrame(t, "req-group", "msg-group", "chat-1", "user-2")
	group := <-dispatcher.requests
	if group.RequestID != "req-group" || group.Message.ConversationKind != channels.ConversationGroup || group.Message.ExternalChatID != "chat-1" || group.Message.ExternalPeerID != "" {
		t.Fatalf("group request = %+v", group)
	}
	connection.reads <- ackFrame(t, readFrame(t, connection.writes).Headers.ReqID, 0)
}

func TestManagerAcceptsCommandedAuthenticationAcknowledgement(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	auth := readFrame(t, connection.writes)
	code := 0
	data, err := encodeFrame(Frame{Cmd: cmdSubscribe, Headers: auth.Headers, ErrCode: &code})
	if err != nil {
		t.Fatal(err)
	}
	connection.reads <- data
	waitReady(t, manager)
}

func TestProviderWaitsForCorrelatedFinalReplyAcknowledgement(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	provider, err := NewProvider(manager, testCorrelations{requestID: "req-final"})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, deliverErr := provider.Deliver(context.Background(), storage.ReplyOutbox{TenantID: "tenant", EventID: "event", ReplyID: "reply", SegmentIndex: 0, SegmentCount: 1, Payload: "final", LeaseOwner: "worker", FencingToken: 1})
		result <- deliverErr
	}()
	final := readFrame(t, connection.writes)
	if final.Cmd != cmdRespond || final.Headers.ReqID != "req-final" {
		t.Fatalf("final frame = %+v", final)
	}
	var body StreamReply
	if err := json.Unmarshal(final.Body, &body); err != nil || !body.Stream.Finish || body.Stream.ID != "req-final" {
		t.Fatalf("final body = %+v", body)
	}
	select {
	case err := <-result:
		t.Fatalf("delivery completed before ack: %v", err)
	default:
	}
	connection.reads <- ackFrame(t, "req-final", 0)
	if err := <-result; err != nil {
		t.Fatalf("delivery error = %v", err)
	}
}

func TestOutboxLeaseDurationCoversReplyAcknowledgement(t *testing.T) {
	if OutboxLeaseDuration <= defaultReplyACKTimeout {
		t.Fatalf("outbox lease duration = %s, reply acknowledgement timeout = %s", OutboxLeaseDuration, defaultReplyACKTimeout)
	}
}

func TestProviderReturnsRetryableFailureWhenAcknowledgedReceiptCannotPersist(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	provider, err := NewProvider(manager, testCorrelations{requestID: "req-final", receiptErr: errors.New("storage unavailable")})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, deliverErr := provider.Deliver(context.Background(), storage.ReplyOutbox{TenantID: "tenant", EventID: "event", ReplyID: "reply", SegmentIndex: 0, SegmentCount: 1, Payload: "final", LeaseOwner: "worker", FencingToken: 1})
		result <- deliverErr
	}()
	final := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, final.Headers.ReqID, 0)
	if err := <-result; !isDeliveryError(err, "unavailable", true) {
		t.Fatalf("receipt persistence error = %v", err)
	}
}

func TestProviderFinishesOnlyFinalDurableSegment(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	provider, err := NewProvider(manager, testCorrelations{requestID: "req-final"})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []storage.ReplyOutbox{
		{TenantID: "tenant", EventID: "event", ReplyID: "reply", SegmentIndex: 0, SegmentCount: 2, Payload: "first", LeaseOwner: "worker", FencingToken: 1},
		{TenantID: "tenant", EventID: "event", ReplyID: "reply", SegmentIndex: 1, SegmentCount: 2, Payload: "second", LeaseOwner: "worker", FencingToken: 1},
	} {
		result := make(chan error, 1)
		go func(value storage.ReplyOutbox) {
			_, deliverErr := provider.Deliver(context.Background(), value)
			result <- deliverErr
		}(value)
		frame := readFrame(t, connection.writes)
		var body StreamReply
		if err := json.Unmarshal(frame.Body, &body); err != nil {
			t.Fatal(err)
		}
		wantFinish := value.SegmentIndex+1 == value.SegmentCount
		if frame.Cmd != cmdRespond || frame.Headers.ReqID != "req-final" || body.Stream.ID != "req-final" || body.Stream.Finish != wantFinish || body.Stream.Content != value.Payload {
			t.Fatalf("segment %d frame = %+v body = %+v", value.SegmentIndex, frame, body)
		}
		connection.reads <- ackFrame(t, "req-final", 0)
		if err := <-result; err != nil {
			t.Fatalf("segment %d delivery error = %v", value.SegmentIndex, err)
		}
	}
}

func TestProviderReconcilesDurableAcknowledgementAfterRestart(t *testing.T) {
	ctx := context.Background()
	store := inmemory.New()
	if _, err := store.CreateSession(ctx, "tenant", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(ctx, storage.MessageEventInput{TenantID: "tenant", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(ctx, storage.ReplyCorrelation{TenantID: "tenant", EventID: "event", RequestID: "req-final"}, []storage.ReplyOutbox{{TenantID: "tenant", EventID: "event", ReplyID: "reply", SegmentIndex: 0, SegmentCount: 1, Payload: "final"}}); err != nil {
		t.Fatal(err)
	}
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	claimed, err := store.ClaimReply(ctx, "tenant", "reply", 0, "worker-before-restart", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewProvider(manager, store)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, deliverErr := provider.Deliver(ctx, claimed)
		result <- deliverErr
	}()
	final := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, final.Headers.ReqID, 0)
	if err := <-result; err != nil {
		t.Fatalf("delivery error = %v", err)
	}
	persisted, err := store.GetReply(ctx, "tenant", "reply", 0)
	if err != nil || persisted.Status != storage.ReplySending || persisted.ProviderMessageID != "req-final:0" {
		t.Fatalf("persisted acknowledgement = %+v, %v", persisted, err)
	}

	time.Sleep(110 * time.Millisecond)
	restartedProvider, err := NewProvider(manager, store)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: restartedProvider, TenantID: "tenant", Owner: "worker-after-restart", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(ctx); err != nil || processed != 1 {
		t.Fatalf("restart recovery = %d, %v", processed, err)
	}
	recovered, err := store.GetReply(ctx, "tenant", "reply", 0)
	if err != nil || recovered.Status != storage.ReplySent || recovered.ProviderMessageID != "req-final:0" {
		t.Fatalf("recovered reply = %+v, %v", recovered, err)
	}
	select {
	case duplicate := <-connection.writes:
		t.Fatalf("restart resent acknowledged frame: %s", string(duplicate))
	default:
	}
}

func TestManagerBoundsReplyQueueAndReconnects(t *testing.T) {
	first, second := newTestConn(), newTestConn()
	dialer := &testDialer{connections: make(chan Conn, 2)}
	dialer.connections <- first
	dialer.connections <- second
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer, queueSize: 1, heartbeat: time.Hour, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	auth := readFrame(t, first.writes)
	first.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	firstReply := make(chan error, 1)
	go func() { firstReply <- manager.sendReply(context.Background(), "req-1", StreamReply{}) }()
	_ = readFrame(t, first.writes)
	secondReply := make(chan error, 1)
	go func() { secondReply <- manager.sendReply(context.Background(), "req-2", StreamReply{}) }()
	waitQueueFull(t, manager)
	if err := manager.sendReply(context.Background(), "req-3", StreamReply{}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("queue error = %v", err)
	}
	first.reads <- ackFrame(t, "req-1", 0)
	if err := <-firstReply; err != nil {
		t.Fatalf("first reply error = %v", err)
	}
	_ = readFrame(t, first.writes)
	first.reads <- ackFrame(t, "req-2", 0)
	if err := <-secondReply; err != nil {
		t.Fatalf("second reply error = %v", err)
	}
	_ = first.Close()
	auth = readFrame(t, second.writes)
	second.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	manager.BeginShutdown()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager did not stop")
	}
	if writes := first.maxWrites.Load(); writes > 1 {
		t.Fatalf("concurrent writes = %d", writes)
	}
}

func TestManagerSendsHeartbeatAndAcceptsAcknowledgement(t *testing.T) {
	connection := newHeartbeatConn()
	dialer := &testDialer{connections: make(chan Conn, 1)}
	dialer.connections <- connection
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer, queueSize: 2, heartbeat: 10 * time.Millisecond, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	ping := readFrame(t, connection.writes)
	if ping.Cmd != cmdPing {
		t.Fatalf("heartbeat command = %q", ping.Cmd)
	}
	connection.reads <- ackFrame(t, ping.Headers.ReqID, 0)
	if !manager.Ready() {
		t.Fatal("heartbeat acknowledgement made manager unavailable")
	}
	manager.BeginShutdown()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager did not stop")
	}
}

func TestPendingReplyFailsWhenConnectionCloses(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	result := make(chan error, 1)
	go func() { result <- manager.sendReply(context.Background(), "req-close", StreamReply{}) }()
	_ = readFrame(t, connection.writes)
	_ = connection.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("pending reply error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending reply was not failed after close")
	}
}

func TestReplyAcknowledgementTimeoutReleasesWriter(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	manager.replyACKTimeout = 10 * time.Millisecond
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	first := make(chan error, 1)
	go func() { first <- manager.sendReply(context.Background(), "req-timeout", StreamReply{}) }()
	_ = readFrame(t, connection.writes)
	if err := <-first; !errors.Is(err, ErrAcknowledgementTimeout) {
		t.Fatalf("first reply error = %v", err)
	}

	second := make(chan error, 1)
	go func() { second <- manager.sendReply(context.Background(), "req-after-timeout", StreamReply{}) }()
	frame := readFrame(t, connection.writes)
	if frame.Headers.ReqID != "req-after-timeout" {
		t.Fatalf("reply after timeout = %+v", frame)
	}
	connection.reads <- ackFrame(t, frame.Headers.ReqID, 0)
	if err := <-second; err != nil {
		t.Fatalf("reply after timeout error = %v", err)
	}
}

func TestHeartbeatDoesNotSetReadDeadline(t *testing.T) {
	connection := newTestConn()
	dialer := &testDialer{connections: make(chan Conn, 1)}
	dialer.connections <- connection
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer, queueSize: 2, heartbeat: 10 * time.Millisecond, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	if ping := readFrame(t, connection.writes); ping.Cmd != cmdPing {
		t.Fatalf("heartbeat command = %q", ping.Cmd)
	}
	if calls := connection.readDeadlineCalls.Load(); calls != 0 {
		t.Fatalf("read deadlines = %d", calls)
	}
	manager.BeginShutdown()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager did not stop")
	}
}

func TestDisconnectedEventStopsManagerWithoutReconnect(t *testing.T) {
	first, second := newTestConn(), newTestConn()
	dialer := &testDialer{connections: make(chan Conn, 2)}
	dialer.connections <- first
	dialer.connections <- second
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer, queueSize: 2, heartbeat: time.Hour, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- manager.Run(context.Background()) }()
	auth := readFrame(t, first.writes)
	first.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	first.reads <- disconnectedEventFrame(t, "event-replaced")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("replacement run error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not stop after replacement event")
	}
	if manager.Ready() || dialer.calls.Load() != 1 {
		t.Fatalf("replacement state = ready:%v calls:%d", manager.Ready(), dialer.calls.Load())
	}
}

func TestConfigurationAndProtocolHelpers(t *testing.T) {
	defaults := withDefaults(Config{})
	if defaults.WSURL != defaultWSURL || defaults.QueueSize != 128 || defaults.HeartbeatInterval != 30*time.Second || defaults.ReconnectBase != time.Second || defaults.ReconnectMax != time.Minute || defaults.ExecutionTimeout != 4*time.Minute || defaults.Dialer == nil {
		t.Fatalf("defaults = %+v", defaults)
	}
	if value, err := normalizeWebSocketURL(" wss://example.test/ "); err != nil || value != "wss://example.test" {
		t.Fatalf("normalized websocket URL = %q, %v", value, err)
	}
	for _, value := range []string{"", "ws://example.test", "wss://example.test/path", "wss://user@example.test", "wss://example.test?query=1"} {
		if _, err := normalizeWebSocketURL(value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("websocket URL %q error = %v", value, err)
		}
	}
	if got := randomizedJitter(0); got != 0 {
		t.Fatalf("zero jitter = %s", got)
	}
	if _, err := encodeFrame(Frame{}); !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("missing request ID encoding error = %v", err)
	}
	if _, err := encodeFrame(Frame{Headers: Headers{ReqID: strings.Repeat("x", 257)}}); !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("oversized request ID encoding error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepBackoff(ctx, time.Second, time.Second, 0) {
		t.Fatal("canceled backoff completed")
	}
}

func TestHeartbeatAndInboundReadHelpers(t *testing.T) {
	queue := make(chan outboundFrame, 1)
	if err := sendHeartbeat(queue, "heartbeat"); err != nil {
		t.Fatal(err)
	}
	outbound := <-queue
	frame, err := decodeFrame(outbound.data)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Cmd != cmdPing {
		t.Fatalf("heartbeat frame = %+v", frame)
	}
	queue <- outboundFrame{data: []byte("occupied")}
	if err := sendHeartbeat(queue, "heartbeat"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue error = %v", err)
	}

	connection := newTestConn()
	reads := make(chan inboundReadResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go readInboundFrames(ctx, connection, reads)
	connection.reads <- ackFrame(t, "read", 0)
	select {
	case result := <-reads:
		if result.err != nil || len(result.data) == 0 {
			t.Fatalf("inbound result = %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("inbound read did not arrive")
	}
}

func TestHeartbeatStateBoundaries(t *testing.T) {
	connection := newTestConn()
	manager := &Manager{conn: connection, ready: true}
	queue := make(chan outboundFrame, 1)
	if err := manager.scheduleHeartbeat(connection, queue); err != nil {
		t.Fatal(err)
	}
	data := <-queue
	ping, err := decodeFrame(data.data)
	if err != nil {
		t.Fatal(err)
	}
	code := 0
	if !manager.acknowledgeHeartbeat(Frame{Headers: Headers{ReqID: ping.Headers.ReqID}, ErrCode: &code}) {
		t.Fatal("heartbeat acknowledgement was not consumed")
	}
	manager.mu.Lock()
	if manager.heartbeatReqID != "" || manager.missedHeartbeats != 0 {
		t.Fatalf("heartbeat state after acknowledgement = id:%q missed:%d", manager.heartbeatReqID, manager.missedHeartbeats)
	}
	manager.mu.Unlock()
	if manager.acknowledgeHeartbeat(Frame{Headers: Headers{ReqID: "other"}}) {
		t.Fatal("unrelated acknowledgement was consumed as a heartbeat")
	}

	if err := manager.scheduleHeartbeat(connection, queue); err != nil {
		t.Fatal(err)
	}
	data = <-queue
	failedPing, err := decodeFrame(data.data)
	if err != nil {
		t.Fatal(err)
	}
	code = 1
	if !manager.acknowledgeHeartbeat(Frame{Headers: Headers{ReqID: failedPing.Headers.ReqID}, ErrCode: &code}) {
		t.Fatal("failed heartbeat acknowledgement was not consumed")
	}
	if manager.Ready() {
		t.Fatal("failed heartbeat acknowledgement left the manager ready")
	}
	select {
	case <-connection.closed:
	case <-time.After(time.Second):
		t.Fatal("failed heartbeat acknowledgement did not close the connection")
	}

	fullConnection := newTestConn()
	fullManager := &Manager{conn: fullConnection, ready: true}
	fullQueue := make(chan outboundFrame, 1)
	fullQueue <- outboundFrame{data: []byte("occupied")}
	if err := fullManager.scheduleHeartbeat(fullConnection, fullQueue); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full heartbeat queue error = %v", err)
	}
	fullManager.mu.Lock()
	if fullManager.heartbeatReqID != "" {
		t.Fatalf("queue rejection left pending heartbeat %q", fullManager.heartbeatReqID)
	}
	fullManager.mu.Unlock()

	if err := (&Manager{}).scheduleHeartbeat(newTestConn(), make(chan outboundFrame, 1)); err != nil {
		t.Fatalf("unready heartbeat error = %v", err)
	}
}

func TestHeartbeatMissesReconnectAndMarksManagerUnavailable(t *testing.T) {
	first, second := newTestConn(), newTestConn()
	dialer := &testDialer{connections: make(chan Conn, 2)}
	dialer.connections <- first
	dialer.connections <- second
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer, queueSize: 2, heartbeat: 5 * time.Millisecond, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	defer func() {
		manager.BeginShutdown()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("manager did not stop")
		}
	}()

	auth := readFrame(t, first.writes)
	first.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	for attempt := 0; attempt < maxMissedHeartbeats; attempt++ {
		if ping := readFrame(t, first.writes); ping.Cmd != cmdPing {
			t.Fatalf("heartbeat command = %q", ping.Cmd)
		}
	}
	select {
	case <-first.closed:
	case <-time.After(time.Second):
		t.Fatal("unacknowledged heartbeats did not close the connection")
	}

	secondAuth := readFrame(t, second.writes)
	if manager.Ready() {
		t.Fatal("manager remained ready while the replacement connection was unauthenticated")
	}
	second.reads <- ackFrame(t, secondAuth.Headers.ReqID, 0)
	waitReady(t, manager)
}

func TestManagerAuthenticationAndReplyStateBoundaries(t *testing.T) {
	manager := &Manager{authReqID: "auth"}
	if err := manager.handleInboundFrame(context.Background(), Frame{Headers: Headers{ReqID: "auth"}}); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("missing authentication code error = %v", err)
	}
	code := 0
	if err := manager.handleInboundFrame(context.Background(), Frame{Headers: Headers{ReqID: "auth"}, ErrCode: &code}); err != nil || !manager.Ready() {
		t.Fatalf("authentication result = %v ready=%v", err, manager.Ready())
	}
	ack, done := make(chan error, 1), make(chan struct{})
	manager.pending = &pendingReply{reqID: "reply", ack: ack, done: done}
	manager.completeReplyAck("other", nil)
	manager.clearPendingReplyFor("reply", ErrAcknowledgementTimeout)
	if err := <-ack; !errors.Is(err, ErrAcknowledgementTimeout) {
		t.Fatalf("cleared reply error = %v", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("cleared reply did not unblock writer")
	}
	if err := (&Manager{}).sendReply(context.Background(), "", StreamReply{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid reply error = %v", err)
	}
	if err := (&Manager{}).sendReply(context.Background(), "reply", StreamReply{}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("unready reply error = %v", err)
	}
	ack, done = make(chan error, 1), make(chan struct{})
	manager.pending = &pendingReply{reqID: "rejected", ack: ack, done: done}
	rejected := 1
	manager.acknowledgeReply(Frame{Headers: Headers{ReqID: "rejected"}, ErrCode: &rejected})
	if err := <-ack; !errors.Is(err, ErrClosed) {
		t.Fatalf("rejected acknowledgement error = %v", err)
	}
}

func TestProviderAndBindingProviderBoundaries(t *testing.T) {
	if _, err := NewProvider(nil, testCorrelations{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil manager provider error = %v", err)
	}
	manager := &Manager{}
	provider, err := NewProvider(manager, testCorrelations{requestID: "request"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Deliver(context.Background(), storage.ReplyOutbox{}); err == nil {
		t.Fatal("invalid durable reply was accepted")
	}
	value := storage.ReplyOutbox{TenantID: "tenant", EventID: "event", ReplyID: "reply", Payload: "payload", LeaseOwner: "worker", FencingToken: 1}
	if _, err := provider.Deliver(context.Background(), value); err == nil {
		t.Fatal("unready durable reply was accepted")
	}
	value.ProviderMessageID = "request:0"
	if receipt, err := provider.Deliver(context.Background(), value); err != nil || receipt != "request:0" {
		t.Fatalf("cached receipt = %q, %v", receipt, err)
	}
	if status, receipt, err := provider.Reconcile(context.Background(), value); err != nil || status != outbox.DeliveryAccepted || receipt != "request:0" {
		t.Fatalf("reconcile = %q %q %v", status, receipt, err)
	}
	if status, _, err := (&Provider{}).Reconcile(context.Background(), value); err != nil || status != outbox.DeliveryUnknown {
		t.Fatalf("nil provider reconcile = %q %v", status, err)
	}

	if _, err := NewBindingProvider(testCorrelations{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty binding provider error = %v", err)
	}
	boundManager := &Manager{target: channels.RoutingTarget{BindingID: "binding"}}
	bound, err := NewBindingProvider(testCorrelations{}, boundManager)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bound.Deliver(context.Background(), value); err == nil {
		t.Fatal("unrouted durable reply was accepted")
	}
	value.ProviderMessageID = ""
	value.ReplyTarget.BindingID = "binding"
	boundManager.ready = true
	if _, err := bound.Deliver(context.Background(), value); !isDeliveryError(err, "unavailable", true) {
		t.Fatalf("bound unavailable delivery error = %v", err)
	}
	if status, _, err := bound.Reconcile(context.Background(), value); err != nil || status != outbox.DeliveryUnknown {
		t.Fatalf("bound reconcile = %q %v", status, err)
	}
	var nilBindingProvider *BindingProvider
	if status, _, err := nilBindingProvider.Reconcile(context.Background(), value); !isDeliveryError(err, "invalid", false) || status != outbox.DeliveryUnknown {
		t.Fatalf("nil binding provider reconcile = %q %v", status, err)
	}
}

func TestConstructorsUseTrustedBindingAndDefaults(t *testing.T) {
	target, consumer := testRoutingTarget(t)
	manager, err := New(Config{BotID: " bot-1 ", Secret: "secret", WSURL: " wss://example.test/ ", Target: target, Dispatcher: &testDispatcher{}})
	if err != nil {
		t.Fatal(err)
	}
	if manager.botID != "bot-1" || manager.wsURL != "wss://example.test" || manager.Channel() != channels.ChannelWeComAIBot || manager.dialer == nil {
		t.Fatalf("manager = %+v", manager)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{BotID: "bot", Secret: "secret", WSURL: "ws://example.test", Target: target, Dispatcher: &testDispatcher{}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("insecure manager configuration error = %v", err)
	}
	bound, err := NewForBinding(context.Background(), BindingConfig{Target: target, Bindings: consumer, Credentials: testAIBotCredentials{secret: "secret"}, Dispatcher: &testDispatcher{}})
	if err != nil || bound.botID != "bot-1" {
		t.Fatalf("binding manager = %+v %v", bound, err)
	}
	if _, err := NewForBinding(context.Background(), BindingConfig{Target: target, Bindings: consumer, Credentials: testAIBotCredentials{}, Dispatcher: &testDispatcher{}}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("missing binding secret error = %v", err)
	}
}

func TestWritePumpFailureAndDrainPaths(t *testing.T) {
	manager := &Manager{}
	for _, connection := range []Conn{writeFailureConn{testConn: newTestConn(), deadlineErr: errors.New("deadline")}, writeFailureConn{testConn: newTestConn(), writeErr: errors.New("write")}} {
		queue := make(chan outboundFrame, 1)
		queue <- outboundFrame{data: []byte("frame")}
		errs := make(chan error, 1)
		go manager.writePump(context.Background(), connection, queue, errs)
		select {
		case err := <-errs:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("write failure error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("write failure was not reported")
		}
	}
	queue := make(chan outboundFrame, 2)
	queue <- outboundFrame{data: []byte("queued"), ack: make(chan error, 1)}
	drainOutboundReplies(queue, ErrClosed)
	if len(queue) != 0 {
		t.Fatal("queued replies were not drained")
	}
}

func TestManagerLifecycleAndWritePumpBoundaries(t *testing.T) {
	var nilManager *Manager
	if nilManager.Ready() {
		t.Fatal("nil manager reported ready")
	}
	if err := nilManager.Run(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil manager run error = %v", err)
	}
	manager := &Manager{}
	if err := manager.Run(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil run context error = %v", err)
	}
	manager.closing = true
	if err := manager.Run(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("closing manager run error = %v", err)
	}
	manager.closing = false
	manager.runCancel = func() {}
	if err := manager.Run(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("concurrent manager run error = %v", err)
	}
	manager.runCancel = nil
	if err := manager.serveConnection(context.Background(), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil connection error = %v", err)
	}

	connection := newTestConn()
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	done := runWritePump(manager, ctx, connection, make(chan outboundFrame), errs)
	cancel()
	if err := <-errs; !errors.Is(err, ErrClosed) {
		t.Fatalf("canceled write pump error = %v", err)
	}
	<-done

	nilQueue := make(chan outboundFrame, 1)
	nilQueue <- outboundFrame{}
	errs = make(chan error, 1)
	done = runWritePump(manager, context.Background(), connection, nilQueue, errs)
	if err := <-errs; err != nil {
		t.Fatalf("empty outbound frame error = %v", err)
	}
	<-done

	expiredCtx, expire := context.WithCancel(context.Background())
	expire()
	expiredAck := make(chan error, 1)
	expiredQueue := make(chan outboundFrame, 2)
	expiredQueue <- outboundFrame{context: expiredCtx, data: []byte("expired"), ack: expiredAck}
	expiredQueue <- outboundFrame{}
	errs = make(chan error, 1)
	done = runWritePump(manager, context.Background(), connection, expiredQueue, errs)
	if err := <-expiredAck; !errors.Is(err, ErrAcknowledgementTimeout) {
		t.Fatalf("expired outbound acknowledgement = %v", err)
	}
	if err := <-errs; err != nil {
		t.Fatalf("expired outbound pump error = %v", err)
	}
	<-done

	pendingAck, pendingDone := make(chan error, 1), make(chan struct{})
	manager.pending = &pendingReply{reqID: "active", ack: pendingAck, done: pendingDone}
	conflictAck := make(chan error, 1)
	conflictQueue := make(chan outboundFrame, 2)
	conflictQueue <- outboundFrame{data: []byte("reply"), replyReqID: "conflict", ack: conflictAck, done: make(chan struct{})}
	conflictQueue <- outboundFrame{}
	errs = make(chan error, 1)
	done = runWritePump(manager, context.Background(), connection, conflictQueue, errs)
	if err := <-conflictAck; !errors.Is(err, ErrClosed) {
		t.Fatalf("conflicting reply acknowledgement = %v", err)
	}
	if err := <-errs; err != nil {
		t.Fatalf("conflicting reply pump error = %v", err)
	}
	<-done
	if err := <-pendingAck; !errors.Is(err, ErrClosed) {
		t.Fatalf("existing reply acknowledgement = %v", err)
	}
	select {
	case <-pendingDone:
	default:
		t.Fatal("existing pending reply did not unblock")
	}
}

func TestManagerCloseStopsAnAuthenticatedConnection(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 1)
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	if err := manager.Close(); err != nil {
		t.Fatalf("manager close error = %v", err)
	}
	stop()
	if manager.Ready() {
		t.Fatal("closed manager reported ready")
	}
	select {
	case <-connection.closed:
	default:
		t.Fatal("manager close left the active connection open")
	}
}

func TestManagerReconnectFailuresRespectCancellation(t *testing.T) {
	dialed := make(chan struct{}, 1)
	manager := &Manager{
		botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL,
		dialer: dialerFunc(func(context.Context, string, http.Header) (Conn, error) {
			dialed <- struct{}{}
			return nil, errors.New("dial failed")
		}),
		reconnectBase: time.Hour, reconnectMax: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	<-dialed
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reconnect error = %v", err)
	}
	if !sleepBackoff(context.Background(), time.Nanosecond, time.Nanosecond, 0) {
		t.Fatal("ready backoff did not complete")
	}
	if got := min(2, 1); got != 1 {
		t.Fatalf("bounded reconnect exponent = %d", got)
	}
}

func TestManagerAuthenticationFailureClosesConnection(t *testing.T) {
	connection := newTestConn()
	dialer := &testDialer{connections: make(chan Conn, 1)}
	dialer.connections <- connection
	manager := &Manager{
		botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer,
		queueSize: 1, heartbeat: time.Hour, reconnectBase: time.Hour, reconnectMax: time.Hour,
		executionTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 1)
	select {
	case <-connection.closed:
	case <-time.After(time.Second):
		t.Fatal("authentication failure left the connection open")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("authentication failure run error = %v", err)
	}
}

func TestManagerReconnectsWhenAuthenticationAcknowledgementTimesOut(t *testing.T) {
	first, second := newTestConn(), newTestConn()
	dialer := &testDialer{connections: make(chan Conn, 2)}
	dialer.connections <- first
	dialer.connections <- second
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer, queueSize: 1, heartbeat: time.Hour, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, authACKTimeout: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	defer func() {
		manager.BeginShutdown()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("manager did not stop")
		}
	}()

	_ = readFrame(t, first.writes)
	select {
	case <-first.closed:
	case <-time.After(time.Second):
		t.Fatal("unauthenticated connection was not closed")
	}
	secondAuth := readFrame(t, second.writes)
	second.reads <- ackFrame(t, secondAuth.Headers.ReqID, 0)
	waitReady(t, manager)
}

func TestManagerCancellationDoesNotQueueAuthenticationOrReplies(t *testing.T) {
	manager := &Manager{botID: "bot-1", secret: "secret-1"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.enqueueAuth(ctx, make(chan outboundFrame)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled authentication enqueue error = %v", err)
	}
	manager.queue = make(chan outboundFrame)
	if err := manager.sendReply(ctx, "reply", StreamReply{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reply enqueue error = %v", err)
	}
	manager.queue = nil
}

func TestManagerIgnoresInvalidCallbacksAndEvents(t *testing.T) {
	manager := &Manager{botID: "bot-1", ready: true}
	for _, frame := range []Frame{
		{Body: []byte("not-json")},
		{Body: mustJSON(Event{AIBotID: "other"})},
		{Body: mustJSON(Event{AIBotID: "bot-1", Event: struct {
			EventType string `json:"eventtype"`
		}{EventType: "subscribed_event"}})},
	} {
		if err := manager.handleEventCallback(frame); err != nil {
			t.Fatalf("ignored event error = %v", err)
		}
	}
	if !manager.Ready() {
		t.Fatal("ignored event changed connection readiness")
	}

	called := make(chan struct{}, 1)
	manager.dispatcher = dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		called <- struct{}{}
		return nil, errors.New("dispatch failed")
	})
	manager.handleCallback(context.Background(), Frame{Body: []byte("not-json")})
	select {
	case <-called:
		t.Fatal("malformed callback reached dispatcher")
	default:
	}
	frame, err := decodeFrame(callbackFrame(t, "callback", "message", "", "user"))
	if err != nil {
		t.Fatal(err)
	}
	manager.handleCallback(context.Background(), frame)
	<-called

	manager.dispatcher = dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		called <- struct{}{}
		return nil, nil
	})
	manager.handleCallback(context.Background(), frame)
	<-called
}

func TestTrustedBindingAndDurableReplyRejectionBoundaries(t *testing.T) {
	target, consumer := testRoutingTarget(t)
	if _, err := New(Config{Secret: "secret", Target: target, Dispatcher: &testDispatcher{}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing bot ID error = %v", err)
	}
	if _, err := New(Config{BotID: "bot", Target: target, Dispatcher: &testDispatcher{}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing bot secret error = %v", err)
	}
	if _, err := New(Config{BotID: "bot", Secret: "secret", Target: target}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing dispatcher error = %v", err)
	}
	if _, err := NewForBinding(nil, BindingConfig{Target: target, Bindings: consumer, Credentials: testAIBotCredentials{secret: "secret"}, Dispatcher: &testDispatcher{}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil binding context error = %v", err)
	}
	staleTarget := target
	staleTarget.BindingVersion++
	if _, err := NewForBinding(context.Background(), BindingConfig{Target: staleTarget, Bindings: consumer, Credentials: testAIBotCredentials{secret: "secret"}, Dispatcher: &testDispatcher{}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("stale binding error = %v", err)
	}
	if _, err := NewForBinding(context.Background(), BindingConfig{Target: target, Bindings: consumer, Credentials: testAIBotCredentials{err: errors.New("secret unavailable")}, Dispatcher: &testDispatcher{}}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("credential resolution error = %v", err)
	}

	manager := &Manager{ready: true}
	value := storage.ReplyOutbox{TenantID: "tenant", EventID: "event", ReplyID: "reply", Payload: "payload", LeaseOwner: "worker", FencingToken: 1}
	for _, correlations := range []DeliveryStore{
		testCorrelations{},
		testCorrelations{err: errors.New("correlation unavailable")},
	} {
		provider, err := NewProvider(manager, correlations)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := provider.Deliver(context.Background(), value); !isDeliveryError(err, "unavailable", true) {
			t.Fatalf("correlation failure error = %v", err)
		}
	}
	boundManager := &Manager{target: channels.RoutingTarget{BindingID: "binding"}}
	if _, err := NewBindingProvider(testCorrelations{}, boundManager, boundManager); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate binding manager error = %v", err)
	}
	if _, err := NewBindingProvider(nil, boundManager); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil delivery store error = %v", err)
	}
	var provider *BindingProvider
	if _, err := provider.Deliver(context.Background(), value); !isDeliveryError(err, "invalid", false) {
		t.Fatalf("nil binding provider error = %v", err)
	}
}

type testDispatcher struct {
	requests chan gateway.DispatchRequest
	events   func() <-chan gateway.DispatchEvent
}

func (d *testDispatcher) Dispatch(_ context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	if d.requests != nil {
		d.requests <- request
	}
	if d.events == nil {
		result := make(chan gateway.DispatchEvent)
		close(result)
		return result, nil
	}
	return d.events(), nil
}

type dispatchFunc func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error)

func (fn dispatchFunc) Dispatch(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	return fn(ctx, request)
}

type testCorrelations struct {
	requestID  string
	err        error
	receiptErr error
}

func (c testCorrelations) GetReplyCorrelation(context.Context, string, string) (storage.ReplyCorrelation, error) {
	return storage.ReplyCorrelation{RequestID: c.requestID}, c.err
}

func (c testCorrelations) RecordReplyReceipt(_ context.Context, receipt storage.ReplyReceipt) (storage.ReplyOutbox, error) {
	return storage.ReplyOutbox{TenantID: receipt.TenantID, ReplyID: receipt.ReplyID, SegmentIndex: receipt.SegmentIndex, ProviderMessageID: receipt.ProviderID}, c.receiptErr
}

type testDialer struct {
	connections chan Conn
	calls       atomic.Int32
}

type dialerFunc func(context.Context, string, http.Header) (Conn, error)

func (fn dialerFunc) DialContext(ctx context.Context, wsURL string, header http.Header) (Conn, error) {
	return fn(ctx, wsURL, header)
}

func (d *testDialer) DialContext(ctx context.Context, _ string, _ http.Header) (Conn, error) {
	d.calls.Add(1)
	select {
	case connection := <-d.connections:
		return connection, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type testAIBotCredentials struct {
	secret string
	err    error
}

func (c testAIBotCredentials) Resolve(_ context.Context, _ channels.SecretScope) (Credentials, error) {
	return Credentials{BotSecret: c.secret}, c.err
}

func isDeliveryError(err error, class string, retryable bool) bool {
	var deliveryErr *outbox.DeliveryError
	return errors.As(err, &deliveryErr) && deliveryErr.Class == class && deliveryErr.Retryable == retryable
}

func runWritePump(manager *Manager, ctx context.Context, connection Conn, queue <-chan outboundFrame, errs chan<- error) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.writePump(ctx, connection, queue, errs)
	}()
	return done
}

type targetConsumer struct{ binding *channels.Binding }

func (c targetConsumer) LookupCandidates(context.Context, channels.Channel, string) ([]channels.CandidateBindingContext, error) {
	return nil, nil
}

func (c targetConsumer) Get(_ context.Context, tenantID, bindingID string) (*channels.Binding, error) {
	if c.binding == nil || c.binding.TenantID != tenantID || c.binding.BindingID != bindingID {
		return nil, channels.ErrNotFound
	}
	value := c.binding.Clone()
	return &value, nil
}

func (targetConsumer) ConsumeCandidate(context.Context, channels.CandidateBindingContext) (*channels.Binding, error) {
	return nil, channels.ErrCandidateUnavailable
}

type targetTenantRepository struct {
	tenant.Repository
	value *tenant.Tenant
}

func (r targetTenantRepository) Get(_ context.Context, tenantID string) (*tenant.Tenant, error) {
	if r.value == nil || r.value.TenantID != tenantID {
		return nil, tenant.ErrNotFound
	}
	value := r.value.Clone()
	return &value, nil
}

type targetAppRepository struct {
	appmodel.Repository
	value *appmodel.App
}

func (r targetAppRepository) Get(_ context.Context, tenantID, appID string) (*appmodel.App, error) {
	if r.value == nil || r.value.TenantID != tenantID || r.value.AppID != appID {
		return nil, appmodel.ErrNotFound
	}
	value := r.value.Clone()
	return &value, nil
}

func testRoutingTarget(t *testing.T) (channels.RoutingTarget, targetConsumer) {
	t.Helper()
	root, err := tenant.NewTenant(tenant.CreateInput{TenantKey: "aibot-test", DisplayName: "AI Bot Test", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	appRoot, err := appmodel.NewApp(appmodel.CreateInput{TenantID: root.TenantID, AppKey: "aibot", DisplayName: "AI Bot", Description: "test"})
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(1)
	appRoot.Status, appRoot.CurrentRevision, appRoot.Version, appRoot.UpdatedAt = appmodel.StatusActive, &revision, 2, appRoot.CreatedAt.Add(time.Second)
	if err := appRoot.Validate(); err != nil {
		t.Fatal(err)
	}
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeComAIBot, "aibot-test-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channels.NewBinding(channels.CreateInput{TenantID: root.TenantID, BindingKey: "aibot", Channel: channels.ChannelWeComAIBot, ProviderAccountID: "bot-account", PublicRouteKeyDigest: routeDigest, AppID: appRoot.AppID, SecretRef: "secret/aibot", Protocol: channels.ProtocolConfiguration{WeComAIBot: &channels.WeComAIBotProtocolConfiguration{BotID: "bot-1"}}, Status: channels.StatusActive})
	if err != nil {
		t.Fatal(err)
	}
	consumer := targetConsumer{binding: binding}
	target, err := channels.ResolveConfiguredRoutingTarget(context.Background(), consumer, targetTenantRepository{value: root}, targetAppRepository{value: appRoot}, root.TenantID, binding.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	return target, consumer
}

type testConn struct {
	reads             chan []byte
	writes            chan []byte
	closed            chan struct{}
	closeOnce         sync.Once
	writesNow         atomic.Int32
	maxWrites         atomic.Int32
	readDeadlineCalls atomic.Int32
}

func newTestConn() *testConn {
	return &testConn{reads: make(chan []byte, 16), writes: make(chan []byte, 16), closed: make(chan struct{})}
}
func (c *testConn) ReadMessage() (int, []byte, error) {
	select {
	case data := <-c.reads:
		return 1, data, nil
	case <-c.closed:
		return 0, nil, errors.New("closed")
	}
}
func (c *testConn) WriteMessage(_ int, data []byte) error {
	active := c.writesNow.Add(1)
	for {
		maximum := c.maxWrites.Load()
		if active <= maximum || c.maxWrites.CompareAndSwap(maximum, active) {
			break
		}
	}
	defer c.writesNow.Add(-1)
	select {
	case c.writes <- data:
		return nil
	case <-c.closed:
		return errors.New("closed")
	}
}
func (c *testConn) SetReadDeadline(time.Time) error { c.readDeadlineCalls.Add(1); return nil }
func (*testConn) SetWriteDeadline(time.Time) error  { return nil }
func (*testConn) SetPongHandler(func(string) error) {}
func (c *testConn) Close() error                    { c.closeOnce.Do(func() { close(c.closed) }); return nil }

type writeFailureConn struct {
	*testConn
	deadlineErr error
	writeErr    error
}

func (c writeFailureConn) SetWriteDeadline(time.Time) error { return c.deadlineErr }
func (c writeFailureConn) WriteMessage(int, []byte) error   { return c.writeErr }

type heartbeatConn struct {
	*testConn
	mu       sync.Mutex
	deadline time.Time
}

func newHeartbeatConn() *heartbeatConn { return &heartbeatConn{testConn: newTestConn()} }
func (c *heartbeatConn) SetReadDeadline(value time.Time) error {
	c.mu.Lock()
	c.deadline = value
	c.mu.Unlock()
	return nil
}
func (c *heartbeatConn) ReadMessage() (int, []byte, error) {
	c.mu.Lock()
	deadline := c.deadline
	c.mu.Unlock()
	if deadline.IsZero() {
		return c.testConn.ReadMessage()
	}
	delay := time.Until(deadline)
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case data := <-c.reads:
		return 1, data, nil
	case <-c.closed:
		return 0, nil, errors.New("closed")
	case <-timer.C:
		return 0, nil, timeoutError{}
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func startTestManager(t *testing.T, connection *testConn, dispatcher gateway.DispatchService, queueSize int) (*Manager, func()) {
	t.Helper()
	dialer := &testDialer{connections: make(chan Conn, 1)}
	dialer.connections <- connection
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dispatcher: dispatcher, dialer: dialer, queueSize: queueSize, heartbeat: time.Hour, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	return manager, func() {
		manager.BeginShutdown()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("manager did not stop")
		}
	}
}

func readFrame(t *testing.T, input <-chan []byte) Frame {
	t.Helper()
	select {
	case data := <-input:
		frame, err := decodeFrame(data)
		if err != nil {
			t.Fatal(err)
		}
		return frame
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for frame")
		return Frame{}
	}
}
func ackFrame(t *testing.T, reqID string, code int) []byte {
	t.Helper()
	frame := Frame{Headers: Headers{ReqID: reqID}, ErrCode: &code}
	data, err := encodeFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func callbackFrame(t *testing.T, reqID, msgID, chatID, userID string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"msgid": msgID, "aibotid": "bot-1", "chatid": chatID, "msgtype": "text", "from": map[string]string{"userid": userID}, "text": map[string]string{"content": "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeFrame(Frame{Cmd: cmdCallback, Headers: Headers{ReqID: reqID}, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func disconnectedEventFrame(t *testing.T, reqID string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"msgid": "event", "aibotid": "bot-1", "event": map[string]string{"eventtype": "disconnected_event"}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeFrame(Frame{Cmd: cmdEventCallback, Headers: Headers{ReqID: reqID}, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func waitReady(t *testing.T, manager *Manager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if manager.Ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("manager never became ready")
}
func waitQueueFull(t *testing.T, manager *Manager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		queued := manager.queue != nil && len(manager.queue) == cap(manager.queue)
		manager.mu.Unlock()
		if queued {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("reply queue never filled")
}

var _ outbox.Provider = (*Provider)(nil)
