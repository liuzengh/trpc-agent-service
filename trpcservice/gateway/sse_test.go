package gateway_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

var cursorKey = []byte("0123456789abcdef0123456789abcdef")

func TestCursorCodecBindsTenantRequestAndRejectsTampering(t *testing.T) {
	codec, err := gateway.NewCursorCodec(cursorKey)
	if err != nil {
		t.Fatal(err)
	}
	key := gateway.ExecutionKey{TenantID: "tenant-a", RequestID: "request-1"}
	value, err := codec.Encode(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if sequence, err := codec.Decode(value, key); err != nil || sequence != 2 {
		t.Fatalf("sequence=%d err=%v", sequence, err)
	}
	if _, err := codec.Decode(value+"x", key); err == nil {
		t.Fatal("tampered cursor accepted")
	}
	if _, err := codec.Decode(value, gateway.ExecutionKey{TenantID: "tenant-b", RequestID: key.RequestID}); err == nil {
		t.Fatal("cross-tenant cursor accepted")
	}
	if _, err := gateway.NewCursorCodec([]byte("short")); err == nil {
		t.Fatal("short signing key accepted")
	}
}

func TestTerminalEventStoreReplaysImmutableResultAndRejectsFutureSequence(t *testing.T) {
	tasks := &executionReaderStub{status: terminalStatus(runtime.OutcomeSucceeded)}
	results := messagingmemory.New()
	putTerminalResult(t, results)
	store := gateway.TerminalEventStore{Tasks: tasks, Results: results}
	key := gateway.ExecutionKey{TenantID: "tenant-a", RequestID: "request-1"}
	first, err := store.Replay(context.Background(), key, 0, 1)
	if err != nil || len(first.Events) != 1 || first.Events[0].Type != "message.completed" || first.Terminal || first.LastSequence != 2 {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	second, err := store.Replay(context.Background(), key, 1, 1)
	if err != nil || len(second.Events) != 1 || second.Events[0].Type != "run.completed" || !second.Terminal {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	if _, err := store.Replay(context.Background(), key, 3, 1); err == nil {
		t.Fatal("future sequence accepted")
	}
}

func TestSSEHandlerAuthenticatesAndReplaysFromSignedCursor(t *testing.T) {
	codec, _ := gateway.NewCursorCodec(cursorKey)
	results := messagingmemory.New()
	putTerminalResult(t, results)
	handler := &gateway.SSEHandler{Events: gateway.TerminalEventStore{Tasks: &executionReaderStub{status: terminalStatus(runtime.OutcomeSucceeded)}, Results: results},
		Principals: controlPrincipalResolver{principal: gateway.Principal{Authenticated: true, TenantID: "tenant-a", SubjectID: "user", CanRead: true}}, Cursors: codec}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/agent-runs/request-1/events", nil))
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), "event: ") != 2 ||
		!strings.Contains(response.Body.String(), "event: message.completed") || !strings.Contains(response.Body.String(), "event: run.completed") {
		t.Fatalf("code=%d body=%q", response.Code, response.Body.String())
	}
	cursor, _ := codec.Encode(gateway.ExecutionKey{TenantID: "tenant-a", RequestID: "request-1"}, 1)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/agent-runs/request-1/events?cursor="+cursor, nil))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "message.completed") || !strings.Contains(response.Body.String(), "run.completed") {
		t.Fatalf("resume code=%d body=%q", response.Code, response.Body.String())
	}
}

func TestSSEHandlerDefaultsAndClampsReplayLimit(t *testing.T) {
	codec, _ := gateway.NewCursorCodec(cursorKey)
	for _, test := range []struct {
		name, configured string
		limit, want      int
	}{
		{name: "default", configured: "zero", limit: 0, want: 64},
		{name: "configured", configured: "within-bound", limit: 128, want: 128},
		{name: "clamped", configured: "above-bound", limit: 1_000, want: 256},
	} {
		t.Run(test.name+"/"+test.configured, func(t *testing.T) {
			store := &eventStoreStub{terminal: true}
			handler := &gateway.SSEHandler{Events: store, ReplayLimit: test.limit,
				Principals: controlPrincipalResolver{principal: gateway.Principal{Authenticated: true,
					TenantID: "tenant-a", SubjectID: "user", CanRead: true}}, Cursors: codec}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/agent-runs/request-1/events", nil))
			if response.Code != http.StatusOK || store.observedLimit() != test.want {
				t.Fatalf("code=%d limit=%d want=%d", response.Code, store.observedLimit(), test.want)
			}
		})
	}
}

func TestSSEHandlerFailsClosedBeforeStreamingAndDisconnectOnlyStopsSubscription(t *testing.T) {
	codec, _ := gateway.NewCursorCodec(cursorKey)
	unauthenticated := &gateway.SSEHandler{Events: &eventStoreStub{}, Principals: controlPrincipalResolver{}, Cursors: codec}
	response := httptest.NewRecorder()
	unauthenticated.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/agent-runs/request-1/events", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated code=%d", response.Code)
	}

	key := gateway.ExecutionKey{TenantID: "tenant-a", RequestID: "request-1"}
	future, _ := codec.Encode(key, 9)
	handler := &gateway.SSEHandler{Events: &eventStoreStub{futureAt: 1}, Principals: controlPrincipalResolver{principal: gateway.Principal{
		Authenticated: true, TenantID: key.TenantID, SubjectID: "user", CanRead: true,
	}}, Cursors: codec}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/agent-runs/request-1/events?cursor="+future, nil))
	if response.Code != http.StatusConflict || strings.Contains(response.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("future code=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
	}

	store := &eventStoreStub{}
	handler.Events, handler.PollInterval = store, time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/v1/agent-runs/request-1/events", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), request)
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscription did not stop after disconnect")
	}
	if store.cancelCalls != 0 {
		t.Fatalf("disconnect requested business cancellation %d times", store.cancelCalls)
	}
}

type executionReaderStub struct{ status gateway.ExecutionStatus }

func (s *executionReaderStub) GetExecution(context.Context, gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	return s.status, nil
}

func terminalStatus(outcome runtime.Outcome) gateway.ExecutionStatus {
	return gateway.ExecutionStatus{Envelope: runtime.ExecutionEnvelope{TenantID: "tenant-a", RequestID: "request-1", CreatedAt: time.Unix(1_800_000_000, 0).UTC()},
		Outcome: outcome, ResultRef: "result://request-1", Version: 2}
}

func putTerminalResult(t *testing.T, store messaging.ResultStore) {
	t.Helper()
	if err := store.PutResult(context.Background(), messaging.ResultRecord{TenantID: "tenant-a", RequestID: "request-1",
		ResultRef: "result://request-1", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
}

type eventStoreStub struct {
	mu          sync.Mutex
	calls       int
	cancelCalls int
	futureAt    uint64
	limit       int
	terminal    bool
}

func (s *eventStoreStub) Replay(ctx context.Context, key gateway.ExecutionKey, after uint64, limit int) (gateway.EventPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.limit = limit
	if s.futureAt > 0 && after > s.futureAt {
		return gateway.EventPage{}, runtime.ErrVersionConflict
	}
	if err := ctx.Err(); err != nil {
		return gateway.EventPage{}, err
	}
	return gateway.EventPage{Terminal: s.terminal}, nil
}

func (s *eventStoreStub) observedLimit() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}
