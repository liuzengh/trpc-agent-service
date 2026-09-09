package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// feishuAPIStub is a minimal scriptable Feishu Open API for worker tests.
type feishuAPIStub struct {
	mu        sync.Mutex
	sendCalls int
	sendCodes []int
}

func (stub *feishuAPIStub) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	if strings.Contains(request.URL.Path, "tenant_access_token") {
		_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "tenant_access_token": "t-stub", "expire": 7200})
		return
	}
	stub.sendCalls++
	code := 0
	if len(stub.sendCodes) > 0 {
		code = stub.sendCodes[0]
		stub.sendCodes = stub.sendCodes[1:]
	}
	_ = json.NewEncoder(writer).Encode(map[string]any{"code": code})
}

func (stub *feishuAPIStub) calls() int {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.sendCalls
}

func feishuWorkerMessage() gateway.OutboundMessage {
	return gateway.OutboundMessage{TenantID: "tenant", AppID: "app", BindingID: "feishu-a", OutboxID: "outbox", ExternalUserID: "ou_alice", Text: "hello feishu"}
}

func newFeishuWorker(t *testing.T, stub *feishuAPIStub, server *httptest.Server, store *workerStore, limiter Limiter) *Worker {
	t.Helper()
	sender := &feishu.Sender{Tokens: &feishu.AppTokenSource{AppID: "cli_a", AppSecret: "secret", BaseURL: server.URL}, BaseURL: server.URL}
	router, err := NewRouter(Route{Binding: BindingKey{TenantID: "tenant", BindingID: "feishu-a"}, Sender: sender})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(store, router, limiter, nil, WorkerConfig{Owner: "worker", ClaimTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestFeishuSenderFlowsThroughOutboxWorker(t *testing.T) {
	claim := func() Claim {
		return Claim{Message: feishuWorkerMessage(), Owner: "worker", ClaimToken: "token", Attempt: 1, Status: StatusClaimed, LeaseUntil: time.Now().Add(time.Hour)}
	}
	newStore := func() (*workerStore, *eventLog) {
		log := &eventLog{}
		return &workerStore{log: log}, log
	}
	passLimiter := limiterFunc(func(context.Context, gateway.OutboundMessage) error { return nil })

	t.Run("success marks sent", func(t *testing.T) {
		stub := &feishuAPIStub{}
		server := httptest.NewServer(stub)
		defer server.Close()
		store, _ := newStore()
		worker := newFeishuWorker(t, stub, server, store, passLimiter)
		worker.deliver(context.Background(), claim())
		if !store.markedSent || stub.calls() != 1 {
			t.Fatalf("sent=%v calls=%d", store.markedSent, stub.calls())
		}
	})

	t.Run("retryable code retries through worker", func(t *testing.T) {
		stub := &feishuAPIStub{sendCodes: []int{99991400}}
		server := httptest.NewServer(stub)
		defer server.Close()
		store, _ := newStore()
		worker := newFeishuWorker(t, stub, server, store, passLimiter)
		worker.deliver(context.Background(), claim())
		if store.markedSent || !store.failRetryable {
			t.Fatalf("sent=%v retryable=%v", store.markedSent, store.failRetryable)
		}
	})

	t.Run("permanent rejection goes to dlq path", func(t *testing.T) {
		stub := &feishuAPIStub{sendCodes: []int{230001}}
		server := httptest.NewServer(stub)
		defer server.Close()
		store, _ := newStore()
		worker := newFeishuWorker(t, stub, server, store, passLimiter)
		worker.deliver(context.Background(), claim())
		if store.markedSent || store.failRetryable {
			t.Fatalf("sent=%v retryable=%v", store.markedSent, store.failRetryable)
		}
	})

	t.Run("limiter gates every chunk", func(t *testing.T) {
		stub := &feishuAPIStub{}
		server := httptest.NewServer(stub)
		defer server.Close()
		store, _ := newStore()
		var limiterCalls int
		limiter := limiterFunc(func(context.Context, gateway.OutboundMessage) error {
			limiterCalls++
			return nil
		})
		sender := &feishu.Sender{Tokens: &feishu.AppTokenSource{AppID: "cli_a", AppSecret: "secret", BaseURL: server.URL}, BaseURL: server.URL, MaxBytes: 8}
		router, err := NewRouter(Route{Binding: BindingKey{TenantID: "tenant", BindingID: "feishu-a"}, Sender: sender})
		if err != nil {
			t.Fatal(err)
		}
		worker, err := NewWorker(store, router, limiter, nil, WorkerConfig{Owner: "worker", ClaimTTL: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		message := feishuWorkerMessage()
		message.Text = "你好世界你好世界" // 24 bytes -> 4 rune-aligned chunks.
		worker.deliver(context.Background(), Claim{Message: message, Owner: "worker", ClaimToken: "token", Attempt: 1, Status: StatusClaimed, LeaseUntil: time.Now().Add(time.Hour)})
		if !store.markedSent || stub.calls() != 4 || limiterCalls != 4 {
			t.Fatalf("sent=%v sends=%d limits=%d", store.markedSent, stub.calls(), limiterCalls)
		}
	})

	t.Run("uncertain outcome blocks redrive", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "tenant_access_token") {
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "t", "expire": 7200})
				return
			}
			panic(http.ErrAbortHandler)
		}))
		store, _ := newStore()
		sender := &feishu.Sender{Tokens: &feishu.AppTokenSource{AppID: "cli_a", AppSecret: "secret", BaseURL: server.URL}, BaseURL: server.URL, Client: server.Client()}
		router, err := NewRouter(Route{Binding: BindingKey{TenantID: "tenant", BindingID: "feishu-a"}, Sender: sender})
		if err != nil {
			t.Fatal(err)
		}
		worker, err := NewWorker(store, router, passLimiter, nil, WorkerConfig{Owner: "worker", ClaimTTL: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		worker.deliver(context.Background(), claim())
		if !store.markedUncertain || store.markedSent {
			t.Fatalf("uncertain=%v sent=%v", store.markedUncertain, store.markedSent)
		}
		server.Close()
	})
}
