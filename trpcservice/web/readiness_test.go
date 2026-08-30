package web

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
)

type testReadiness struct{ err error }

func (g testReadiness) Ready(context.Context) error { return g.err }

type recordingWebhookIngress struct {
	calls atomic.Int32
}

func (i *recordingWebhookIngress) Handle(context.Context, string, string, *http.Request, []byte) gateway.WebhookResult {
	i.calls.Add(1)
	return gateway.WebhookResult{Status: http.StatusAccepted, ContentType: "application/json", Body: []byte(`{"accepted":true}`)}
}

type countingBody struct {
	reads atomic.Int32
}

func (b *countingBody) Read([]byte) (int, error) {
	b.reads.Add(1)
	return 0, io.EOF
}

func (b *countingBody) Close() error { return nil }

type blockingBody struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	data    []byte
}

func (b *blockingBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func (b *blockingBody) Close() error { return nil }

func TestWebhookDrainingGateRejectsBeforeBodyRead(t *testing.T) {
	server := NewServer(platform.NewMemoryStore(), platform.Runner{})
	ingress := &recordingWebhookIngress{}
	server.AsyncIngress = ingress
	server.SetAccepting(false)
	body := &countingBody{}
	request := httptest.NewRequest(http.MethodPost, "/webhook/lark/test-app", body)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining webhook status=%d", response.Code)
	}
	if body.reads.Load() != 0 || ingress.calls.Load() != 0 {
		t.Fatalf("draining read body=%d ingress calls=%d", body.reads.Load(), ingress.calls.Load())
	}

	server.BeginDraining()
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("liveness during draining status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"status":"not_ready"`) {
		t.Fatalf("health during draining status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestWebhookAlreadyEnteredHandlerMayFinishDuringDrain(t *testing.T) {
	server := NewServer(platform.NewMemoryStore(), platform.Runner{})
	ingress := &recordingWebhookIngress{}
	server.AsyncIngress = ingress
	body := &blockingBody{started: make(chan struct{}), release: make(chan struct{}), data: []byte(`{"event":"entered"}`)}
	request := httptest.NewRequest(http.MethodPost, "/webhook/lark/test-app", body)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(response, request)
		close(done)
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("webhook handler did not enter body read")
	}
	server.BeginDraining()
	close(body.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("entered webhook did not finish")
	}
	if response.Code != http.StatusAccepted || ingress.calls.Load() != 1 {
		t.Fatalf("entered webhook status=%d ingress calls=%d", response.Code, ingress.calls.Load())
	}
}

func TestHealthReflectsReadinessGate(t *testing.T) {
	server := NewServer(platform.NewMemoryStore(), platform.Runner{})
	server.Readiness = testReadiness{err: errors.New("migration blocked")}
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("blocked readiness status=%d", response.Code)
	}

	server.Readiness = testReadiness{}
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("ready status=%d", response.Code)
	}
}
