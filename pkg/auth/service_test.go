package auth

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"
)

type memoryNonces struct {
	mu   sync.Mutex
	used map[string]bool
}

func (m *memoryNonces) UseOnce(_ context.Context, service, nonce string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := service + ":" + nonce
	if m.used[key] {
		return false, nil
	}
	m.used[key] = true
	return true, nil
}

func TestServiceAuthBindsBodyAndRejectsReplay(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	nonces := &memoryNonces{used: make(map[string]bool)}
	verifier, _ := NewVerifier(secret, nonces, "consumer")
	signer, _ := NewSigner("consumer", secret)
	called := 0
	handler := verifier.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ }))
	body := []byte(`{"tenantId":"t1"}`)
	req := httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader(body))
	if err := signer.Sign(req, body); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK || called != 1 {
		t.Fatalf("signed request rejected: %d", recorder.Code)
	}

	replay := httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader(body))
	replay.Header = req.Header.Clone()
	replayRecorder := httptest.NewRecorder()
	handler.ServeHTTP(replayRecorder, replay)
	if replayRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("replay status=%d", replayRecorder.Code)
	}
}

func TestServiceAuthRejectsDuplicateSecurityHeaders(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	verifier, _ := NewVerifier(secret, &memoryNonces{used: make(map[string]bool)}, "consumer")
	signer, _ := NewSigner("consumer", secret)
	body := []byte(`{"tenantId":"t1"}`)
	request := httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader(body))
	if err := signer.Sign(request, body); err != nil {
		t.Fatal(err)
	}
	request.Header.Add(headerSignature, request.Header.Get(headerSignature))
	response := httptest.NewRecorder()
	verifier.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler called for ambiguous authentication headers")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestServiceAuthBindsTraceParent(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	nonces := &memoryNonces{used: make(map[string]bool)}
	verifier, _ := NewVerifier(secret, nonces, "consumer")
	signer, _ := NewSigner("consumer", secret)
	body := []byte(`{"tenantId":"t1"}`)
	request := httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader(body))
	request.Header.Set(headerTrace, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err := signer.Sign(request, body); err != nil {
		t.Fatal(err)
	}
	request.Header.Set(headerTrace, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
	response := httptest.NewRecorder()
	verifier.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler called after traceparent mutation")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("mutated traceparent status=%d, want 401", response.Code)
	}
}

func TestServiceAuthRejectsAmbiguousTraceParent(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	verifier, _ := NewVerifier(secret, &memoryNonces{used: make(map[string]bool)}, "consumer")
	body := []byte(`{"tenantId":"t1"}`)
	request := httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader(body))
	request.Header.Set(headerService, "consumer")
	request.Header.Set(headerTimestamp, strconv.FormatInt(time.Now().Unix(), 10))
	request.Header.Set(headerNonce, "0123456789abcdef0123456789abcdef")
	request.Header.Set(headerSignature, "0000000000000000000000000000000000000000000000000000000000000000")
	request.Header.Add(headerTrace, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	request.Header.Add(headerTrace, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
	response := httptest.NewRecorder()
	verifier.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler called with duplicate traceparent")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("duplicate traceparent status=%d, want 401", response.Code)
	}
}

func TestServiceAuthRejectsDurationOverflowTimestamp(t *testing.T) {
	secret := "0123456789abcdef0123456789abcdef"
	verifier, _ := NewVerifier(secret, &memoryNonces{used: make(map[string]bool)}, "consumer")
	verifier.now = func() time.Time { return time.Unix(0, 0) }
	request := httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader([]byte(`{}`)))
	request.Header.Set(headerService, "consumer")
	request.Header.Set(headerTimestamp, strconv.FormatInt(-1<<63, 10))
	request.Header.Set(headerNonce, "0123456789abcdef0123456789abcdef")
	request.Header.Set(headerSignature, "0000000000000000000000000000000000000000000000000000000000000000")
	response := httptest.NewRecorder()
	verifier.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler called for an out-of-range timestamp")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("duration overflow timestamp status=%d, want 401", response.Code)
	}
}

func TestServiceAuthSignerRejectsInvalidStateWithoutPanicking(t *testing.T) {
	request := &http.Request{Method: http.MethodPost, URL: &url.URL{Path: "/process"}}
	if err := (*Signer)(nil).Sign(request, nil); err == nil {
		t.Fatal("nil signer accepted")
	}
	signer := &Signer{service: "consumer", secret: []byte("short"), now: time.Now}
	if err := signer.Sign(request, nil); err == nil {
		t.Fatal("short-secret signer accepted")
	}
	signer, err := NewSigner("consumer", "0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if err := signer.Sign(request, nil); err != nil {
		t.Fatalf("nil request header should be initialized: %v", err)
	}
}

func TestServiceAuthMiddlewareRejectsInvalidStateWithoutPanicking(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/process", nil)
	response := httptest.NewRecorder()
	(*Verifier)(nil).Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler called with nil verifier")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil verifier status=%d, want 503", response.Code)
	}

	verifier, err := NewVerifier("0123456789abcdef0123456789abcdef", &memoryNonces{used: make(map[string]bool)}, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, "/process", nil)
	request.Body = nil
	response = httptest.NewRecorder()
	verifier.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler called for unsigned request")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("nil body status=%d, want 401", response.Code)
	}
}
