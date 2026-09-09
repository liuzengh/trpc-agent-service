package telegramingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testToken = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
const testSecret = "telegram-ingress-local-test-secret-32"

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func testPoller(t *testing.T, gateway *httptest.Server, api http.HandlerFunc) *Poller {
	t.Helper()
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	p, err := New(Config{
		BotToken: testToken, WebhookSecret: testSecret, GatewayBaseURL: gateway.URL,
		RouteKey: "route-for-this-bot", StatePath: filepath.Join(t.TempDir(), "telegram.json"),
		ExpectedBotID: 123456789, PollTimeout: time.Second,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := url.Parse(server.URL)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	p.apiClient.Transport = transportFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host != "api.telegram.org" || request.URL.Scheme != "https" {
			return nil, errors.New("noncanonical bot API host")
		}
		copy := request.Clone(request.Context())
		address := *request.URL
		address.Scheme, address.Host = endpoint.Scheme, endpoint.Host
		copy.URL = &address
		return transport.RoundTrip(copy)
	})
	return p
}

func apiFixture(updates http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			_, _ = io.WriteString(w, `{"ok":true,"result":{"id":123456789,"is_bot":true}}`)
		case strings.HasSuffix(r.URL.Path, "/getWebhookInfo"):
			_, _ = io.WriteString(w, `{"ok":true,"result":{"url":""}}`)
		case strings.HasSuffix(r.URL.Path, "/getUpdates"):
			updates(w, r)
		default:
			http.Error(w, "unexpected API method", 404)
		}
	}
}

func TestCheckpointFailureReplaysExactBytesAfterRestart(t *testing.T) {
	// Whitespace and characters subject to JSON escaping must survive the
	// durable spool, because Gateway hashes the original request bytes.
	const update = `{ "update_id": 41, "message": {"text":"<sample>&", "message_id": 7} }`
	received := make(chan []byte, 2)
	var gatewayRejected atomic.Bool
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/webhook" || r.URL.Query().Get("token") != "route-for-this-bot" ||
			r.Header.Get("X-Telegram-Bot-Api-Secret-Token") != testSecret || r.Header.Get("Content-Type") != "application/json" {
			gatewayRejected.Store(true)
			http.Error(w, "rejected", 401)
			return
		}
		received <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer gateway.Close()
	var polls atomic.Int32
	api := apiFixture(func(w http.ResponseWriter, r *http.Request) {
		polls.Add(1)
		_, _ = fmt.Fprintf(w, `{"ok":true,"result":[%s]}`, update)
	})
	p := testPoller(t, gateway, api)
	originalWriter := p.writeState
	p.writeState = func(path string, state *checkpoint) error {
		if state.Offset == 42 && state.Pending == nil {
			return errors.New("simulated storage failure with sensitive diagnostic")
		}
		return originalWriter(path, state)
	}
	if err := p.Run(context.Background()); !errors.Is(err, ErrStateWrite) {
		t.Fatalf("first run = %v, want state failure", err)
	}
	saved, err := loadCheckpoint(p.config.StatePath)
	if err != nil || saved == nil || saved.Offset != 0 || saved.Pending == nil || string(saved.Pending.Body) != update {
		t.Fatalf("unconfirmed offset or raw update was not retained: err=%v", err)
	}
	p2 := testPoller(t, gateway, api)
	p2.config.StatePath = p.config.StatePath
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p2.notify = func(event Event) {
		if event.Operation == "gateway" && event.Outcome == "acknowledged" {
			cancel()
		}
	}
	if err := p2.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("restart = %v", err)
	}
	if gatewayRejected.Load() || polls.Load() != 1 || !bytes.Equal(<-received, <-received) {
		t.Fatal("restart did not reuse authenticated, byte-identical pending request before polling")
	}
	saved, err = loadCheckpoint(p.config.StatePath)
	if err != nil || saved.Offset != 42 || saved.Pending != nil {
		t.Fatalf("confirmed checkpoint missing: err=%v", err)
	}
	disk, _ := os.ReadFile(p.config.StatePath)
	if bytes.Contains(disk, []byte(testToken)) || bytes.Contains(disk, []byte(testSecret)) || bytes.Contains(disk, []byte("sample")) {
		t.Fatal("checkpoint retained credentials or an acknowledged update")
	}
}

func TestSpoolFailurePreventsGatewayAndOffsetConfirmation(t *testing.T) {
	var deliveries atomic.Int32
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { deliveries.Add(1) }))
	defer gateway.Close()
	p := testPoller(t, gateway, apiFixture(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"ok":true,"result":[{"update_id":9}]}`)
	}))
	original := p.writeState
	p.writeState = func(path string, state *checkpoint) error {
		if state.Pending != nil {
			return ErrStateWrite
		}
		return original(path, state)
	}
	if err := p.Run(context.Background()); !errors.Is(err, ErrStateWrite) || deliveries.Load() != 0 {
		t.Fatalf("spool failure forwarded update: deliveries=%d err=%v", deliveries.Load(), err)
	}
	saved, err := loadCheckpoint(p.config.StatePath)
	if err != nil || saved.Offset != 0 || saved.Pending != nil {
		t.Fatal("failed spool changed checkpoint")
	}
}

func TestAcknowledgedBatchUsesCommittedOffset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls, deliveries atomic.Int32
	var invalidOffset atomic.Bool
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveries.Add(1)
		w.WriteHeader(200)
	}))
	defer gateway.Close()
	p := testPoller(t, gateway, apiFixture(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Offset int64 `json:"offset"`
			Limit  int   `json:"limit"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		if calls.Add(1) == 1 {
			invalidOffset.Store(input.Offset != 0 || input.Limit != 100)
			_, _ = io.WriteString(w, `{"ok":true,"result":[{"update_id":11},{"update_id":12}]}`)
			return
		}
		if input.Offset != 13 {
			invalidOffset.Store(true)
		}
		cancel()
		_, _ = io.WriteString(w, `{"ok":true,"result":[]}`)
	}))
	if err := p.Run(ctx); !errors.Is(err, context.Canceled) || invalidOffset.Load() || deliveries.Load() != 2 {
		t.Fatalf("batch offset=%t deliveries=%d err=%v", invalidOffset.Load(), deliveries.Load(), err)
	}
}

func TestActiveWebhookAndBotMismatchRefusePolling(t *testing.T) {
	for _, active := range []bool{true, false} {
		t.Run(fmt.Sprintf("active=%t", active), func(t *testing.T) {
			var unwanted atomic.Int32
			gateway := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { unwanted.Add(1) }))
			defer gateway.Close()
			p := testPoller(t, gateway, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/getMe"):
					_, _ = io.WriteString(w, `{"ok":true,"result":{"id":123456789,"is_bot":true}}`)
				case strings.HasSuffix(r.URL.Path, "/getWebhookInfo"):
					_, _ = io.WriteString(w, `{"ok":true,"result":{"url":"https://existing.invalid/private-route"}}`)
				default:
					unwanted.Add(1)
					http.Error(w, "unexpected method", 400)
				}
			})
			want := ErrWebhookActive
			if !active {
				p.config.ExpectedBotID++
				want = ErrBotIdentity
			}
			if err := p.Run(context.Background()); !errors.Is(err, want) || unwanted.Load() != 0 {
				t.Fatalf("startup=%v unwanted calls=%d", err, unwanted.Load())
			}
			if _, err := os.Stat(p.config.StatePath); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("rejected startup wrote polling checkpoint")
			}
		})
	}
}

func TestGatewayPermanentErrorsRetainPendingWithoutSensitiveErrors(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 409, 413, 302} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var received atomic.Int32
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received.Add(1)
				http.Error(w, testToken+testSecret+"private message text", status)
			}))
			defer gateway.Close()
			p := testPoller(t, gateway, apiFixture(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, `{"ok":true,"result":[{"update_id":19,"message":{"text":"private message text"}}]}`)
			}))
			err := p.Run(context.Background())
			if !errors.Is(err, ErrGateway) || received.Load() != 1 {
				t.Fatalf("permanent response retried: err=%v calls=%d", err, received.Load())
			}
			for _, secret := range []string{testToken, testSecret, "private message text", gateway.URL, p.config.RouteKey} {
				if strings.Contains(err.Error(), secret) {
					t.Fatal("Gateway error leaked request or response data")
				}
			}
			state, err := loadCheckpoint(p.config.StatePath)
			if err != nil || state.Offset != 0 || state.Pending == nil || state.Pending.ID != 19 {
				t.Fatal("Gateway rejection discarded pending update")
			}
		})
	}
}

func TestProviderRetriesRespectHintsAndRedactFailures(t *testing.T) {
	gateway := httptest.NewServer(http.NotFoundHandler())
	defer gateway.Close()
	var attempts atomic.Int32
	p := testPoller(t, gateway, func(w http.ResponseWriter, r *http.Request) {
		switch attempts.Add(1) {
		case 1:
			w.WriteHeader(429)
			_, _ = fmt.Fprintf(w, `{"ok":false,"error_code":429,"description":%q,"parameters":{"retry_after":7}}`, testToken)
		case 2:
			http.Error(w, testSecret, 503)
		default:
			w.WriteHeader(401)
			_, _ = fmt.Fprintf(w, `{"ok":false,"error_code":401,"description":%q}`, testToken+testSecret)
		}
	})
	var waits []time.Duration
	p.wait = func(_ context.Context, delay time.Duration) error { waits = append(waits, delay); return nil }
	err := p.Run(context.Background())
	if !errors.Is(err, ErrProvider) || attempts.Load() != 3 || len(waits) != 2 || waits[0] != 7*time.Second || waits[1] != 2*time.Second {
		t.Fatalf("retry contract failed: err=%v attempts=%d waits=%v", err, attempts.Load(), waits)
	}
	if strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), testSecret) {
		t.Fatal("provider diagnostics leaked secrets")
	}
}

func TestGatewayRetriesSamePendingBytesBeforeAcknowledgingOffset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const update = `{ "update_id": 51, "message": {"text":"test"} }`
	var calls atomic.Int32
	bodies := make(chan []byte, 3)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- body
		switch calls.Add(1) {
		case 1:
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(429)
		case 2:
			w.WriteHeader(503)
		default:
			w.WriteHeader(200)
		}
	}))
	defer gateway.Close()
	p := testPoller(t, gateway, apiFixture(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"ok":true,"result":[%s]}`, update)
	}))
	var waits []time.Duration
	p.wait = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		state, err := loadCheckpoint(p.config.StatePath)
		if err != nil || state.Offset != 0 || state.Pending == nil || string(state.Pending.Body) != update {
			return errors.New("retry changed unconfirmed checkpoint")
		}
		return nil
	}
	p.notify = func(event Event) {
		if event.Outcome == "acknowledged" {
			cancel()
		}
	}
	if err := p.Run(ctx); !errors.Is(err, context.Canceled) || calls.Load() != 3 || len(waits) != 2 || waits[0] != 3*time.Second {
		t.Fatalf("Gateway retries failed: err=%v calls=%d waits=%v", err, calls.Load(), waits)
	}
	for range 3 {
		if string(<-bodies) != update {
			t.Fatal("Gateway retry changed raw update bytes")
		}
	}
	state, err := loadCheckpoint(p.config.StatePath)
	if err != nil || state.Offset != 52 || state.Pending != nil {
		t.Fatal("confirmed Gateway retry did not commit offset")
	}
}

func TestMalformedUpdateDoesNotAdvanceOffset(t *testing.T) {
	for _, raw := range []string{`{}`, `{"update_id":-1}`, `{"update_id":9223372036854775807}`, `{"update_id":"42"}`} {
		t.Run(raw, func(t *testing.T) {
			var calls atomic.Int32
			gateway := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
			defer gateway.Close()
			p := testPoller(t, gateway, apiFixture(func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"ok":true,"result":[%s]}`, raw)
			}))
			if err := p.Run(context.Background()); !errors.Is(err, ErrProtocol) || calls.Load() != 0 {
				t.Fatalf("malformed update forwarded: err=%v calls=%d", err, calls.Load())
			}
			state, err := loadCheckpoint(p.config.StatePath)
			if err != nil || state.Offset != 0 || state.Pending != nil {
				t.Fatal("malformed update changed checkpoint")
			}
		})
	}
}

func TestTransportFailureAndCancellation(t *testing.T) {
	gateway := httptest.NewServer(http.NotFoundHandler())
	defer gateway.Close()
	p := testPoller(t, gateway, http.NotFound)
	p.apiClient.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("transport includes " + testToken)
	})
	ctx, cancel := context.WithCancel(context.Background())
	p.wait = func(context.Context, time.Duration) error { cancel(); return ctx.Err() }
	if err := p.Run(ctx); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), testToken) {
		t.Fatalf("transport cancellation=%v", err)
	}
}

func TestCancellationInterruptsLongPollAndPendingGateway(t *testing.T) {
	for _, gatewayBlocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("gateway=%t", gatewayBlocked), func(t *testing.T) {
			started := make(chan struct{})
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				<-r.Context().Done()
			}))
			defer gateway.Close()
			p := testPoller(t, gateway, apiFixture(func(w http.ResponseWriter, r *http.Request) {
				if gatewayBlocked {
					_, _ = io.WriteString(w, `{"ok":true,"result":[{"update_id":29}]}`)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				<-r.Context().Done()
			}))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- p.Run(ctx) }()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("request did not enter cancellation boundary")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled request=%v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("cancelled request leaked a running poller")
			}
			state, err := loadCheckpoint(p.config.StatePath)
			if err != nil || state.Offset != 0 || (state.Pending != nil) != gatewayBlocked {
				t.Fatal("cancelled request confirmed or discarded pending work")
			}
			lock, err := acquireStateLock(p.config.StatePath)
			if err != nil {
				t.Fatalf("cancellation retained process lock: %v", err)
			}
			lock.Close()
		})
	}
}

func TestStateBindingAndSingleProcessLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "telegram.json")
	first, err := acquireStateLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := acquireStateLock(path); !errors.Is(err, ErrAlreadyLocked) {
		if second != nil {
			second.Close()
		}
		t.Fatalf("concurrent state lock=%v", err)
	}
	first.Close()
	for _, differentBot := range []bool{false, true} {
		t.Run(fmt.Sprintf("differentBot=%t", differentBot), func(t *testing.T) {
			var polls atomic.Int32
			gateway := httptest.NewServer(http.NotFoundHandler())
			defer gateway.Close()
			p := testPoller(t, gateway, apiFixture(func(w http.ResponseWriter, r *http.Request) { polls.Add(1) }))
			state := &checkpoint{Version: 1, BotID: 123456789, RouteHash: p.routeHash, Offset: 99}
			if differentBot {
				state.BotID++
			} else {
				state.RouteHash = strings.Repeat("a", 64)
			}
			if err := saveCheckpoint(p.config.StatePath, state); err != nil {
				t.Fatal(err)
			}
			if err := p.Run(context.Background()); !errors.Is(err, ErrStateBinding) || polls.Load() != 0 {
				t.Fatalf("foreign state accepted: err=%v polls=%d", err, polls.Load())
			}
		})
	}
}

func TestCorruptStateDoesNotResetOffset(t *testing.T) {
	var polls atomic.Int32
	gateway := httptest.NewServer(http.NotFoundHandler())
	defer gateway.Close()
	p := testPoller(t, gateway, apiFixture(func(w http.ResponseWriter, r *http.Request) { polls.Add(1) }))
	const truncated = `{"version":1,"offset":1234`
	if err := os.WriteFile(p.config.StatePath, []byte(truncated), 0600); err != nil {
		t.Fatal(err)
	}
	if err := p.Run(context.Background()); !errors.Is(err, ErrStateRead) || polls.Load() != 0 {
		t.Fatalf("corrupt state accepted: err=%v polls=%d", err, polls.Load())
	}
	after, _ := os.ReadFile(p.config.StatePath)
	if string(after) != truncated {
		t.Fatal("corrupt checkpoint was silently overwritten")
	}
}
