package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testToken = "123456:test_token_not_a_real_credential"

func testClient(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	c, e := New(Options{Token: testToken, BaseURL: s.URL})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(c.Close)
	return c
}

func TestPollOnceKeepsRawFieldsAndNeverOwnsOffset(t *testing.T) {
	var calls int
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/getUpdates") {
			t.Error("wrong request")
		}
		var p PollRequest
		if e := json.NewDecoder(r.Body).Decode(&p); e != nil {
			t.Error(e)
		}
		if p.Offset != 101 || p.TimeoutSeconds != 1 || p.AllowedUpdates == nil {
			t.Errorf("unexpected request: %+v", p)
		}
		fmt.Fprint(w, `{"ok":true,"result":[{"update_id":101,"future":{"n":9007199254740991},"message":{"text":"hi"}}]}`)
	})
	for range 2 {
		u, e := c.PollOnce(context.Background(), PollRequest{101, 10, 1, []string{"message"}})
		if e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(string(u[0]), `"future":{"n":9007199254740991}`) {
			t.Fatal("lost unknown raw field")
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestProtocolRejectsBadOffsetsAndOrdering(t *testing.T) {
	for _, body := range []string{`null`, `[{"update_id":1},{"update_id":1}]`, `[{"update_id":2},{"update_id":1}]`, `[{"message":{}}]`, `[{"update_id":1.5}]`, `[{"update_id":-1}]`} {
		t.Run(body, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, `{"ok":true,"result":%s}`, body) })
			if _, e := c.PollOnce(context.Background(), PollRequest{0, 10, 0, []string{}}); !errors.Is(e, ErrProtocol) {
				t.Fatal(e)
			}
		})
	}
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) { t.Error("invalid input sent") })
	for _, r := range []PollRequest{{-1, 10, 1, []string{}}, {0, 0, 1, []string{}}, {0, 101, 1, []string{}}, {0, 10, 21, []string{}}, {0, 10, 1, nil}} {
		if _, e := c.PollOnce(context.Background(), r); !errors.Is(e, ErrRequest) {
			t.Fatal(e)
		}
	}
}

func TestProviderErrorIsClassifiedAndRedacted(t *testing.T) {
	for _, code := range []int{401, 409, 429, 500} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				fmt.Fprintf(w, `{"ok":false,"error_code":%d,"description":%q,"parameters":{"retry_after":7}}`, code, testToken)
			})
			_, e := c.Identity(context.Background())
			var api *APIError
			if !errors.As(e, &api) || api.Code != code || api.RetryAfter != 7 {
				t.Fatal(e)
			}
			if strings.Contains(e.Error(), testToken) {
				t.Fatal("secret leaked")
			}
		})
	}
}

func TestRedirectsAndTransportErrorsNeverExposeToken(t *testing.T) {
	var hits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer target.Close()
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL+"/"+testToken, 307) })
	if _, e := c.Identity(context.Background()); !errors.Is(e, ErrProtocol) {
		t.Fatal(e)
	}
	if hits.Load() != 0 {
		t.Fatal("followed token redirect")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := c.Identity(ctx)
	if !errors.Is(e, context.Canceled) || strings.Contains(e.Error(), testToken) {
		t.Fatal(e)
	}
}

func TestConstructionDoesNotPerformRequestsAndPreservesClient(t *testing.T) {
	h := &http.Client{Timeout: time.Minute}
	c, e := New(Options{Token: testToken, HTTPClient: h})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if h.Timeout != time.Minute || h.CheckRedirect != nil {
		t.Fatal("mutated supplied client")
	}
	for _, base := range []string{"http://example.com", "https://user:pass@example.com", "https://example.com/path", "https://example.com?token=x"} {
		if _, e := New(Options{Token: testToken, BaseURL: base}); !errors.Is(e, ErrConfig) {
			t.Fatal(base, e)
		}
	}
}

func TestModeOperationsPreserveBacklog(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		var p map[string]json.RawMessage
		if e := json.NewDecoder(r.Body).Decode(&p); e != nil {
			t.Error(e)
		}
		if string(p["drop_pending_updates"]) != "false" {
			t.Error("must explicitly preserve queue")
		}
		fmt.Fprint(w, `{"ok":true,"result":true}`)
	})
	if e := c.DeleteWebhook(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := c.SetWebhook(context.Background(), "https://gateway.example/v1/telegram/a", "secret", []string{"message"}); e != nil {
		t.Fatal(e)
	}
}

func TestClientFormattingAndMissingWebhookFields(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"ok":true,"result":{}}`) })
	for _, value := range []string{fmt.Sprint(c), fmt.Sprintf("%#v", c)} {
		if strings.Contains(value, testToken) {
			t.Fatal("formatting leaked token")
		}
	}
	raw, e := json.Marshal(c)
	if e != nil || strings.Contains(string(raw), testToken) {
		t.Fatal("JSON leaked token")
	}
	if _, e = c.WebhookInfo(context.Background()); !errors.Is(e, ErrProtocol) {
		t.Fatal("missing URL treated as empty webhook", e)
	}
}
