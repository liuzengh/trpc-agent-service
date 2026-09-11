package wecom_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
)

func TestAuthenticationProbeClosesAndNeverReplies(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		want string
		auth *bool
	}{
		{"accepted", 0, "WECOM_AUTHENTICATED", boolPtr(true)},
		{"rejected", 40013, "WECOM_AUTH_REJECTED", boolPtr(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var connections, commands atomic.Int32
			closed := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				defer close(closed)
				connections.Add(1)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, raw, err := conn.Read(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				commands.Add(1)
				var f struct {
					Cmd     string            `json:"cmd"`
					Headers map[string]string `json:"headers"`
					Body    map[string]string `json:"body"`
				}
				if json.Unmarshal(raw, &f) != nil {
					t.Fatal("json")
				}
				if f.Cmd != "aibot_subscribe" || f.Body["bot_id"] != "probe-bot" || f.Body["secret"] != "probe-secret" {
					t.Error("wrong auth")
				}
				reply, _ := json.Marshal(map[string]any{"headers": f.Headers, "errcode": tc.code, "errmsg": "provider-private-text"})
				_ = conn.Write(ctx, websocket.MessageText, reply)
				for {
					if _, _, err = conn.Read(ctx); err != nil {
						return
					}
					commands.Add(1)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result, err := wecom.ProbeAuthentication(ctx, wecom.Config{BotID: "probe-bot", Secret: "probe-secret", URL: "ws" + strings.TrimPrefix(server.URL, "http"), MaxReconnects: 5})
			if err != nil || result.Code != tc.want || result.Authenticated == nil || *result.Authenticated != *tc.auth {
				t.Fatalf("%+v %v", result, err)
			}
			select {
			case <-closed:
			case <-ctx.Done():
				t.Fatal("probe retained connection")
			}
			if connections.Load() != 1 || commands.Load() != 1 {
				t.Fatalf("unexpected reconnect/reply %d %d", connections.Load(), commands.Load())
			}
			encoded, _ := json.Marshal(result)
			if strings.Contains(string(encoded), "private") || strings.Contains(string(encoded), "secret") {
				t.Fatal("credential/provider text exposed")
			}
		})
	}
}
func boolPtr(v bool) *bool { return &v }
func TestAuthenticationProbeTimeoutIsNotCredentialRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, _ = conn.Read(ctx)
		_, _, _ = conn.Read(ctx)
	}))
	defer server.Close()
	result, err := wecom.ProbeAuthentication(context.Background(), wecom.Config{BotID: "probe-bot", Secret: "probe-secret", URL: "ws" + strings.TrimPrefix(server.URL, "http"), AckTimeout: 30 * time.Millisecond})
	if err != nil || result.Code != "PROVIDER_TIMEOUT" || result.Authenticated != nil {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestAuthenticationProbeCanceledDoesNotDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := wecom.ProbeAuthentication(ctx, wecom.Config{BotID: "probe-bot", Secret: "probe-secret"})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// A matched ACK is an authentication fact even if the peer closes immediately.
// Keep the smallest state buffer so lossy lifecycle notifications cannot hide it.
func TestAuthenticationProbePreservesAckAfterImmediateClose(t *testing.T) {
	for _, code := range []int{0, 40013} {
		want := "WECOM_AUTHENTICATED"
		if code != 0 {
			want = "WECOM_AUTH_REJECTED"
		}
		t.Run(want, func(t *testing.T) {
			var connections atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				connections.Add(1)
				ctx, cancel := context.WithTimeout(r.Context(), time.Second)
				defer cancel()
				_, raw, err := conn.Read(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				var frame struct {
					Headers map[string]string `json:"headers"`
				}
				if err := json.Unmarshal(raw, &frame); err != nil {
					t.Error(err)
					return
				}
				ack, _ := json.Marshal(map[string]any{"headers": frame.Headers, "errcode": code})
				if err := conn.Write(ctx, websocket.MessageText, ack); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			for i := 0; i < 100; i++ {
				result, err := wecom.ProbeAuthentication(context.Background(), wecom.Config{BotID: "probe-bot", Secret: "fixture-secret", URL: "ws" + strings.TrimPrefix(server.URL, "http"), StateBuffer: 1})
				if err != nil || result.Code != want || result.Authenticated == nil || *result.Authenticated != (code == 0) {
					t.Fatalf("iteration %d: got %+v err=%v; want %s", i, result, err, want)
				}
			}
			if code != 0 {
				before := connections.Load()
				client, err := wecom.NewClient(wecom.Config{BotID: "probe-bot", Secret: "fixture-secret", URL: "ws" + strings.TrimPrefix(server.URL, "http"), MaxReconnects: 3})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err = client.Run(ctx, func(context.Context, wecom.Event) error { return nil })
				var failure *wecom.AuthenticationError
				if !errors.As(err, &failure) || failure.Certainty != wecom.Rejected || connections.Load()-before != 1 {
					t.Fatalf("explicit rejection retried or lost: err=%v connections=%d", err, connections.Load()-before)
				}
			}
		})
	}
}
