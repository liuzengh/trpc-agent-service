package wecompreflight

import (
	"context"
	"encoding/json"
	"github.com/coder/websocket"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeUsesBoundedSubscriptionAndCloses(t *testing.T) {
	var calls atomic.Int32
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		defer close(closed)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return
		}
		calls.Add(1)
		var f map[string]any
		_ = json.Unmarshal(raw, &f)
		if f["cmd"] != "aibot_subscribe" {
			t.Error("unexpected method")
		}
		body, _ := json.Marshal(map[string]any{"headers": f["headers"], "errcode": 0})
		_ = conn.Write(ctx, websocket.MessageText, body)
		for {
			if _, _, err = conn.Read(ctx); err != nil {
				return
			}
			calls.Add(1)
		}
	}))
	defer server.Close()
	client := New()
	defer client.Close()
	client.endpoint = "ws" + strings.TrimPrefix(server.URL, "http")
	result, err := client.InspectConnection(context.Background(), app.NewSecret("test-secret"), "test-bot")
	if err != nil || result.Code != "WECOM_AUTHENTICATED" || result.Authenticated == nil || !*result.Authenticated {
		t.Fatalf("%+v %v", result, err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("subscription leaked")
	}
	if calls.Load() != 1 {
		t.Fatal("probe sent business commands")
	}
}
func TestProbeRejectsRedirectAndCanceledContext(t *testing.T) {
	var downstream atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { downstream.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer redirect.Close()
	client := New()
	defer client.Close()
	client.endpoint = "ws" + strings.TrimPrefix(redirect.URL, "http")
	result, err := client.InspectConnection(context.Background(), app.NewSecret("test-secret"), "test-bot")
	if err != nil || result.Code != "PROVIDER_NETWORK" || downstream.Load() != 0 {
		t.Fatal("redirect followed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = client.InspectConnection(ctx, app.NewSecret("test-secret"), "test-bot"); err != app.ErrExpired {
		t.Fatal(err)
	}
}
