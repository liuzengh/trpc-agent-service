package webui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
)

func TestWebUIMessageAndSSEEndToEnd(t *testing.T) {
	hub := NewHub()
	adapter, err := New(hub, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	mux := http.NewServeMux()
	var inbound gateway.InboundMessage
	sink := channels.Sink(func(
		_ context.Context,
		message gateway.InboundMessage,
	) (gateway.Result, error) {
		inbound = message
		return gateway.Result{
			Outcome:   gateway.OutcomeIngested,
			SessionID: "tenant-a:webui:" + message.SenderID,
		}, nil
	})
	if err := adapter.Run(context.Background(), mux, sink); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	post := httptest.NewRequest(
		http.MethodPost,
		"/channels/webui/binding-a/messages",
		bytes.NewBufferString(`{"id":"uuid-1","text":"hello"}`),
	)
	post.Header.Set("Content-Type", "application/json")
	postResponse := httptest.NewRecorder()
	mux.ServeHTTP(postResponse, post)
	if postResponse.Code != http.StatusOK {
		t.Fatalf("POST status = %d, body = %s", postResponse.Code, postResponse.Body.String())
	}
	if inbound.MsgID != "uuid-1" || inbound.RouteKey != "binding-a" || inbound.Text != "hello" {
		t.Fatalf("inbound message = %#v", inbound)
	}
	cookies := postResponse.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != cookieName {
		t.Fatalf("response cookies = %#v", cookies)
	}
	var result gateway.Result
	if err := json.NewDecoder(postResponse.Body).Decode(&result); err != nil {
		t.Fatalf("decode POST response: %v", err)
	}
	if result.SessionID == "" {
		t.Fatal("POST response omitted session_id")
	}

	events := make(chan reply.Event, 2)
	events <- reply.Event{Type: "text_delta", Text: "world"}
	events <- reply.Event{Type: "done"}
	close(events)
	if err := adapter.NewReplier(tenant.Snapshot{}).Reply(
		context.Background(), result.SessionID, inbound, events,
	); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}

	get := httptest.NewRequest(
		http.MethodGet,
		"/channels/webui/binding-a/stream?session="+result.SessionID,
		nil,
	)
	get.AddCookie(cookies[0])
	getResponse := httptest.NewRecorder()
	mux.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("SSE status = %d, body = %s", getResponse.Code, getResponse.Body.String())
	}
	body := getResponse.Body.String()
	if !strings.Contains(body, "event: text_delta") ||
		!strings.Contains(body, `"text":"world"`) ||
		!strings.Contains(body, "event: done") {
		t.Fatalf("SSE body = %q", body)
	}
}

func TestWebUIStreamOwnerIsolation(t *testing.T) {
	hub := NewHub()
	if err := hub.Ensure("session-a", "owner-a"); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if _, _, err := hub.Subscribe("session-a", "owner-b"); err != errStreamForbidden {
		t.Fatalf("Subscribe(other owner) error = %v, want forbidden", err)
	}
	if err := hub.Ensure("session-a", "owner-b"); err != errStreamForbidden {
		t.Fatalf("Ensure(other owner) error = %v, want forbidden", err)
	}
}

func TestWebUIDesksListsCatalog(t *testing.T) {
	catalog := desksCatalog{desks: []Desk{
		{
			RouteKey:   "binding-a",
			TenantName: "Tenant A",
			AppName:    "tenant-a-support",
		},
	}}
	adapter, err := New(NewHub(), nil, catalog)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, unusedSink); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/channels/webui/desks", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var desks []Desk
	if err := json.NewDecoder(response.Body).Decode(&desks); err != nil {
		t.Fatalf("decode desks: %v", err)
	}
	if len(desks) != 1 || desks[0].RouteKey != "binding-a" {
		t.Fatalf("desks = %#v", desks)
	}
}

func TestWebUIDesksWithoutCatalog(t *testing.T) {
	adapter, err := New(NewHub(), nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, unusedSink); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/channels/webui/desks", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if strings.TrimSpace(response.Body.String()) != "[]" {
		t.Fatalf("body = %q", response.Body.String())
	}
}

type desksCatalog struct {
	desks []Desk
}

func (c desksCatalog) ListDesks(context.Context) ([]Desk, error) {
	return c.desks, nil
}

var unusedSink = channels.Sink(func(
	context.Context,
	gateway.InboundMessage,
) (gateway.Result, error) {
	return gateway.Result{}, nil
})

func TestWebUIValidation(t *testing.T) {
	if _, err := New(nil, nil, nil); err == nil {
		t.Fatal("New() accepted nil hub")
	}
	adapter, err := New(NewHub(), nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := adapter.Run(context.Background(), nil, nil); err == nil {
		t.Fatal("Run() accepted nil dependencies")
	}
}
