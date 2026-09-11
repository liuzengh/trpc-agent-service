package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// fakeChatBus simulates the inbound + outbound sides for handler tests.
type fakeChatBus struct {
	published []*bus.Message
	routes    map[string]string // "tenant:session" -> agent
	// queued outbound messages, each with a fake stream id.
	queued []struct {
		id string
		m  *bus.Message
	}
}

func (f *fakeChatBus) PublishInbound(_ context.Context, m *bus.Message) error {
	f.published = append(f.published, m)
	return nil
}

func (f *fakeChatBus) SetRoute(_ context.Context, tenant, session, agent string) error {
	f.routes[tenant+":"+session] = agent
	return nil
}

func (f *fakeChatBus) Route(_ context.Context, tenant, session string) (string, error) {
	return f.routes[tenant+":"+session], nil
}

func (f *fakeChatBus) ReadOutbound(_ context.Context, fromID string) ([]*bus.Message, string, error) {
	start := 0
	if fromID != "" && fromID != "0" && fromID != "$" {
		start = atoiOrZero(strings.TrimSuffix(fromID, "-0"))
	}
	if fromID == "$" {
		start = len(f.queued)
	}
	out := []*bus.Message{}
	cursor := fromID
	for i := start; i < len(f.queued); i++ {
		out = append(out, f.queued[i].m)
		cursor = f.queued[i].id
	}
	if fromID == "$" && len(out) == 0 {
		cursor = "$"
	}
	return out, cursor, nil
}

func atoiOrZero(s string) int {
	n := 0
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			break
		}
		n = n*10 + int(ch-'0')
	}
	return n
}

func adminReply(session, text, id string) *bus.Message {
	content := model.NewAssistantMessage(text)
	return &bus.Message{ID: id, TenantID: "t1", SessionID: session, Channel: "admin", Content: &content}
}

func newChatMux(f *fakeChatBus) *http.ServeMux {
	mux := http.NewServeMux()
	NewChatAPI(f).Register(mux)
	return mux
}

// postChat sends an authenticated chat POST directly to the mux.
func postChat(mux *http.ServeMux, claims *auth.Claims, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/chat", strings.NewReader(body))
	if claims != nil {
		req = req.WithContext(context.WithValue(req.Context(), AuthUserKey, claims))
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestChatSendRoutesAndPublishes(t *testing.T) {
	f := &fakeChatBus{routes: map[string]string{}}
	mux := newChatMux(f)
	// The send path now requires an authenticated caller; the owner may target
	// any tenant, mirroring the production auth middleware.
	owner := &auth.Claims{TenantID: "t1", UserID: "root", Role: member.RoleOwner}

	// first message: explicit agent binds the session route
	rec := postChat(mux, owner, `{"tenant_id":"t1","agent_id":"a1","session_id":"s1","text":"hi"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	var out chatSendResponse
	_ = json.NewDecoder(rec.Body).Decode(&out)
	if out.SessionID != "s1" || out.AgentID != "a1" || out.MessageID == "" {
		t.Errorf("response = %+v", out)
	}
	if len(f.published) != 1 {
		t.Fatalf("published = %d, want 1", len(f.published))
	}
	m := f.published[0]
	if m.TenantID != "t1" || m.AgentID != "a1" || m.Channel != "admin" || m.SessionID != "s1" {
		t.Errorf("message envelope = %+v", m)
	}
	if m.UserID != "root" {
		t.Errorf("user_id = %q, want root (from claims)", m.UserID)
	}
	if m.Content == nil || m.Content.Content != "hi" {
		t.Errorf("content = %+v, want hi", m.Content)
	}

	// subsequent message without agent_id resolves via the route
	rec = postChat(mux, owner, `{"tenant_id":"t1","session_id":"s1","text":"again"}`)
	_ = json.NewDecoder(rec.Body).Decode(&out)
	if out.AgentID != "a1" {
		t.Errorf("routed agent = %q, want a1", out.AgentID)
	}

	// unknown session + no agent -> 400
	rec = postChat(mux, owner, `{"tenant_id":"t1","session_id":"ghost","text":"x"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("no-agent status = %d, want 400", rec.Code)
	}

	// empty text -> 400
	rec = postChat(mux, owner, `{"tenant_id":"t1","agent_id":"a1","text":""}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("empty text status = %d, want 400", rec.Code)
	}
}

func TestChatSendRequiresAuth(t *testing.T) {
	f := &fakeChatBus{routes: map[string]string{}}
	mux := newChatMux(f)
	rec := postChat(mux, nil, `{"tenant_id":"t1","agent_id":"a1","session_id":"s1","text":"hi"}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestChatSendPinsTenantToToken(t *testing.T) {
	f := &fakeChatBus{routes: map[string]string{}}
	mux := newChatMux(f)
	// A member may not push a turn into another tenant via a crafted body.
	alice := &auth.Claims{TenantID: "t1", UserID: "alice", Role: member.RoleMember}
	rec := postChat(mux, alice, `{"tenant_id":"t2","agent_id":"a1","session_id":"s1","text":"hi"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if len(f.published) != 1 || f.published[0].TenantID != "t1" {
		t.Errorf("published tenant = %+v, want t1 (pinned to token)", f.published)
	}
}

func TestChatStreamForwardsOnlySessionAdminReplies(t *testing.T) {
	f := &fakeChatBus{routes: map[string]string{}}
	// one matching reply, one other-session reply, one non-admin reply
	f.queued = []struct {
		id string
		m  *bus.Message
	}{
		{id: "0-0", m: adminReply("s1", "answer one", "a-1")},
		{id: "1-0", m: adminReply("s2", "other session", "a-2")},
		{id: "2-0", m: adminReply("s1", "non-admin", "a-3")},
	}
	f.queued[2].m.Channel = "wecom"
	mux := http.NewServeMux()
	NewChatAPI(f).Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/chat/stream?session_id=s1&cursor=0", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("content-type = %q", ct)
	}

	buf := make([]byte, 4096)
	deadline := time.Now().Add(3 * time.Second)
	got := ""
	for time.Now().Before(deadline) && !strings.Contains(got, "answer one") {
		n, _ := resp.Body.Read(buf)
		got += string(buf[:n])
	}
	if !strings.Contains(got, "answer one") {
		t.Fatalf("stream did not deliver matching reply, got: %q", got)
	}
	if strings.Contains(got, "other session") || strings.Contains(got, "non-admin") {
		t.Errorf("stream leaked non-matching messages: %q", got)
	}
}
