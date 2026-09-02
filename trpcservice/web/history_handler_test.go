package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/chat"
)

type fakeLedger struct {
	turns    []chat.Turn
	sessions []chat.Session
	messages []chat.Message
}

func (f *fakeLedger) RecordTurn(_ context.Context, t chat.Turn) error {
	f.turns = append(f.turns, t)
	return nil
}
func (f *fakeLedger) Sessions(_ context.Context, q chat.SessionQuery) ([]chat.Session, error) {
	out := []chat.Session{}
	for _, s := range f.sessions {
		if (q.TenantID == "" || s.TenantID == q.TenantID) && (q.MemberID == "" || s.MemberID == q.MemberID) {
			out = append(out, s)
		}
	}
	return out, nil
}
func (f *fakeLedger) Messages(_ context.Context, q chat.MessageQuery) ([]chat.Message, error) {
	out := []chat.Message{}
	for _, m := range f.messages {
		if m.SessionID == q.SessionID {
			out = append(out, m)
		}
	}
	return out, nil
}

func newHistoryServer(ledger chat.Ledger) *httptest.Server {
	mux := http.NewServeMux()
	NewChatHistoryAPI(ledger).Register(mux)
	return httptest.NewServer(mux)
}

func TestChatHistorySessionsAndMessages(t *testing.T) {
	f := &fakeLedger{
		sessions: []chat.Session{{SessionID: "s1", TenantID: "t1", AgentID: "a1", MemberID: "u1", Channel: "admin"}},
		messages: []chat.Message{
			{SessionID: "s1", MessageID: "um-1", Role: chat.RoleUser, Content: "hello"},
			{SessionID: "s1", MessageID: "am-1", Role: chat.RoleAssistant, Content: "hi"},
		},
	}
	srv := newHistoryServer(f)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/sessions?tenant_id=t1")
	if err != nil {
		t.Fatal(err)
	}
	var sessions []chat.Session
	if err := json.NewDecoder(resp.Body).Decode(&sessions); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(sessions) != 1 || sessions[0].SessionID != "s1" {
		t.Errorf("sessions = %+v", sessions)
	}

	// tenant filter
	resp, _ = http.Get(srv.URL + "/sessions?tenant_id=other")
	_ = json.NewDecoder(resp.Body).Decode(&sessions)
	resp.Body.Close()
	if len(sessions) != 0 {
		t.Errorf("other tenant sessions = %+v", sessions)
	}

	// messages for the session
	resp, err = http.Get(srv.URL + "/sessions/s1/messages")
	if err != nil {
		t.Fatal(err)
	}
	var msgs []chat.Message
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(msgs) != 2 || msgs[0].Role != chat.RoleUser {
		t.Errorf("messages = %+v", msgs)
	}

	// empty session -> empty list, 200
	resp, _ = http.Get(srv.URL + "/sessions/ghost/messages")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("ghost session status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}
