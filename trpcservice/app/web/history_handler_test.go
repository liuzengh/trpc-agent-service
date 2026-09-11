package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/chat"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"
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
func (f *fakeLedger) Session(_ context.Context, sessionID string) (*chat.Session, error) {
	for i := range f.sessions {
		if f.sessions[i].SessionID == sessionID {
			cp := f.sessions[i]
			return &cp, nil
		}
	}
	return nil, chat.ErrSessionNotFound
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

func newHistoryMux(ledger chat.Ledger) *http.ServeMux {
	mux := http.NewServeMux()
	NewChatHistoryAPI(ledger).Register(mux)
	return mux
}

// serveWithClaims invokes the mux directly with an authenticated context, the
// same way the auth middleware injects claims (context values do not cross an
// httptest.Server network boundary).
func serveWithClaims(mux *http.ServeMux, claims *auth.Claims, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if claims != nil {
		req = req.WithContext(context.WithValue(req.Context(), AuthUserKey, claims))
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func decodeSessions(t *testing.T, rec *httptest.ResponseRecorder) []chat.Session {
	t.Helper()
	var out []chat.Session
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestChatHistoryOwnerSeesTenantFilteredSessions(t *testing.T) {
	f := &fakeLedger{
		sessions: []chat.Session{
			{SessionID: "s1", TenantID: "t1", AgentID: "a1", MemberID: "u1", Channel: "admin"},
			{SessionID: "s2", TenantID: "t2", AgentID: "a1", MemberID: "u2", Channel: "admin"},
		},
		messages: []chat.Message{
			{SessionID: "s1", MessageID: "um-1", Role: chat.RoleUser, Content: "hello"},
			{SessionID: "s1", MessageID: "am-1", Role: chat.RoleAssistant, Content: "hi"},
		},
	}
	mux := newHistoryMux(f)
	owner := &auth.Claims{TenantID: "t0", UserID: "root", Role: member.RoleOwner}

	// Owner may filter by any tenant; an explicit filter is honoured.
	sessions := decodeSessions(t, serveWithClaims(mux, owner, http.MethodGet, "/sessions?tenant_id=t1"))
	if len(sessions) != 1 || sessions[0].SessionID != "s1" {
		t.Errorf("sessions = %+v, want only s1", sessions)
	}

	// Owner with no filter sees every tenant.
	if sessions = decodeSessions(t, serveWithClaims(mux, owner, http.MethodGet, "/sessions")); len(sessions) != 2 {
		t.Errorf("owner sessions = %+v, want both tenants", sessions)
	}
}

func TestChatHistoryMemberOnlySeesOwnSessions(t *testing.T) {
	f := &fakeLedger{
		sessions: []chat.Session{
			{SessionID: "s1", TenantID: "t1", AgentID: "a1", MemberID: "alice", Channel: "admin"},
			{SessionID: "s2", TenantID: "t1", AgentID: "a1", MemberID: "bob", Channel: "admin"},
			{SessionID: "s3", TenantID: "t2", AgentID: "a1", MemberID: "alice", Channel: "admin"},
		},
	}
	mux := newHistoryMux(f)
	alice := &auth.Claims{TenantID: "t1", UserID: "alice", Role: member.RoleMember}

	// A hand-written tenant_id or member_id is ignored: the member is pinned to
	// their own tenant and their own member id.
	sessions := decodeSessions(t, serveWithClaims(mux, alice, http.MethodGet, "/sessions?tenant_id=t2&member_id=bob"))
	if len(sessions) != 1 || sessions[0].SessionID != "s1" {
		t.Errorf("member sessions = %+v, want only s1 (own session)", sessions)
	}
}

func TestChatHistoryRowLevelMessageAccess(t *testing.T) {
	f := &fakeLedger{
		sessions: []chat.Session{
			{SessionID: "s1", TenantID: "t1", AgentID: "a1", MemberID: "alice", Channel: "admin"},
			{SessionID: "s2", TenantID: "t1", AgentID: "a1", MemberID: "bob", Channel: "admin"},
		},
		messages: []chat.Message{
			{SessionID: "s1", MessageID: "um-1", Role: chat.RoleUser, Content: "hello"},
			{SessionID: "s2", MessageID: "um-2", Role: chat.RoleUser, Content: "hi"},
		},
	}
	mux := newHistoryMux(f)

	// A member can read their own session's messages...
	alice := &auth.Claims{TenantID: "t1", UserID: "alice", Role: member.RoleMember}
	if rec := serveWithClaims(mux, alice, http.MethodGet, "/sessions/s1/messages"); rec.Code != http.StatusOK {
		t.Fatalf("own session status = %d, want 200", rec.Code)
	}

	// ...but is told 404 (not 403) for a colleague's session, never confirming
	// that it exists.
	if rec := serveWithClaims(mux, alice, http.MethodGet, "/sessions/s2/messages"); rec.Code != http.StatusNotFound {
		t.Fatalf("foreign session status = %d, want 404", rec.Code)
	}

	// An admin of the same tenant may read any of its sessions.
	admin := &auth.Claims{TenantID: "t1", UserID: "admin", Role: member.RoleAdmin}
	if rec := serveWithClaims(mux, admin, http.MethodGet, "/sessions/s2/messages"); rec.Code != http.StatusOK {
		t.Fatalf("admin session status = %d, want 200", rec.Code)
	}
}

func TestChatHistoryUnknownSessionIs404(t *testing.T) {
	mux := newHistoryMux(&fakeLedger{})
	owner := &auth.Claims{TenantID: "t0", UserID: "root", Role: member.RoleOwner}
	if rec := serveWithClaims(mux, owner, http.MethodGet, "/sessions/ghost/messages"); rec.Code != http.StatusNotFound {
		t.Fatalf("ghost session status = %d, want 404", rec.Code)
	}
}
