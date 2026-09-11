package web

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/chat"
)

// ChatHistoryAPI exposes the business conversation ledger (chat_sessions +
// chat_messages): session listing and turn-paginated history. Only registered
// when the MySQL ledger is available.
type ChatHistoryAPI struct {
	ledger chat.Ledger
}

// NewChatHistoryAPI returns the conversation history API over a ledger.
func NewChatHistoryAPI(ledger chat.Ledger) *ChatHistoryAPI {
	return &ChatHistoryAPI{ledger: ledger}
}

// Register mounts history routes.
func (a *ChatHistoryAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /sessions", a.listSessions)
	mux.HandleFunc("GET /sessions/{id}/messages", a.listMessages)
}

func (a *ChatHistoryAPI) listSessions(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	q := chat.SessionQuery{
		// Row-level rule: owner reads every tenant, admin their own, a member
		// only the sessions they produced themselves.
		TenantID: ScopeTenant(claims, r.URL.Query().Get("tenant_id")),
		MemberID: ScopeMember(claims, r.URL.Query().Get("member_id")),
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.Limit = n
		}
	}
	sessions, err := a.ledger.Sessions(r.Context(), q)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

func (a *ChatHistoryAPI) listMessages(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	// Authorise against the session's own tenant/member first: without this a
	// member could read another member's conversation by guessing a session id.
	sess, err := a.ledger.Session(r.Context(), sessionID)
	if err != nil {
		if errors.Is(err, chat.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !CanReadSession(GetClaims(r.Context()), sess.TenantID, sess.MemberID) {
		// 404 rather than 403: never confirm that a foreign session exists.
		writeError(w, http.StatusNotFound, chat.ErrSessionNotFound)
		return
	}

	q := chat.MessageQuery{SessionID: sessionID}
	if v := r.URL.Query().Get("before_turn"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			q.BeforeTurn = n
		}
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.Limit = n
		}
	}
	messages, err := a.ledger.Messages(r.Context(), q)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, messages)
}
