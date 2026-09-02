package web

import (
	"net/http"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/chat"
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
	q := chat.SessionQuery{
		TenantID: r.URL.Query().Get("tenant_id"),
		MemberID: r.URL.Query().Get("member_id"),
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
	q := chat.MessageQuery{SessionID: r.PathValue("id")}
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
