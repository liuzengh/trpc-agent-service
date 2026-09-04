package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// ChatBus is the slice of the message bus the admin chat API needs. It is an
// interface so handler tests can fake delivery; bus.RedisBus satisfies it.
type ChatBus interface {
	PublishInbound(ctx context.Context, m *bus.Message) error
	ReadOutbound(ctx context.Context, fromID string) ([]*bus.Message, string, error)
	Route(ctx context.Context, tenantID, sessionID string) (string, error)
	SetRoute(ctx context.Context, tenantID, sessionID, agentID string) error
}

// ChatAPI lets the admin frontend talk to agents over the same worker pipeline
// the IM channels use: POST /chat publishes an inbound admin message, the
// worker replies through the outbox → stream:outbound, and GET /chat/stream
// forwards those replies to the browser over SSE.
type ChatAPI struct {
	bus ChatBus
}

// NewChatAPI returns the admin chat API bound to the given bus.
func NewChatAPI(b ChatBus) *ChatAPI {
	return &ChatAPI{bus: b}
}

// Register mounts chat routes.
func (a *ChatAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /chat", a.send)
	mux.HandleFunc("GET /chat/stream", a.stream)
}

// chatSendRequest is the POST /chat body.
type chatSendRequest struct {
	TenantID  string `json:"tenant_id"`
	AgentID   string `json:"agent_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	Text      string `json:"text"`
}

type chatSendResponse struct {
	MessageID string `json:"message_id"`
	SessionID string `json:"session_id"`
	AgentID   string `json:"agent_id"`
}

func (a *ChatAPI) send(w http.ResponseWriter, r *http.Request) {
	var req chatSendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("bad chat request: %w", err))
		return
	}
	if req.TenantID == "" {
		writeError(w, http.StatusBadRequest, errors.New("tenant_id is required"))
		return
	}
	if strings.TrimSpace(req.Text) == "" {
		writeError(w, http.StatusBadRequest, errors.New("text is required"))
		return
	}
	if req.SessionID == "" {
		req.SessionID = uuid.NewString()
	}
	// Resolve the agent: an explicit agent_id binds the session on first use;
	// otherwise an existing route is required.
	agentID := req.AgentID
	if agentID == "" {
		got, err := a.bus.Route(r.Context(), req.TenantID, req.SessionID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		agentID = got
	}
	if agentID == "" {
		writeError(w, http.StatusBadRequest,
			errors.New("no agent bound to this session; provide agent_id on the first message"))
		return
	}
	if req.AgentID != "" {
		if err := a.bus.SetRoute(r.Context(), req.TenantID, req.SessionID, agentID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	userID := req.UserID
	if userID == "" {
		userID = "admin"
	}
	content := model.NewUserMessage(req.Text)
	msg := &bus.Message{
		ID:        uuid.NewString(),
		TraceID:   uuid.NewString(),
		TenantID:  req.TenantID,
		AgentID:   agentID,
		SessionID: req.SessionID,
		Channel:   "admin",
		UserID:    userID,
		Content:   &content,
	}
	if err := a.bus.PublishInbound(r.Context(), msg); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, chatSendResponse{
		MessageID: msg.ID,
		SessionID: req.SessionID,
		AgentID:   agentID,
	})
}

// stream is a Server-Sent-Events endpoint. It follows stream:outbound (without
// a consumer group) and forwards admin-channel replies belonging to the given
// session. cursor may be a stream id or "$" for only-new messages.
func (a *ChatAPI) stream(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		writeError(w, http.StatusBadRequest, errors.New("session_id is required"))
		return
	}
	cursor := r.URL.Query().Get("cursor")
	if cursor == "" {
		cursor = "$"
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// keep-alive comment so proxies hold the connection open between events
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		msgs, next, err := a.bus.ReadOutbound(r.Context(), cursor)
		if err != nil {
			fmt.Fprintf(w, "event: error\ndata: {\"error\": %q}\n\n", err.Error())
			flusher.Flush()
			return
		}
		if len(msgs) > 0 {
			cursor = next
		}
		for _, m := range msgs {
			if m == nil || m.Channel != "admin" || m.SessionID != sessionID || m.Content == nil {
				continue
			}
			text := strings.TrimSpace(m.Content.Content)
			if text == "" {
				continue
			}
			payload, _ := json.Marshal(struct {
				Text      string `json:"text"`
				MessageID string `json:"message_id"`
				At        int64  `json:"at"`
			}{Text: text, MessageID: m.ID, At: time.Now().UnixMilli()})
			fmt.Fprintf(w, "id: %s\nevent: message\ndata: %s\n\n", next, payload)
			flusher.Flush()
		}
		// ReadOutbound already waits ~1s; the extra sleep keeps the loop tame
		// when nothing matched.
		if len(msgs) == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
}
