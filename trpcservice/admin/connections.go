package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

// ErrConnectionUnavailable reports missing runtime support.
var ErrConnectionUnavailable = errors.New("channel connections unavailable")

// ErrAgentNotReady reports an unconfigured tenant execution environment.
var ErrAgentNotReady = errors.New("tenant agent is not ready")

// ErrConnectionFailed is safe to return after provider authentication fails.
var ErrConnectionFailed = errors.New("channel connection failed")

// ConnectInput carries credentials only across the authenticated write boundary.
// It must never be logged, persisted in a Binding, or returned to the browser.
type ConnectInput struct {
	Channel channels.Channel `json:"channel"`
	BotID   string           `json:"bot_id"`
	Secret  string           `json:"secret"`
}

// Connection is a secret-free snapshot of one process-owned channel session.
type Connection struct {
	BindingID string           `json:"binding_id"`
	Channel   channels.Channel `json:"channel"`
	BotID     string           `json:"bot_id"`
	URL       string           `json:"url,omitempty"`
	Ready     bool             `json:"ready"`
	Replied   bool             `json:"replied"`
}

// ChannelConnections is the Admin consumer's live channel lifecycle boundary.
type ChannelConnections interface {
	Connect(context.Context, string, ConnectInput, channels.ChangeMetadata) (Connection, error)
	List(context.Context, string) ([]Connection, error)
	Disconnect(context.Context, string, string, channels.ChangeMetadata) error
}

func (h *Handler) connections(r *http.Request, p Principal, tenantID string, parts []string) (int, any, error) {
	if h.config.Connections == nil {
		return 0, nil, ErrConnectionUnavailable
	}
	metadata := channels.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, Reason: "web channel connection", CorrelationID: r.Header.Get("X-Request-ID")}
	if metadata.CorrelationID == "" {
		metadata.CorrelationID = "web-channel"
	}
	if len(parts) == 0 && r.Method == http.MethodGet {
		values, err := h.config.Connections.List(r.Context(), tenantID)
		return http.StatusOK, values, err
	}
	if len(parts) == 0 && r.Method == http.MethodPost {
		var input ConnectInput
		decoder := json.NewDecoder(io.LimitReader(r.Body, 8193))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			return 0, nil, errInvalidRequest
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return 0, nil, errInvalidRequest
		}
		value, err := h.config.Connections.Connect(r.Context(), tenantID, input, metadata)
		return http.StatusCreated, value, err
	}
	if len(parts) == 1 && r.Method == http.MethodDelete {
		err := h.config.Connections.Disconnect(r.Context(), tenantID, parts[0], metadata)
		return http.StatusOK, map[string]bool{"disconnected": err == nil}, err
	}
	return 0, nil, errNotFound
}
