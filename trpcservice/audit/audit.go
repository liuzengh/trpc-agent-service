// Package audit persists tenant-scoped security and execution decisions.
package audit

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"go.opentelemetry.io/otel/trace"
)

type Event struct {
	ID               string         `json:"audit_id,omitempty"`
	TenantID         string         `json:"tenant_id"`
	Channel          string         `json:"channel,omitempty"`
	ChannelBindingID string         `json:"channel_binding_id,omitempty"`
	UserID           string         `json:"user_id,omitempty"`
	SessionID        string         `json:"session_id,omitempty"`
	MessageID        string         `json:"message_id,omitempty"`
	RequestID        string         `json:"request_id,omitempty"`
	TraceID          string         `json:"trace_id,omitempty"`
	AgentName        string         `json:"agent_name,omitempty"`
	RevisionID       string         `json:"revision_id,omitempty"`
	ToolName         string         `json:"tool_name,omitempty"`
	Decision         string         `json:"decision"`
	Latency          time.Duration  `json:"latency"`
	ErrorType        string         `json:"error_type,omitempty"`
	Cost             float64        `json:"cost"`
	Details          map[string]any `json:"details"`
	OccurredAt       time.Time      `json:"occurred_at"`
}

type Writer interface {
	Record(ctx context.Context, event Event) error
	Ready(ctx context.Context) error
	Close() error
}

type Query struct {
	TenantID string
	Decision string
	TraceID  string
	Limit    int
}

type Reader interface {
	Query(ctx context.Context, query Query) ([]Event, error)
}

func TraceID(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return ""
	}
	return spanContext.TraceID().String()
}

func sanitizeEvent(event Event) Event {
	if event.ID == "" {
		event.ID = "audit-" + uuid.NewString()
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	event.OccurredAt = event.OccurredAt.UTC()
	event.Details = redactMap(event.Details)
	return event
}

func redactMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return map[string]any{}
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		if sensitiveKey(key) {
			result[key] = "[REDACTED]"
			continue
		}
		result[key] = redactValue(value)
	}
	return result
}

func redactValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return redactMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = redactValue(item)
		}
		return out
	case json.RawMessage:
		return "[JSON]"
	case string:
		return platformlog.Redact(v)
	default:
		return value
	}
}

func sensitiveKey(key string) bool {
	key = strings.ToLower(key)
	for _, fragment := range []string{
		"secret", "token", "password", "api_key", "authorization", "cookie",
	} {
		if strings.Contains(key, fragment) {
			return true
		}
	}
	return false
}
