// Package audit persists tenant-scoped security and execution decisions.
package audit

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type Event struct {
	TenantID         string
	Channel          string
	ChannelBindingID string
	UserID           string
	SessionID        string
	MessageID        string
	RequestID        string
	TraceID          string
	AgentName        string
	RevisionID       string
	ToolName         string
	Decision         string
	Latency          time.Duration
	ErrorType        string
	Cost             float64
	Details          map[string]any
	OccurredAt       time.Time
}

type Writer interface {
	Record(ctx context.Context, event Event) error
	Ready(ctx context.Context) error
	Close() error
}

func TraceID(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return ""
	}
	return spanContext.TraceID().String()
}

func sanitizeEvent(event Event) Event {
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
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
		switch typed := value.(type) {
		case map[string]any:
			result[key] = redactMap(typed)
		case json.RawMessage:
			result[key] = "[JSON]"
		default:
			result[key] = typed
		}
	}
	return result
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
