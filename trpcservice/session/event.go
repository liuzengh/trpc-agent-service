package session

import (
	"fmt"
	"strings"
	"time"
)

type EventType string

const (
	EventUserReceived       EventType = "user.received"
	EventAgentStarted       EventType = "agent.started"
	EventToolStarted        EventType = "tool.started"
	EventToolCompleted      EventType = "tool.completed"
	EventToolFailed         EventType = "tool.failed"
	EventAssistantCompleted EventType = "assistant.completed"
	EventExecutionFailed    EventType = "execution.failed"
	EventExecutionCanceled  EventType = "execution.canceled"
)

type SessionEvent struct {
	TenantID      string         `json:"tenant_id"`
	SessionID     string         `json:"session_id"`
	Seq           int64          `json:"seq"`
	EventID       string         `json:"event_id"`
	EventType     EventType      `json:"event_type"`
	MessageID     string         `json:"message_id"`
	ExecutionID   string         `json:"execution_id"`
	ParentEventID string         `json:"parent_event_id,omitempty"`
	Attempt       int            `json:"attempt"`
	TraceID       string         `json:"trace_id"`
	Payload       map[string]any `json:"payload,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
}

func (e SessionEvent) Validate() error {
	for name, value := range map[string]string{
		"tenant_id": e.TenantID, "session_id": e.SessionID, "event_id": e.EventID,
		"message_id": e.MessageID, "execution_id": e.ExecutionID, "trace_id": e.TraceID,
	} {
		if err := validateID(value); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrInvalidEvent, name, err)
		}
	}
	if e.Seq < 1 || e.Attempt < 1 || !validEventType(e.EventType) {
		return fmt.Errorf("%w: invalid sequence, attempt, or event type", ErrInvalidEvent)
	}
	if containsSensitiveKey(e.Payload) {
		return fmt.Errorf("%w: sensitive payload field", ErrInvalidEvent)
	}
	return nil
}

func validEventType(value EventType) bool {
	switch value {
	case EventUserReceived, EventAgentStarted, EventToolStarted, EventToolCompleted,
		EventToolFailed, EventAssistantCompleted, EventExecutionFailed, EventExecutionCanceled:
		return true
	default:
		return false
	}
}

func normalizeKey(value string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "-", "_"))
}

func containsSensitiveKey(value map[string]any) bool {
	for key, nested := range value {
		if isSensitiveText(key) {
			if redacted, ok := nested.(string); ok && redacted == "[REDACTED]" {
				continue
			}
			return true
		}
		switch typed := nested.(type) {
		case map[string]any:
			if containsSensitiveKey(typed) {
				return true
			}
		case []any:
			for _, item := range typed {
				if child, ok := item.(map[string]any); ok && containsSensitiveKey(child) {
					return true
				}
				if text, ok := item.(string); ok && isSensitiveText(text) && text != "[REDACTED]" {
					return true
				}
			}
		case string:
			if typed != "[REDACTED]" && isSensitiveText(typed) {
				return true
			}
		}
	}
	return false
}

func RedactPayload(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		if isSensitiveText(key) {
			output[key] = "[REDACTED]"
			continue
		}
		output[key] = redactPayloadValue(value)
	}
	return output
}

func redactPayloadValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return RedactPayload(typed)
	case []any:
		output := make([]any, len(typed))
		for index, item := range typed {
			output[index] = redactPayloadValue(item)
		}
		return output
	case string:
		if isSensitiveText(typed) {
			return "[REDACTED]"
		}
	}
	return value
}

func isSensitiveText(value string) bool {
	normalized := normalizeKey(value)
	for _, candidate := range []string{"authorization", "apikey", "api_key", "secret", "token", "password", "cookie", "systemprompt", "system_prompt", "prompt", "private_key"} {
		if normalized == candidate || strings.Contains(normalized, candidate) {
			return true
		}
	}
	return false
}
