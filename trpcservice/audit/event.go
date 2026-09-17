// Package audit defines durable, versioned audit facts.
package audit

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Event struct {
	SchemaVersion    uint16    `json:"schema_version"`
	AuditID          string    `json:"audit_id"`
	TenantID         string    `json:"tenant_id"`
	Channel          string    `json:"channel,omitempty"`
	UserID           string    `json:"user_id,omitempty"`
	SessionID        string    `json:"session_id,omitempty"`
	RequestID        string    `json:"request_id,omitempty"`
	AgentAppID       string    `json:"agent_app_id,omitempty"`
	AgentAppRevision int64     `json:"agent_app_revision,omitempty"`
	AgentName        string    `json:"agent_name,omitempty"`
	ToolName         string    `json:"tool_name,omitempty"`
	Action           string    `json:"action"`
	Decision         string    `json:"decision"`
	ReasonCode       string    `json:"reason_code,omitempty"`
	LatencyMS        int64     `json:"latency_ms"`
	ErrorType        string    `json:"error_type,omitempty"`
	CostMicros       int64     `json:"cost_micros"`
	Currency         string    `json:"currency,omitempty"`
	InputTokens      int64     `json:"input_tokens,omitempty"`
	OutputTokens     int64     `json:"output_tokens,omitempty"`
	ConfigVersion    int64     `json:"config_version,omitempty"`
	PolicyVersion    int64     `json:"policy_version,omitempty"`
	ContentDigest    string    `json:"content_digest,omitempty"`
	TraceID          string    `json:"trace_id,omitempty"`
	ResourceRefs     []string  `json:"resource_refs,omitempty"`
	OccurredAt       time.Time `json:"occurred_at"`
}
type AuditEvent = Event

type Sink interface {
	Emit(context.Context, Event) error
}

func Validate(event Event) error {
	if event.SchemaVersion != 1 || !plain(event.AuditID, 256) || !plain(event.TenantID, 128) ||
		!plain(event.Action, 128) || !plain(event.Decision, 128) || event.OccurredAt.IsZero() ||
		event.AgentAppRevision < 0 || event.LatencyMS < 0 || event.CostMicros < 0 || event.InputTokens < 0 ||
		event.OutputTokens < 0 || event.ConfigVersion < 0 || event.PolicyVersion < 0 || len(event.ResourceRefs) > 32 {
		return runtime.ErrInvalidEnvelope
	}
	for _, value := range []struct {
		text string
		max  int
	}{{event.Channel, 64}, {event.UserID, 256}, {event.SessionID, 256}, {event.RequestID, 256}, {event.AgentAppID, 256},
		{event.AgentName, 256}, {event.ToolName, 256}, {event.ReasonCode, 128}, {event.ErrorType, 128}, {event.Currency, 3}} {
		if value.text != "" && !plain(value.text, value.max) {
			return runtime.ErrInvalidEnvelope
		}
	}
	if event.Currency != "" && (len(event.Currency) != 3 || event.Currency != strings.ToUpper(event.Currency)) {
		return runtime.ErrInvalidEnvelope
	}
	for _, ref := range event.ResourceRefs {
		if !plain(ref, 1024) {
			return runtime.ErrInvalidEnvelope
		}
	}
	if event.ContentDigest != "" && !hexValue(event.ContentDigest, 64) {
		return runtime.ErrInvalidEnvelope
	}
	if event.TraceID != "" && !hexValue(event.TraceID, 32) {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func Digest(event Event) (string, error) {
	event.OccurredAt = event.OccurredAt.UTC()
	if err := Validate(event); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func StableID(tenantID, outboxID string) (string, error) {
	if !plain(tenantID, 128) || !plain(outboxID, 512) {
		return "", runtime.ErrInvariantViolation
	}
	digest := sha256.Sum256([]byte("audit-outbox-v1\x00" + tenantID + "\x00" + outboxID))
	return "aud1_" + base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func plain(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func hexValue(value string, length int) bool {
	if len(value) != length || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
