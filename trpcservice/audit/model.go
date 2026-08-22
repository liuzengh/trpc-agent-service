package audit

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type AuditLog struct {
	TenantID     string            `json:"tenant_id"`
	AuditID      string            `json:"audit_id"`
	TraceID      string            `json:"trace_id"`
	RequestID    string            `json:"request_id"`
	ExecutionID  string            `json:"execution_id"`
	Channel      string            `json:"channel"`
	ExternalUser string            `json:"external_user"`
	SessionID    string            `json:"session_id"`
	AgentAppID   string            `json:"agent_app_id"`
	ToolName     string            `json:"tool_name,omitempty"`
	Decision     string            `json:"decision"`
	Latency      time.Duration     `json:"latency"`
	CostCents    int64             `json:"cost_cents"`
	ErrorType    string            `json:"error_type,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
}

var ErrInvalidAudit = errors.New("invalid audit log")

func (a AuditLog) Validate() error {
	for name, value := range map[string]string{"tenant_id": a.TenantID, "audit_id": a.AuditID, "trace_id": a.TraceID, "request_id": a.RequestID, "execution_id": a.ExecutionID, "channel": a.Channel, "decision": a.Decision} {
		if strings.TrimSpace(value) == "" || len(value) > 256 {
			return fmt.Errorf("%w: %s is required and bounded", ErrInvalidAudit, name)
		}
	}
	if a.Latency < 0 || a.CostCents < 0 {
		return fmt.Errorf("%w: latency and cost must be non-negative", ErrInvalidAudit)
	}
	for key, value := range a.Metadata {
		if value == "[REDACTED]" {
			continue
		}
		if sensitiveKey(key) || sensitiveKey(value) {
			return fmt.Errorf("%w: sensitive metadata field %q", ErrInvalidAudit, key)
		}
	}
	return nil
}
