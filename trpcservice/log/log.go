// Package log configures audit logging and secret redaction.
package log

import (
	"context"
	"time"
)

// AuditRecord is the stable cross-component audit schema.
type AuditRecord struct {
	TenantID  string
	Channel   string
	UserID    string
	SessionID string
	AgentName string
	ToolName  string
	Decision  string
	Latency   time.Duration
	ErrorType string
	Cost      float64
	TraceID   string
	RequestID string
	TS        time.Time
}

// AuditWriter must not block the request path; concrete batching arrives in T13.
type AuditWriter interface {
	Write(context.Context, AuditRecord)
}

// NopAuditWriter discards audit records.
type NopAuditWriter struct{}

// Write implements AuditWriter.
func (NopAuditWriter) Write(context.Context, AuditRecord) {}
