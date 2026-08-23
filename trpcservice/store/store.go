// Package store provides the durable inbox/outbox and audit system of record.
package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
)

type DispatchTask struct {
	ID       string                   `json:"id"`
	Attempts int                      `json:"attempts,omitempty"`
	Message  channels.InboundEnvelope `json:"message"`
}

type ReplyTask struct {
	ID      string                    `json:"id"`
	Message channels.OutboundEnvelope `json:"message"`
}

type AuditLog struct {
	TenantID  string        `json:"tenant_id"`
	Channel   string        `json:"channel"`
	UserID    string        `json:"user_id"`
	SessionID string        `json:"session_id"`
	AgentName string        `json:"agent_name"`
	ToolName  string        `json:"tool_name,omitempty"`
	Decision  string        `json:"decision"`
	Latency   time.Duration `json:"latency"`
	ErrorType string        `json:"error_type,omitempty"`
	Cost      float64       `json:"cost"`
	TraceID   string        `json:"trace_id"`
	CreatedAt time.Time     `json:"created_at"`
}

type Stats struct {
	InboundTotal  int64 `json:"inbound_total"`
	DispatchReady int64 `json:"dispatch_ready"`
	ReplyReady    int64 `json:"reply_ready"`
	AuditTotal    int64 `json:"audit_total"`
}

type Repository interface {
	Migrate(context.Context) error
	SeedTenants(context.Context, []tenant.Tenant) error
	ListTenants(context.Context) ([]tenant.Tenant, error)
	CreateAgent(context.Context, string, string, string) error
	CreateAgentVersion(context.Context, string, string, string, json.RawMessage) error
	PublishAgent(context.Context, string, string, string) error
	SaveChannelBinding(context.Context, string, tenant.ChannelBinding) error
	SaveBackendProfile(context.Context, string, string, tenant.BackendProfile) error
	AcceptInbound(context.Context, channels.InboundEnvelope) (bool, error)
	ClaimDispatch(context.Context, string, int, time.Duration) ([]DispatchTask, error)
	CompleteDispatch(context.Context, string) error
	RetryDispatch(context.Context, string, error) error
	StoreReply(context.Context, channels.OutboundEnvelope) error
	CommitAgentResult(context.Context, channels.InboundEnvelope, channels.OutboundEnvelope) error
	ReplyExists(context.Context, string) (bool, error)
	ClaimReplies(context.Context, string, int, time.Duration) ([]ReplyTask, error)
	CompleteReply(context.Context, string) error
	RetryReply(context.Context, string, error) error
	AppendAudit(context.Context, AuditLog) error
	Stats(context.Context) (Stats, error)
	Close()
}
