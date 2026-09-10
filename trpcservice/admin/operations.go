package admin

import (
	"context"
	"time"
)

// BackendReadiness is a safe operational summary. It contains no endpoint
// credentials or provider response bodies.
type BackendReadiness struct {
	Name     string `json:"name"`
	Provider string `json:"provider"`
	Status   string `json:"status"`
}

// RecentError is a bounded metadata-only operational signal. It excludes
// prompts, provider bodies, tool arguments, and credentials.
type RecentError struct {
	TenantID   string    `json:"tenant_id"`
	AppID      string    `json:"app_id"`
	EventType  string    `json:"event_type"`
	ErrorType  string    `json:"error_type"`
	TraceID    string    `json:"trace_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

// OperationsSummary is the small control-plane view used by Overview and
// Operations. It is not a replacement for Prometheus metrics.
type OperationsSummary struct {
	GatewayReadiness  string             `json:"gateway_readiness"`
	WorkerReadiness   string             `json:"worker_readiness"`
	WorkerCount       int                `json:"worker_count"`
	WorkerCapacity    int                `json:"worker_capacity"`
	WorkerUtilization float64            `json:"worker_utilization"`
	ActiveExecutions  int                `json:"active_executions"`
	QueueBacklog      int                `json:"queue_backlog"`
	RetryBacklog      int                `json:"retry_backlog"`
	ReplyBacklog      int                `json:"reply_backlog"`
	PendingApprovals  int                `json:"pending_approvals"`
	ActiveMigrations  int                `json:"active_migrations"`
	StuckMigrations   int                `json:"stuck_migrations"`
	MigrationProgress float64            `json:"migration_progress"`
	AuditBacklog      int                `json:"audit_backlog"`
	ChannelReadiness  string             `json:"channel_readiness"`
	RecentErrors      []RecentError      `json:"recent_errors,omitempty"`
	Backends          []BackendReadiness `json:"backends"`
	JaegerURL         string             `json:"jaeger_url,omitempty"`
	PrometheusURL     string             `json:"prometheus_url,omitempty"`
	GrafanaURL        string             `json:"grafana_url,omitempty"`
	GeneratedAt       time.Time          `json:"generated_at"`
}

type OperationsReader interface {
	OperationsSummary(context.Context) (OperationsSummary, error)
}

type ScopedOperationsReader interface {
	OperationsSummaryForPrincipal(context.Context, AdminPrincipal) (OperationsSummary, error)
}

type OperationsFunc func(context.Context) (OperationsSummary, error)

func (f OperationsFunc) OperationsSummary(ctx context.Context) (OperationsSummary, error) {
	return f(ctx)
}
