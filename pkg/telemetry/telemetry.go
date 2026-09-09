//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package telemetry

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"
)

var (
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agent_requests_total",
			Help: "Total number of agent requests",
		},
		[]string{"tenant_id", "channel", "status", "error_type"},
	)

	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agent_request_duration_seconds",
			Help:    "Agent request latency",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
		[]string{"tenant_id", "agent_name"},
	)

	tokenConsumption = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agent_tokens_total",
			Help: "Total tokens consumed",
		},
		[]string{"tenant_id", "model", "type"},
	)

	costUSD = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agent_cost_usd_total",
			Help: "Total cost in USD",
		},
		[]string{"tenant_id"},
	)
)

// Collector collects telemetry data.
type Collector struct {
	// identityKey is shared by replicas so the same user has a stable,
	// tenant-scoped pseudonym without exposing the provider identifier.
	identityKey []byte

	// auditMu serialises writes so concurrent workers cannot interleave
	// partial JSON lines into the same sink.
	auditMu   sync.Mutex
	auditSink io.Writer
}

// NewCollector creates a new telemetry collector writing audit entries to
// stderr. Use NewCollectorWithAuditSink to direct them elsewhere.
func NewCollector() *Collector {
	return NewCollectorWithAuditSink(os.Stderr)
}

// NewCollectorWithAuditSink creates a collector that writes audit entries to
// sink. A nil sink discards them.
func NewCollectorWithAuditSink(sink io.Writer) *Collector {
	return NewCollectorWithAuditSinkAndIdentityKey(sink, nil)
}

// NewCollectorWithAuditSinkAndIdentityKey creates a collector with an optional
// shared HMAC key for tenant-scoped user pseudonyms. A missing key deliberately
// produces a fixed redaction marker rather than leaking the raw external ID;
// production deployments should configure the same key on every replica.
func NewCollectorWithAuditSinkAndIdentityKey(sink io.Writer, identityKey []byte) *Collector {
	if sink == nil {
		sink = io.Discard
	}
	return &Collector{
		auditSink:   sink,
		identityKey: append([]byte(nil), identityKey...),
	}
}

// StartTimer starts a timer for duration measurement.
func (c *Collector) StartTimer() time.Time {
	return time.Now()
}

// RecordRequestDuration records request duration.
func (c *Collector) RecordRequestDuration(tenantID, agentName string, startTime time.Time) {
	duration := time.Since(startTime).Seconds()
	requestDuration.WithLabelValues(MetricTenantLabel(tenantID), MetricAgentLabel(tenantID, agentName)).Observe(duration)
}

// RecordTokens records token consumption.
func (c *Collector) RecordTokens(tenantID, model string, promptTokens, completionTokens int) {
	label := MetricTenantLabel(tenantID)
	modelLabel := MetricModelLabel(tenantID, model)
	tokenConsumption.WithLabelValues(label, modelLabel, "prompt").Add(float64(promptTokens))
	tokenConsumption.WithLabelValues(label, modelLabel, "completion").Add(float64(completionTokens))
}

// RecordAccountedTokens records the Worker ledger amount. The amount can be
// provider-reported total usage or a conservative reservation when usage is
// unavailable. It must not be mislabeled as prompt-only or completion-only.
func (c *Collector) RecordAccountedTokens(tenantID, model string, tokens int) {
	if tokens <= 0 {
		return
	}
	tokenConsumption.WithLabelValues(MetricTenantLabel(tenantID), MetricModelLabel(tenantID, model), "accounted").Add(float64(tokens))
}

// RecordCost records cost in USD.
func (c *Collector) RecordCost(tenantID string, cost float64) {
	costUSD.WithLabelValues(MetricTenantLabel(tenantID)).Add(cost)
}

// RecordSuccess records a successful request.
func (c *Collector) RecordSuccess(tenantID, channel string) {
	requestsTotal.WithLabelValues(MetricTenantLabel(tenantID), channel, "success", "").Inc()
}

// RecordError records an error.
func (c *Collector) RecordError(tenantID, channel, errorType string) {
	requestsTotal.WithLabelValues(MetricTenantLabel(tenantID), channel, "error", errorType).Inc()
}

// AuditLog represents an audit log entry. Every field is non-sensitive by
// construction: identifiers and decisions only. Credentials, model API keys and
// raw message content are deliberately absent so that an audit record can never
// become a secret-leakage vector.
type AuditLog struct {
	TenantID    string    `json:"tenant_id"`
	ChannelType string    `json:"channel"`
	UserID      string    `json:"user_id"`
	SessionID   string    `json:"session_id"`
	AgentName   string    `json:"agent_name"`
	ToolName    string    `json:"tool_name,omitempty"`
	Decision    string    `json:"decision"`
	LatencyMS   int       `json:"latency_ms"`
	ErrorType   string    `json:"error_type,omitempty"`
	TokenCount  int       `json:"token_count"`
	CostUSD     float64   `json:"cost_usd"`
	TraceID     string    `json:"trace_id"`
	Timestamp   time.Time `json:"timestamp"`
}

// LogAudit writes an audit entry as one JSON object per line, stamping it with
// the trace ID from ctx so records correlate with the surrounding span. It
// returns an error when the authoritative sink write fails. Callers must not
// confirm a successful operation whose required audit record was not persisted.
func (c *Collector) LogAudit(ctx context.Context, entry *AuditLog) error {
	if entry == nil {
		return fmt.Errorf("telemetry: nil audit entry")
	}
	// Do not mutate a caller-owned record while applying trace and privacy
	// metadata. This keeps retries and alternate sinks deterministic.
	copy := *entry
	entry = &copy
	entry.UserID = pseudonymizeUserID(c.identityKey, entry.TenantID, entry.UserID)

	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.HasTraceID() {
		entry.TraceID = sc.TraceID().String()
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	line, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("telemetry: marshal audit entry: %w", err)
	}

	c.auditMu.Lock()
	defer c.auditMu.Unlock()
	if _, err := c.auditSink.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("telemetry: write audit entry: %w", err)
	}
	return nil
}

const redactedIdentity = "***REDACTED***"

func pseudonymizeUserID(key []byte, tenantID, userID string) string {
	if userID == "" {
		return ""
	}
	if len(key) == 0 {
		return redactedIdentity
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(tenantID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(userID))
	digest := hex.EncodeToString(mac.Sum(nil))
	return "hmac:" + digest[:32]
}
