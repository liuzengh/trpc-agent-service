// Package tenant models the top-level isolation root for config, data, tools,
// audit, and cost ownership.
package tenant

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
)

type Status string

const (
	StatusActive    Status = "active"
	StatusSuspended Status = "suspended"
	StatusDisabled  Status = "disabled"
)

type AuditPayloadMode string
type LogMaskingLevel string

const (
	AuditMetadataOnly       AuditPayloadMode = "metadata_only"
	AuditRedacted           AuditPayloadMode = "redacted"
	AuditEncryptedReference AuditPayloadMode = "encrypted_reference"
	MaskingNone             LogMaskingLevel  = "none"
	MaskingBasic            LogMaskingLevel  = "basic"
	MaskingStrict           LogMaskingLevel  = "strict"
)

// Tenant is intentionally narrow. Usage facts and provider configuration live
// in append-only ledgers and immutable snapshots, not in this root row.
type Tenant struct {
	TenantID                string
	TenantKey               string
	DisplayName             string
	Status                  Status
	RequestLimitPerMinute   *int64
	MaxConcurrentExecutions *int
	MonthlyTokenBudget      *int64
	MonthlyCostBudgetMicros *int64
	BillingCurrency         string
	AuditRetentionDays      int
	AuditPayloadMode        AuditPayloadMode
	LogMaskingLevel         LogMaskingLevel
	RedactionRules          []redaction.Rule
	TraceSamplingRate       float64
	DefaultAgentAppID       string
	DefaultBackendProfileID string
	ActiveConfigVersion     int64
	Version                 int64
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

// RedactionProgram compiles the versioned tenant policy before it can be
// handed to a sink. Callers must reject a compilation error rather than log
// under a weaker policy.
func (t Tenant) RedactionProgram() (*redaction.Program, error) {
	return redaction.Compile(redaction.Config{Level: redaction.Level(t.LogMaskingLevel), Rules: t.RedactionRules})
}

// ContextWithRedaction binds this immutable tenant version's compiled policy
// to a request context for downstream sinks.
func (t Tenant) ContextWithRedaction(ctx context.Context) (context.Context, error) {
	program, err := t.RedactionProgram()
	if err != nil {
		return nil, err
	}
	return redaction.ContextWithProgram(ctx, program), nil
}
