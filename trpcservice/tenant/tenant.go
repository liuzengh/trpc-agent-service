// Package tenant models multi-tenant isolation for config, data, tools, and keys.
//
// The package owns the tenant / agent app / channel binding models, their
// loading from PG, and the cached Resolver the Gateway uses to route an
// inbound callback to its tenant and app:
//
//	webhook_path → channel_binding → tenant + agent_app
//
// The same Resolver serves the Worker's per-app Runner assembly
// (Resolver.AppByID: app config + tenant policies by app ID). The stamped
// tenant_id then travels on the message through the Worker into audit and
// metrics, giving every downstream component its isolation key.
package tenant

import (
	"encoding/json"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// Status values stored in the status columns.
const (
	StatusActive   = "active"
	StatusDisabled = "disabled"
)

// Tenant is one row of the tenant table. Dynamic config (model_config,
// tool_policy, audit_policy, rate_policy, storage_config) is carried raw and
// parsed by its consumers (agent assembly, guardrail, gateway rate limiting,
// storage routing) — routing itself only needs identity and status.
type Tenant struct {
	ID              string
	Name            string
	ModelConfig     json.RawMessage
	ToolPolicy      json.RawMessage
	AuditPolicy     json.RawMessage
	GuardrailPolicy json.RawMessage
	RatePolicy      json.RawMessage
	StorageConfig   json.RawMessage
	Status          string
}

// RateLimit is the tenant's gateway admission quota: qps is the token refill
// rate, burst the bucket capacity. Zero values inherit the platform default.
// SendQPS/SendBurst override the outbound pacing for this tenant's sends.
type RateLimit struct {
	QPS       float64 `json:"qps"`
	Burst     int     `json:"burst"`
	SendQPS   float64 `json:"send_qps"`
	SendBurst int     `json:"send_burst"`
}

// GuardrailPolicy is the tenant's governance config: input user allowlist and
// deny words, output deny words, and the daily token budget. Empty fields
// inherit the platform defaults.
type GuardrailPolicy struct {
	// InputAllowUsers, when non-empty, restricts the agent to these IM user
	// IDs.
	InputAllowUsers []string `json:"input_allow_users"`
	// InputDenyWords replaces the platform input denylist when non-empty.
	InputDenyWords []string `json:"input_deny_words"`
	// OutputDenyWords blocks replies containing any of them.
	OutputDenyWords []string `json:"output_deny_words"`
	// MaxTokensPerDay is the daily token budget (prompt + completion); 0 is
	// unlimited.
	MaxTokensPerDay int64 `json:"max_tokens_per_day"`
}

// ParseGuardrailPolicy decodes tenant.guardrail_policy; empty or invalid
// yields the zero value (platform defaults).
func ParseGuardrailPolicy(raw json.RawMessage) GuardrailPolicy {
	var p GuardrailPolicy
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			plog.Warnf("invalid tenant guardrail_policy ignored: %v", err)
		}
	}
	return p
}

// ParseRateLimit decodes tenant.rate_policy; an empty or invalid policy
// yields the zero value (all limits at platform defaults).
func ParseRateLimit(raw json.RawMessage) RateLimit {
	var rl RateLimit
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &rl); err != nil {
			plog.Warnf("invalid tenant rate_policy ignored: %v", err)
		}
	}
	return rl
}

// Migration is one active storage_migration row: a tenant's backend switch
// in flight. Terminal phases (done/failed/aborted) are not loaded into the
// resolver snapshot.
type Migration struct {
	ID          string
	TenantID    string
	Resource    string // "session" (knowledge / artifact arrive later)
	FromBackend string // redis / postgres
	ToBackend   string
	Phase       string // dual_write / backfilling / observing
}

// Migration phases.
const (
	// PhaseDualWrite: writes fan out to both backends, reads stay on From.
	PhaseDualWrite = "dual_write"
	// PhaseBackfilling: historical data is being copied; dual write on.
	PhaseBackfilling = "backfilling"
	// PhaseObserving: reads switched to To after the consistency check; dual
	// write stays on for the observation window, then the migration is done.
	PhaseObserving = "observing"
	// Terminal phases.
	PhaseDone    = "done"
	PhaseFailed  = "failed"
	PhaseAborted = "aborted"
)

// AgentApp is one row of agent_app: a versioned agent configuration owned by
// a tenant (status draft / published / disabled).
type AgentApp struct {
	ID        string
	TenantID  string
	Name      string
	AgentType string
	Config    json.RawMessage
	Version   int
	Status    string
}

// ChannelBinding is one row of channel_binding: binds an IM channel webhook
// to an agent app. Secret material stays as references (token_ref /
// aeskey_ref) and is resolved through config.SecretResolver at use time —
// never logged.
type ChannelBinding struct {
	ID          string
	TenantID    string
	Channel     string
	AppID       string
	WebhookPath string
	TokenRef    string
	AESKeyRef   string
	Config      json.RawMessage
	Status      string
}
