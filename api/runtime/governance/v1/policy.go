// Package governancev1 defines the stable, non-secret usage-policy snapshot
// exchanged by Control, Gateway, and Worker. It is a runtime contract, not a
// Control persistence model.
package governancev1

import (
	"errors"
	"regexp"
	"slices"
	"strings"
)

const SchemaVersion = 1

var (
	ErrInvalidPolicy = errors.New("invalid tenant usage policy")
	identifier       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type Policy struct {
	SchemaVersion int             `json:"schema_version"`
	TenantID      string          `json:"tenant_id"`
	Revision      int64           `json:"revision"`
	Enabled       bool            `json:"enabled"`
	IM            IMPolicy        `json:"im"`
	Requests      RequestPolicy   `json:"requests"`
	Execution     ExecutionPolicy `json:"execution"`
	Tokens        TokenPolicy     `json:"tokens"`
}

type IMPolicy struct {
	AllowAll bool     `json:"allow_all"`
	Rules    []IMRule `json:"rules"`
}

type IMRule struct {
	AccountID string   `json:"account_id"`
	BindingID string   `json:"binding_id,omitempty"`
	UserIDs   []string `json:"user_ids"`
	GroupIDs  []string `json:"group_ids"`
}

type RequestPolicy struct {
	TenantPerMinute int `json:"tenant_per_minute"`
	UserPerMinute   int `json:"user_per_minute"`
}

type ExecutionPolicy struct {
	MaxConcurrentRuns int `json:"max_concurrent_runs"`
}

type TokenPolicy struct {
	PeriodSeconds       int64 `json:"period_seconds"`
	Limit               int64 `json:"limit"`
	ReservationPerRun   int64 `json:"reservation_per_run"`
	InputMicrosPerMTok  int64 `json:"input_micros_per_million_tokens"`
	OutputMicrosPerMTok int64 `json:"output_micros_per_million_tokens"`
}

func Disabled(tenant string) Policy {
	return Policy{SchemaVersion: SchemaVersion, TenantID: tenant}
}

func (p Policy) Validate() error {
	if p.SchemaVersion != SchemaVersion || !identifier.MatchString(p.TenantID) || p.Revision < 0 || p.Revision > 9007199254740991 {
		return ErrInvalidPolicy
	}
	if !p.Enabled {
		if p.IM.AllowAll || len(p.IM.Rules) != 0 || p.Requests != (RequestPolicy{}) || p.Execution != (ExecutionPolicy{}) || p.Tokens != (TokenPolicy{}) {
			return ErrInvalidPolicy
		}
		return nil
	}
	if p.Requests.TenantPerMinute < 1 || p.Requests.TenantPerMinute > 1_000_000 || p.Requests.UserPerMinute < 1 || p.Requests.UserPerMinute > p.Requests.TenantPerMinute || p.Execution.MaxConcurrentRuns < 1 || p.Execution.MaxConcurrentRuns > 100_000 {
		return ErrInvalidPolicy
	}
	if p.Tokens.PeriodSeconds < 3600 || p.Tokens.PeriodSeconds > 31_536_000 || p.Tokens.Limit < 1 || p.Tokens.Limit > 9007199254740991 || p.Tokens.ReservationPerRun < 1 || p.Tokens.ReservationPerRun > p.Tokens.Limit || p.Tokens.InputMicrosPerMTok < 0 || p.Tokens.InputMicrosPerMTok > 1_000_000_000 || p.Tokens.OutputMicrosPerMTok < 0 || p.Tokens.OutputMicrosPerMTok > 1_000_000_000 {
		return ErrInvalidPolicy
	}
	if p.IM.AllowAll && len(p.IM.Rules) != 0 || !p.IM.AllowAll && len(p.IM.Rules) == 0 || len(p.IM.Rules) > 256 {
		return ErrInvalidPolicy
	}
	seen := map[string]bool{}
	for _, r := range p.IM.Rules {
		if !identifier.MatchString(r.AccountID) || r.BindingID != "" && !identifier.MatchString(r.BindingID) || len(r.UserIDs) > 2048 || len(r.GroupIDs) > 2048 || len(r.UserIDs)+len(r.GroupIDs) == 0 {
			return ErrInvalidPolicy
		}
		key := r.AccountID + "\x00" + r.BindingID
		if seen[key] || !validOpaqueSet(r.UserIDs) || !validOpaqueSet(r.GroupIDs) {
			return ErrInvalidPolicy
		}
		seen[key] = true
	}
	return nil
}

func validOpaqueSet(values []string) bool {
	if !slices.IsSorted(values) {
		return false
	}
	last := ""
	for _, value := range values {
		if value == last || value == "" || strings.TrimSpace(value) != value || len(value) > 256 || strings.ContainsAny(value, "\x00\r\n") {
			return false
		}
		last = value
	}
	return true
}

func (p Policy) Allows(account, binding, sender, conversation string) bool {
	if !p.Enabled || p.IM.AllowAll {
		return true
	}
	for _, r := range p.IM.Rules {
		if r.AccountID == account && (r.BindingID == "" || r.BindingID == binding) && (slices.Contains(r.UserIDs, sender) || slices.Contains(r.GroupIDs, conversation)) {
			return true
		}
	}
	return false
}

// UsageSummary is a Worker-owned accounting projection. Unknown attempts retain
// their reservation and are never reported as zero use.
type UsageSummary struct {
	TenantID            string `json:"tenant_id"`
	PolicyRevision      int64  `json:"policy_revision"`
	PeriodStart         string `json:"period_start,omitempty"`
	PeriodSeconds       int64  `json:"period_seconds"`
	TokenLimit          int64  `json:"token_limit"`
	UsedTokens          int64  `json:"used_tokens"`
	ReservedTokens      int64  `json:"reserved_tokens"`
	UnknownUsageCount   int64  `json:"unknown_usage_count"`
	PendingUsageCount   int64  `json:"pending_usage_count"`
	EstimatedCostMicros int64  `json:"estimated_cost_micros"`
}
