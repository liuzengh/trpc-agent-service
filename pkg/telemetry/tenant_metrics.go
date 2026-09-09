//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package telemetry

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

const (
	// MetricsTenantAllowlistEnv contains a comma-separated list of tenant IDs
	// that may retain an exact Prometheus label. All other tenants aggregate to
	// MetricsTenantOtherLabel, keeping process-local metric cardinality bounded.
	MetricsTenantAllowlistEnv = "METRICS_TENANT_ALLOWLIST"
	// MetricsAgentAllowlistEnv contains comma-separated tenant/agent pairs that
	// may retain an exact agent_name label. An exact tenant label alone never
	// authorizes a dynamic secondary label.
	MetricsAgentAllowlistEnv = "METRICS_AGENT_ALLOWLIST"
	// MetricsModelAllowlistEnv contains comma-separated tenant/model pairs that
	// may retain an exact model label.
	MetricsModelAllowlistEnv = "METRICS_MODEL_ALLOWLIST"

	// MaxMetricsTenantAllowlist bounds the number of tenant-specific metric
	// series an operator can enable in one process.
	MaxMetricsTenantAllowlist = 100
	// MaxMetricsScopedAllowlist bounds exact tenant/dimension pairs. Pair-based
	// limits avoid the tenant x dimension cross-product of independent lists.
	MaxMetricsScopedAllowlist = 200

	// MetricsTenantOtherLabel aggregates valid tenant IDs outside the explicit
	// allowlist. It is intentionally not a tenant identifier.
	MetricsTenantOtherLabel = "__other__"
	// MetricsTenantUnknownLabel represents a missing tenant ID at a metrics
	// call site. It remains distinct from ordinary non-allowlisted tenants.
	MetricsTenantUnknownLabel = "__unknown__"
)

// TenantMetricLabeler maps tenant IDs to a bounded Prometheus label space.
// It is immutable after construction so concurrent metric recording requires
// no locks and configuration changes cannot mutate a live map.
type TenantMetricLabeler struct {
	allowed       map[string]struct{}
	allowedAgents map[string]struct{}
	allowedModels map[string]struct{}
}

var processTenantMetricLabeler atomic.Pointer[TenantMetricLabeler]

// NewTenantMetricLabeler validates an operator-owned exact-label allowlist.
// An empty list is valid and aggregates every non-empty tenant into other.
func NewTenantMetricLabeler(tenantIDs []string) (*TenantMetricLabeler, error) {
	return NewMetricLabeler(tenantIDs, nil, nil)
}

// NewMetricLabeler validates the tenant allowlist and exact tenant/name pairs.
// Scoped entries use the form tenant-id/name. The slash is reserved as the
// separator and is not accepted inside either side.
func NewMetricLabeler(tenantIDs, agentScopes, modelScopes []string) (*TenantMetricLabeler, error) {
	allowed := make(map[string]struct{}, len(tenantIDs))
	for _, raw := range tenantIDs {
		tenantID := strings.TrimSpace(raw)
		if tenantID == "" {
			return nil, fmt.Errorf("metrics tenant allowlist contains an empty tenant id")
		}
		if !validMetricTenantID(tenantID) {
			return nil, fmt.Errorf("metrics tenant allowlist contains an invalid tenant id")
		}
		if _, exists := allowed[tenantID]; exists {
			return nil, fmt.Errorf("metrics tenant allowlist contains duplicate tenant id %q", tenantID)
		}
		if len(allowed) >= MaxMetricsTenantAllowlist {
			return nil, fmt.Errorf("metrics tenant allowlist exceeds %d entries", MaxMetricsTenantAllowlist)
		}
		allowed[tenantID] = struct{}{}
	}
	agents, err := newScopedMetricAllowlist(agentScopes, allowed, "agent")
	if err != nil {
		return nil, err
	}
	models, err := newScopedMetricAllowlist(modelScopes, allowed, "model")
	if err != nil {
		return nil, err
	}
	return &TenantMetricLabeler{
		allowed:       allowed,
		allowedAgents: agents,
		allowedModels: models,
	}, nil
}

// ParseTenantMetricAllowlist parses the composition-root environment format.
// Whitespace around commas is ignored; an unset or blank value is the safe
// default that aggregates all tenant-specific metric labels.
func ParseTenantMetricAllowlist(value string) (*TenantMetricLabeler, error) {
	if strings.TrimSpace(value) == "" {
		return NewTenantMetricLabeler(nil)
	}
	return NewTenantMetricLabeler(strings.Split(value, ","))
}

// ParseMetricLabelAllowlists parses all process-wide cardinality controls.
func ParseMetricLabelAllowlists(tenants, agents, models string) (*TenantMetricLabeler, error) {
	return NewMetricLabeler(splitMetricAllowlist(tenants), splitMetricAllowlist(agents), splitMetricAllowlist(models))
}

// ConfigureTenantMetricLabels installs the process-wide immutable mapper.
// Call it once during process startup, before serving traffic. Invalid operator
// configuration returns an error so a typo cannot silently reintroduce
// unbounded tenant labels.
func ConfigureTenantMetricLabels(value string) error {
	return ConfigureMetricLabels(value, "", "")
}

// ConfigureMetricLabels installs one immutable policy for every dynamic label
// dimension. Keeping the policy in one atomic snapshot prevents a request from
// observing mismatched tenant and secondary-label configuration.
func ConfigureMetricLabels(tenants, agents, models string) error {
	labeler, err := ParseMetricLabelAllowlists(tenants, agents, models)
	if err != nil {
		return err
	}
	processTenantMetricLabeler.Store(labeler)
	return nil
}

// ConfigureMetricLabelsFromEnv configures every dynamic Prometheus label from
// the composition-root environment.
func ConfigureMetricLabelsFromEnv() error {
	return ConfigureMetricLabels(
		os.Getenv(MetricsTenantAllowlistEnv),
		os.Getenv(MetricsAgentAllowlistEnv),
		os.Getenv(MetricsModelAllowlistEnv),
	)
}

// ConfigureTenantMetricLabelsFromEnv is retained for source compatibility.
// New composition roots should use ConfigureMetricLabelsFromEnv.
func ConfigureTenantMetricLabelsFromEnv() error {
	return ConfigureMetricLabelsFromEnv()
}

// MetricTenantLabel returns the bounded label used by every tenant-scoped
// Prometheus metric. Until the composition root configures an allowlist, the
// fail-safe empty policy aggregates non-empty tenants into other.
func MetricTenantLabel(tenantID string) string {
	return currentMetricLabeler().Label(tenantID)
}

// MetricAgentLabel returns an exact agent label only for an explicitly
// allowlisted tenant/agent pair.
func MetricAgentLabel(tenantID, agentName string) string {
	labeler := currentMetricLabeler()
	return labeler.scopedLabel(tenantID, agentName, labeler.allowedAgents)
}

// MetricModelLabel returns an exact model label only for an explicitly
// allowlisted tenant/model pair.
func MetricModelLabel(tenantID, modelName string) string {
	labeler := currentMetricLabeler()
	return labeler.scopedLabel(tenantID, modelName, labeler.allowedModels)
}

// Label returns an exact allowlisted tenant ID or one of the bounded aggregate
// labels. Runtime tenant IDs are not normalized: malformed values do not gain
// an exact label even if whitespace would make them resemble an allowlist item.
func (l *TenantMetricLabeler) Label(tenantID string) string {
	if tenantID == "" {
		return MetricsTenantUnknownLabel
	}
	if l != nil {
		if _, allowed := l.allowed[tenantID]; allowed {
			return tenantID
		}
	}
	return MetricsTenantOtherLabel
}

func (l *TenantMetricLabeler) scopedLabel(tenantID, value string, allowed map[string]struct{}) string {
	tenantLabel := l.Label(tenantID)
	if tenantLabel != tenantID {
		return MetricsTenantOtherLabel
	}
	if value == "" {
		return MetricsTenantUnknownLabel
	}
	if _, ok := allowed[metricScopeKey(tenantID, value)]; ok {
		return value
	}
	return MetricsTenantOtherLabel
}

func currentMetricLabeler() *TenantMetricLabeler {
	if labeler := processTenantMetricLabeler.Load(); labeler != nil {
		return labeler
	}
	return &TenantMetricLabeler{}
}

func splitMetricAllowlist(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func newScopedMetricAllowlist(scopes []string, tenants map[string]struct{}, kind string) (map[string]struct{}, error) {
	allowed := make(map[string]struct{}, len(scopes))
	for _, raw := range scopes {
		scope := strings.TrimSpace(raw)
		parts := strings.Split(scope, "/")
		if len(parts) != 2 || !validMetricTenantID(parts[0]) || !validMetricDimensionValue(parts[1]) {
			return nil, fmt.Errorf("metrics %s allowlist contains an invalid tenant/%s pair", kind, kind)
		}
		if _, ok := tenants[parts[0]]; !ok {
			return nil, fmt.Errorf("metrics %s allowlist references a tenant outside the tenant allowlist", kind)
		}
		key := metricScopeKey(parts[0], parts[1])
		if _, exists := allowed[key]; exists {
			return nil, fmt.Errorf("metrics %s allowlist contains a duplicate pair", kind)
		}
		if len(allowed) >= MaxMetricsScopedAllowlist {
			return nil, fmt.Errorf("metrics %s allowlist exceeds %d entries", kind, MaxMetricsScopedAllowlist)
		}
		allowed[key] = struct{}{}
	}
	return allowed, nil
}

func metricScopeKey(tenantID, value string) string {
	return tenantID + "\x00" + value
}

func validMetricDimensionValue(value string) bool {
	return value != "" && !strings.Contains(value, "/") && validMetricTenantID(value)
}

func validMetricTenantID(value string) bool {
	if len(value) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.In(character, unicode.Cf) {
			return false
		}
	}
	return true
}
