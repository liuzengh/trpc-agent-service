// Package inmemory provides the single-process Tenant Repository contract.
package inmemory

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Repository struct {
	mu      sync.RWMutex
	tenants map[string]tenant.Tenant
	keys    map[string]string
	changes []tenant.ChangeFact
	outbox  []tenant.OutboxFact
}

func New() *Repository {
	return &Repository{tenants: make(map[string]tenant.Tenant), keys: make(map[string]string)}
}

func (r *Repository) Create(ctx context.Context, in tenant.CreateInput) (tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return tenant.Tenant{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return tenant.Tenant{}, err
	}
	value := in.Tenant
	value.Status = tenant.StatusActive
	value.Version = 1
	if value.BillingCurrency == "" {
		value.BillingCurrency = "USD"
	}
	if value.AuditRetentionDays == 0 {
		value.AuditRetentionDays = 180
	}
	if value.AuditPayloadMode == "" {
		value.AuditPayloadMode = tenant.AuditRedacted
	}
	if value.LogMaskingLevel == "" {
		value.LogMaskingLevel = tenant.MaskingBasic
	}
	if err := value.Validate(); err != nil {
		return tenant.Tenant{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tenants[value.TenantID]; ok {
		return tenant.Tenant{}, tenant.ErrVersionConflict
	}
	if _, ok := r.keys[value.TenantKey]; ok {
		return tenant.Tenant{}, tenant.ErrKeyConflict
	}
	now := time.Now().UTC()
	value.CreatedAt, value.UpdatedAt = now, now
	r.tenants[value.TenantID] = clone(value)
	r.keys[value.TenantKey] = value.TenantID
	r.record(value.TenantID, "tenant.created", 0, 1, in.ChangeMetadata)
	return clone(value), nil
}

func (r *Repository) Get(ctx context.Context, id string) (tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return tenant.Tenant{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.tenants[id]
	if !ok {
		return tenant.Tenant{}, tenant.ErrNotFound
	}
	return clone(value), nil
}

func (r *Repository) UpdateConfiguration(ctx context.Context, in tenant.UpdateConfigurationInput) (tenant.ChangeResult, error) {
	if err := ctx.Err(); err != nil {
		return tenant.ChangeResult{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return tenant.ChangeResult{}, err
	}
	if err := in.Tenant.Validate(); err != nil {
		return tenant.ChangeResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old, ok := r.tenants[in.Tenant.TenantID]
	if !ok {
		return tenant.ChangeResult{}, tenant.ErrNotFound
	}
	if old.Version != in.ExpectedVersion {
		return tenant.ChangeResult{}, tenant.ErrVersionConflict
	}
	if old.Status == tenant.StatusDisabled {
		return tenant.ChangeResult{}, tenant.ErrStatusConflict
	}
	next := clone(in.Tenant)
	next.TenantID, next.TenantKey, next.Status = old.TenantID, old.TenantKey, old.Status
	next.Version, next.CreatedAt, next.UpdatedAt = old.Version+1, old.CreatedAt, time.Now().UTC()
	r.tenants[next.TenantID] = clone(next)
	r.record(next.TenantID, "tenant.configuration.updated", old.Version, next.Version, in.ChangeMetadata)
	return tenant.ChangeResult{Tenant: clone(next)}, nil
}

func (r *Repository) TransitionStatus(ctx context.Context, in tenant.TransitionStatusInput) (tenant.ChangeResult, error) {
	if err := ctx.Err(); err != nil {
		return tenant.ChangeResult{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return tenant.ChangeResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old, ok := r.tenants[in.TenantID]
	if !ok {
		return tenant.ChangeResult{}, tenant.ErrNotFound
	}
	if old.Version != in.ExpectedVersion {
		return tenant.ChangeResult{}, tenant.ErrVersionConflict
	}
	if !legal(old.Status, in.NextStatus) {
		return tenant.ChangeResult{}, tenant.ErrStatusConflict
	}
	next := clone(old)
	next.Status, next.Version, next.UpdatedAt = in.NextStatus, old.Version+1, time.Now().UTC()
	r.tenants[in.TenantID] = next
	r.changes = append(r.changes, tenant.ChangeFact{TenantID: in.TenantID, Kind: "tenant.status.changed", PreviousStatus: string(old.Status), NextStatus: string(next.Status), PreviousVersion: old.Version, NextVersion: next.Version, Metadata: in.ChangeMetadata})
	r.outbox = append(r.outbox,
		tenant.OutboxFact{TenantID: in.TenantID, Kind: "audit", IdempotencyKey: factKey(in.TenantID, "status", next.Version), Version: next.Version},
		tenant.OutboxFact{TenantID: in.TenantID, Kind: "tenant-control", IdempotencyKey: factKey(in.TenantID, "status-control", next.Version), Version: next.Version})
	return tenant.ChangeResult{Tenant: clone(next)}, nil
}

func (r *Repository) Facts(tenantID string) ([]tenant.ChangeFact, []tenant.OutboxFact) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var changes []tenant.ChangeFact
	var outbox []tenant.OutboxFact
	for _, fact := range r.changes {
		if fact.TenantID == tenantID {
			changes = append(changes, fact)
		}
	}
	for _, fact := range r.outbox {
		if fact.TenantID == tenantID {
			outbox = append(outbox, fact)
		}
	}
	return changes, outbox
}

func (r *Repository) record(id, kind string, previous, next int64, metadata tenant.ChangeMetadata) {
	r.changes = append(r.changes, tenant.ChangeFact{TenantID: id, Kind: kind, PreviousVersion: previous, NextVersion: next, Metadata: metadata})
	r.outbox = append(r.outbox,
		tenant.OutboxFact{TenantID: id, Kind: "audit", IdempotencyKey: factKey(id, kind, next), Version: next},
		tenant.OutboxFact{TenantID: id, Kind: "tenant-control", IdempotencyKey: factKey(id, kind+"-control", next), Version: next})
}

func factKey(id, kind string, version int64) string {
	return id + ":" + kind + ":" + strconv.FormatInt(version, 10)
}
func legal(from, to tenant.Status) bool {
	return from == tenant.StatusActive && (to == tenant.StatusSuspended || to == tenant.StatusDisabled) ||
		from == tenant.StatusSuspended && (to == tenant.StatusActive || to == tenant.StatusDisabled)
}
func clone(in tenant.Tenant) tenant.Tenant {
	out := in
	if in.RequestLimitPerMinute != nil {
		v := *in.RequestLimitPerMinute
		out.RequestLimitPerMinute = &v
	}
	if in.MaxConcurrentExecutions != nil {
		v := *in.MaxConcurrentExecutions
		out.MaxConcurrentExecutions = &v
	}
	if in.MonthlyTokenBudget != nil {
		v := *in.MonthlyTokenBudget
		out.MonthlyTokenBudget = &v
	}
	if in.MonthlyCostBudgetMicros != nil {
		v := *in.MonthlyCostBudgetMicros
		out.MonthlyCostBudgetMicros = &v
	}
	if in.RedactionRules != nil {
		out.RedactionRules = make([]redaction.Rule, len(in.RedactionRules))
		for index, rule := range in.RedactionRules {
			out.RedactionRules[index] = rule
			out.RedactionRules[index].KeyFragments = append([]string(nil), rule.KeyFragments...)
		}
	}
	return out
}
