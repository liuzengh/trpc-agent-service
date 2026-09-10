// Package inmemory provides the single-process tenant repository.
package inmemory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// List returns a stable page of only the requested tenant scopes, ordered by
// tenant ID. Scope filtering happens before pagination.
//
//nolint:gocyclo // Collection listing coordinates scope, filter, and paging boundaries.
func (r *InMemoryRepository) List(ctx context.Context, scopes []string, query, status, cursor string, limit int) ([]*tenant.Tenant, string, error) {
	if err := checkContext(ctx); err != nil {
		return nil, "", err
	}
	visible := make(map[string]struct{}, len(scopes))
	all := false
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "*" {
			all = true
		} else if scope != "" {
			visible[scope] = struct{}{}
		}
	}
	if !all && len(visible) == 0 {
		return []*tenant.Tenant{}, "", nil
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeListCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if err := r.rLock(ctx); err != nil {
		return nil, "", err
	}
	defer r.rUnlock()
	items := make([]*tenant.Tenant, 0, len(r.byID))
	query = strings.ToLower(strings.TrimSpace(query))
	status = strings.TrimSpace(status)
	for _, value := range r.byID {
		if !all {
			if _, ok := visible[value.TenantID]; !ok {
				continue
			}
		}
		if status != "" && string(value.Status) != status {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.TenantID+" "+value.TenantKey+" "+value.DisplayName), query) {
			continue
		}
		items = append(items, cloneTenant(value))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].TenantID < items[j].TenantID })
	if offset >= len(items) {
		return []*tenant.Tenant{}, "", nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = encodeListCursor(end)
	}
	return items[offset:end], next, nil
}

func encodeListCursor(offset int) string { return fmt.Sprintf("%d", offset) }
func decodeListCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	var offset int
	if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return offset, nil
}

// InMemoryRepository is a single-process repository for development and
// tests. It does not provide cross-node sharing or durability.
type InMemoryRepository struct {
	mu    contextRWMutex
	byID  map[string]*tenant.Tenant
	byKey map[string]string
}

// NewInMemoryRepository creates an empty repository.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{byID: make(map[string]*tenant.Tenant), byKey: make(map[string]string)}
}

// NewRepository is the concise constructor for the InMemory implementation.
func NewRepository() *InMemoryRepository { return NewInMemoryRepository() }

var _ tenant.Repository = (*InMemoryRepository)(nil)

// Create stores a new tenant in memory.
func (r *InMemoryRepository) Create(ctx context.Context, input tenant.CreateInput) (*tenant.Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, err := tenant.NewTenant(input)
	if err != nil {
		return nil, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if _, exists := r.byID[t.TenantID]; exists {
		return nil, fmt.Errorf("%w: %s", tenant.ErrDuplicateKey, t.TenantID)
	}
	if _, exists := r.byKey[t.TenantKey]; exists {
		return nil, fmt.Errorf("%w: %s", tenant.ErrDuplicateKey, t.TenantKey)
	}
	copy := t.Clone()
	r.byID[t.TenantID] = &copy
	r.byKey[t.TenantKey] = t.TenantID
	return cloneTenant(t), nil
}

// Get loads a tenant by its stable identifier.
func (r *InMemoryRepository) Get(ctx context.Context, tenantID string) (*tenant.Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.rLock(ctx); err != nil {
		return nil, err
	}
	defer r.rUnlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, ok := r.byID[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", tenant.ErrNotFound, tenantID)
	}
	return cloneTenant(t), nil
}

// Count returns the number of persisted tenants for bootstrap's first-tenant
// authorization boundary.
func (r *InMemoryRepository) Count(ctx context.Context) (int, error) {
	if err := r.rLock(ctx); err != nil {
		return 0, err
	}
	defer r.rUnlock()
	return len(r.byID), nil
}

// UpdateConfiguration applies an expected-version tenant configuration update.
func (r *InMemoryRepository) UpdateConfiguration(ctx context.Context, input tenant.UpdateConfigurationInput) (*tenant.Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if input.AuditRetentionDays == 0 {
		input.AuditRetentionDays = 90
	}
	if input.LogMaskingLevel == "" {
		input.LogMaskingLevel = tenant.MaskingBasic
	}
	if err := tenant.ValidateConfiguration(input.DisplayName, input.RateLimitRPM, input.MaxConcurrentExecutions, input.MonthlyTokenBudget, input.MonthlySpendLimitMinor, input.BillingCurrency, input.AuditRetentionDays, input.LogMaskingLevel, input.TraceSamplingRate); err != nil {
		return nil, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, ok := r.byID[input.TenantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", tenant.ErrNotFound, input.TenantID)
	}
	if t.Status == tenant.StatusDisabled {
		return nil, tenant.ErrDisabled
	}
	if input.ExpectedVersion != t.Version {
		return nil, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, input.ExpectedVersion, t.Version)
	}
	updated := *t
	updated.DisplayName = strings.TrimSpace(input.DisplayName)
	updated.RateLimitRPM = cloneInt64(input.RateLimitRPM)
	updated.MaxConcurrentExecutions = cloneInt64(input.MaxConcurrentExecutions)
	updated.MonthlyTokenBudget = cloneInt64(input.MonthlyTokenBudget)
	updated.MonthlySpendLimitMinor = cloneInt64(input.MonthlySpendLimitMinor)
	updated.BillingCurrency = input.BillingCurrency
	updated.AuditRetentionDays = input.AuditRetentionDays
	updated.LogMaskingLevel = input.LogMaskingLevel
	updated.TraceSamplingRate = input.TraceSamplingRate
	updated.DefaultAgentAppID = cloneString(input.DefaultAgentAppID)
	updated.DefaultBackendProfileID = cloneString(input.DefaultBackendProfileID)
	updated.Version++
	updated.UpdatedAt = time.Now().UTC()
	r.byID[input.TenantID] = &updated
	return cloneTenant(&updated), nil
}

// TransitionStatus changes a tenant status with optimistic concurrency.
func (r *InMemoryRepository) TransitionStatus(ctx context.Context, input tenant.TransitionStatusInput) (*tenant.Tenant, tenant.StatusChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	if err := validateMetadata(input.Metadata); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	t, ok := r.byID[input.TenantID]
	if !ok {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: %s", tenant.ErrNotFound, input.TenantID)
	}
	if t.Status == tenant.StatusDisabled {
		return nil, tenant.StatusChangeEvent{}, tenant.ErrDisabled
	}
	if input.ExpectedVersion != t.Version {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, input.ExpectedVersion, t.Version)
	}
	if !validTransition(t.Status, input.NextStatus) {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: %s -> %s", tenant.ErrInvalidTransition, t.Status, input.NextStatus)
	}
	now := time.Now().UTC()
	event := tenant.StatusChangeEvent{TenantID: t.TenantID, PreviousStatus: t.Status, NextStatus: input.NextStatus, ActorType: strings.TrimSpace(input.Metadata.ActorType), ActorID: strings.TrimSpace(input.Metadata.ActorID), Reason: strings.TrimSpace(input.Metadata.Reason), CorrelationID: strings.TrimSpace(input.Metadata.CorrelationID), PreviousVersion: t.Version, NextVersion: t.Version + 1, OccurredAt: now}
	updated := *t
	updated.Status = input.NextStatus
	updated.Version++
	updated.UpdatedAt = now
	r.byID[input.TenantID] = &updated
	return cloneTenant(&updated), event, nil
}

func validateMetadata(m tenant.TransitionMetadata) error {
	reason := strings.TrimSpace(m.Reason)
	if strings.TrimSpace(m.ActorType) == "" || strings.TrimSpace(m.ActorID) == "" || reason == "" || strings.TrimSpace(m.CorrelationID) == "" {
		return fmt.Errorf("%w: transition metadata requires actor, reason, and correlation ID", tenant.ErrInvalid)
	}
	if len([]rune(reason)) > 1000 {
		return fmt.Errorf("%w: transition reason must contain at most 1000 characters", tenant.ErrInvalid)
	}
	return nil
}

func validTransition(from, to tenant.Status) bool {
	switch {
	case from == tenant.StatusActive && (to == tenant.StatusSuspended || to == tenant.StatusDisabled):
		return true
	case from == tenant.StatusSuspended && (to == tenant.StatusActive || to == tenant.StatusDisabled):
		return true
	default:
		return false
	}
}

func cloneTenant(t *tenant.Tenant) *tenant.Tenant {
	if t == nil {
		return nil
	}
	c := t.Clone()
	return &c
}

func cloneInt64(v *int64) *int64 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func cloneString(v *string) *string {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (r *InMemoryRepository) lock(ctx context.Context) error {
	return r.mu.lock(ctx)
}

func (r *InMemoryRepository) unlock() { r.mu.unlock() }

func (r *InMemoryRepository) rLock(ctx context.Context) error {
	return r.mu.rlock(ctx)
}

func (r *InMemoryRepository) rUnlock() { r.mu.runlock() }
