package admin

import (
	"context"
	"encoding/json"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type TenantPolicyInput struct {
	TenantID        string          `json:"tenant_id"`
	ExpectedVersion int64           `json:"expected_version"`
	QuotaConfig     json.RawMessage `json:"quota_config"`
	AuditPolicy     json.RawMessage `json:"audit_policy"`
}

func (s *Service) UpdateTenantPolicies(ctx context.Context, input TenantPolicyInput) (controlplane.Tenant, error) {
	t, err := s.repository.GetTenant(ctx, input.TenantID)
	if err != nil {
		return t, err
	}
	if t.Version != input.ExpectedVersion {
		return controlplane.Tenant{}, controlplane.ErrConflict
	}
	if len(input.QuotaConfig) == 0 {
		input.QuotaConfig = t.QuotaConfig
	}
	if len(input.AuditPolicy) == 0 {
		input.AuditPolicy = t.AuditPolicy
	}
	if err := normalizeJSON(&input.QuotaConfig); err != nil {
		return controlplane.Tenant{}, invalidf("quota policy must be an object")
	}
	if err := normalizeJSON(&input.AuditPolicy); err != nil {
		return controlplane.Tenant{}, invalidf("audit policy must be an object")
	}
	if _, err := tenant.ParseQuotaPolicy(input.QuotaConfig); err != nil {
		return controlplane.Tenant{}, invalidf("invalid quota policy")
	}
	if _, err := audit.ParsePolicy(input.AuditPolicy); err != nil {
		return controlplane.Tenant{}, invalidf("invalid audit policy")
	}
	repo, ok := s.repository.(controlplane.TenantPolicyRepository)
	if !ok {
		return controlplane.Tenant{}, invalidf("tenant policy updates unavailable")
	}
	if err := s.record(ctx, t.ID, "admin_tenant_policy_update_requested", map[string]any{"expected_version": input.ExpectedVersion}); err != nil {
		return controlplane.Tenant{}, err
	}
	if err := repo.UpdateTenantPolicies(ctx, t.ID, input.ExpectedVersion, input.QuotaConfig, input.AuditPolicy, PrincipalName(ctx), audit.TraceID(ctx)); err != nil {
		return controlplane.Tenant{}, err
	}
	return s.repository.GetTenant(ctx, t.ID)
}
