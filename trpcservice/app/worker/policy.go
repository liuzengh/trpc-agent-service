package worker

import (
	"context"
	"log/slog"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
)

// TenantSource resolves the governance config of the tenant an inbound
// message belongs to. *tenant.Manager satisfies it (MySQL or in-memory). A
// nil source means no tenant governance is configured: all checks use the
// safe defaults below (redaction on, budget unlimited, users allowed,
// tools unrestricted).
type TenantSource interface {
	Get(ctx context.Context, id string) (*tenant.Tenant, error)
}

// tenantPolicy is the governance snapshot for one tenant, resolved once per
// inbound message and carried on the context so handle (IM allow-list) and
// run (budget / tool whitelist / approval union / redaction) agree on the
// same decision.
type tenantPolicy struct {
	// TokenQuota > 0 enforces a per-tenant token budget.
	TokenQuota int64
	// Redact controls tool-argument/result redaction (default on).
	Redact bool
	// IMAllowUsers, when non-empty, is the allow-list of IM user ids.
	IMAllowUsers map[string]struct{}
	// ToolWhitelist, when non-empty, restricts the static tools an agent may
	// mount (knowledge_search tools are never restricted).
	ToolWhitelist map[string]struct{}
	// ForceApproval is the tenant-mandated approval tool-id set, union-ed
	// with the agent-level approval list and risk_level=high.
	ForceApproval map[string]struct{}
}

// defaultTenantPolicy returns the safe defaults applied when no tenant source
// is wired or the tenant has no explicit governance: redaction on, budget
// unlimited, everyone allowed, every tool allowed, no forced approvals.
func defaultTenantPolicy() *tenantPolicy {
	return &tenantPolicy{Redact: true}
}

// policyContextKey carries the resolved tenant policy through handle -> run.
type policyContextKey struct{}

// withTenantPolicy attaches the governance snapshot to the per-turn context.
func withTenantPolicy(ctx context.Context, p *tenantPolicy) context.Context {
	return context.WithValue(ctx, policyContextKey{}, p)
}

// tenantPolicyFrom returns the snapshot stored by handle, or the default when
// absent (e.g. tests calling run directly).
func tenantPolicyFrom(ctx context.Context) *tenantPolicy {
	if p, ok := ctx.Value(policyContextKey{}).(*tenantPolicy); ok && p != nil {
		return p
	}
	return defaultTenantPolicy()
}

// resolveTenantPolicy loads and normalizes the governance config of one
// tenant. A missing tenant or an unconfigured source yields the defaults —
// governance must never hard-fail message handling.
func (w *Worker) resolveTenantPolicy(ctx context.Context, tenantID string) *tenantPolicy {
	if w.tenants == nil {
		return defaultTenantPolicy()
	}
	t, err := w.tenants.Get(ctx, tenantID)
	if err != nil {
		// Governance never hard-fails message handling; a store error falls
		// back to the safe defaults (redaction on, quota unlimited, allow-list
		// and whitelist wide open). That fail-open is a deliberate availability
		// trade-off — but it silently disables the IM allow-list / tool
		// whitelist / force-approval, so it is logged at Error to page on a
		// security-control outage rather than disappear into routine noise.
		slog.Error("worker: tenant governance lookup failed, applying defaults",
			"tenant", tenantID, "err", err)
		return defaultTenantPolicy()
	}
	if t == nil {
		return defaultTenantPolicy()
	}
	p := &tenantPolicy{Redact: true}
	if t.Quota != nil && t.Quota.TokenQuota > 0 {
		p.TokenQuota = t.Quota.TokenQuota
	}
	if t.AuditPolicy == nil {
		return p
	}
	if !t.AuditPolicy.RedactEnabled() {
		p.Redact = false
	}
	if len(t.AuditPolicy.IMAllowUsers) > 0 {
		p.IMAllowUsers = make(map[string]struct{}, len(t.AuditPolicy.IMAllowUsers))
		for _, u := range t.AuditPolicy.IMAllowUsers {
			p.IMAllowUsers[u] = struct{}{}
		}
	}
	if len(t.AuditPolicy.ToolWhitelist) > 0 {
		p.ToolWhitelist = make(map[string]struct{}, len(t.AuditPolicy.ToolWhitelist))
		for _, id := range t.AuditPolicy.ToolWhitelist {
			p.ToolWhitelist[id] = struct{}{}
		}
	}
	if len(t.AuditPolicy.ForceApprovalTools) > 0 {
		p.ForceApproval = make(map[string]struct{}, len(t.AuditPolicy.ForceApprovalTools))
		for _, id := range t.AuditPolicy.ForceApprovalTools {
			p.ForceApproval[id] = struct{}{}
		}
	}
	return p
}

// userAllowed reports whether an IM user may talk to the tenant's agents.
// An empty allow-list (or nil policy) allows everyone.
func (p *tenantPolicy) userAllowed(userID string) bool {
	if len(p.IMAllowUsers) == 0 {
		return true
	}
	_, ok := p.IMAllowUsers[userID]
	return ok
}

// imUserAllowed reports whether an inbound message passes the tenant's IM
// user allow-list. The allow-list governs external IM senders only: platform
// console traffic (channel "admin", or a message without a channel) is
// authenticated by the platform itself and must never be dropped because of a
// tenant's external-user list.
func (p *tenantPolicy) imUserAllowed(channel, userID string) bool {
	if channel == "" || channel == "admin" {
		return true
	}
	return p.userAllowed(userID)
}

// toolAllowed reports whether a static tool id passes the tenant whitelist.
// An empty whitelist allows everything; knowledge_search tools are appended
// after this filter and therefore never restricted.
func (p *tenantPolicy) toolAllowed(toolID string) bool {
	if len(p.ToolWhitelist) == 0 {
		return true
	}
	_, ok := p.ToolWhitelist[toolID]
	return ok
}

// budgetExceeded reports whether a tenant has consumed its whole token quota.
// A nil usage meter (or a zero/absent quota) never enforces a budget. The
// meter reads usage_records, which the worker writes only after a turn
// finishes, so this is an eventually-consistent soft limit: concurrent turns
// can briefly overshoot before the meter catches up.
func budgetExceeded(ctx context.Context, quota int64, tenantID string, usage func(context.Context, string) (int64, error)) (bool, error) {
	if quota <= 0 || usage == nil {
		return false, nil
	}
	used, err := usage(ctx, tenantID)
	if err != nil {
		return false, err
	}
	return used >= quota, nil
}

// redactionPlugin returns the sensitive-data redaction filter for a policy.
// A tenant that explicitly opts out (Redact=false) gets nil and runs with no
// redaction; any other policy (default included) redacts.
func redactionPlugin(policy *tenantPolicy) *governance.RedactionFilter {
	if policy != nil && !policy.Redact {
		return nil
	}
	return governance.NewRedactionFilter()
}
