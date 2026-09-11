package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
)

// fakeTenantSource is a TenantSource over a static map for governance tests.
type fakeTenantSource struct {
	tenants map[string]*tenant.Tenant
	err     error
}

func (f *fakeTenantSource) Get(_ context.Context, id string) (*tenant.Tenant, error) {
	if f.err != nil {
		return nil, f.err
	}
	t, ok := f.tenants[id]
	if !ok {
		return nil, tenant.ErrNotFound
	}
	return t, nil
}

func TestResolveTenantPolicyDefaults(t *testing.T) {
	ctx := context.Background()

	// No source wired: safe defaults (redact on, no quota, allow all).
	w := &Worker{}
	p := w.resolveTenantPolicy(ctx, "t1")
	if !p.Redact {
		t.Error("default policy should redact")
	}
	if p.TokenQuota != 0 {
		t.Errorf("TokenQuota = %d, want 0 (unlimited)", p.TokenQuota)
	}
	if !p.userAllowed("anyone") {
		t.Error("empty allow-list should allow everyone")
	}
	if !p.toolAllowed("echo") {
		t.Error("empty whitelist should allow every tool")
	}

	// Source present but tenant unknown / store error: still defaults.
	src := &fakeTenantSource{tenants: map[string]*tenant.Tenant{}}
	w2 := &Worker{tenants: src}
	if p := w2.resolveTenantPolicy(ctx, "ghost"); !p.Redact || !p.userAllowed("u") {
		t.Error("unknown tenant should fall back to defaults")
	}
	errSrc := &fakeTenantSource{err: errors.New("boom")}
	if p := (&Worker{tenants: errSrc}).resolveTenantPolicy(ctx, "t1"); !p.Redact {
		t.Error("store error should fall back to defaults, not fail the turn")
	}
}

func TestResolveTenantPolicyFromConfig(t *testing.T) {
	ctx := context.Background()
	redactOff := false
	src := &fakeTenantSource{tenants: map[string]*tenant.Tenant{
		"t1": {
			ID:    "t1",
			Quota: &tenant.Quota{TokenQuota: 50_000},
			AuditPolicy: &tenant.AuditPolicy{
				Redact:             &redactOff,
				IMAllowUsers:       []string{"alice"},
				ToolWhitelist:      []string{"echo"},
				ForceApprovalTools: []string{"code-exec"},
			},
		},
	}}
	p := (&Worker{tenants: src}).resolveTenantPolicy(ctx, "t1")

	if p.TokenQuota != 50_000 {
		t.Errorf("TokenQuota = %d, want 50000", p.TokenQuota)
	}
	if p.Redact {
		t.Error("Redact should be off (explicit false)")
	}
	if !p.userAllowed("alice") {
		t.Error("alice should be allowed")
	}
	if p.userAllowed("mallory") {
		t.Error("mallory should be denied")
	}
	if !p.toolAllowed("echo") {
		t.Error("echo should pass the whitelist")
	}
	if p.toolAllowed("code-exec") {
		t.Error("code-exec is not whitelisted and should be filtered")
	}
	if _, ok := p.ForceApproval["code-exec"]; !ok {
		t.Error("code-exec should be tenant-force-approved")
	}
}

func TestDefaultPolicyAllowLists(t *testing.T) {
	def := defaultTenantPolicy()
	if !def.userAllowed("") {
		t.Error("empty user id should be allowed under default policy")
	}
	if !def.toolAllowed("") {
		t.Error("empty tool id should be allowed under default policy")
	}
}

// Regression: the tenant IM allow-list must gate only IM-originated traffic.
// Platform console messages (channel "admin", or a missing channel) are
// authenticated by the platform itself and must never be dropped because of a
// tenant's external-user allow-list (e.g. the default console user "admin").
func TestIMAllowListGatesOnlyIMChannels(t *testing.T) {
	def := defaultTenantPolicy()
	if !def.imUserAllowed("admin", "mallory") {
		t.Error("default policy must allow console traffic")
	}
	if !def.imUserAllowed("wecom", "mallory") {
		t.Error("default policy must allow IM traffic (empty allow-list)")
	}

	allowAlice := &tenantPolicy{Redact: true, IMAllowUsers: map[string]struct{}{"alice": {}}}
	if !allowAlice.imUserAllowed("wecom", "alice") {
		t.Error("whitelisted IM user should be allowed")
	}
	if allowAlice.imUserAllowed("wecom", "mallory") {
		t.Error("non-whitelisted IM user must be dropped")
	}
	if allowAlice.imUserAllowed("feishu", "mallory") {
		t.Error("non-whitelisted IM user must be dropped on any IM channel")
	}
	if !allowAlice.imUserAllowed("admin", "mallory") {
		t.Error("console traffic must not be gated by the IM allow-list")
	}
	if !allowAlice.imUserAllowed("", "mallory") {
		t.Error("channel-less messages must not be gated by the IM allow-list")
	}
}

func TestPolicyCarriedOnContext(t *testing.T) {
	ctx := withTenantPolicy(context.Background(), &tenantPolicy{TokenQuota: 7})
	p := tenantPolicyFrom(ctx)
	if p == nil || p.TokenQuota != 7 {
		t.Errorf("policy lost through context: %+v", p)
	}
	// Absent policy -> default.
	if q := tenantPolicyFrom(context.Background()); q == nil || !q.Redact {
		t.Error("missing policy should fall back to defaults")
	}
}

func TestBudgetGate(t *testing.T) {
	ctx := context.Background()
	noUsage := func(context.Context, string) (int64, error) { return 0, nil }
	usage := func(n int64) func(context.Context, string) (int64, error) {
		return func(_ context.Context, _ string) (int64, error) { return n, nil }
	}

	// Absent quota or absent meter never enforces a budget.
	if ok, err := budgetExceeded(ctx, 0, "t1", noUsage); ok || err != nil {
		t.Errorf("quota 0: (ok, err) = (%v, %v), want (false, nil)", ok, err)
	}
	if ok, err := budgetExceeded(ctx, 100, "t1", nil); ok || err != nil {
		t.Errorf("nil meter: (ok, err) = (%v, %v), want (false, nil)", ok, err)
	}

	// Boundary: reaching the quota exactly is exceeded.
	if ok, err := budgetExceeded(ctx, 100, "t1", usage(100)); !ok || err != nil {
		t.Errorf("used == quota: (ok, err) = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := budgetExceeded(ctx, 100, "t1", usage(99)); ok || err != nil {
		t.Errorf("used < quota: (ok, err) = (%v, %v), want (false, nil)", ok, err)
	}

	// Meter errors surface (the caller logs and proceeds), they do not read
	// as an exceeded budget.
	if ok, err := budgetExceeded(ctx, 100, "t1", func(context.Context, string) (int64, error) {
		return 0, errors.New("meter down")
	}); ok || err == nil {
		t.Errorf("meter error: (ok, err) = (%v, %v), want (false, error)", ok, err)
	}
}

func TestRedactionPluginDecision(t *testing.T) {
	if redactionPlugin(defaultTenantPolicy()) == nil {
		t.Error("default policy should redact")
	}
	if redactionPlugin(&tenantPolicy{Redact: true}) == nil {
		t.Error("explicit redact=true should redact")
	}
	if redactionPlugin(nil) == nil {
		t.Error("nil policy should redact (safe default)")
	}
	if p := redactionPlugin(&tenantPolicy{Redact: false}); p != nil {
		t.Error("redact=false must not install a redaction plugin")
	}
}
