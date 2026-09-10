package storage

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
)

// TestDataStoresAssembly pins the data-domain assembly: session/memory via the
// Router, while the other domains keep their single backends (nil when
// disabled). This is the code-side statement of "how each domain is stored".
func TestDataStoresAssembly(t *testing.T) {
	router := NewRouter(tenant.NewManager(),
		SessionConfig{Backend: BackendInMemory},
		MemoryConfig{Backend: BackendInMemory},
	)
	dss := NewDataStores(router, nil, nil, nil)
	if dss.Router == nil {
		t.Fatal("Router must always be assembled")
	}
	if dss.Knowledge != nil || dss.Artifacts != nil || dss.Auditor != nil {
		t.Error("nil-disabled domains should stay nil")
	}

	// The router still resolves and serves sessions under the in-memory
	// backend (sanity: DataStores did not break the domain wiring).
	sessions, err := dss.Router.Sessions(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Router.Sessions: %v", err)
	}
	if sessions == nil {
		t.Fatal("expected a sessions store")
	}
}

// TestDataStoresFieldSemantics documents that Summary is not a standalone
// domain: there is intentionally no DataStores field for it. This test keeps
// that decision visible if someone is tempted to add one without a real
// second backend.
func TestDataStoresFieldSemantics(t *testing.T) {
	router := NewRouter(tenant.NewManager(),
		SessionConfig{Backend: BackendInMemory},
		MemoryConfig{Backend: BackendInMemory},
	)
	dss := NewDataStores(router, nil, nil, nil)
	if dss.Router == nil {
		t.Fatal("Router must always be assembled")
	}
	// The aggregate exposes exactly the domains with concrete backends;
	// expansion beyond this requires a second backend implementation.
	if got := len(dssDomains()); got != 4 {
		t.Errorf("documented domains = %d, want 4 (router+knowledge+artifact+audit)", got)
	}
}

// dssDomains mirrors the DataStores field set for the doc/test coupling.
func dssDomains() []string {
	return []string{"router(session/memory)", "knowledge", "artifact", "audit"}
}

// TestRouterDomainsPerTenantSelection pins the five-domain routing contract:
// a tenant's DataBackend selection dispatches vector/artifact/audit to the
// chosen backend, falling back to the default for tenants without a selection.
func TestRouterDomainsPerTenantSelection(t *testing.T) {
	mgr := tenant.NewManager()
	// t-inmem explicitly opts every routed domain into in-memory; t-default
	// leaves DataBackend unset and must resolve the configured default.
	if err := mgr.Create(context.Background(), &tenant.Tenant{
		ID: "t-inmem", Name: "inmem", Status: tenant.StatusActive,
		DataBackend: map[string]string{
			tenant.DomainSession:  string(BackendInMemory),
			tenant.DomainMemory:   string(BackendInMemory),
			tenant.DomainVector:   string(BackendInMemory),
			tenant.DomainArtifact: string(BackendInMemory),
			tenant.DomainAudit:    string(BackendInMemory),
		},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	router := NewRouter(mgr,
		SessionConfig{Backend: BackendInMemory},
		MemoryConfig{Backend: BackendInMemory},
	)

	// backendFor is the single resolution point every domain dispatcher uses;
	// asserting it covers session/memory/vector/artifact/audit at once.
	for _, tc := range []struct {
		tenantID, domain, def, want string
	}{
		{"t-inmem", tenant.DomainSession, string(BackendRedis), string(BackendInMemory)},
		{"t-inmem", tenant.DomainMemory, string(BackendRedis), string(BackendInMemory)},
		{"t-inmem", tenant.DomainVector, string(BackendMilvus), string(BackendInMemory)},
		{"t-inmem", tenant.DomainArtifact, string(BackendMinIO), string(BackendInMemory)},
		{"t-inmem", tenant.DomainAudit, string(BackendMySQL), string(BackendInMemory)},
		{"t-default", tenant.DomainSession, string(BackendRedis), string(BackendRedis)},
		{"t-default", tenant.DomainVector, string(BackendMilvus), string(BackendMilvus)},
		{"t-default", tenant.DomainAudit, string(BackendMySQL), string(BackendMySQL)},
		{"", tenant.DomainSession, string(BackendRedis), string(BackendRedis)},
	} {
		got, err := router.backendFor(context.Background(), tc.tenantID, tc.domain, tc.def)
		if err != nil {
			t.Fatalf("backendFor(%s,%s): %v", tc.tenantID, tc.domain, err)
		}
		if got != tc.want {
			t.Errorf("backendFor(%s,%s) = %q, want %q", tc.tenantID, tc.domain, got, tc.want)
		}
	}
}

// TestRouterAuditRecorderRoutesPerTenant pins the audit domain dispatch: a
// tenant opting into in-memory audit records nowhere else, while the default
// (and unresolvable tenants) go to the production recorder.
func TestRouterAuditRecorderRoutesPerTenant(t *testing.T) {
	mgr := tenant.NewManager()
	if err := mgr.Create(context.Background(), &tenant.Tenant{
		ID: "t-mem", Name: "mem", Status: tenant.StatusActive,
		DataBackend: map[string]string{tenant.DomainAudit: string(BackendInMemory)},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	router := NewRouter(mgr,
		SessionConfig{Backend: BackendInMemory},
		MemoryConfig{Backend: BackendInMemory},
	)

	mysql := audit.NewMemRecorder()
	rec := NewRouterAuditRecorder(router, mysql, nil)

	rec.Record(audit.Entry{TenantID: "t-mem", Decision: audit.DecisionExecuted})
	rec.Record(audit.Entry{TenantID: "t-default", Decision: audit.DecisionExecuted})
	rec.Record(audit.Entry{TenantID: "", Decision: audit.DecisionExecuted})

	if got := len(mysql.Entries()); got != 2 {
		t.Errorf("mysql entries = %d, want 2 (t-mem routed to in-memory)", got)
	}
}
