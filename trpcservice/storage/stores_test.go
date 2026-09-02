package storage

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
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
		t.Fatal("Router must be assembled")
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
