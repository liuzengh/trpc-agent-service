package storage

import (
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// TestRouterDefaultsAndPerTenantBuilds pins the runtime meaning of
// backend_profiles: a tenant without a profile stays on the platform default,
// a tenant with one gets its own service, and a broken profile degrades to
// the default instead of breaking the message path.
func TestRouterDefaultsAndPerTenantBuilds(t *testing.T) {
	defaultSvc := inmemory.NewSessionService()
	r := NewRouter(defaultSvc, "redis://127.0.0.1:1")
	t.Cleanup(func() { _ = r.Close() })

	if got := r.For("acme"); got != defaultSvc {
		t.Fatal("a tenant without a profile must get the default service")
	}

	r.ApplyProfiles(map[string]BackendSetting{
		"mem":   {Backend: "memory"},
		"red":   {Backend: "redis", KeyPrefix: "tas:red:", SessionTTL: time.Hour},
		"bogus": {Backend: "postgres"},
	})

	mem := r.For("mem")
	if mem == defaultSvc {
		t.Fatal("a memory-profile tenant must get its own service, not the default")
	}
	red := r.For("red")
	if red == nil || red == defaultSvc || red == mem {
		t.Fatal("a redis-profile tenant must get its own service")
	}
	if r.For("red") != red {
		t.Fatal("a second lookup must return the cached service")
	}
	if got := r.For("bogus"); got != defaultSvc {
		t.Fatal("an unknown backend must fall back to the default")
	}
}

// TestRouterApplyProfilesRebuilds: a reload replaces the built services (the
// whole point of ApplyProfiles), and clearing the table returns tenants to
// the default.
func TestRouterApplyProfilesRebuilds(t *testing.T) {
	defaultSvc := inmemory.NewSessionService()
	r := NewRouter(defaultSvc, "redis://127.0.0.1:1")
	t.Cleanup(func() { _ = r.Close() })

	settings := map[string]BackendSetting{"red": {Backend: "redis", KeyPrefix: "tas:red:"}}
	r.ApplyProfiles(settings)
	first := r.For("red")

	r.ApplyProfiles(settings)
	second := r.For("red")
	if second == first {
		t.Fatal("ApplyProfiles must rebuild the tenant's service")
	}

	r.ApplyProfiles(nil)
	if got := r.For("red"); got != defaultSvc {
		t.Fatal("clearing the profiles must return the tenant to the default")
	}
}
