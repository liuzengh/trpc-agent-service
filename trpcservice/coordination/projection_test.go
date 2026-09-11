package coordination

import (
	"context"
	"testing"
)

func TestSessionProjectionRefusesAnOlderVersion(t *testing.T) {
	r := testRedis(t)
	p := NewSessionProjection(r, 0)
	ctx := context.Background()

	written, err := p.Project(ctx, ProjectedSession{
		TenantID: "acme", SessionPK: 1, Version: 5,
		State: map[string][]byte{"k": []byte("v5")},
	})
	if err != nil {
		t.Fatalf("first project: %v", err)
	}
	if !written {
		t.Fatal("first project reported not-written")
	}

	// An older version must not overwrite a newer one — this is the exact
	// failure mode "version CAS 禁止倒序覆盖" in the approved plan names, and
	// it is the only thing standing between a delayed worker and a cache that
	// shows a rolled-back conversation.
	written, err = p.Project(ctx, ProjectedSession{
		TenantID: "acme", SessionPK: 1, Version: 4,
		State: map[string][]byte{"k": []byte("v4-stale")},
	})
	if err != nil {
		t.Fatalf("stale project: %v", err)
	}
	if written {
		t.Fatal("an older version overwrote a newer projection")
	}

	got, found, err := p.Read(ctx, "acme", 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !found {
		t.Fatal("projection vanished after a rejected write")
	}
	if got.Version != 5 || string(got.State["k"]) != "v5" {
		t.Fatalf("read back %+v, want the version-5 state", got)
	}

	written, err = p.Project(ctx, ProjectedSession{
		TenantID: "acme", SessionPK: 1, Version: 6,
		State: map[string][]byte{"k": []byte("v6")},
	})
	if err != nil || !written {
		t.Fatalf("newer project = %v, %v; want accepted", written, err)
	}
	got, _, _ = p.Read(ctx, "acme", 1)
	if got.Version != 6 {
		t.Fatalf("version after newer project = %d, want 6", got.Version)
	}
}

func TestSessionProjectionIsPerTenant(t *testing.T) {
	r := testRedis(t)
	p := NewSessionProjection(r, 0)
	ctx := context.Background()

	// Two tenants with the same numeric session_pk share nothing: the key
	// itself carries the tenant, and this test exists to catch a future
	// refactor that drops it and makes "project the whole database" one
	// accidental collision away.
	if _, err := p.Project(ctx, ProjectedSession{TenantID: "acme", SessionPK: 1, Version: 1, State: map[string][]byte{"who": []byte("acme")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Project(ctx, ProjectedSession{TenantID: "globex", SessionPK: 1, Version: 1, State: map[string][]byte{"who": []byte("globex")}}); err != nil {
		t.Fatal(err)
	}

	acme, found, err := p.Read(ctx, "acme", 1)
	if err != nil || !found {
		t.Fatalf("read acme: %v %v", found, err)
	}
	if string(acme.State["who"]) != "acme" {
		t.Fatalf("acme's projection reads back as %q", acme.State["who"])
	}
	globex, found, err := p.Read(ctx, "globex", 1)
	if err != nil || !found {
		t.Fatalf("read globex: %v %v", found, err)
	}
	if string(globex.State["who"]) != "globex" {
		t.Fatalf("globex's projection reads back as %q", globex.State["who"])
	}

	// Advancing one tenant's version must leave the other's untouched, still
	// readable at its own version.
	if _, err := p.Project(ctx, ProjectedSession{TenantID: "acme", SessionPK: 1, Version: 2, State: map[string][]byte{"who": []byte("acme2")}}); err != nil {
		t.Fatal(err)
	}
	globexAfter, _, _ := p.Read(ctx, "globex", 1)
	if globexAfter.Version != 1 || string(globexAfter.State["who"]) != "globex" {
		t.Fatalf("globex's projection changed while acme's was updated: %+v", globexAfter)
	}
}

func TestSessionProjectionReadMissesCleanly(t *testing.T) {
	r := testRedis(t)
	p := NewSessionProjection(r, 0)
	ctx := context.Background()

	got, found, err := p.Read(ctx, "nobody", 42)
	if err != nil {
		t.Fatalf("read of a missing projection must not error: %v", err)
	}
	if found {
		t.Fatalf("read reported a phantom projection: %+v", got)
	}

	if err := p.Forget(ctx, "nobody", 42); err != nil {
		t.Fatalf("forgetting something already absent: %v", err)
	}
}
