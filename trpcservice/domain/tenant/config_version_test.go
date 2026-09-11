package tenant

import (
	"context"
	"errors"
	"testing"
)

func mustCreate(t *testing.T, m *Manager, id string) {
	t.Helper()
	if err := m.Create(context.Background(), &Tenant{ID: id, Name: id, Status: StatusActive}); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
}

// TestUpdateRecordsConfigVersions verifies every successful update snapshots
// the tenant with a monotonically increasing version.
func TestUpdateRecordsConfigVersions(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	mustCreate(t, m, "t1")

	if err := m.Update(ctx, &Tenant{ID: "t1", Name: "t1", Status: StatusActive, Quota: &Quota{TokenQuota: 1000}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := m.Update(ctx, &Tenant{ID: "t1", Name: "t1", Status: StatusActive, Quota: &Quota{TokenQuota: 5000}}); err != nil {
		t.Fatalf("update 2: %v", err)
	}

	vs, err := m.ConfigVersions(ctx, "t1")
	if err != nil {
		t.Fatalf("config versions: %v", err)
	}
	if len(vs) != 2 {
		t.Fatalf("versions len = %d, want 2", len(vs))
	}
	// Newest first.
	if vs[0].Version != 2 || vs[1].Version != 1 {
		t.Errorf("version order = %d,%d, want 2,1", vs[0].Version, vs[1].Version)
	}
	if vs[0].Config.Quota == nil || vs[0].Config.Quota.TokenQuota != 5000 {
		t.Errorf("newest quota not snapshot: %+v", vs[0].Config.Quota)
	}
	if vs[1].Config.Quota == nil || vs[1].Config.Quota.TokenQuota != 1000 {
		t.Errorf("oldest quota not snapshot: %+v", vs[1].Config.Quota)
	}
}

// TestRollbackConfigRestoresSnapshotAndAdvances verifies rollback applies the
// chosen historical config and records a fresh head version.
func TestRollbackConfigRestoresSnapshotAndAdvances(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	mustCreate(t, m, "t1")

	// v1: 1000 -> v2: 5000
	if err := m.Update(ctx, &Tenant{ID: "t1", Name: "t1", Status: StatusActive, Quota: &Quota{TokenQuota: 1000}}); err != nil {
		t.Fatal(err)
	}
	if err := m.Update(ctx, &Tenant{ID: "t1", Name: "t1", Status: StatusActive, Quota: &Quota{TokenQuota: 5000}}); err != nil {
		t.Fatal(err)
	}

	got, err := m.RollbackConfig(ctx, "t1", 1)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got.Quota == nil || got.Quota.TokenQuota != 1000 {
		t.Errorf("after rollback quota = %+v, want 1000", got.Quota)
	}

	vs, err := m.ConfigVersions(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 3 {
		t.Fatalf("versions after rollback = %d, want 3", len(vs))
	}
	if vs[0].Version != 3 {
		t.Errorf("head version = %d, want 3", vs[0].Version)
	}
	if vs[0].Config.Quota == nil || vs[0].Config.Quota.TokenQuota != 1000 {
		t.Errorf("head snapshot should carry rolled-back config")
	}
}

func TestRollbackUnknownVersion(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	mustCreate(t, m, "t1")

	if _, err := m.RollbackConfig(ctx, "t1", 99); !errors.Is(err, ErrConfigVersionNotFound) {
		t.Errorf("rollback unknown version err = %v, want ErrConfigVersionNotFound", err)
	}
}

// TestSnapshotDoesNotLeakMutation checks history entries are deep copies: a
// later external mutation of a snapshot's config must not corrupt history.
func TestSnapshotDoesNotLeakMutation(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	mustCreate(t, m, "t1")

	up := &Tenant{ID: "t1", Name: "t1", Status: StatusActive, Quota: &Quota{TokenQuota: 7}}
	if err := m.Update(ctx, up); err != nil {
		t.Fatal(err)
	}
	vs, _ := m.ConfigVersions(ctx, "t1")
	// Mutate the returned copy; stored snapshot must stay intact.
	vs[0].Config.Quota.TokenQuota = 999
	cur, err := m.Get(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if cur.Quota.TokenQuota != 7 {
		t.Errorf("live tenant mutated via history copy: %+v", cur.Quota)
	}
}
