package member

import (
	"context"
	"testing"
)

func TestMemStoreRejectsMemberInMoreThanOneTenant(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	if err := store.Create(ctx, &Member{TenantID: "tenant-a", UserID: "alice", Role: RoleMember}); err != nil {
		t.Fatalf("create first membership: %v", err)
	}
	if err := store.Create(ctx, &Member{TenantID: "tenant-b", UserID: "alice", Role: RoleMember}); err == nil {
		t.Fatal("expected one-member-one-tenant constraint to reject the second tenant")
	}
}

func TestMemStoreFindsMemberByUserID(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	if err := store.Create(ctx, &Member{TenantID: "tenant-a", UserID: "alice", Role: RoleMember}); err != nil {
		t.Fatal(err)
	}

	got, err := store.GetByUserID(ctx, "alice")
	if err != nil {
		t.Fatalf("get by user id: %v", err)
	}
	if got.TenantID != "tenant-a" {
		t.Fatalf("tenant = %q, want tenant-a", got.TenantID)
	}
}
