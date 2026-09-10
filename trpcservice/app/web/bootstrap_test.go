package web

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"golang.org/x/crypto/bcrypt"
)

func TestEnsureInitialOwnerCreatesTenantAndMember(t *testing.T) {
	ctx := context.Background()
	tenants := tenant.NewManager()
	members := member.NewManager()

	if err := EnsureInitialOwner(ctx, tenants, members, "tenant-a", "admin", "secret"); err != nil {
		t.Fatal(err)
	}

	gotTenant, err := tenants.Get(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if gotTenant.Name != "tenant-a" {
		t.Fatalf("tenant name = %q, want tenant-a", gotTenant.Name)
	}
	gotMember, err := members.GetByUserID(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if gotMember.TenantID != "tenant-a" || gotMember.Role != member.RoleOwner || gotMember.Password == "" {
		t.Fatalf("member = %+v, want owner bound to tenant-a with password hash", gotMember)
	}
}

func TestEnsureInitialOwnerIsIdempotent(t *testing.T) {
	ctx := context.Background()
	tenants := tenant.NewManager()
	members := member.NewManager()

	if err := EnsureInitialOwner(ctx, tenants, members, "tenant-a", "admin", "secret"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureInitialOwner(ctx, tenants, members, "tenant-a", "admin", "changed"); err != nil {
		t.Fatal(err)
	}

	all, err := members.List(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("members = %d, want 1", len(all))
	}
}

func TestEnsureInitialOwnerBackfillsLegacyEmptyPassword(t *testing.T) {
	ctx := context.Background()
	tenants := tenant.NewManager()
	members := member.NewManager()
	if err := tenants.Create(ctx, &tenant.Tenant{ID: "tenant-a", Name: "tenant-a", Status: tenant.StatusActive}); err != nil {
		t.Fatal(err)
	}
	if err := members.Create(ctx, &member.Member{
		TenantID: "tenant-a",
		UserID:   "admin",
		Role:     member.RoleOwner,
	}); err != nil {
		t.Fatal(err)
	}

	if err := EnsureInitialOwner(ctx, tenants, members, "tenant-a", "admin", "secret"); err != nil {
		t.Fatal(err)
	}
	got, err := members.GetByUserID(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if got.Password == "" {
		t.Fatal("expected legacy empty password to be backfilled")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(got.Password), []byte("secret")); err != nil {
		t.Fatalf("backfilled password does not match bootstrap password: %v", err)
	}
}
