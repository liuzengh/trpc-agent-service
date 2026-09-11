package web

import (
	"context"
	"errors"
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

// errMemberNotFound stands in for the store's lookup failure: the member domain
// reports "not found" as a plain error rather than a sentinel.
var errMemberNotFound = errors.New("member not found")

// racingMemberStore makes the create path lose a race: the initial lookup says
// the member does not exist, and the insert is then rejected as a duplicate
// because another node created it in between — after which the row is visible,
// which is exactly what a losing node observes against a real database.
type racingMemberStore struct {
	member.Store
	inner member.Store
	raced bool
}

func (s *racingMemberStore) GetByUserID(ctx context.Context, userID string) (*member.Member, error) {
	if !s.raced {
		return nil, errMemberNotFound
	}
	return s.inner.GetByUserID(ctx, userID)
}

func (s *racingMemberStore) Create(ctx context.Context, m *member.Member) error {
	if !s.raced {
		s.raced = true
		return errors.New("create member: Error 1062 (23000): Duplicate entry 'tenant-a-admin' for key 'PRIMARY'")
	}
	return s.inner.Create(ctx, m)
}

// TestEnsureInitialOwnerToleratesConcurrentBootstrap covers several nodes
// starting against one database at the same time: node A creates the owner,
// node B's create fails on the primary key. The losing node must treat that as
// success (the desired state exists) instead of logging a bootstrap failure and
// leaving the operator to wonder whether the platform has an owner.
func TestEnsureInitialOwnerToleratesConcurrentBootstrap(t *testing.T) {
	ctx := context.Background()
	tenants := tenant.NewManager()
	inner := member.NewMemStore()
	racing := &racingMemberStore{inner: inner}
	members := member.NewManagerWithStore(racing)

	if err := tenants.Create(ctx, &tenant.Tenant{ID: "tenant-a", Name: "tenant-a", Status: tenant.StatusActive}); err != nil {
		t.Fatal(err)
	}
	// The winner: another node already bootstrapped the owner.
	winnerHash, err := bcrypt.GenerateFromPassword([]byte("winner-secret"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := inner.Create(ctx, &member.Member{
		TenantID: "tenant-a",
		UserID:   "admin",
		Role:     member.RoleOwner,
		Password: string(winnerHash),
	}); err != nil {
		t.Fatal(err)
	}

	if err := EnsureInitialOwner(ctx, tenants, members, "tenant-a", "admin", "loser-secret"); err != nil {
		t.Fatalf("losing node reported failure: %v", err)
	}

	got, err := inner.GetByUserID(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(got.Password), []byte("winner-secret")); err != nil {
		t.Fatalf("bootstrap overwrote the winning node's password: %v", err)
	}
}

// TestEnsureInitialOwnerStillReportsRealFailures guards the tolerance above: a
// create that failed for a reason that left no member behind must still surface.
func TestEnsureInitialOwnerStillReportsRealFailures(t *testing.T) {
	ctx := context.Background()
	tenants := tenant.NewManager()
	members := member.NewManagerWithStore(failingMemberStore{Store: member.NewMemStore()})
	if err := tenants.Create(ctx, &tenant.Tenant{ID: "tenant-a", Name: "tenant-a", Status: tenant.StatusActive}); err != nil {
		t.Fatal(err)
	}

	if err := EnsureInitialOwner(ctx, tenants, members, "tenant-a", "admin", "secret"); err == nil {
		t.Fatal("expected a create failure to be reported when no member exists")
	}
}

type failingMemberStore struct {
	member.Store
}

func (s failingMemberStore) GetByUserID(context.Context, string) (*member.Member, error) {
	return nil, errMemberNotFound
}

func (s failingMemberStore) Create(context.Context, *member.Member) error {
	return errors.New("create member: connection refused")
}
