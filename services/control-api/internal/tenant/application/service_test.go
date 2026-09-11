package application_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

func TestProvisionTenantCreatesActiveTenantWithOwnerAtomically(t *testing.T) {
	now := time.Date(2026, time.August, 31, 14, 0, 0, 0, time.UTC)
	store := &tenantStoreStub{}
	accounts := &accountLookupStub{active: true}
	service := application.NewService(application.Dependencies{
		Store:           store,
		Accounts:        accounts,
		NewTenantID:     func() (string, error) { return "tenant-1", nil },
		NewMembershipID: func() (string, error) { return "membership-1", nil },
		Now:             func() time.Time { return now },
	})

	result, err := service.ProvisionTenant(context.Background(), application.ProvisionTenantCommand{
		Slug: "team-a", Name: "Team A", OwnerUserID: "user-1", ActorUserID: "operator-1",
	})
	if err != nil {
		t.Fatalf("ProvisionTenant() error = %v", err)
	}
	if result.Tenant.ID != "tenant-1" || result.Owner.ID != "membership-1" {
		t.Fatalf("result = %#v", result)
	}
	if result.Tenant.Status != domain.TenantStatusActive || result.Owner.Role != domain.MembershipRoleOwner {
		t.Fatalf("tenant/owner state = %#v/%#v", result.Tenant, result.Owner)
	}
	if store.provisionCalls != 1 {
		t.Fatalf("provision calls = %d, want 1", store.provisionCalls)
	}
}

func TestOwnerAddsExistingActiveMember(t *testing.T) {
	store := &tenantStoreStub{memberships: map[string]domain.Membership{
		"owner": {TenantID: "tenant-1", UserID: "owner", Role: domain.MembershipRoleOwner},
	}}
	service := application.NewService(application.Dependencies{
		Store:           store,
		Accounts:        &accountLookupStub{active: true},
		NewTenantID:     func() (string, error) { return "tenant-2", nil },
		NewMembershipID: func() (string, error) { return "membership-2", nil },
		Now:             time.Now,
	})

	membership, err := service.AddMember(context.Background(), application.AddMemberCommand{
		TenantID: "tenant-1", ActorUserID: "owner", UserID: "user-2",
	})
	if err != nil {
		t.Fatalf("AddMember() error = %v", err)
	}
	if membership.Role != domain.MembershipRoleMember || membership.UserID != "user-2" {
		t.Fatalf("membership = %#v", membership)
	}
}

func TestMemberCannotManageMemberships(t *testing.T) {
	store := &tenantStoreStub{memberships: map[string]domain.Membership{
		"member": {TenantID: "tenant-1", UserID: "member", Role: domain.MembershipRoleMember},
	}}
	service := application.NewService(application.Dependencies{
		Store: store, Accounts: &accountLookupStub{active: true},
		NewTenantID:     func() (string, error) { return "tenant-2", nil },
		NewMembershipID: func() (string, error) { return "membership-2", nil }, Now: time.Now,
	})

	_, err := service.AddMember(context.Background(), application.AddMemberCommand{
		TenantID: "tenant-1", ActorUserID: "member", UserID: "user-2",
	})
	if !errors.Is(err, application.ErrTenantForbidden) {
		t.Fatalf("AddMember() error = %v, want ErrTenantForbidden", err)
	}
	err = service.RemoveMember(context.Background(), application.RemoveMemberCommand{
		TenantID: "tenant-1", ActorUserID: "member", UserID: "user-2",
	})
	if !errors.Is(err, application.ErrTenantForbidden) {
		t.Fatalf("RemoveMember() error = %v, want ErrTenantForbidden", err)
	}
}

func TestOnlyOwnerCanListMembers(t *testing.T) {
	store := &tenantStoreStub{
		memberships: map[string]domain.Membership{
			"owner":  {TenantID: "tenant-1", UserID: "owner", Role: domain.MembershipRoleOwner},
			"member": {TenantID: "tenant-1", UserID: "member", Role: domain.MembershipRoleMember},
		},
		members: []domain.Membership{{TenantID: "tenant-1", UserID: "owner"}},
	}
	service := newTenantService(store, &candidateQueryStub{})

	members, err := service.ListMembers(context.Background(), "tenant-1", "owner")
	if err != nil {
		t.Fatalf("ListMembers(owner) error = %v", err)
	}
	if len(members) != 1 || store.listMembersCalls != 1 {
		t.Fatalf("members/calls = %#v/%d", members, store.listMembersCalls)
	}

	_, err = service.ListMembers(context.Background(), "tenant-1", "member")
	if !errors.Is(err, application.ErrTenantForbidden) {
		t.Fatalf("ListMembers(member) error = %v, want ErrTenantForbidden", err)
	}
	if store.listMembersCalls != 1 {
		t.Fatalf("ListMembers store calls = %d, want 1", store.listMembersCalls)
	}
}

func TestOwnerSearchesPaginatedMemberCandidates(t *testing.T) {
	store := &tenantStoreStub{memberships: map[string]domain.Membership{
		"owner": {TenantID: "tenant-1", UserID: "owner", Role: domain.MembershipRoleOwner},
	}}
	candidates := &candidateQueryStub{result: application.MemberCandidatePage{
		Candidates: []application.MemberCandidate{{
			UserID: "user-2", Username: "Alice", DisplayName: "Alice Example",
		}},
		Total: 1,
	}}
	service := newTenantService(store, candidates)

	result, err := service.SearchMemberCandidates(
		context.Background(),
		application.SearchMemberCandidatesQuery{
			TenantID: "tenant-1", ActorUserID: "owner", Query: "  ALIce  ",
			Page: application.Page{Offset: 2, Limit: 200},
		},
	)
	if err != nil {
		t.Fatalf("SearchMemberCandidates() error = %v", err)
	}
	if candidates.calls != 1 || candidates.tenantID != "tenant-1" || candidates.query != "alice" {
		t.Fatalf("candidate query = calls:%d tenant:%q query:%q", candidates.calls, candidates.tenantID, candidates.query)
	}
	if candidates.page != (application.Page{Offset: 2, Limit: 100}) {
		t.Fatalf("candidate page = %#v", candidates.page)
	}
	if result.Offset != 2 || result.Limit != 100 || result.Total != 1 ||
		len(result.Candidates) != 1 || result.Candidates[0].UserID != "user-2" {
		t.Fatalf("result = %#v", result)
	}
}

func TestMemberCannotSearchMemberCandidates(t *testing.T) {
	store := &tenantStoreStub{memberships: map[string]domain.Membership{
		"member": {TenantID: "tenant-1", UserID: "member", Role: domain.MembershipRoleMember},
	}}
	candidates := &candidateQueryStub{}
	service := newTenantService(store, candidates)

	_, err := service.SearchMemberCandidates(
		context.Background(),
		application.SearchMemberCandidatesQuery{
			TenantID: "tenant-1", ActorUserID: "member", Query: "   ",
		},
	)
	if !errors.Is(err, application.ErrTenantForbidden) {
		t.Fatalf("SearchMemberCandidates() error = %v, want ErrTenantForbidden", err)
	}
	if candidates.calls != 0 {
		t.Fatalf("candidate query calls = %d, want 0", candidates.calls)
	}
	_, err = service.SearchMemberCandidates(
		context.Background(),
		application.SearchMemberCandidatesQuery{
			TenantID: "tenant-1", ActorUserID: "outsider", Query: "alice",
		},
	)
	if !errors.Is(err, application.ErrTenantForbidden) {
		t.Fatalf("SearchMemberCandidates(outsider) error = %v, want ErrTenantForbidden", err)
	}
	if candidates.calls != 0 {
		t.Fatalf("candidate query calls = %d, want 0", candidates.calls)
	}
}

func TestOwnerMustProvideValidMemberCandidateQuery(t *testing.T) {
	store := &tenantStoreStub{memberships: map[string]domain.Membership{
		"owner": {TenantID: "tenant-1", UserID: "owner", Role: domain.MembershipRoleOwner},
	}}
	for _, query := range []string{"   ", strings.Repeat("界", 257)} {
		candidates := &candidateQueryStub{}
		service := newTenantService(store, candidates)
		_, err := service.SearchMemberCandidates(
			context.Background(),
			application.SearchMemberCandidatesQuery{
				TenantID: "tenant-1", ActorUserID: "owner", Query: query,
			},
		)
		if !errors.Is(err, application.ErrInvalidCandidateQuery) {
			t.Fatalf("SearchMemberCandidates(%q) error = %v, want ErrInvalidCandidateQuery", query, err)
		}
		if candidates.calls != 0 {
			t.Fatalf("candidate query calls = %d, want 0", candidates.calls)
		}
	}
}

func TestOwnerMembershipCannotBeRemovedByOrdinaryRemove(t *testing.T) {
	store := &tenantStoreStub{memberships: map[string]domain.Membership{
		"owner":  {TenantID: "tenant-1", UserID: "owner", Role: domain.MembershipRoleOwner},
		"owner2": {TenantID: "tenant-1", UserID: "owner2", Role: domain.MembershipRoleOwner},
	}}
	service := application.NewService(application.Dependencies{
		Store: store, Accounts: &accountLookupStub{},
		NewTenantID:     func() (string, error) { return "tenant-id", nil },
		NewMembershipID: func() (string, error) { return "membership-id", nil }, Now: time.Now,
	})

	err := service.RemoveMember(context.Background(), application.RemoveMemberCommand{
		TenantID: "tenant-1", ActorUserID: "owner", UserID: "owner2",
	})
	if !errors.Is(err, application.ErrOwnerRequiresTransfer) {
		t.Fatalf("RemoveMember() error = %v, want ErrOwnerRequiresTransfer", err)
	}
}

type tenantStoreStub struct {
	provisionCalls   int
	listMembersCalls int
	tenant           domain.Tenant
	owner            domain.Membership
	memberships      map[string]domain.Membership
	members          []domain.Membership
}

func (s *tenantStoreStub) ProvisionTenant(
	_ context.Context,
	tenant domain.Tenant,
	owner domain.Membership,
) error {
	s.provisionCalls++
	s.tenant = tenant
	s.owner = owner
	return nil
}

func (s *tenantStoreStub) GetTenantMembership(
	_ context.Context,
	_, userID string,
) (domain.TenantMembership, error) {
	membership, ok := s.memberships[userID]
	if !ok {
		return domain.TenantMembership{}, application.ErrMembershipNotFound
	}
	return domain.TenantMembership{
		Tenant:     domain.Tenant{ID: membership.TenantID, Status: domain.TenantStatusActive},
		Membership: membership,
	}, nil
}

func (s *tenantStoreStub) GetMembership(
	_ context.Context,
	_, userID string,
) (domain.Membership, error) {
	membership, ok := s.memberships[userID]
	if !ok {
		return domain.Membership{}, application.ErrMembershipNotFound
	}
	return membership, nil
}

func (s *tenantStoreStub) CreateMembership(_ context.Context, membership domain.Membership) error {
	if s.memberships == nil {
		s.memberships = make(map[string]domain.Membership)
	}
	s.memberships[membership.UserID] = membership
	return nil
}

func (s *tenantStoreStub) DeleteMembership(context.Context, string, string) error { return nil }

func (s *tenantStoreStub) ListMyTenants(context.Context, string) ([]domain.TenantMembership, error) {
	return nil, nil
}

func (s *tenantStoreStub) ListMembers(context.Context, string) ([]domain.Membership, error) {
	s.listMembersCalls++
	return s.members, nil
}

func (s *tenantStoreStub) ListTenants(context.Context, application.Page) (application.TenantPage, error) {
	return application.TenantPage{}, nil
}

type accountLookupStub struct {
	active bool
	err    error
}

func (s *accountLookupStub) IsActiveAccount(context.Context, string) (bool, error) {
	return s.active, s.err
}

type candidateQueryStub struct {
	calls    int
	tenantID string
	query    string
	page     application.Page
	result   application.MemberCandidatePage
	err      error
}

func (s *candidateQueryStub) SearchMemberCandidates(
	_ context.Context,
	tenantID, query string,
	page application.Page,
) (application.MemberCandidatePage, error) {
	s.calls++
	s.tenantID = tenantID
	s.query = query
	s.page = page
	return s.result, s.err
}

func newTenantService(
	store *tenantStoreStub,
	candidates application.MemberCandidateQuery,
) *application.Service {
	return application.NewService(application.Dependencies{
		Store: store, Accounts: &accountLookupStub{active: true}, Candidates: candidates,
		NewTenantID:     func() (string, error) { return "tenant-id", nil },
		NewMembershipID: func() (string, error) { return "membership-id", nil },
		Now:             time.Now,
	})
}
