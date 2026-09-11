package application

import (
	"context"
	"errors"
	"testing"

	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
)

type accessStub struct{ member, owner bool }

func (a accessStub) IsActiveMember(context.Context, string, string) (bool, error) {
	return a.member, nil
}
func (a accessStub) IsActiveOwner(context.Context, string, string) (bool, error) { return a.owner, nil }

type storeStub struct {
	policy governancev1.Policy
	found  bool
}

func (s *storeStub) Get(context.Context, string) (governancev1.Policy, bool, error) {
	return s.policy, s.found, nil
}
func (s *storeStub) Replace(_ context.Context, user, key string, expected int64, digest string, candidate governancev1.Policy) (governancev1.Policy, error) {
	if user != "owner" || key != "request-1" || expected != 0 || len(digest) != 71 {
		return governancev1.Policy{}, errors.New("incorrect command metadata")
	}
	s.policy, s.found = candidate, true
	return candidate, nil
}

func TestDefaultPolicyAndOwnerReplacement(t *testing.T) {
	store := &storeStub{}
	service, err := New(accessStub{member: true, owner: true}, store)
	if err != nil {
		t.Fatal(err)
	}
	p, err := service.Get(context.Background(), "tenant", "member")
	if err != nil || p.Enabled || p.Revision != 0 || p.TenantID != "tenant" {
		t.Fatalf("default: %+v %v", p, err)
	}
	candidate := governancev1.Policy{
		SchemaVersion: 1, Enabled: true,
		IM:        governancev1.IMPolicy{AllowAll: true, Rules: []governancev1.IMRule{}},
		Requests:  governancev1.RequestPolicy{TenantPerMinute: 10, UserPerMinute: 2},
		Execution: governancev1.ExecutionPolicy{MaxConcurrentRuns: 2},
		Tokens:    governancev1.TokenPolicy{PeriodSeconds: 3600, Limit: 10000, ReservationPerRun: 100},
	}
	p, err = service.Replace(context.Background(), "tenant", "owner", "request-1", 0, candidate)
	if err != nil || p.Revision != 1 || p.TenantID != "tenant" {
		t.Fatalf("replacement: %+v %v", p, err)
	}
}

func TestReplacementRequiresOwnerAndCASInput(t *testing.T) {
	service, _ := New(accessStub{member: true}, &storeStub{})
	p := governancev1.Disabled("tenant")
	p.Revision = 1
	if _, err := service.Replace(context.Background(), "tenant", "member", "request-1", 0, p); !errors.Is(err, ErrInvalid) {
		t.Fatalf("client revision must be zero: %v", err)
	}
	p.Revision = 0
	if _, err := service.Replace(context.Background(), "tenant", "member", "request-1", 0, p); !errors.Is(err, ErrForbidden) {
		t.Fatalf("owner authorization: %v", err)
	}
}
