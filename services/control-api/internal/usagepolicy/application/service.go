package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
)

var (
	ErrForbidden   = errors.New("usage policy forbidden")
	ErrInvalid     = errors.New("usage policy invalid")
	ErrConflict    = errors.New("usage policy conflict")
	ErrUnavailable = errors.New("usage policy unavailable")
)

type Access interface {
	IsActiveMember(context.Context, string, string) (bool, error)
	IsActiveOwner(context.Context, string, string) (bool, error)
}
type Store interface {
	Get(context.Context, string) (governancev1.Policy, bool, error)
	Replace(context.Context, string, string, int64, string, governancev1.Policy) (governancev1.Policy, error)
}
type Service struct {
	access Access
	store  Store
}

func New(access Access, store Store) (*Service, error) {
	if access == nil || store == nil {
		return nil, ErrUnavailable
	}
	return &Service{access: access, store: store}, nil
}
func (s *Service) Get(ctx context.Context, tenant, user string) (governancev1.Policy, error) {
	ok, err := s.access.IsActiveMember(ctx, tenant, user)
	if err != nil {
		return governancev1.Policy{}, ErrUnavailable
	}
	if !ok {
		return governancev1.Policy{}, ErrForbidden
	}
	return s.Runtime(ctx, tenant)
}
func (s *Service) Runtime(ctx context.Context, tenant string) (governancev1.Policy, error) {
	p, found, err := s.store.Get(ctx, tenant)
	if err != nil {
		return governancev1.Policy{}, ErrUnavailable
	}
	if !found {
		return governancev1.Disabled(tenant), nil
	}
	if err = p.Validate(); err != nil {
		return governancev1.Policy{}, ErrUnavailable
	}
	return p, nil
}
func (s *Service) Replace(ctx context.Context, tenant, user, key string, expected int64, candidate governancev1.Policy) (governancev1.Policy, error) {
	if len(key) < 8 || len(key) > 128 || expected < 0 || candidate.TenantID != "" && candidate.TenantID != tenant || candidate.Revision != 0 || candidate.SchemaVersion != governancev1.SchemaVersion {
		return governancev1.Policy{}, ErrInvalid
	}
	ok, err := s.access.IsActiveOwner(ctx, tenant, user)
	if err != nil {
		return governancev1.Policy{}, ErrUnavailable
	}
	if !ok {
		return governancev1.Policy{}, ErrForbidden
	}
	candidate.TenantID, candidate.Revision = tenant, expected+1
	if err = candidate.Validate(); err != nil {
		return governancev1.Policy{}, ErrInvalid
	}
	raw, _ := json.Marshal(struct {
		Expected int64               `json:"expected_revision"`
		Policy   governancev1.Policy `json:"policy"`
	}{expected, candidate})
	digestBytes := sha256.Sum256(raw)
	return s.store.Replace(ctx, user, key, expected, "sha256:"+hex.EncodeToString(digestBytes[:]), candidate)
}
