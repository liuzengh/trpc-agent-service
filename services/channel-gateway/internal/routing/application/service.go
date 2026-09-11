// Package application exposes routing's consumer-owned use cases and durable
// store port. Receipts, projection updates, and replay checkpoint are atomic.
package application

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
)

var (
	ErrUnavailable        = domain.ErrUnavailable
	ErrGenerationConflict = domain.ErrGenerationConflict
	ErrInvalidEvent       = domain.ErrInvalidEvent
	ErrProjectionApplyLag = domain.ErrProjectionApplyLag
)

type Store interface {
	BeginReplay(context.Context, domain.ReplaySource) error
	ObserveSource(context.Context, domain.ReplaySource) error
	ApplyFromStream(context.Context, domain.StreamPosition, domain.RouteEvent) error
	Quarantine(context.Context, domain.StreamPosition, domain.QuarantineReason, string) error
	QueryProjectionHealth(context.Context) (domain.ProjectionHealth, error)
	Resolve(context.Context, string, string) (domain.RouteSnapshot, error)
}
type Service struct{ store Store }

func NewService(store Store) (*Service, error) {
	if store == nil {
		return nil, errors.New("routing store is required")
	}
	return &Service{store: store}, nil
}

// Apply intentionally rejects unsequenced writes. Production callers use the
// trusted-stream API; there is no default offline bypass of replay readiness.
func (s *Service) Apply(context.Context, domain.RouteEvent) error {
	return domain.ErrStreamPositionRequired
}
func (s *Service) BeginReplay(ctx context.Context, source domain.ReplaySource) error {
	return s.store.BeginReplay(ctx, source)
}
func (s *Service) ApplyFromStream(ctx context.Context, position domain.StreamPosition, event domain.RouteEvent) error {
	if err := position.Validate(); err != nil {
		return err
	}
	return s.store.ApplyFromStream(ctx, position, event)
}
func (s *Service) Quarantine(ctx context.Context, p domain.StreamPosition, r domain.QuarantineReason, digest string) error {
	if err := r.Validate(); err != nil {
		return err
	}
	return s.store.Quarantine(ctx, p, r, digest)
}
func (s *Service) QueryProjectionHealth(ctx context.Context) (domain.ProjectionHealth, error) {
	return s.store.QueryProjectionHealth(ctx)
}
func (s *Service) Resolve(ctx context.Context, provider, account string) (domain.RouteSnapshot, error) {
	if err := domain.ValidateAccount(provider, account); err != nil {
		return domain.RouteSnapshot{}, err
	}
	return s.store.Resolve(ctx, provider, account)
}

func (s *Service) ResolveFor(ctx context.Context, provider, account string, cohort domain.Cohort) (domain.RouteSnapshot, bool, error) {
	route, err := s.Resolve(ctx, provider, account)
	if err != nil {
		return domain.RouteSnapshot{}, false, err
	}
	return route.Select(cohort)
}

func (s *Service) ObserveSource(ctx context.Context, source domain.ReplaySource) error {
	return s.store.ObserveSource(ctx, source)
}
