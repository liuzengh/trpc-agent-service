package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Store exposes the explicit v2 coordination contracts while Backend retains
// the legacy repository methods used by P0-05 callers.
type Store struct {
	Backend        *Backend
	epochAuthority storage.EpochAuthority
}

func NewStore(b *Backend) *Store { return &Store{Backend: b} }

func NewStoreWithEpochAuthority(b *Backend, authority storage.EpochAuthority) *Store {
	return &Store{Backend: b, epochAuthority: authority}
}

func (s *Store) claimEpoch(ctx context.Context, key storage.DedupKey) (storage.Epoch, error) {
	if s.epochAuthority == nil {
		return 1, nil
	}
	return s.epochAuthority.GetEpoch(ctx, key.TenantID, storage.ClaimEpochResource(key))
}

func (s *Store) validateClaimEpoch(ctx context.Context, key storage.DedupKey, epoch storage.Epoch) error {
	if s.epochAuthority == nil {
		if epoch != 1 {
			return storage.ErrEpochRejected
		}
		return nil
	}
	return s.epochAuthority.ValidateEpoch(ctx, key.TenantID, storage.ClaimEpochResource(key), epoch)
}

func (s *Store) leaseEpoch(ctx context.Context, tc tenant.TenantContext, resource string) (storage.Epoch, error) {
	if s.epochAuthority == nil {
		return 1, nil
	}
	return s.epochAuthority.GetEpoch(ctx, tc.TenantID, resource)
}

func (s *Store) validateLeaseEpoch(ctx context.Context, tc tenant.TenantContext, resource string, epoch storage.Epoch) error {
	if s.epochAuthority == nil {
		if epoch != 1 {
			return storage.ErrEpochRejected
		}
		return nil
	}
	return s.epochAuthority.ValidateEpoch(ctx, tc.TenantID, resource, epoch)
}

func (s *Store) Claim(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, ttl time.Duration, owner string) (storage.Claim, error) {
	epoch, err := s.claimEpoch(ctx, key)
	if err != nil {
		return storage.Claim{}, err
	}
	claim, err := s.Backend.claimWithEpoch(ctx, tc, key, ttl, owner, epoch)
	if err != nil {
		return storage.Claim{}, err
	}
	return claim, nil
}

func (s *Store) Complete(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner, response string, guard storage.OperationGuard) error {
	if guard.Backend != storage.BackendRedis || guard.Epoch == 0 {
		return storage.ErrEpochRejected
	}
	if guard.OwnerID != owner || guard.FenceToken == 0 {
		return storage.ErrFenceRejected
	}
	if err := s.validateClaimEpoch(ctx, key, guard.Epoch); err != nil {
		return err
	}
	return s.Backend.writeClaimOwner(ctx, tc, key, "completed", response, owner, guard.FenceToken)
}

func (s *Store) Fail(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner string, guard storage.OperationGuard, retryable bool) error {
	if guard.Backend != storage.BackendRedis || guard.Epoch == 0 {
		return storage.ErrEpochRejected
	}
	if guard.OwnerID != owner || guard.FenceToken == 0 {
		return storage.ErrFenceRejected
	}
	if err := s.validateClaimEpoch(ctx, key, guard.Epoch); err != nil {
		return err
	}
	status := "completed"
	if retryable {
		status = "acquired"
	}
	return s.Backend.writeClaimOwner(ctx, tc, key, status, "", owner, guard.FenceToken)
}

func mapLeaseError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrLeaseLost) {
		return storage.ErrLeaseLost
	}
	if errors.Is(err, ErrFenceRejected) {
		return fmt.Errorf("%w: %v", storage.ErrFenceRejected, ErrFenceRejected)
	}
	if errors.Is(err, ErrAlreadyClaimed) {
		return storage.ErrLeaseLost
	}
	return err
}

func (s *Store) Acquire(ctx context.Context, tc tenant.TenantContext, resource, owner string, ttl time.Duration) (storage.Lease, error) {
	epoch, err := s.leaseEpoch(ctx, tc, resource)
	if err != nil {
		return storage.Lease{}, err
	}
	l, err := s.Backend.acquireWithEpoch(ctx, tc, LeaseSession, resource, owner, ttl, epoch)
	if err != nil {
		return storage.Lease{}, mapLeaseError(err)
	}
	return storage.Lease{TenantID: tc.TenantID, SessionID: resource, ResourceID: resource, OwnerID: l.OwnerID, FenceToken: l.FenceToken, ExpiresAt: l.ExpiresAt, Backend: storage.BackendRedis, Epoch: epoch}, nil
}

func (s *Store) Renew(ctx context.Context, tc tenant.TenantContext, l storage.Lease, ttl time.Duration) (storage.Lease, error) {
	if l.TenantID != tc.TenantID || l.Backend != storage.BackendRedis || l.Epoch == 0 {
		return l, storage.ErrEpochRejected
	}
	if err := s.validateLeaseEpoch(ctx, tc, l.ResourceID, l.Epoch); err != nil {
		return l, err
	}
	updated, err := s.Backend.Renew(ctx, Lease{Kind: LeaseSession, TenantID: l.TenantID, ResourceID: l.ResourceID, OwnerID: l.OwnerID, FenceToken: l.FenceToken, ExpiresAt: l.ExpiresAt, Epoch: l.Epoch}, ttl)
	if err != nil {
		return l, mapLeaseError(err)
	}
	l.ExpiresAt = updated.ExpiresAt
	return l, nil
}

func (s *Store) Release(ctx context.Context, tc tenant.TenantContext, l storage.Lease) error {
	if l.TenantID != tc.TenantID || l.Backend != storage.BackendRedis || l.Epoch == 0 {
		return storage.ErrEpochRejected
	}
	if err := s.validateLeaseEpoch(ctx, tc, l.ResourceID, l.Epoch); err != nil {
		return err
	}
	return mapLeaseError(s.Backend.Release(ctx, Lease{Kind: LeaseSession, TenantID: l.TenantID, ResourceID: l.ResourceID, OwnerID: l.OwnerID, FenceToken: l.FenceToken, ExpiresAt: l.ExpiresAt, Epoch: l.Epoch}))
}

func (s *Store) Validate(ctx context.Context, tc tenant.TenantContext, l storage.Lease) error {
	if l.TenantID != tc.TenantID || l.Backend != storage.BackendRedis || l.Epoch == 0 {
		return storage.ErrEpochRejected
	}
	if err := s.validateLeaseEpoch(ctx, tc, l.ResourceID, l.Epoch); err != nil {
		return err
	}
	return mapLeaseError(s.Backend.Validate(ctx, Lease{Kind: LeaseSession, TenantID: l.TenantID, ResourceID: l.ResourceID, OwnerID: l.OwnerID, FenceToken: l.FenceToken, ExpiresAt: l.ExpiresAt, Epoch: l.Epoch}))
}

var _ storage.ClaimStore = (*Store)(nil)
var _ storage.LeaseStore = (*Store)(nil)
