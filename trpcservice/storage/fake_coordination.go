package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// FakeCoordinationStore adapts the legacy FakeRepository to the v2 fencing contracts.
// It is intentionally test-only in usage; production failover uses real backends.
type FakeCoordinationStore struct {
	Repository     *FakeRepository
	epochAuthority EpochAuthority
}

func NewFakeCoordinationStore() *FakeCoordinationStore {
	return &FakeCoordinationStore{Repository: NewFakeRepository()}
}

func NewFakeCoordinationStoreWithEpochAuthority(authority EpochAuthority) *FakeCoordinationStore {
	return &FakeCoordinationStore{Repository: NewFakeRepository(), epochAuthority: authority}
}

func (f *FakeCoordinationStore) claimEpoch(ctx context.Context, key DedupKey) (Epoch, error) {
	if f.epochAuthority == nil {
		return 1, nil
	}
	return f.epochAuthority.GetEpoch(ctx, key.TenantID, ClaimEpochResource(key))
}

func (f *FakeCoordinationStore) validateClaimEpoch(ctx context.Context, key DedupKey, epoch Epoch) error {
	if f.epochAuthority == nil {
		if epoch != 1 {
			return ErrEpochRejected
		}
		return nil
	}
	return f.epochAuthority.ValidateEpoch(ctx, key.TenantID, ClaimEpochResource(key), epoch)
}

func (f *FakeCoordinationStore) Claim(ctx context.Context, tc tenant.TenantContext, key DedupKey, ttl time.Duration, owner string) (Claim, error) {
	if err := f.Repository.contextOK(ctx, tc); err != nil {
		return Claim{}, err
	}
	if owner == "" {
		return Claim{}, fmt.Errorf("%w: owner is required", ErrInvalidOwner)
	}
	if ttl <= 0 {
		return Claim{}, fmt.Errorf("%w: ttl must be positive", ErrInvalidArgument)
	}
	if err := ValidateDedupKey(key); err != nil {
		return Claim{}, err
	}
	if key.TenantID != tc.TenantID {
		return Claim{}, ErrTenantMismatch
	}
	authorityEpoch, err := f.claimEpoch(ctx, key)
	if err != nil {
		return Claim{}, err
	}
	now := time.Now().UTC()
	f.Repository.mu.Lock()
	defer f.Repository.mu.Unlock()
	claimKey := dedupKeyString(key)
	if current, ok := f.Repository.claims[claimKey]; ok {
		if current.Status == ClaimCompleted || current.ExpiresAt.After(now) {
			current.Backend, current.Epoch = BackendPostgres, authorityEpoch
			return current, nil
		}
		current.Attempt++
		current.FenceToken++
		current.OwnerID = owner
		current.Status = ClaimAcquired
		current.ClaimedAt = now
		current.ExpiresAt = now.Add(ttl)
		current.Backend, current.Epoch = BackendPostgres, authorityEpoch
		f.Repository.claims[claimKey] = current
		return current, nil
	}
	claim := Claim{Key: key, Status: ClaimAcquired, OwnerID: owner, Attempt: 1, FenceToken: 1, ClaimedAt: now, ExpiresAt: now.Add(ttl), Backend: BackendPostgres, Epoch: authorityEpoch}
	f.Repository.claims[claimKey] = claim
	return claim, nil
}

func (f *FakeCoordinationStore) Complete(ctx context.Context, tc tenant.TenantContext, key DedupKey, owner, response string, guard OperationGuard) error {
	if guard.Epoch == 0 || guard.Backend != BackendPostgres {
		return ErrEpochRejected
	}
	if err := f.validateClaimEpoch(ctx, key, guard.Epoch); err != nil {
		return err
	}
	if guard.OwnerID != owner || guard.FenceToken == 0 {
		return ErrFenceRejected
	}
	return f.Repository.Complete(ctx, tc, key, response, guard.FenceToken)
}

func (f *FakeCoordinationStore) Fail(ctx context.Context, tc tenant.TenantContext, key DedupKey, owner string, guard OperationGuard, retryable bool) error {
	if guard.Epoch == 0 || guard.Backend != BackendPostgres {
		return ErrEpochRejected
	}
	if err := f.validateClaimEpoch(ctx, key, guard.Epoch); err != nil {
		return err
	}
	if guard.OwnerID != owner || guard.FenceToken == 0 {
		return ErrFenceRejected
	}
	return f.Repository.Fail(ctx, tc, key, guard.FenceToken, retryable)
}

var _ ClaimStore = (*FakeCoordinationStore)(nil)
