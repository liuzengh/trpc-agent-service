package configpub

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// fakeRepository is an in-memory Repository for coordinator unit tests. It
// mirrors the PostgreSQL transactional semantics: CAS on the active revision,
// draft-only activation, previously-published-only rollback targets, and
// committed-operation idempotency.
type fakeRepository struct {
	mu           sync.Mutex
	revisions    map[string]map[int64]Revision // tenantID -> version -> revision
	rollouts     map[string]RolloutState
	operations   map[string]OperationRecord // tenantID|operationID -> record
	ownedHooks   map[string]BindingOwnership
	failNext     error
	failOnKind   string
	clock        time.Time
	createCalls  int
	publishCalls int
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{
		revisions:  map[string]map[int64]Revision{},
		rollouts:   map[string]RolloutState{},
		operations: map[string]OperationRecord{},
		ownedHooks: map[string]BindingOwnership{},
	}
}

func (f *fakeRepository) tick() { f.clock = f.clock.Add(time.Second) }

func (f *fakeRepository) CreateRevision(ctx context.Context, tc tenant.TenantContext, version int64, doc Document, actorCategory string, now time.Time) (Revision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return Revision{}, err
	}
	if _, err := EncodeDocument(doc); err != nil {
		return Revision{}, err
	}
	fingerprint, err := Fingerprint(doc)
	if err != nil {
		return Revision{}, err
	}
	byTenant := f.revisions[tc.TenantID]
	if byTenant == nil {
		byTenant = map[int64]Revision{}
		f.revisions[tc.TenantID] = byTenant
	}
	if existing, ok := byTenant[version]; ok {
		if existing.Fingerprint == fingerprint {
			return existing, nil
		}
		return Revision{}, ErrRevisionState
	}
	for _, rev := range byTenant {
		if rev.Fingerprint == fingerprint {
			return Revision{}, ErrDuplicateContent
		}
	}
	revision := Revision{TenantID: tc.TenantID, Version: version, Status: StatusDraft, Document: doc, Fingerprint: fingerprint, ActorCategory: actorCategory, CreatedAt: now}
	byTenant[version] = revision
	return revision, nil
}

func (f *fakeRepository) Revision(ctx context.Context, tenantID string, version int64) (Revision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rev, ok := f.revisions[tenantID][version]
	if !ok {
		return Revision{}, ErrRevisionNotFound
	}
	return rev, nil
}

func (f *fakeRepository) ValidateRevision(ctx context.Context, tc tenant.TenantContext, version int64, validator Validator, actorCategory string, now time.Time) (Revision, error) {
	f.mu.Lock()
	rev, ok := f.revisions[tc.TenantID][version]
	if !ok {
		f.mu.Unlock()
		return Revision{}, ErrRevisionNotFound
	}
	if rev.Status != StatusDraft {
		f.mu.Unlock()
		return Revision{}, ErrRevisionState
	}
	f.mu.Unlock()

	// Validation may consult the binding checker. Do not hold the repository
	// mutex while making that callback or the in-memory test double deadlocks.
	if err := validator.Validate(ctx, tc.TenantID, rev.Document); err != nil {
		if isUnavailable(err) {
			return Revision{}, err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		rev.Status = StatusRejected
		rev.ReasonCategory = CategoryOf(err)
		if rev.ReasonCategory == "" {
			rev.ReasonCategory = ReasonValidationFailed
		}
		rev.RejectedAt = now
		f.revisions[tc.TenantID][version] = rev
		return rev, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rev.Status = StatusValidated
	rev.ValidatedAt = now
	f.revisions[tc.TenantID][version] = rev
	return rev, nil
}

func (f *fakeRepository) Operation(ctx context.Context, tenantID, operationID string) (OperationRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.operations[tenantID+"|"+operationID]
	return record, ok, nil
}

func (f *fakeRepository) active(tenantID string) (int64, bool) {
	versions := make([]int64, 0, len(f.revisions[tenantID]))
	for version := range f.revisions[tenantID] {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	for _, version := range versions {
		if f.revisions[tenantID][version].Status == StatusPublished {
			return version, true
		}
	}
	return 0, false
}

func (f *fakeRepository) Publish(ctx context.Context, tc tenant.TenantContext, op OperationRecord, percentage int, now time.Time) (OperationRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishCalls++
	if f.failOnKind == OperationPublish {
		return OperationRecord{}, f.failNext
	}
	rev, ok := f.revisions[tc.TenantID][op.TargetVersion]
	if !ok {
		return OperationRecord{}, ErrRevisionNotFound
	}
	if rev.Status != StatusValidated {
		return OperationRecord{}, ErrRevisionState
	}
	active, activeFound := f.active(tc.TenantID)
	if active != op.ExpectedActiveVersion || (!activeFound && op.ExpectedActiveVersion != 0) {
		return OperationRecord{}, ErrStaleExpectedVersion
	}
	if percentage < 100 && !activeFound {
		return OperationRecord{}, ErrInvalidRollout
	}
	if activeFound {
		prev := f.revisions[tc.TenantID][active]
		prev.Status = StatusSuperseded
		prev.SupersededAt = now
		f.revisions[tc.TenantID][active] = prev
	}
	rev.Status = StatusPublished
	rev.PublishedAt = now
	f.revisions[tc.TenantID][op.TargetVersion] = rev
	baseline := active
	if baseline == 0 {
		baseline = op.TargetVersion
	}
	f.rollouts[tc.TenantID] = RolloutState{TenantID: tc.TenantID, ActiveVersion: op.TargetVersion, BaselineVersion: baseline, Percentage: percentage, UpdatedAt: now}
	op.ResultActiveVersion = op.TargetVersion
	op.Outcome = OutcomeCommitted
	f.operations[tc.TenantID+"|"+op.OperationID] = op
	return op, nil
}

func (f *fakeRepository) SetRollout(ctx context.Context, tc tenant.TenantContext, op OperationRecord, percentage int, now time.Time) (OperationRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	active, activeFound := f.active(tc.TenantID)
	if !activeFound || active != op.TargetVersion || active != op.ExpectedActiveVersion {
		return OperationRecord{}, ErrStaleExpectedVersion
	}
	state, ok := f.rollouts[tc.TenantID]
	if !ok {
		return OperationRecord{}, ErrRevisionState
	}
	state.Percentage = percentage
	state.UpdatedAt = now
	f.rollouts[tc.TenantID] = state
	op.ResultActiveVersion = active
	op.Outcome = OutcomeCommitted
	f.operations[tc.TenantID+"|"+op.OperationID] = op
	return op, nil
}

func (f *fakeRepository) Rollback(ctx context.Context, tc tenant.TenantContext, op OperationRecord, now time.Time) (OperationRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	active, activeFound := f.active(tc.TenantID)
	if !activeFound || active != op.ExpectedActiveVersion {
		return OperationRecord{}, ErrStaleExpectedVersion
	}
	if op.TargetVersion == active {
		return OperationRecord{}, ErrRevisionState
	}
	target, ok := f.revisions[tc.TenantID][op.TargetVersion]
	if !ok {
		return OperationRecord{}, ErrRevisionNotFound
	}
	if target.Status != StatusSuperseded && target.Status != StatusRecalled {
		return OperationRecord{}, ErrRevisionState
	}
	current := f.revisions[tc.TenantID][active]
	current.Status = StatusRecalled
	current.RecalledAt = now
	f.revisions[tc.TenantID][active] = current
	target.Status = StatusPublished
	target.PublishedAt = now
	f.revisions[tc.TenantID][op.TargetVersion] = target
	f.rollouts[tc.TenantID] = RolloutState{TenantID: tc.TenantID, ActiveVersion: op.TargetVersion, BaselineVersion: op.TargetVersion, Percentage: 100, UpdatedAt: now}
	op.ResultActiveVersion = op.TargetVersion
	op.Outcome = OutcomeCommitted
	f.operations[tc.TenantID+"|"+op.OperationID] = op
	return op, nil
}

func (f *fakeRepository) Rollout(ctx context.Context, tenantID string) (RolloutState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.rollouts[tenantID]
	return state, ok, nil
}

func (f *fakeRepository) BindingOwnershipFor(ctx context.Context, tenantID, channel, bindingID string) (BindingOwnership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ownership, ok := f.ownedHooks[channel+"|"+bindingID]
	if !ok {
		return BindingOwned, nil // tests default to owned
	}
	return ownership, nil
}

var _ Repository = (*fakeRepository)(nil)
var _ BindingChecker = (*fakeRepository)(nil)
