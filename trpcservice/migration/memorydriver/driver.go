package memorydriver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const Domain = "memory"

// Exporter and Applier are narrow persistence ports. They keep the migration
// state machine independent of Redis and PostgreSQL SDKs, and let a future
// remote vector/external-memory adapter provide the same evidence shape.
type Exporter interface {
	ExportTenant(context.Context, string) ([]Image, string, error)
}
type Applier interface {
	Apply(context.Context, []Image) (string, error)
}

// Driver advances the backfill and verification stages only. Entering
// dual-write, recording mutations, cutover and rollback remain authority
// operations; this makes it impossible for a bulk-copy process to activate a
// candidate backend by itself.
type Driver struct {
	Authority migration.Repository
	Source    Exporter
	Target    Applier
	Ledger    MutationLedger
	// SourceUser/TargetUser are used only by online dual-write repair.  The
	// bulk exporter stays separate so a SCAN backfill cannot be accidentally
	// used as a point-in-time per-user repair source.
	SourceUser, ReverseUser   UserSnapshotReader
	TargetUser, ReverseTarget UserApplier
}

type RepairRequest struct {
	TenantID, MigrationID, WorkerID string
	Limit                           int
	Now                             time.Time
	Lease, RetryDelay               time.Duration
}

type RepairResult struct{ Claimed, Applied, Retried int }

// Repair drains durable mutation intents after a target write failed.  It
// never enables dual-write itself; the authority state and epoch are checked
// before any target backend is touched.
func (d Driver) Repair(ctx context.Context, in RepairRequest) (RepairResult, error) {
	if d.Authority == nil || d.Ledger == nil || d.SourceUser == nil || d.TargetUser == nil {
		return RepairResult{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, in.TenantID, in.MigrationID)
	if err != nil {
		return RepairResult{}, err
	}
	if current.Domain != Domain || !repairWritable(current.State) || in.WorkerID == "" || in.Limit < 1 ||
		in.Now.IsZero() || in.Lease <= 0 || in.RetryDelay < 0 {
		return RepairResult{}, runtime.ErrInvariantViolation
	}
	claimed, err := d.Ledger.Claim(ctx, ClaimRequest{TenantID: in.TenantID, MigrationID: in.MigrationID,
		WorkerID: in.WorkerID, Limit: in.Limit, Now: in.Now, Lease: in.Lease})
	if err != nil {
		return RepairResult{}, err
	}
	result := RepairResult{Claimed: len(claimed)}
	for _, item := range claimed {
		applyErr := d.applyRepair(ctx, current, item, in.Now)
		if applyErr == nil {
			result.Applied++
			continue
		}
		_, retryErr := d.Ledger.MarkRetry(ctx, RetryRequest{TenantID: item.TenantID, MigrationID: item.MigrationID,
			MutationID: item.MutationID, WorkerID: item.LeaseOwner, Key: item.Key, ExpectedVersion: item.Version,
			ErrorClass: errorClass(applyErr), At: in.Now, NotBefore: in.Now.Add(in.RetryDelay)})
		if retryErr != nil {
			return result, retryErr
		}
		result.Retried++
	}
	return result, nil
}

func (d Driver) applyRepair(ctx context.Context, current migration.Migration, item Mutation, at time.Time) error {
	if item.TenantID != current.TenantID || item.MigrationID != current.MigrationID || item.Epoch != current.Epoch ||
		item.State != StateApplying || item.Key.TenantID != item.TenantID {
		return runtime.ErrInvariantViolation
	}
	reader, target := d.SourceUser, d.TargetUser
	if item.Direction == DirectionReverse {
		reader, target = d.ReverseUser, d.ReverseTarget
	} else if item.Direction != DirectionForward {
		return runtime.ErrInvariantViolation
	}
	if reader == nil || target == nil {
		return runtime.ErrBackendUnavailable
	}
	images, sourceDigest, err := reader.LoadUser(ctx, item.Key)
	if err != nil {
		return err
	}
	if sourceDigest == "" {
		return runtime.ErrInvariantViolation
	}
	targetDigest, err := target.ApplyUser(ctx, item.Key, images)
	if err != nil {
		return err
	}
	if targetDigest != sourceDigest {
		return runtime.ErrInvariantViolation
	}
	_, err = d.Ledger.MarkApplied(ctx, CompleteRequest{TenantID: item.TenantID, MigrationID: item.MigrationID,
		MutationID: item.MutationID, WorkerID: item.LeaseOwner, Key: item.Key, ExpectedVersion: item.Version,
		TargetDigest: targetDigest, At: at})
	return err
}

func repairWritable(state migration.State) bool {
	return state == migration.StateDualWrite || state == migration.StateBackfill || state == migration.StateVerify ||
		state == migration.StateCutover || state == migration.StateObserve
}

func errorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, runtime.ErrInvariantViolation), errors.Is(err, runtime.ErrIdempotencyCollision):
		return "invariant"
	case errors.Is(err, runtime.ErrTenantScope):
		return "tenant_scope"
	default:
		return "target_unavailable"
	}
}

func (d Driver) BackfillOnce(ctx context.Context, tenantID, migrationID string, at time.Time) (migration.BatchResult, error) {
	if d.Authority == nil || d.Source == nil || d.Target == nil {
		return migration.BatchResult{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, tenantID, migrationID)
	if err != nil {
		return migration.BatchResult{}, err
	}
	if current.Domain != Domain || current.State != migration.StateBackfill || current.BackfillComplete || at.IsZero() {
		return migration.BatchResult{}, runtime.ErrInvariantViolation
	}
	images, sourceDigest, err := d.Source.ExportTenant(ctx, tenantID)
	if err != nil {
		return migration.BatchResult{}, err
	}
	targetDigest, err := d.Target.Apply(ctx, images)
	if err != nil {
		return migration.BatchResult{}, err
	}
	if sourceDigest == "" || sourceDigest != targetDigest {
		return migration.BatchResult{}, runtime.ErrInvariantViolation
	}
	batchID := stableBatchID(migrationID, current.NextBatchSeq, sourceDigest)
	return d.Authority.CommitBatch(ctx, migration.BatchRequest{TenantID: tenantID, MigrationID: migrationID, BatchID: batchID,
		Epoch: current.Epoch, ExpectedVersion: current.Version, BatchSeq: current.NextBatchSeq,
		FromCheckpoint: current.BackfillCheckpoint, ToCheckpoint: sourceDigest, Digest: sourceDigest,
		RecordCount: int64(len(images)), Complete: true, CommittedAt: at.UTC()})
}

func (d Driver) EnterVerify(ctx context.Context, tenantID, migrationID string, at time.Time) (migration.Migration, error) {
	if d.Authority == nil || d.Ledger == nil || at.IsZero() {
		return migration.Migration{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, tenantID, migrationID)
	if err != nil {
		return migration.Migration{}, err
	}
	if current.Domain != Domain || current.State != migration.StateBackfill || !current.BackfillComplete {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	outstanding, err := d.Ledger.Outstanding(ctx, tenantID, migrationID)
	if err != nil {
		return migration.Migration{}, err
	}
	if outstanding != 0 {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	return d.Authority.Transition(ctx, migration.TransitionRequest{TenantID: tenantID, MigrationID: migrationID,
		ExpectedVersion: current.Version, To: migration.StateVerify, At: at.UTC()})
}

// Verify compares a fresh tenant export with a target snapshot. The caller is
// required to have enabled durable dual-write before invoking this method;
// otherwise an SCAN export has no stable concurrency meaning.
func (d Driver) Verify(ctx context.Context, tenantID, migrationID string, target Exporter) (migration.Verification, error) {
	if d.Authority == nil || d.Ledger == nil || d.Source == nil || target == nil {
		return migration.Verification{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, tenantID, migrationID)
	if err != nil {
		return migration.Verification{}, err
	}
	if current.Domain != Domain || current.State != migration.StateVerify {
		return migration.Verification{}, runtime.ErrInvariantViolation
	}
	outstanding, err := d.Ledger.Outstanding(ctx, tenantID, migrationID)
	if err != nil {
		return migration.Verification{}, err
	}
	if outstanding != 0 {
		return migration.Verification{}, runtime.ErrInvariantViolation
	}
	source, sourceDigest, err := d.Source.ExportTenant(ctx, tenantID)
	if err != nil {
		return migration.Verification{}, err
	}
	targetImages, targetDigest, err := target.ExportTenant(ctx, tenantID)
	if err != nil {
		return migration.Verification{}, err
	}
	if sourceDigest == "" || sourceDigest != targetDigest || len(source) != len(targetImages) {
		return migration.Verification{}, runtime.ErrInvariantViolation
	}
	return migration.Verification{SourceCount: int64(len(source)), TargetCount: int64(len(targetImages)),
		SourceDigest: sourceDigest, TargetDigest: targetDigest, SourceWatermark: sourceDigest, TargetWatermark: targetDigest,
		SampleDigest: sourceDigest}, nil
}

func stableBatchID(_ string, sequence int64, digest string) string {
	// Batch IDs are already scoped by (tenant_id,migration_id) in the
	// authority. Keeping this bounded avoids letting a user-supplied migration
	// ID exceed the database's 128-byte batch-id contract.
	return fmt.Sprintf("memory-%d-%s", sequence, digest[:16])
}
