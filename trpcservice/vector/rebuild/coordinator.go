package rebuild

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

// TaskEnqueuer is the narrow transaction-aware enqueue boundary the rebuild
// coordinator consumes. It is satisfied by the durable P1-06C task repository
// through a composition-local adapter; repairs never bypass task fencing.
type TaskEnqueuer interface {
	EnqueueTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error)
	// RedriveTx enqueues the task or re-drives an already succeeded task of
	// the same logical identity back to pending (idempotent projection).
	RedriveTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error)
}

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// Dependencies carries the injected rebuild dependencies. Nil members fail
// closed; there is no fake or in-memory fallback.
type Dependencies struct {
	Pool       *pgxpool.Pool
	Runs       RunRepository
	Tasks      TaskEnqueuer
	Leases     storage.LeaseStore
	Reader     vector.IdentityReader
	Projection vector.ProjectionConfig
	Clock      Clock
}

// Coordinator drives durable rebuild runs and bounded reconciliation. All
// backend work happens outside PostgreSQL transactions except the atomic
// "enqueue batch + advance cursor" transaction.
type Coordinator struct {
	cfg         Config
	deps        Dependencies
	fingerprint string
}

// NewCoordinator fails closed unless the configuration is complete and every
// dependency is present.
func NewCoordinator(cfg Config, deps Dependencies) (*Coordinator, error) {
	cfg, err := cfg.WithDefaults()
	if err != nil {
		return nil, err
	}
	if deps.Pool == nil || deps.Runs == nil || deps.Tasks == nil || deps.Leases == nil || deps.Reader == nil {
		return nil, ErrInvalidConfig
	}
	fingerprint, err := ProjectionFingerprint(deps.Projection)
	if err != nil {
		return nil, err
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}
	return &Coordinator{cfg: cfg, deps: deps, fingerprint: fingerprint}, nil
}

func (c *Coordinator) leaseResource(runID string) string { return "rebuild-" + runID }

type leaseGrant struct {
	lease   storage.Lease
	ref     LeaseRef
	release func() error
}

func (c *Coordinator) acquire(parent context.Context, tc tenant.TenantContext, runID string) (leaseGrant, error) {
	opCtx, cancel := context.WithTimeout(parent, c.cfg.OperationTimeout)
	defer func() { _ = cancel }()
	lease, err := c.deps.Leases.Acquire(opCtx, tc, c.leaseResource(runID), c.cfg.Owner, c.cfg.LeaseTTL)
	if err != nil {
		return leaseGrant{}, ErrUnavailable
	}
	grant := leaseGrant{lease: lease, ref: LeaseRef{Owner: lease.OwnerID, Epoch: lease.Epoch, Fence: lease.FenceToken, ExpiresAt: lease.ExpiresAt}}
	grant.release = func() error {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), c.cfg.OperationTimeout)
		defer releaseCancel()
		return c.deps.Leases.Release(releaseCtx, tc, lease)
	}
	return grant, nil
}

// CreateRun registers a new durable rebuild run for the trusted tenant.
func (c *Coordinator) CreateRun(ctx context.Context, tc tenant.TenantContext, now time.Time) (Run, error) {
	if ctx == nil || tc.Validate() != nil {
		return Run{}, ErrTenantMismatch
	}
	run := Run{
		TenantID: tc.TenantID, RunID: NewRunID(tc.TenantID, c.fingerprint, c.deps.Clock.Now()),
		Fingerprint: c.fingerprint, Mode: "rebuild", Phase: PhasePending,
		MaxAttempts: c.cfg.MaxAttempts,
		DeadlineAt:  c.deps.Clock.Now().Add(c.cfg.RunTimeout),
	}
	return c.deps.Runs.Create(ctx, tc, run, now)
}

// Scan resumes or runs the durable keyset scan. Every batch commits its task
// enqueues and the cursor advance atomically; a crash resumes from the
// durable cursor. Completing the scan only moves the run to scanned: backend
// convergence is proven by bounded reconciliation afterwards.
func (c *Coordinator) Scan(ctx context.Context, tc tenant.TenantContext, runID string) (Run, error) {
	if ctx == nil || tc.Validate() != nil {
		return Run{}, ErrTenantMismatch
	}
	if !validRunID(runID) {
		return Run{}, ErrInvalidRun
	}
	run, err := c.deps.Runs.Get(ctx, tc, runID)
	if err != nil {
		return Run{}, err
	}
	if run.Fingerprint != c.fingerprint {
		return Run{}, ErrInvalidRun
	}
	if run.Phase.Terminal() || run.Phase == PhaseScanned {
		return run, nil
	}
	scanCtx, cancel := context.WithTimeout(ctx, c.cfg.RunTimeout)
	defer cancel()
	if deadline := run.DeadlineAt; !deadline.IsZero() && deadline.Before(c.deps.Clock.Now().Add(c.cfg.OperationTimeout)) {
		scanCtx, cancel = context.WithDeadline(ctx, deadline)
	}
	defer cancel()
	grant, err := c.acquire(scanCtx, tc, runID)
	if err != nil {
		return Run{}, err
	}
	claimed, err := c.deps.Runs.Claim(scanCtx, tc, runID, grant.ref, c.cfg.MaxAttempts, c.deps.Clock.Now())
	if err != nil {
		_ = grant.release()
		return Run{}, err
	}
	cursor := claimed.Cursor
	batches := int64(0)
	for {
		if err := scanCtx.Err(); err != nil {
			_ = grant.release()
			return Run{}, err
		}
		if batches >= c.cfg.MaxBatches {
			_ = grant.release()
			return Run{}, ErrScanBudget
		}
		batches++
		batchCtx, batchCancel := context.WithTimeout(scanCtx, c.cfg.OperationTimeout)
		tx, txErr := c.deps.Pool.Begin(batchCtx)
		if txErr != nil {
			batchCancel()
			_ = grant.release()
			return Run{}, ErrUnavailable
		}
		rows, queryErr := tx.Query(batchCtx, `SELECT memory_id, scope, content, version, source_seq, deleted
			FROM memory WHERE tenant_id=$1 AND memory_id > COALESCE(NULLIF($2,''),'')
			ORDER BY memory_id ASC LIMIT $3`, tc.TenantID, cursor, c.cfg.BatchSize)
		if queryErr != nil {
			batchCancel()
			_ = tx.Rollback(batchCtx)
			_ = grant.release()
			return Run{}, ErrUnavailable
		}
		type memoryRow struct {
			id, scope, content string
			version, sequence  int64
			deleted            bool
		}
		var batch []memoryRow
		for rows.Next() {
			var row memoryRow
			if scanErr := rows.Scan(&row.id, &row.scope, &row.content, &row.version, &row.sequence, &row.deleted); scanErr != nil {
				rows.Close()
				batchCancel()
				_ = tx.Rollback(batchCtx)
				_ = grant.release()
				return Run{}, ErrUnavailable
			}
			batch = append(batch, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			batchCancel()
			_ = tx.Rollback(batchCtx)
			_ = grant.release()
			return Run{}, ErrUnavailable
		}
		rows.Close()
		if len(batch) == 0 {
			if commitErr := tx.Commit(batchCtx); commitErr != nil {
				batchCancel()
				_ = grant.release()
				return Run{}, ErrUnavailable
			}
			batchCancel()
			completed, markErr := c.deps.Runs.MarkScanned(scanCtx, tc, runID, grant.ref, c.deps.Clock.Now())
			if markErr != nil {
				_ = grant.release()
				return Run{}, markErr
			}
			_ = grant.release()
			return completed, nil
		}
		now := c.deps.Clock.Now()
		batchScanned, batchEnqueued, batchTombstoned := int64(len(batch)), int64(0), int64(0)
		for _, row := range batch {
			source := vector.SourceDocument{
				SourceType:      vector.SourceTypeMemory,
				SourceID:        row.id,
				ProjectionScope: "memory:" + row.scope,
				SourceVersion:   row.version,
				SourceSequence:  row.sequence,
				Content:         row.content,
				Deleted:         row.deleted,
				Model:           c.deps.Projection.Model,
				ModelVersion:    c.deps.Projection.ModelVersion,
				Dimension:       c.deps.Projection.Dimension,
				SchemaVersion:   c.deps.Projection.SchemaVersion,
			}
			if row.deleted {
				source.Content = ""
			}
			ref, refErr := vector.BuildDocumentRef(tenant.WithContext(batchCtx, tc), source)
			if refErr != nil {
				batchCancel()
				_ = tx.Rollback(batchCtx)
				_ = grant.release()
				return Run{}, ErrInvalidRun
			}
			if _, taskErr := c.deps.Tasks.EnqueueTx(batchCtx, tx, tc, ref, now); taskErr != nil {
				batchCancel()
				_ = tx.Rollback(batchCtx)
				_ = grant.release()
				return Run{}, ErrUnavailable
			}
			if row.deleted {
				batchTombstoned++
			} else {
				batchEnqueued++
			}
			cursor = row.id
		}
		if advanceErr := c.deps.Runs.AdvanceCursorTx(batchCtx, tx, tc, runID, grant.ref, cursor, batchScanned, batchEnqueued, batchTombstoned, now); advanceErr != nil {
			batchCancel()
			_ = tx.Rollback(batchCtx)
			_ = grant.release()
			return Run{}, advanceErr
		}
		if commitErr := tx.Commit(batchCtx); commitErr != nil {
			batchCancel()
			_ = grant.release()
			return Run{}, ErrUnavailable
		}
		batchCancel()
	}
}

type expectedDocument struct {
	ref     vector.VectorDocumentRef
	deleted bool
	present bool
}

// Reconcile inspects the derived index against authoritative PostgreSQL facts
// and classifies bounded drift. DryRun is the default policy; repairs always
// go through the transactional task boundary, never direct backend writes.
func (c *Coordinator) Reconcile(ctx context.Context, tc tenant.TenantContext, runID string, policy ReconcilePolicy) (Report, error) {
	report, _, _, err := c.reconcileOnce(ctx, tc, runID, policy)
	return report, err
}

func (c *Coordinator) reconcileOnce(ctx context.Context, tc tenant.TenantContext, runID string, policy ReconcilePolicy) (Report, int, LeaseRef, error) {
	report := Report{DryRun: policy.DryRun}
	if ctx == nil || tc.Validate() != nil {
		return report, 0, LeaseRef{}, ErrTenantMismatch
	}
	run, err := c.deps.Runs.Get(ctx, tc, runID)
	if err != nil {
		return report, 0, LeaseRef{}, err
	}
	if run.Fingerprint != c.fingerprint || run.Phase != PhaseScanned {
		return report, 0, LeaseRef{}, ErrNotRunning
	}
	reconcileCtx, cancel := context.WithTimeout(ctx, c.cfg.OperationTimeout)
	defer cancel()
	grant, err := c.acquire(reconcileCtx, tc, runID)
	if err != nil {
		return report, 0, LeaseRef{}, err
	}
	defer func() { _ = grant.release() }()
	claimed, err := c.deps.Runs.Claim(reconcileCtx, tc, runID, grant.ref, c.cfg.MaxAttempts, c.deps.Clock.Now())
	if err != nil {
		return report, 0, LeaseRef{}, err
	}
	_ = claimed
	tcCtx := tenant.WithContext(reconcileCtx, tc)
	expected := make(map[string]vector.VectorDocumentRef)
	var expectedOrder []string
	deletedSet := make(map[string]bool)
	cursor := ""
	inspected := 0
	for inspected < c.cfg.MaxInspectPerRound {
		rows, queryErr := c.deps.Pool.Query(reconcileCtx, `SELECT memory_id, scope, content, version, source_seq, deleted
			FROM memory WHERE tenant_id=$1 AND memory_id > COALESCE(NULLIF($2,''),'')
			ORDER BY memory_id ASC LIMIT $3`, tc.TenantID, cursor, c.cfg.BatchSize)
		if queryErr != nil {
			return report, 0, LeaseRef{}, ErrUnavailable
		}
		count := 0
		for rows.Next() {
			var id, scope, content string
			var version, sequence int64
			var deleted bool
			if scanErr := rows.Scan(&id, &scope, &content, &version, &sequence, &deleted); scanErr != nil {
				rows.Close()
				return report, 0, LeaseRef{}, ErrUnavailable
			}
			cursor = id
			count++
			inspected++
			if deleted {
				content = ""
			}
			source := vector.SourceDocument{
				SourceType: vector.SourceTypeMemory, SourceID: id,
				ProjectionScope: "memory:" + scope, SourceVersion: version,
				SourceSequence: sequence, Content: content, Deleted: deleted,
				Model: c.deps.Projection.Model, ModelVersion: c.deps.Projection.ModelVersion,
				Dimension: c.deps.Projection.Dimension, SchemaVersion: c.deps.Projection.SchemaVersion,
			}
			ref, refErr := vector.BuildDocumentRef(tenant.WithContext(reconcileCtx, tc), source)
			if refErr != nil {
				rows.Close()
				return report, 0, LeaseRef{}, ErrInvalidRun
			}
			expected[ref.DocumentID] = ref
			expectedOrder = append(expectedOrder, ref.DocumentID)
			deletedSet[ref.DocumentID] = deleted
			if inspected >= c.cfg.MaxInspectPerRound {
				break
			}
		}
		rows.Close()
		if count < c.cfg.BatchSize {
			break
		}
	}
	var repairs []vector.VectorDocumentRef
	for start := 0; start < len(expectedOrder); start += c.cfg.BatchSize {
		if reconcileCtx.Err() != nil {
			return report, 0, LeaseRef{}, reconcileCtx.Err()
		}
		end := start + c.cfg.BatchSize
		if end > len(expectedOrder) {
			end = len(expectedOrder)
		}
		chunk := expectedOrder[start:end]
		identities, inspectErr := c.deps.Reader.InspectIdentities(tcCtx, chunk)
		if inspectErr != nil {
			return report, 0, LeaseRef{}, ErrUnavailable
		}
		for _, documentID := range chunk {
			ref := expected[documentID]
			identity, ok := identities[documentID]
			if !ok {
				if deletedSet[documentID] {
					report.add(ClassConsistent, documentID)
					continue
				}
				report.add(ClassMissing, documentID)
				if !policy.DryRun {
					repairs = append(repairs, ref)
				}
				continue
			}
			if identity.Model != c.deps.Projection.Model || identity.ModelVersion != c.deps.Projection.ModelVersion ||
				identity.SchemaVersion != c.deps.Projection.SchemaVersion || identity.Dimension != c.deps.Projection.Dimension ||
				identity.ProjectionScope != ref.ProjectionScope {
				report.add(ClassWrongProjection, documentID)
				if rebuilt, rebuildErr := identity.Ref(); rebuildErr == nil && !policy.DryRun {
					_ = rebuilt
					repairs = append(repairs, ref)
				}
				continue
			}
			if deletedSet[documentID] {
				report.add(ClassTombstonedPresent, documentID)
				if !policy.DryRun {
					repairs = append(repairs, ref)
				}
				continue
			}
			if identity.SourceVersion != ref.SourceVersion || identity.SourceSequence != ref.SourceSequence || identity.ContentHash != ref.ContentHash {
				report.add(ClassStale, documentID)
				if !policy.DryRun {
					repairs = append(repairs, ref)
				}
				continue
			}
			report.add(ClassConsistent, documentID)
		}
	}
	listed, listErr := c.deps.Reader.ListIdentities(tcCtx, c.cfg.MaxInspectPerRound)
	if listErr != nil {
		return report, 0, LeaseRef{}, ErrUnavailable
	}
	for _, identity := range listed {
		if _, ok := expected[identity.DocumentID]; ok {
			continue
		}
		report.add(ClassOrphan, identity.DocumentID)
		if !policy.DryRun && policy.AllowOrphanDelete && c.cfg.AllowOrphanDelete {
			if ref, refErr := c.orphanDeleteRef(identity); refErr == nil {
				repairs = append(repairs, ref)
			}
		}
	}
	repaired := 0
	if !policy.DryRun && len(repairs) > 0 {
		for start := 0; start < len(repairs); start += c.cfg.BatchSize {
			end := start + c.cfg.BatchSize
			if end > len(repairs) {
				end = len(repairs)
			}
			tx, txErr := c.deps.Pool.Begin(reconcileCtx)
			if txErr != nil {
				return report, 0, LeaseRef{}, ErrUnavailable
			}
			for _, ref := range repairs[start:end] {
				if _, enqueueErr := c.deps.Tasks.RedriveTx(reconcileCtx, tx, tc, ref, c.deps.Clock.Now()); enqueueErr != nil {
					_ = tx.Rollback(reconcileCtx)
					return report, 0, LeaseRef{}, ErrUnavailable
				}
				repaired++
			}
			if commitErr := tx.Commit(reconcileCtx); commitErr != nil {
				return report, 0, LeaseRef{}, ErrUnavailable
			}
		}
	}
	report.Repaired = repaired
	return report, repaired, grant.ref, nil
}

// orphanDeleteRef rebuilds the server-owned delete reference from the listed
// identity. It fails closed unless every field validates server-side, so
// orphan repair can never construct a raw expression or cross-tenant task.
func (c *Coordinator) orphanDeleteRef(identity vector.DocumentIdentity) (vector.VectorDocumentRef, error) {
	source := vector.SourceDocument{
		SourceType: identity.SourceType, SourceID: identity.SourceID,
		ProjectionScope: identity.ProjectionScope, SourceVersion: identity.SourceVersion,
		SourceSequence: identity.SourceSequence, Content: "", Deleted: true,
		Model: identity.Model, ModelVersion: identity.ModelVersion,
		Dimension: identity.Dimension, SchemaVersion: identity.SchemaVersion,
	}
	return vector.BuildDocumentRef(tenant.WithContext(context.Background(), tenant.TenantContext{TenantID: identity.TenantID}), source)
}

// ConvergeAndComplete runs bounded reconciliation rounds until zero drift and
// no non-terminal projection tasks remain, then completes the run under the
// lease. The scan phase alone never marks a run successful.
func (c *Coordinator) ConvergeAndComplete(ctx context.Context, tc tenant.TenantContext, runID string, policy ReconcilePolicy) (Run, Report, error) {
	var last Report
	for round := 0; round < c.cfg.ReconcileRounds; round++ {
		if err := ctx.Err(); err != nil {
			return Run{}, last, err
		}
		report, repaired, claimedRef, err := c.reconcileOnce(ctx, tc, runID, policy)
		if err != nil {
			return Run{}, report, err
		}
		last = report
		drift := report.Missing + report.Stale + report.Tombstoned + report.Invalid + report.WrongProj
		if drift == 0 {
			var pending int64
			if err := c.deps.Pool.QueryRow(ctx, `SELECT count(*) FROM vector_projection_task
				WHERE tenant_id=$1 AND status IN ('pending','retry_wait','running')`, tc.TenantID).Scan(&pending); err != nil {
				return Run{}, report, ErrUnavailable
			}
			if pending == 0 {
				run, completeErr := c.deps.Runs.Complete(ctx, tc, runID, claimedRef, c.deps.Clock.Now())
				return run, report, completeErr
			}
		}
		if repaired == 0 && policy.DryRun {
			return Run{}, report, ErrNotConverged
		}
		if round < c.cfg.ReconcileRounds-1 {
			waitCtx, cancel := context.WithTimeout(ctx, c.cfg.ReconcileInterval)
			<-waitCtx.Done()
			cancel()
		}
	}
	return Run{}, last, ErrNotConverged
}
