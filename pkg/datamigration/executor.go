package datamigration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// Executor advances at most one bounded batch per call. Callers can run it
// from a queue worker and observe each checkpoint; a process crash loses no
// already committed target batch because the source cursor is advanced only
// after Upsert returns successfully.
type Executor struct {
	Store     Store
	Source    Source
	Target    Target
	Hooks     Hooks
	Owner     string
	LeaseTTL  time.Duration
	BatchSize int
}

// migrationPersistenceTimeout bounds cleanup writes after a caller has
// cancelled a migration step. Lease release is a durable safety operation: it
// must not inherit an already-cancelled request context, but it also must not
// wait forever on a failed database. The timeout is deliberately local to this
// package so every public executor operation uses the same bound.
const migrationPersistenceTimeout = 5 * time.Second

func migrationPersistenceContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(parent), migrationPersistenceTimeout)
}

func (e *Executor) validate() error {
	if e == nil || isNilMigrationDependency(e.Store) || isNilMigrationDependency(e.Source) || isNilMigrationDependency(e.Target) {
		return fmt.Errorf("%w: store, source and target are required", ErrMigrationCapability)
	}
	if _, ok := e.Store.(LeaseRenewer); !ok {
		return fmt.Errorf("%w: store lease renewal is required", ErrMigrationCapability)
	}
	if err := validateMigrationLease(e.Owner, e.LeaseTTL); err != nil {
		return err
	}
	if e.BatchSize <= 0 || e.BatchSize > 10000 {
		return fmt.Errorf("%w: batch size must be 1..10000", ErrInvalidMigration)
	}
	return nil
}

func isNilMigrationDependency(value interface{}) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// RunOnce claims a migration and performs one state-machine step. It returns
// the latest durable job even when the step made no phase transition.
func (e *Executor) RunOnce(ctx context.Context, id string) (result Job, resultErr error) {
	if err := e.validate(); err != nil {
		return Job{}, err
	}
	ctx = nonNilMigrationContext(ctx)
	job, err := e.Store.Claim(ctx, id, e.Owner, e.LeaseTTL)
	if err != nil {
		return Job{}, err
	}
	release := func() error {
		releaseCtx, cancelRelease := migrationPersistenceContext(ctx)
		defer cancelRelease()
		return e.Store.Release(releaseCtx, id, e.Owner, job.LeaseVersion)
	}
	defer func() { resultErr = errors.Join(resultErr, release()) }()
	if job.Paused {
		return job, ErrMigrationPaused
	}
	stepErr := e.withLease(ctx, job, func(leaseCtx context.Context) error {
		return e.step(leaseCtx, job)
	})
	if stepErr != nil {
		message := sanitizeMigrationErrorText(stepErr.Error())
		_, persistErr := e.Store.Advance(ctx, id, e.Owner, job.LeaseVersion, JobPatch{LastError: &message})
		if persistErr != nil && !errors.Is(persistErr, ErrMigrationFence) {
			return Job{}, errors.Join(stepErr, fmt.Errorf("persist migration error: %w", persistErr))
		}
		return job, stepErr
	}
	latest, err := e.Store.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	return latest, nil
}

func (e *Executor) step(ctx context.Context, job Job) error {
	ctx = withLeaseFence(ctx, job)
	leaseFence, err := LeaseFenceFromContext(ctx)
	if err != nil {
		return err
	}
	switch job.Phase {
	case PhasePrepare:
		if e.Hooks.Prepare == nil {
			return fmt.Errorf("%w: prepare hook", ErrMigrationCapability)
		}
		if err := e.Hooks.Prepare(ctx, job, leaseFence); err != nil {
			return err
		}
		next := PhaseSnapshotCopy
		_, err := e.Store.Advance(ctx, job.ID, e.Owner, job.LeaseVersion, JobPatch{Phase: &next, LastError: stringPtr("")})
		return err

	case PhaseSnapshotCopy:
		batch, err := e.Source.Snapshot(ctx, job.TenantID, job.Domain, job.Cursor, e.BatchSize)
		if err != nil {
			return fmt.Errorf("snapshot source: %w", err)
		}
		if len(batch.Records) > e.BatchSize {
			return fmt.Errorf("%w: source returned %d records", ErrInvalidRecord, len(batch.Records))
		}
		if err := batch.Validate(job.Cursor, job.SnapshotWatermark); err != nil {
			return err
		}
		if len(batch.Records) > 0 {
			if err := e.Target.Upsert(ctx, job.TenantID, job.Domain, leaseFence, batch.Records); err != nil {
				return fmt.Errorf("snapshot target upsert: %w", err)
			}
		}
		patch := JobPatch{Cursor: &batch.NextCursor, SnapshotWatermark: &batch.Watermark, AppliedWatermark: &batch.Watermark, LastError: stringPtr("")}
		if batch.Done {
			next := PhaseDualWrite
			patch.Phase = &next
		}
		_, err = e.Store.Advance(ctx, job.ID, e.Owner, job.LeaseVersion, patch)
		return err

	case PhaseDualWrite:
		if e.Hooks.EnableDualWrite == nil {
			return fmt.Errorf("%w: dual-write hook", ErrMigrationCapability)
		}
		if err := e.Hooks.EnableDualWrite(ctx, job, leaseFence); err != nil {
			return err
		}
		next := PhaseCatchUp
		// Snapshot and change streams have independent cursors. The durable
		// cursor is reset exactly once at this phase boundary so a restart
		// cannot accidentally feed the snapshot cursor to Changes.
		catchUpCursor := ""
		_, err := e.Store.Advance(ctx, job.ID, e.Owner, job.LeaseVersion, JobPatch{
			Phase: &next, Cursor: &catchUpCursor, LastError: stringPtr(""),
		})
		return err

	case PhaseCatchUp:
		batch, err := e.Source.Changes(ctx, job.TenantID, job.Domain, job.AppliedWatermark, e.BatchSize)
		if err != nil {
			return fmt.Errorf("catch-up source: %w", err)
		}
		if len(batch.Records) > e.BatchSize {
			return fmt.Errorf("%w: source returned %d records", ErrInvalidRecord, len(batch.Records))
		}
		if err := batch.Validate(job.Cursor, job.AppliedWatermark); err != nil {
			return err
		}
		// Changes is keyed only by the applied watermark; it has no cursor
		// argument with which to resume an in-progress page. A non-terminal
		// batch that leaves the watermark unchanged would therefore be queried
		// again forever, even if it reports a fresh opaque cursor.
		if !batch.Done && batch.Watermark <= job.AppliedWatermark {
			return ErrCursorStalled
		}
		if len(batch.Records) > 0 {
			if err := e.Target.Upsert(ctx, job.TenantID, job.Domain, leaseFence, batch.Records); err != nil {
				return fmt.Errorf("catch-up target upsert: %w", err)
			}
		}
		patch := JobPatch{Cursor: &batch.NextCursor, AppliedWatermark: &batch.Watermark, LastError: stringPtr("")}
		if batch.Done {
			next := PhaseValidate
			patch.Phase = &next
		}
		_, err = e.Store.Advance(ctx, job.ID, e.Owner, job.LeaseVersion, patch)
		return err

	case PhaseValidate:
		if e.Hooks.Validate == nil {
			return fmt.Errorf("%w: validate hook", ErrMigrationCapability)
		}
		if err := e.Hooks.Validate(ctx, job, leaseFence); err != nil {
			return err
		}
		next := PhaseReadShadow
		_, err := e.Store.Advance(ctx, job.ID, e.Owner, job.LeaseVersion, JobPatch{Phase: &next, LastError: stringPtr("")})
		return err

	case PhaseReadShadow:
		if e.Hooks.ShadowRead == nil {
			return fmt.Errorf("%w: shadow-read hook", ErrMigrationCapability)
		}
		if err := e.Hooks.ShadowRead(ctx, job, leaseFence); err != nil {
			return err
		}
		next := PhaseCutover
		_, err := e.Store.Advance(ctx, job.ID, e.Owner, job.LeaseVersion, JobPatch{Phase: &next, LastError: stringPtr("")})
		return err

	case PhaseCutover:
		if e.Hooks.Cutover == nil {
			return fmt.Errorf("%w: cutover hook", ErrMigrationCapability)
		}
		if err := e.Hooks.Cutover(ctx, job, leaseFence); err != nil {
			return err
		}
		next := PhaseRollbackWindow
		_, err := e.Store.Advance(ctx, job.ID, e.Owner, job.LeaseVersion, JobPatch{Phase: &next, LastError: stringPtr("")})
		return err

	case PhaseRollbackWindow:
		return ErrApprovalRequired
	case PhaseComplete, PhaseRolledBack:
		return ErrMigrationTerminal
	default:
		return fmt.Errorf("%w: unknown phase %q", ErrInvalidMigration, job.Phase)
	}
}

func (e *Executor) withLease(ctx context.Context, job Job, fn func(context.Context) error) (result error) {
	renewer, ok := e.Store.(LeaseRenewer)
	if !ok {
		return fmt.Errorf("%w: store lease renewal is required", ErrMigrationCapability)
	}
	ctx = nonNilMigrationContext(ctx)
	leaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	interval := e.LeaseTTL / 3
	if interval <= 0 {
		return fmt.Errorf("%w: lease TTL is too short for renewal", ErrInvalidMigration)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	leaseErr := make(chan error, 1)
	var stopOnce sync.Once
	stopLoop := func() {
		stopOnce.Do(func() { close(stop) })
		<-done
	}
	defer func() {
		cancel()
		stopLoop()
		if recovered := recover(); recovered != nil {
			panic(recovered)
		}
		select {
		case err := <-leaseErr:
			result = errors.Join(result, err)
		default:
		}
	}()
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				// Tie an in-flight renewal to the lease context. When the step
				// returns normally, cancellation must stop a late renewal rather
				// than turn its context error into a false migration fence.
				renewCtx, renewCancel := context.WithTimeout(leaseCtx, interval/2)
				_, err := renewer.Renew(renewCtx, job.ID, e.Owner, job.LeaseVersion, e.LeaseTTL)
				renewCancel()
				if err != nil {
					if leaseCtx.Err() != nil {
						return
					}
					select {
					case leaseErr <- fmt.Errorf("%w: migration lease renewal: %s", ErrMigrationFence, sanitizeMigrationErrorText(err.Error())):
					default:
					}
					cancel()
					return
				}
			}
		}
	}()
	result = sanitizeMigrationError(fn(leaseCtx))
	return result
}

// Complete performs the irreversible finalization only when an explicit
// operator call supplies the same owner/fence. It is intentionally separate
// from RunOnce so a worker crash cannot silently cut over or delete the source.
func (e *Executor) Complete(ctx context.Context, id string) (result Job, resultErr error) {
	if err := e.validate(); err != nil {
		return Job{}, err
	}
	ctx = nonNilMigrationContext(ctx)
	job, err := e.Store.Claim(ctx, id, e.Owner, e.LeaseTTL)
	if err != nil {
		return Job{}, err
	}
	defer func() {
		releaseCtx, cancelRelease := migrationPersistenceContext(ctx)
		defer cancelRelease()
		resultErr = errors.Join(resultErr, e.Store.Release(releaseCtx, id, e.Owner, job.LeaseVersion))
	}()
	if job.Phase != PhaseRollbackWindow {
		return job, fmt.Errorf("%w: phase is %s", ErrApprovalRequired, job.Phase)
	}
	if e.Hooks.Complete == nil {
		return job, fmt.Errorf("%w: complete hook", ErrMigrationCapability)
	}
	next := PhaseComplete
	stepErr := e.withLease(ctx, job, func(leaseCtx context.Context) error {
		hookCtx := withLeaseFence(leaseCtx, job)
		leaseFence, err := LeaseFenceFromContext(hookCtx)
		if err != nil {
			return err
		}
		if err := e.Hooks.Complete(hookCtx, job, leaseFence); err != nil {
			return err
		}
		_, err = e.Store.Advance(leaseCtx, id, e.Owner, job.LeaseVersion, JobPatch{Phase: &next, LastError: stringPtr("")})
		return err
	})
	if stepErr != nil {
		return job, stepErr
	}
	latest, err := e.Store.Get(ctx, id)
	if err != nil {
		return job, err
	}
	return latest, nil
}

// Rollback explicitly switches a migration out of its rollback window. The
// source remains available; cleanup is always a separate operator action.
func (e *Executor) Rollback(ctx context.Context, id string) (result Job, resultErr error) {
	if err := e.validate(); err != nil {
		return Job{}, err
	}
	ctx = nonNilMigrationContext(ctx)
	job, err := e.Store.Claim(ctx, id, e.Owner, e.LeaseTTL)
	if err != nil {
		return Job{}, err
	}
	defer func() {
		releaseCtx, cancelRelease := migrationPersistenceContext(ctx)
		defer cancelRelease()
		resultErr = errors.Join(resultErr, e.Store.Release(releaseCtx, id, e.Owner, job.LeaseVersion))
	}()
	if job.Phase != PhaseRollbackWindow {
		return job, fmt.Errorf("%w: rollback is allowed only in rollback window", ErrApprovalRequired)
	}
	if e.Hooks.Rollback == nil {
		return job, fmt.Errorf("%w: rollback hook", ErrMigrationCapability)
	}
	next := PhaseRolledBack
	stepErr := e.withLease(ctx, job, func(leaseCtx context.Context) error {
		hookCtx := withLeaseFence(leaseCtx, job)
		leaseFence, err := LeaseFenceFromContext(hookCtx)
		if err != nil {
			return err
		}
		if err := e.Hooks.Rollback(hookCtx, job, leaseFence); err != nil {
			return err
		}
		_, err = e.Store.Advance(leaseCtx, id, e.Owner, job.LeaseVersion, JobPatch{Phase: &next, LastError: stringPtr("")})
		return err
	})
	if stepErr != nil {
		return job, stepErr
	}
	latest, err := e.Store.Get(ctx, id)
	if err != nil {
		return job, err
	}
	return latest, nil
}

func stringPtr(value string) *string { return &value }
