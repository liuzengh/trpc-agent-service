package artifact

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultArtifactCleanupBatchSize    = 32
	defaultArtifactCleanupLease        = time.Minute
	defaultArtifactCleanupPendingAge   = 15 * time.Minute
	defaultArtifactCleanupRetryInitial = time.Second
	defaultArtifactCleanupRetryMax     = 5 * time.Minute
)

// CleanupCandidate is an SQL-claimed object that is no longer readable from
// the platform metadata path. Record.ConfigVersion identifies the immutable
// backend configuration that owns the object store; it is resolved by the
// worker and is never accepted from an external request.
type CleanupCandidate struct {
	Record   Record
	Attempts int
}

// Validate checks the minimum information required before an exact object
// delete is attempted.
func (c CleanupCandidate) Validate() error {
	if c.Record.ID == "" || c.Record.TenantID == "" || c.Record.AppID == "" ||
		c.Record.SessionPrincipalID == "" || c.Record.SessionID == "" ||
		c.Record.Filename == "" || c.Record.ObjectKey == "" {
		return errors.New("artifact cleanup candidate is incomplete")
	}
	if c.Record.ConfigVersion == "" {
		return errors.New("artifact cleanup config version is required")
	}
	if c.Attempts <= 0 {
		return errors.New("artifact cleanup attempts must be positive")
	}
	return nil
}

// CleanupStore is the durable metadata boundary for cleanup leases. Claiming
// and reference checks happen in SQL; object storage is never scanned to
// discover platform state.
type CleanupStore interface {
	ClaimArtifactCleanup(context.Context, string, time.Time, *time.Time, time.Duration, int) ([]CleanupCandidate, error)
	CompleteArtifactCleanup(context.Context, CleanupCandidate, string) error
	RetryArtifactCleanup(context.Context, CleanupCandidate, string, time.Time, error) error
}

// InboundCleanupCandidate is the durable identity of one pre-admission object.
// Unlike a session artifact, it has no session lane until admission attaches
// it, so its binding/message/item tuple is the ownership boundary.
type InboundCleanupCandidate struct {
	TenantID          string
	AppID             string
	BindingID         string
	ExternalMessageID string
	ItemNo            int
	ArtifactRef       string
	ConfigVersion     string
	ObjectKey         string
	Attempts          int
}

func (c InboundCleanupCandidate) Validate() error {
	if c.TenantID == "" || c.AppID == "" || c.BindingID == "" ||
		c.ExternalMessageID == "" || c.ArtifactRef == "" ||
		c.ConfigVersion == "" || c.ObjectKey == "" {
		return errors.New("inbound artifact cleanup candidate is incomplete")
	}
	if c.ItemNo < 0 {
		return errors.New("inbound artifact cleanup item number must not be negative")
	}
	if c.Attempts <= 0 {
		return errors.New("inbound artifact cleanup attempts must be positive")
	}
	return nil
}

// InboundCleanupStore is optional so existing cleanup users can continue to
// manage session artifacts only. Production PostgreSQL implements both paths.
type InboundCleanupStore interface {
	ClaimInboundArtifactCleanup(context.Context, string, time.Time, time.Duration, int) ([]InboundCleanupCandidate, error)
	CompleteInboundArtifactCleanup(context.Context, InboundCleanupCandidate, string) error
	RetryInboundArtifactCleanup(context.Context, InboundCleanupCandidate, string, time.Time, error) error
}

// ObjectDeleter removes exactly the object named by one SQL candidate.
type ObjectDeleter func(context.Context, CleanupCandidate) error

type InboundObjectDeleter func(context.Context, InboundCleanupCandidate) error

// CleanupOptions controls the bounded background cleanup worker.
type CleanupOptions struct {
	Owner               string
	PendingAge          time.Duration
	RetentionAge        time.Duration
	LeaseDuration       time.Duration
	BatchSize           int
	RetryInitial        time.Duration
	RetryMax            time.Duration
	Now                 func() time.Time
	InboundDeleteObject InboundObjectDeleter
}

// CleanupWorker deletes SQL-authorized orphan, deleted, and expired artifact
// objects. A failed delete is returned to the durable retry queue; a single
// tenant/backend failure does not stop the rest of the batch.
type CleanupWorker struct {
	store        CleanupStore
	deleteObject ObjectDeleter
	options      CleanupOptions
}

// NewCleanupWorker creates one process-owned artifact cleanup worker.
func NewCleanupWorker(store CleanupStore, deleteObject ObjectDeleter, options CleanupOptions) (*CleanupWorker, error) {
	if store == nil || deleteObject == nil {
		return nil, errors.New("artifact cleanup dependencies are required")
	}
	if options.Owner == "" {
		return nil, errors.New("artifact cleanup owner is required")
	}
	if options.PendingAge <= 0 {
		options.PendingAge = defaultArtifactCleanupPendingAge
	}
	if options.RetentionAge < 0 {
		return nil, errors.New("artifact cleanup retention age must not be negative")
	}
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = defaultArtifactCleanupLease
	}
	if options.BatchSize <= 0 {
		options.BatchSize = defaultArtifactCleanupBatchSize
	}
	if options.RetryInitial <= 0 {
		options.RetryInitial = defaultArtifactCleanupRetryInitial
	}
	if options.RetryMax <= 0 {
		options.RetryMax = defaultArtifactCleanupRetryMax
	}
	if options.RetryMax < options.RetryInitial {
		return nil, errors.New("artifact cleanup retry max must not be less than retry initial")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &CleanupWorker{store: store, deleteObject: deleteObject, options: options}, nil
}

// RunPass claims and processes one bounded batch.
func (w *CleanupWorker) RunPass(ctx context.Context) (int, error) {
	if w == nil || w.store == nil || w.deleteObject == nil {
		return 0, errors.New("artifact cleanup worker is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := w.options.Now().UTC()
	pendingBefore := now.Add(-w.options.PendingAge)
	var retentionBefore *time.Time
	if w.options.RetentionAge > 0 {
		value := now.Add(-w.options.RetentionAge)
		retentionBefore = &value
	}
	candidates, err := w.store.ClaimArtifactCleanup(
		ctx,
		w.options.Owner,
		pendingBefore,
		retentionBefore,
		w.options.LeaseDuration,
		w.options.BatchSize,
	)
	if err != nil {
		return 0, fmt.Errorf("claim artifact cleanup: %w", err)
	}
	deleted := 0
	var failures []error
	for _, candidate := range candidates {
		if err := candidate.Validate(); err != nil {
			failures = append(failures, fmt.Errorf("artifact %s: %w", candidate.Record.ID, err))
			continue
		}
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if err := w.deleteObject(ctx, candidate); err != nil {
			if ctx.Err() != nil {
				return deleted, ctx.Err()
			}
			retryAt := w.options.Now().UTC().Add(w.retryDelay(candidate.Attempts))
			if retryErr := w.store.RetryArtifactCleanup(ctx, candidate, w.options.Owner, retryAt, err); retryErr != nil {
				failures = append(failures, fmt.Errorf("retry artifact %s cleanup: %w", candidate.Record.ID, retryErr))
			} else {
				failures = append(failures, fmt.Errorf("artifact %s cleanup: %w", candidate.Record.ID, err))
			}
			continue
		}
		if err := w.store.CompleteArtifactCleanup(ctx, candidate, w.options.Owner); err != nil {
			failures = append(failures, fmt.Errorf("complete artifact %s cleanup: %w", candidate.Record.ID, err))
			continue
		}
		deleted++
	}
	inboundDeleted, inboundErr := w.runInboundPass(ctx, pendingBefore)
	return deleted + inboundDeleted, errors.Join(errors.Join(failures...), inboundErr)
}

func (w *CleanupWorker) runInboundPass(ctx context.Context, pendingBefore time.Time) (int, error) {
	store, ok := w.store.(InboundCleanupStore)
	if !ok || w.options.InboundDeleteObject == nil {
		return 0, nil
	}
	candidates, err := store.ClaimInboundArtifactCleanup(
		ctx, w.options.Owner, pendingBefore, w.options.LeaseDuration, w.options.BatchSize,
	)
	if err != nil {
		return 0, fmt.Errorf("claim inbound artifact cleanup: %w", err)
	}
	deleted := 0
	var failures []error
	for _, candidate := range candidates {
		if err := candidate.Validate(); err != nil {
			failures = append(failures, fmt.Errorf("inbound artifact %s: %w", candidate.ArtifactRef, err))
			continue
		}
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if err := w.options.InboundDeleteObject(ctx, candidate); err != nil {
			if ctx.Err() != nil {
				return deleted, ctx.Err()
			}
			retryAt := w.options.Now().UTC().Add(w.retryDelay(candidate.Attempts))
			if retryErr := store.RetryInboundArtifactCleanup(ctx, candidate, w.options.Owner, retryAt, err); retryErr != nil {
				failures = append(failures, fmt.Errorf("retry inbound artifact %s cleanup: %w", candidate.ArtifactRef, retryErr))
			} else {
				failures = append(failures, fmt.Errorf("inbound artifact %s cleanup: %w", candidate.ArtifactRef, err))
			}
			continue
		}
		if err := store.CompleteInboundArtifactCleanup(ctx, candidate, w.options.Owner); err != nil {
			failures = append(failures, fmt.Errorf("complete inbound artifact %s cleanup: %w", candidate.ArtifactRef, err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(failures...)
}

// Run keeps cleanup alive until the supplied context is canceled.
func (w *CleanupWorker) Run(ctx context.Context, pollInterval time.Duration) error {
	if w == nil {
		return errors.New("artifact cleanup worker is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if pollInterval <= 0 {
		pollInterval = time.Minute
	}
	for {
		if _, err := w.RunPass(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		if err := waitCleanup(ctx, pollInterval); err != nil {
			return err
		}
	}
}

func (w *CleanupWorker) retryDelay(attempts int) time.Duration {
	delay := w.options.RetryInitial
	for i := 1; i < attempts && delay < w.options.RetryMax; i++ {
		delay *= 2
	}
	if delay > w.options.RetryMax {
		return w.options.RetryMax
	}
	return delay
}

func waitCleanup(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
