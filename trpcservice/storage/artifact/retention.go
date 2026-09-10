package artifact

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
)

type RetentionState string

const (
	RetentionActive        RetentionState = "active"
	RetentionDeleteClaimed RetentionState = "delete_claimed"
	RetentionQuarantined   RetentionState = "quarantined"

	RetentionBackendPostgres = "postgres_bytea"
	RetentionBackendObject   = "object"
)

// RetainedArtifact is the minimum durable metadata needed to delete one
// expired Artifact. Content is deliberately excluded from lifecycle claims.
type RetainedArtifact struct {
	TenantID, RequestID, ArtifactID, ContentDigest, Backend, ObjectKey string
	State                                                              RetentionState
	ClaimOwner                                                         string
	ClaimUntil                                                         time.Time
	DeleteAttempt                                                      int
	LastErrorClass                                                     string
	QuarantinedAt                                                      time.Time
	Version                                                            int64
}

type RetentionStore interface {
	// ClaimExpiredArtifacts includes unbound artifacts older than orphanBefore
	// and referenced artifacts only after every durable reference has expired.
	ClaimExpiredArtifacts(context.Context, time.Time, time.Time, string, time.Duration, int) ([]RetainedArtifact, error)
	FinishArtifactDeletion(context.Context, RetainedArtifact) error
	DeferArtifactDeletion(context.Context, RetainedArtifact, time.Time, string) error
	QuarantineArtifactDeletion(context.Context, RetainedArtifact, string, time.Time) error
}

// RetentionReconciler is independent from audit retention and from the
// object-upload orphan reconciler. It deletes only lease-claimed SQL rows.
type RetentionReconciler struct {
	Store         RetentionStore
	Objects       objectstore.Store
	Owner         string
	Now           func() time.Time
	OrphanGrace   time.Duration
	ClaimTTL      time.Duration
	RetryBackoff  time.Duration
	MaxBackoff    time.Duration
	MaxAttempts   int
	BatchSize     int
	PollInterval  time.Duration
	OnError       func(context.Context, error)
	OnQuarantined func(context.Context, RetainedArtifact, error)
}

func (r RetentionReconciler) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	interval := r.PollInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			if r.OnError == nil {
				return err
			}
			r.OnError(ctx, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r RetentionReconciler) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	now := r.now()
	claimTTL := r.ClaimTTL
	if claimTTL <= 0 {
		claimTTL = time.Minute
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = 100
	}
	claimed, err := r.Store.ClaimExpiredArtifacts(ctx, now, now.Add(-r.OrphanGrace), r.Owner, claimTTL, limit)
	if err != nil {
		return 0, err
	}
	handled := 0
	var joined error
	for _, value := range claimed {
		deleteErr := r.deleteContent(ctx, value)
		if deleteErr != nil {
			errorClass := CleanupErrorTransient
			quarantine := value.DeleteAttempt+1 >= r.maxAttempts()
			if errors.Is(deleteErr, runtime.ErrVersionMismatch) {
				errorClass = CleanupErrorVersionMismatch
				quarantine = true
			}
			if quarantine {
				transitionErr := r.Store.QuarantineArtifactDeletion(ctx, value, errorClass, now)
				joined = errors.Join(joined, deleteErr, transitionErr)
				if transitionErr == nil {
					handled++
					if r.OnQuarantined != nil {
						r.OnQuarantined(ctx, value, deleteErr)
					}
				}
				continue
			}
			transitionErr := r.Store.DeferArtifactDeletion(ctx, value, now.Add(r.retryDelay(value.DeleteAttempt)), errorClass)
			joined = errors.Join(joined, deleteErr, transitionErr)
			continue
		}
		if err := r.Store.FinishArtifactDeletion(ctx, value); err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		handled++
	}
	return handled, joined
}

func (r RetentionReconciler) deleteContent(ctx context.Context, value RetainedArtifact) error {
	switch value.Backend {
	case RetentionBackendPostgres:
		if value.ObjectKey != "" {
			return runtime.ErrVersionMismatch
		}
		return nil
	case RetentionBackendObject:
		if r.Objects == nil || value.ObjectKey == "" {
			return runtime.ErrCapabilityUnsupported
		}
		return r.Objects.DeleteObject(ctx, value.TenantID, value.ObjectKey, value.ContentDigest)
	default:
		return runtime.ErrVersionMismatch
	}
}

func (r RetentionReconciler) validate() error {
	if r.Store == nil || r.Owner == "" || r.OrphanGrace <= 0 {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func (r RetentionReconciler) maxAttempts() int {
	if r.MaxAttempts > 0 {
		return r.MaxAttempts
	}
	return 8
}

func (r RetentionReconciler) retryDelay(attempt int) time.Duration {
	base := r.RetryBackoff
	if base <= 0 {
		base = time.Minute
	}
	maximum := r.MaxBackoff
	if maximum <= 0 {
		maximum = time.Hour
	}
	for attempt > 0 && base < maximum {
		if base > maximum/2 {
			return maximum
		}
		base *= 2
		attempt--
	}
	if base > maximum {
		return maximum
	}
	return base
}

func (r RetentionReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}
