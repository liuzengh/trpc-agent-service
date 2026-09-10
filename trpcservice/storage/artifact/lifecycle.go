package artifact

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
)

type ObjectUploadState string

const (
	ObjectUploading      ObjectUploadState = "uploading"
	ObjectCleanupClaimed ObjectUploadState = "cleanup_claimed"
	ObjectQuarantined    ObjectUploadState = "quarantined"
)

const (
	CleanupErrorVersionMismatch = "version_mismatch"
	CleanupErrorTransient       = "transient"
)

// ObjectUpload is a durable protection record created before bytes are sent
// to ObjectStore. It is removed atomically with Artifact metadata commit.
type ObjectUpload struct {
	TenantID, ObjectKey, ArtifactID, RequestID, ContentDigest string
	ContentSize                                               int64
	State                                                     ObjectUploadState
	ProtectUntil, ClaimUntil                                  time.Time
	ClaimOwner                                                string
	CleanupAttempt                                            int
	LastErrorClass                                            string
	QuarantinedAt                                             time.Time
	Version                                                   int64
}

type ObjectLifecycleStore interface {
	ClaimExpiredObjectUploads(context.Context, time.Time, string, time.Duration, int) ([]ObjectUpload, error)
	ObjectUploadReferenced(context.Context, ObjectUpload) (bool, error)
	FinishObjectUploadCleanup(context.Context, ObjectUpload) error
	DeferObjectUploadCleanup(context.Context, ObjectUpload, time.Time, string) error
	QuarantineObjectUpload(context.Context, ObjectUpload, string, time.Time) error
}

// ObjectLifecycleReconciler deletes only expired uploads that have no SQL
// Artifact metadata. The upload intent prevents cleanup from racing the normal
// object-first/metadata-second write path.
type ObjectLifecycleReconciler struct {
	Store         ObjectLifecycleStore
	Objects       objectstore.Store
	Owner         string
	Now           func() time.Time
	ClaimTTL      time.Duration
	RetryBackoff  time.Duration
	MaxBackoff    time.Duration
	MaxAttempts   int
	BatchSize     int
	PollInterval  time.Duration
	OnError       func(context.Context, error)
	OnQuarantined func(context.Context, ObjectUpload, error)
}

func (r ObjectLifecycleReconciler) Run(ctx context.Context) error {
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

func (r ObjectLifecycleReconciler) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	now := r.now()
	if now.IsZero() {
		return 0, runtime.ErrInvariantViolation
	}
	claimTTL := r.ClaimTTL
	if claimTTL <= 0 {
		claimTTL = time.Minute
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = 100
	}
	uploads, err := r.Store.ClaimExpiredObjectUploads(ctx, now, r.Owner, claimTTL, limit)
	if err != nil {
		return 0, err
	}
	handled := 0
	var joined error
	for _, upload := range uploads {
		referenced, referenceErr := r.Store.ObjectUploadReferenced(ctx, upload)
		if referenceErr == nil && !referenced {
			referenceErr = r.Objects.DeleteObject(ctx, upload.TenantID, upload.ObjectKey, upload.ContentDigest)
		}
		if referenceErr != nil {
			errorClass := CleanupErrorTransient
			quarantine := upload.CleanupAttempt+1 >= r.maxAttempts()
			if errors.Is(referenceErr, runtime.ErrVersionMismatch) {
				errorClass = CleanupErrorVersionMismatch
				quarantine = true
			}
			if quarantine {
				quarantineErr := r.Store.QuarantineObjectUpload(ctx, upload, errorClass, now)
				joined = errors.Join(joined, referenceErr, quarantineErr)
				if quarantineErr == nil && r.OnQuarantined != nil {
					r.OnQuarantined(ctx, upload, referenceErr)
				}
				if quarantineErr == nil {
					handled++
				}
				continue
			}
			deferErr := r.Store.DeferObjectUploadCleanup(ctx, upload, now.Add(r.retryBackoff(upload.CleanupAttempt)), errorClass)
			joined = errors.Join(joined, referenceErr, deferErr)
			continue
		}
		if err := r.Store.FinishObjectUploadCleanup(ctx, upload); err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		handled++
	}
	return handled, joined
}

func (r ObjectLifecycleReconciler) maxAttempts() int {
	if r.MaxAttempts > 0 {
		return r.MaxAttempts
	}
	return 8
}

func (r ObjectLifecycleReconciler) retryBackoff(attempt int) time.Duration {
	base := r.RetryBackoff
	if base <= 0 {
		base = time.Minute
	}
	maximum := r.MaxBackoff
	if maximum <= 0 {
		maximum = time.Hour
	}
	if base >= maximum {
		return maximum
	}
	backoff := base
	for i := 0; i < attempt && backoff < maximum; i++ {
		if backoff > maximum/2 {
			return maximum
		}
		backoff *= 2
	}
	if backoff > maximum {
		return maximum
	}
	return backoff
}

func (r ObjectLifecycleReconciler) validate() error {
	if r.Store == nil || r.Objects == nil || r.Owner == "" {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func (r ObjectLifecycleReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}
