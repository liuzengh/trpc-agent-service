package artifact_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
	objectmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/inmemory"
)

func TestObjectLifecycleReconcilerDeletesOnlyUnreferencedUploads(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	orphan, orphanObject := lifecycleFixture(t, "tenant-a", "request-a", 0, []byte("orphan"), now)
	referenced, referencedObject := lifecycleFixture(t, "tenant-a", "request-b", 0, []byte("referenced"), now)
	objects := objectmemory.New()
	for _, value := range []objectstore.Object{orphanObject, referencedObject} {
		if _, err := objects.PutObject(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	store := &lifecycleStoreStub{uploads: []artifact.ObjectUpload{orphan, referenced}, referenced: map[string]bool{referenced.ObjectKey: true}}
	reconciler := artifact.ObjectLifecycleReconciler{Store: store, Objects: objects, Owner: "lifecycle-1", Now: func() time.Time { return now }}
	handled, err := reconciler.RunOnce(context.Background())
	if err != nil || handled != 2 || len(store.finished) != 2 || len(store.deferred) != 0 {
		t.Fatalf("handled=%d finished=%d deferred=%d err=%v", handled, len(store.finished), len(store.deferred), err)
	}
	if _, err := objects.GetObject(context.Background(), orphan.TenantID, orphan.ObjectKey); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("orphan still present err=%v", err)
	}
	if _, err := objects.GetObject(context.Background(), referenced.TenantID, referenced.ObjectKey); err != nil {
		t.Fatalf("referenced object deleted: %v", err)
	}
}

func TestObjectLifecycleReconcilerDefersDeleteFailure(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	upload, _ := lifecycleFixture(t, "tenant-a", "request-a", 0, []byte("orphan"), now)
	store := &lifecycleStoreStub{uploads: []artifact.ObjectUpload{upload}, referenced: map[string]bool{}}
	reconciler := artifact.ObjectLifecycleReconciler{Store: store, Objects: failingObjectStore{}, Owner: "lifecycle-1",
		Now: func() time.Time { return now }, RetryBackoff: 5 * time.Minute}
	handled, err := reconciler.RunOnce(context.Background())
	if !errors.Is(err, runtime.ErrBackendUnavailable) || handled != 0 || len(store.deferred) != 1 ||
		!store.deferred[0].until.Equal(now.Add(5*time.Minute)) || store.deferred[0].errorClass != artifact.CleanupErrorTransient {
		t.Fatalf("handled=%d deferred=%#v err=%v", handled, store.deferred, err)
	}
}

func TestObjectLifecycleReconcilerQuarantinesVersionMismatchImmediately(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	upload, _ := lifecycleFixture(t, "tenant-a", "request-a", 0, []byte("suspicious"), now)
	store := &lifecycleStoreStub{uploads: []artifact.ObjectUpload{upload}, referenceErr: runtime.ErrVersionMismatch}
	var alerted bool
	reconciler := artifact.ObjectLifecycleReconciler{Store: store, Objects: objectmemory.New(), Owner: "lifecycle-1",
		Now: func() time.Time { return now }, OnQuarantined: func(_ context.Context, got artifact.ObjectUpload, cause error) {
			alerted = got.ObjectKey == upload.ObjectKey && errors.Is(cause, runtime.ErrVersionMismatch)
		}}
	handled, err := reconciler.RunOnce(context.Background())
	if !errors.Is(err, runtime.ErrVersionMismatch) || handled != 1 || len(store.deferred) != 0 ||
		len(store.quarantined) != 1 || store.quarantined[0].errorClass != artifact.CleanupErrorVersionMismatch || !alerted {
		t.Fatalf("handled=%d deferred=%#v quarantined=%#v alerted=%t err=%v",
			handled, store.deferred, store.quarantined, alerted, err)
	}
}

func TestObjectLifecycleReconcilerQuarantinesAtAttemptLimit(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	upload, _ := lifecycleFixture(t, "tenant-a", "request-a", 0, []byte("orphan"), now)
	upload.CleanupAttempt = 2
	store := &lifecycleStoreStub{uploads: []artifact.ObjectUpload{upload}}
	reconciler := artifact.ObjectLifecycleReconciler{Store: store, Objects: failingObjectStore{}, Owner: "lifecycle-1",
		Now: func() time.Time { return now }, MaxAttempts: 3}
	handled, err := reconciler.RunOnce(context.Background())
	if !errors.Is(err, runtime.ErrBackendUnavailable) || handled != 1 || len(store.deferred) != 0 ||
		len(store.quarantined) != 1 || store.quarantined[0].errorClass != artifact.CleanupErrorTransient {
		t.Fatalf("handled=%d deferred=%#v quarantined=%#v err=%v", handled, store.deferred, store.quarantined, err)
	}
}

func TestObjectLifecycleReconcilerBoundsExponentialBackoff(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	upload, _ := lifecycleFixture(t, "tenant-a", "request-a", 0, []byte("orphan"), now)
	upload.CleanupAttempt = 6
	store := &lifecycleStoreStub{uploads: []artifact.ObjectUpload{upload}}
	reconciler := artifact.ObjectLifecycleReconciler{Store: store, Objects: failingObjectStore{}, Owner: "lifecycle-1",
		Now: func() time.Time { return now }, RetryBackoff: time.Minute, MaxBackoff: 10 * time.Minute, MaxAttempts: 10}
	_, _ = reconciler.RunOnce(context.Background())
	if len(store.deferred) != 1 || !store.deferred[0].until.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("deferred=%#v", store.deferred)
	}
}

func lifecycleFixture(t *testing.T, tenantID, requestID string, ordinal int, content []byte, now time.Time) (artifact.ObjectUpload, objectstore.Object) {
	t.Helper()
	source := sha256.Sum256([]byte(requestID))
	artifactID, _, err := artifact.StableIdentity(tenantID, requestID, ordinal, hex.EncodeToString(source[:]))
	if err != nil {
		t.Fatal(err)
	}
	objectKey, err := objectstore.StableKey(tenantID, artifactID)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	digestHex := hex.EncodeToString(digest[:])
	upload := artifact.ObjectUpload{TenantID: tenantID, ObjectKey: objectKey, ArtifactID: artifactID, RequestID: requestID,
		ContentDigest: digestHex, ContentSize: int64(len(content)), State: artifact.ObjectCleanupClaimed,
		ProtectUntil: now.Add(-time.Hour), ClaimOwner: "lifecycle-1", ClaimUntil: now.Add(time.Minute), Version: 2}
	return upload, objectstore.Object{TenantID: tenantID, ObjectKey: objectKey, ContentDigest: digestHex, Content: content}
}

type lifecycleStoreStub struct {
	uploads      []artifact.ObjectUpload
	referenced   map[string]bool
	referenceErr error
	finished     []artifact.ObjectUpload
	deferred     []cleanupTransition
	quarantined  []cleanupTransition
}

type cleanupTransition struct {
	until      time.Time
	errorClass string
}

func (s *lifecycleStoreStub) ClaimExpiredObjectUploads(context.Context, time.Time, string, time.Duration, int) ([]artifact.ObjectUpload, error) {
	return append([]artifact.ObjectUpload(nil), s.uploads...), nil
}

func (s *lifecycleStoreStub) ObjectUploadReferenced(_ context.Context, upload artifact.ObjectUpload) (bool, error) {
	return s.referenced[upload.ObjectKey], s.referenceErr
}

func (s *lifecycleStoreStub) FinishObjectUploadCleanup(_ context.Context, upload artifact.ObjectUpload) error {
	s.finished = append(s.finished, upload)
	return nil
}

func (s *lifecycleStoreStub) DeferObjectUploadCleanup(
	_ context.Context, _ artifact.ObjectUpload, until time.Time, errorClass string,
) error {
	s.deferred = append(s.deferred, cleanupTransition{until: until, errorClass: errorClass})
	return nil
}

func (s *lifecycleStoreStub) QuarantineObjectUpload(
	_ context.Context, _ artifact.ObjectUpload, errorClass string, at time.Time,
) error {
	s.quarantined = append(s.quarantined, cleanupTransition{until: at, errorClass: errorClass})
	return nil
}

type failingObjectStore struct{}

func (failingObjectStore) PutObject(context.Context, objectstore.Object) (objectstore.Object, error) {
	return objectstore.Object{}, runtime.ErrBackendUnavailable
}

func (failingObjectStore) GetObject(context.Context, string, string) (objectstore.Object, error) {
	return objectstore.Object{}, runtime.ErrBackendUnavailable
}

func (failingObjectStore) DeleteObject(context.Context, string, string, string) error {
	return runtime.ErrBackendUnavailable
}
