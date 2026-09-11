package backend

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	s3storage "trpc.group/trpc-go/trpc-agent-go/storage/s3"
)

func TestObjectArtifactConcurrentSaveAllocatesUniqueRevisions(t *testing.T) {
	metadata := newFakeArtifactMetadata()
	blobs := newFakeBlobStore()
	service, err := newObjectArtifact(metadata, blobs, "tenant-a", "tenant-a:assistant", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	info := artifact.SessionInfo{AppName: "tenant-a:assistant", UserID: "user-a", SessionID: "session-a"}

	const count = 32
	versions := make(chan int, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			version, err := service.SaveArtifact(context.Background(), info, "report.txt", &artifact.Artifact{Data: []byte{value}})
			versions <- version
			errs <- err
		}(byte(i))
	}
	wg.Wait()
	close(versions)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("save failed: %v", err)
		}
	}
	got := make([]int, 0, count)
	for version := range versions {
		got = append(got, version)
	}
	sort.Ints(got)
	for i := range got {
		if got[i] != i {
			t.Fatalf("revisions = %v", got)
		}
	}
	listed, err := service.ListVersions(context.Background(), info, "report.txt")
	if err != nil || len(listed) != count {
		t.Fatalf("listed versions = %v, err=%v", listed, err)
	}
}

func TestObjectArtifactReconcilesAmbiguousUpload(t *testing.T) {
	metadata := newFakeArtifactMetadata()
	blobs := newFakeBlobStore()
	blobs.putErrAfterWrite = errors.New("timeout after remote commit")
	service, _ := newObjectArtifact(metadata, blobs, "tenant-a", "tenant-a:assistant", 0)
	defer service.Close()
	info := artifact.SessionInfo{AppName: "tenant-a:assistant", UserID: "user-a", SessionID: "session-a"}

	if _, err := service.SaveArtifact(context.Background(), info, "report.txt", &artifact.Artifact{Data: []byte("durable")}); !errors.Is(err, ErrArtifactUnavailable) {
		t.Fatalf("ambiguous save error = %v", err)
	}
	if value, err := service.LoadArtifact(context.Background(), info, "report.txt", nil); err != nil || value != nil {
		t.Fatalf("pending artifact was visible: value=%v err=%v", value, err)
	}
	blobs.putErrAfterWrite = nil
	repaired, err := service.Reconcile(context.Background(), 10)
	if err != nil || repaired != 1 {
		t.Fatalf("reconcile = %d, %v", repaired, err)
	}
	value, err := service.LoadArtifact(context.Background(), info, "report.txt", nil)
	if err != nil || string(value.Data) != "durable" {
		t.Fatalf("reconciled load = %#v, %v", value, err)
	}
}

func TestObjectArtifactReconcilesInterruptedDelete(t *testing.T) {
	metadata := newFakeArtifactMetadata()
	blobs := newFakeBlobStore()
	service, _ := newObjectArtifact(metadata, blobs, "tenant-a", "tenant-a:assistant", 0)
	defer service.Close()
	info := artifact.SessionInfo{AppName: "tenant-a:assistant", UserID: "user-a", SessionID: "session-a"}
	if _, err := service.SaveArtifact(context.Background(), info, "report.txt", &artifact.Artifact{Data: []byte("data")}); err != nil {
		t.Fatal(err)
	}
	blobs.deleteErr = errors.New("temporary S3 failure")
	if err := service.DeleteArtifact(context.Background(), info, "report.txt"); !errors.Is(err, ErrArtifactUnavailable) {
		t.Fatalf("delete error = %v", err)
	}
	if value, err := service.LoadArtifact(context.Background(), info, "report.txt", nil); err != nil || value != nil {
		t.Fatalf("delete-pending artifact was visible: value=%v err=%v", value, err)
	}
	blobs.deleteErr = nil
	if repaired, err := service.Reconcile(context.Background(), 10); err != nil || repaired != 1 {
		t.Fatalf("delete reconcile = %d, %v", repaired, err)
	}
	if blobs.objectCount() != 0 {
		t.Fatalf("orphaned objects = %d", blobs.objectCount())
	}
}

func TestObjectArtifactDetectsBlobCorruptionAndRejectsScopeSpoofing(t *testing.T) {
	metadata := newFakeArtifactMetadata()
	blobs := newFakeBlobStore()
	service, _ := newObjectArtifact(metadata, blobs, "tenant-a", "tenant-a:assistant", 0)
	defer service.Close()
	info := artifact.SessionInfo{AppName: "tenant-a:assistant", UserID: "user-a", SessionID: "session-a"}
	if _, err := service.SaveArtifact(context.Background(), info, "report.txt", &artifact.Artifact{Data: []byte("correct")}); err != nil {
		t.Fatal(err)
	}
	blobs.corruptAll([]byte("wrong"))
	if _, err := service.LoadArtifact(context.Background(), info, "report.txt", nil); !errors.Is(err, ErrArtifactInconsistent) {
		t.Fatalf("corrupt load error = %v", err)
	}
	info.AppName = "tenant-b:assistant"
	if _, err := service.SaveArtifact(context.Background(), info, "report.txt", &artifact.Artifact{Data: []byte("x")}); !errors.Is(err, ErrArtifactInvalid) {
		t.Fatalf("scope spoof error = %v", err)
	}
}

func TestObjectArtifactTracksFailedOrphanCompensation(t *testing.T) {
	metadata := newFakeArtifactMetadata()
	metadata.rejectReady = true
	blobs := newFakeBlobStore()
	blobs.deleteErr = errors.New("delete unavailable")
	service, _ := newObjectArtifact(metadata, blobs, "tenant-a", "tenant-a:assistant", 0)
	defer service.Close()
	info := artifact.SessionInfo{AppName: "tenant-a:assistant", UserID: "user-a", SessionID: "session-a"}
	if _, err := service.SaveArtifact(context.Background(), info, "raced.bin", &artifact.Artifact{Data: []byte("orphan")}); !errors.Is(err, ErrArtifactInconsistent) {
		t.Fatalf("raced save error = %v", err)
	}
	blobs.deleteErr = nil
	if repaired, err := service.Reconcile(context.Background(), 10); err != nil || repaired != 1 {
		t.Fatalf("orphan reconcile = %d, %v", repaired, err)
	}
	if blobs.objectCount() != 0 {
		t.Fatalf("orphaned objects = %d", blobs.objectCount())
	}
}

type fakeArtifactMetadata struct {
	mu          sync.Mutex
	records     map[string]artifactRecord
	rejectReady bool
}

func newFakeArtifactMetadata() *fakeArtifactMetadata {
	return &fakeArtifactMetadata{records: map[string]artifactRecord{}}
}

func fakeRecordKey(scope artifactScope, filename string, revision int) string {
	return artifactLockKey(scope, filename) + "/" + strconv.Itoa(revision)
}

func (m *fakeArtifactMetadata) Reserve(_ context.Context, req artifactReservation) (artifactRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	revision := 0
	for _, record := range m.records {
		if artifactLockKey(record.Scope, record.Filename) == artifactLockKey(req.Scope, req.Filename) && record.Revision >= revision {
			revision = record.Revision + 1
		}
	}
	record := artifactRecord{Scope: req.Scope, Filename: req.Filename, Revision: revision,
		ObjectKey: req.ObjectPrefix + "/" + strconv.Itoa(revision), MediaType: req.MediaType,
		Size: req.Size, SHA256: req.SHA256, State: "upload_pending", UpdatedAt: time.Now().Add(-time.Minute)}
	m.records[fakeRecordKey(req.Scope, req.Filename, revision)] = record
	return record, nil
}

func (m *fakeArtifactMetadata) MarkReady(_ context.Context, record artifactRecord) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fakeRecordKey(record.Scope, record.Filename, record.Revision)
	current, ok := m.records[key]
	if !ok || current.State != "upload_pending" {
		return false, nil
	}
	if m.rejectReady {
		current.State = "deleted"
		m.records[key] = current
		return false, nil
	}
	current.State = "ready"
	m.records[key] = current
	return true, nil
}

func (m *fakeArtifactMetadata) MarkUploadFailed(_ context.Context, record artifactRecord, errorType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fakeRecordKey(record.Scope, record.Filename, record.Revision)
	current := m.records[key]
	if errorType != "put_ambiguous" {
		current.State = "upload_failed"
	}
	m.records[key] = current
	return nil
}

func (m *fakeArtifactMetadata) MarkDeleteRetry(_ context.Context, record artifactRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fakeRecordKey(record.Scope, record.Filename, record.Revision)
	current := m.records[key]
	if current.State == "deleted" {
		current.State = "delete_pending"
		current.UpdatedAt = time.Now().Add(-time.Minute)
		m.records[key] = current
	}
	return nil
}

func (m *fakeArtifactMetadata) Find(_ context.Context, scope artifactScope, filename string, version *int) (artifactRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var selected artifactRecord
	found := false
	for _, record := range m.records {
		if artifactLockKey(record.Scope, record.Filename) != artifactLockKey(scope, filename) || record.State != "ready" {
			continue
		}
		if version != nil && record.Revision != *version {
			continue
		}
		if !found || record.Revision > selected.Revision {
			selected, found = record, true
		}
	}
	return selected, found, nil
}

func (m *fakeArtifactMetadata) ListKeys(_ context.Context, scope artifactScope) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := map[string]struct{}{}
	for _, record := range m.records {
		if record.Scope.TenantID == scope.TenantID && record.Scope.AppName == scope.AppName && record.Scope.UserID == scope.UserID &&
			(record.Scope.SessionID == "" || record.Scope.SessionID == scope.SessionID) && record.State == "ready" {
			set[record.Filename] = struct{}{}
		}
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	return keys, nil
}

func (m *fakeArtifactMetadata) ListVersions(_ context.Context, scope artifactScope, filename string) ([]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var versions []int
	for _, record := range m.records {
		if artifactLockKey(record.Scope, record.Filename) == artifactLockKey(scope, filename) && record.State == "ready" {
			versions = append(versions, record.Revision)
		}
	}
	return versions, nil
}

func (m *fakeArtifactMetadata) MarkDeletePending(_ context.Context, scope artifactScope, filename string) ([]artifactRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var records []artifactRecord
	for key, record := range m.records {
		if artifactLockKey(record.Scope, record.Filename) == artifactLockKey(scope, filename) && record.State != "deleted" {
			record.State = "delete_pending"
			record.UpdatedAt = time.Now().Add(-time.Minute)
			m.records[key] = record
			records = append(records, record)
		}
	}
	return records, nil
}

func (m *fakeArtifactMetadata) MarkDeleted(_ context.Context, records []artifactRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, record := range records {
		key := fakeRecordKey(record.Scope, record.Filename, record.Revision)
		current := m.records[key]
		if current.State == "delete_pending" {
			current.State = "deleted"
			m.records[key] = current
		}
	}
	return nil
}

func (m *fakeArtifactMetadata) ListRecoverable(_ context.Context, tenantID, appName string, _ time.Time, limit int) ([]artifactRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var records []artifactRecord
	for _, record := range m.records {
		if record.Scope.TenantID == tenantID && record.Scope.AppName == appName && (record.State == "upload_pending" || record.State == "delete_pending") {
			records = append(records, record)
			if len(records) == limit {
				break
			}
		}
	}
	return records, nil
}

type fakeBlobStore struct {
	mu               sync.Mutex
	objects          map[string][]byte
	mediaTypes       map[string]string
	putErrAfterWrite error
	deleteErr        error
}

func newFakeBlobStore() *fakeBlobStore {
	return &fakeBlobStore{objects: map[string][]byte{}, mediaTypes: map[string]string{}}
}

func (b *fakeBlobStore) PutObject(_ context.Context, key string, data []byte, mediaType string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = append([]byte(nil), data...)
	b.mediaTypes[key] = mediaType
	return b.putErrAfterWrite
}

func (b *fakeBlobStore) GetObject(_ context.Context, key string) ([]byte, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	if !ok {
		return nil, "", s3storage.ErrNotFound
	}
	return append([]byte(nil), data...), b.mediaTypes[key], nil
}

func (b *fakeBlobStore) ListObjects(_ context.Context, prefix string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var keys []string
	for key := range b.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (b *fakeBlobStore) DeleteObjects(_ context.Context, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deleteErr != nil {
		return b.deleteErr
	}
	for _, key := range keys {
		delete(b.objects, key)
		delete(b.mediaTypes, key)
	}
	return nil
}

func (b *fakeBlobStore) Close() error { return nil }

func (b *fakeBlobStore) corruptAll(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key := range b.objects {
		b.objects[key] = append([]byte(nil), data...)
	}
}

func (b *fakeBlobStore) objectCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.objects)
}
