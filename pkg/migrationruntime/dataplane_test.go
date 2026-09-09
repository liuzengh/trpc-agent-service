package migrationruntime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"
)

type captureArtifactService struct {
	dataPlaneArtifactService
	entry artifactplane.MigrationEntry
	value *artifact.Artifact
	err   error
}

func (s *captureArtifactService) ReadMigrationVersion(context.Context, artifact.SessionInfo, string, int) (artifactplane.MigrationEntry, *artifact.Artifact, error) {
	return s.entry, s.value, s.err
}

func TestArtifactLiveReadAcceptsUnassignedJournalVersion(t *testing.T) {
	entry := artifactplane.MigrationEntry{AppName: "support", UserID: "user", SessionID: "session", Filename: "report.txt", Version: 0, MIMEType: "text/plain"}
	key, err := artifactEntryKey(entry)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"live", "deleted", "absent"} {
		for _, version := range []int64{0, 9, -1} {
			t.Run(fmt.Sprintf("%s/version_%d", state, version), func(t *testing.T) {
				service := &captureArtifactService{entry: entry, value: &artifact.Artifact{Name: entry.Filename, MimeType: entry.MIMEType, Data: []byte("body")}}
				service.entry.Deleted = state == "deleted"
				if state == "absent" {
					service.entry, service.value, service.err = artifactplane.MigrationEntry{}, nil, sql.ErrNoRows
				}
				backend := &artifactLiveBackend{service: service, tenantID: "tenant-a"}
				record, err := backend.Read(context.Background(), key, version)
				if version < 0 {
					if !errors.Is(err, datamigration.ErrInvalidRecord) {
						t.Fatalf("negative version accepted: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := record.Validate(); err != nil {
					t.Fatal(err)
				}
				if record.Key != key || record.Version != version || record.Deleted != (state != "live") {
					t.Fatalf("capture identity or state changed: %+v", record)
				}
				if state == "live" && string(record.Payload) != "body" {
					t.Fatalf("capture body=%q", record.Payload)
				}
			})
		}
	}
}

func TestArtifactLiveReadKeepsUncommittedMIMEIdentityAbsent(t *testing.T) {
	entry := artifactplane.MigrationEntry{AppName: "support", UserID: "user", SessionID: "session", Filename: "report", Version: 0, MIMEType: "text/plain"}
	key, err := artifactEntryKey(entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.MIMEType = "application/json"
	service := &captureArtifactService{entry: entry, value: &artifact.Artifact{MimeType: entry.MIMEType, Data: []byte("{}")}}
	backend := &artifactLiveBackend{service: service, tenantID: "tenant-a"}
	record, err := backend.Read(context.Background(), key, 0)
	if err != nil || !record.Deleted || record.Key != key || len(record.Payload) != 0 {
		t.Fatalf("uncommitted MIME identity became another record: %+v err=%v", record, err)
	}
	actualKey, err := artifactEntryKey(entry)
	if err != nil {
		t.Fatal(err)
	}
	record, err = backend.Read(context.Background(), actualKey, 0)
	if err != nil || record.Deleted || string(record.Payload) != "{}" {
		t.Fatalf("successful retry became absent: %+v err=%v", record, err)
	}
}

type dataPlaneTestCoordinator struct {
	profile  string
	keys     []string
	rejected error
}

func (c *dataPlaneTestCoordinator) WithRead(ctx context.Context, _ string, _ datamigration.Domain, fallback string, fn func(context.Context, string) error) error {
	profile := c.profile
	if profile == "" {
		profile = fallback
	}
	return fn(ctx, profile)
}
func (c *dataPlaneTestCoordinator) WithWrite(ctx context.Context, tenantID string, domain datamigration.Domain, fallback string, keys []string, fn func(context.Context, string) error) error {
	return c.WithWriteKeys(ctx, tenantID, domain, fallback, func(context.Context, string) ([]string, error) { return keys, nil }, fn)
}
func (c *dataPlaneTestCoordinator) WithWriteKeys(ctx context.Context, tenantID string, domain datamigration.Domain, fallback string, keysFn func(context.Context, string) ([]string, error), fn func(context.Context, string) error) error {
	return c.WithRead(ctx, tenantID, domain, fallback, func(ctx context.Context, profile string) error {
		keys, err := keysFn(ctx, profile)
		if err != nil {
			return err
		}
		c.keys = append([]string(nil), keys...)
		if c.rejected != nil {
			return c.rejected
		}
		return fn(ctx, profile)
	})
}

type closingVectorStore struct {
	vectorstore.VectorStore
	closes int
}

func (s *closingVectorStore) Close() error { s.closes++; return nil }

func TestLiveKnowledgeRetainsBorrowedBackendAndRoutesOldWrapper(t *testing.T) {
	ctx := context.Background()
	coordinator := &dataPlaneTestCoordinator{}
	source := &closingVectorStore{VectorStore: inmemory.New()}
	target := &closingVectorStore{VectorStore: inmemory.New()}
	opens := 0
	runtime := &DataPlaneRuntime{coordinator: coordinator, openKnowledge: func(context.Context, string, string, string) (vectorstore.VectorStore, error) {
		opens++
		return target, nil
	}}
	wrapped, err := runtime.DecorateKnowledge(ctx, "tenant-a", "support", "source", source)
	if err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Add(ctx, &document.Document{ID: "source-doc", Content: "source"}, []float64{1, 0}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := wrapped.Get(ctx, "source-doc"); err != nil {
		t.Fatal(err)
	}
	if opens != 0 {
		t.Fatalf("fallback recreated backend %d times", opens)
	}
	coordinator.profile = "target"
	if err := wrapped.Add(ctx, &document.Document{ID: "target-doc", Content: "target"}, []float64{1, 0}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := wrapped.Get(ctx, "target-doc"); err != nil {
			t.Fatal(err)
		}
	}
	if opens != 1 {
		t.Fatalf("destination opened %d times, want once", opens)
	}
	if sourceDoc, _, _ := source.Get(ctx, "target-doc"); sourceDoc != nil {
		t.Fatal("old wrapper wrote the original profile after route change")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if target.closes != 1 || source.closes != 0 {
		t.Fatalf("close counts target=%d borrowed=%d", target.closes, source.closes)
	}
}

func TestLiveKnowledgeBulkIntentIncludesAllMatchingDocuments(t *testing.T) {
	ctx := context.Background()
	coordinator := &dataPlaneTestCoordinator{}
	source := &closingVectorStore{VectorStore: inmemory.New()}
	for _, id := range []string{"first", "second", "keep"} {
		kind := "remove"
		if id == "keep" {
			kind = "retain"
		}
		if err := source.Add(ctx, &document.Document{ID: id, Metadata: map[string]any{"kind": kind}}, []float64{1, 0}); err != nil {
			t.Fatal(err)
		}
	}
	runtime := &DataPlaneRuntime{coordinator: coordinator}
	wrapped, err := runtime.DecorateKnowledge(ctx, "tenant-a", "support", "source", source)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	coordinator.rejected = errors.New("journal unavailable")
	err = wrapped.DeleteByFilter(ctx, vectorstore.WithDeleteFilter(map[string]any{"kind": "remove"}))
	if !errors.Is(err, coordinator.rejected) {
		t.Fatalf("delete error=%v", err)
	}
	if count, _ := source.Count(ctx); count != 3 {
		t.Fatal("bulk side effect ran before durable intent")
	}
	if len(coordinator.keys) != 2 {
		t.Fatalf("captured %d keys, want both matching documents", len(coordinator.keys))
	}
	coordinator.rejected = nil
	if err := wrapped.DeleteByFilter(ctx, vectorstore.WithDeleteFilter(map[string]any{"kind": "remove"})); err != nil {
		t.Fatal(err)
	}
	if count, _ := source.Count(ctx); count != 1 {
		t.Fatalf("remaining count=%d", count)
	}
	if _, err := wrapped.UpdateByFilter(ctx, vectorstore.WithUpdateByFilterDocumentIDs([]string{"keep"}), vectorstore.WithUpdateByFilterUpdates(map[string]any{"metadata._tsa_tenant_id": "other"})); !errors.Is(err, knowledgeplane.ErrScopeViolation) {
		t.Fatalf("reserved metadata update=%v", err)
	}
}

func TestDataPlaneCacheDoesNotEvictBorrowedOrActiveDestination(t *testing.T) {
	closed := make(map[string]int)
	var mu sync.Mutex
	cache := newDataPlaneCache("source", "source", func(_ context.Context, profile string) (string, error) { return profile, nil }, func(value string) error { mu.Lock(); closed[value]++; mu.Unlock(); return nil })
	_, release, err := cache.borrow(context.Background(), "first")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cache.borrow(context.Background(), "second"); !errors.Is(err, datamigration.ErrMigrationCapability) {
		t.Fatalf("active destination evicted: %v", err)
	}
	release()
	_, releaseSecond, err := cache.borrow(context.Background(), "second")
	if err != nil {
		t.Fatal(err)
	}
	if closed["first"] != 1 || closed["source"] != 0 {
		t.Fatalf("unexpected closes=%v", closed)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if closed["second"] != 0 {
		t.Fatal("active borrower closed during drain")
	}
	releaseSecond()
	if closed["second"] != 1 {
		t.Fatal("destination not closed after final release")
	}
}
