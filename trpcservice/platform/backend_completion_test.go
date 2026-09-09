package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	artifactmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestRedisRichDataVisibleAcrossInstancesAndFenced(t *testing.T) {
	server := miniredis.RunT(t)
	a, b := NewRedisStore(server.Addr()), NewRedisStore(server.Addr())
	defer a.Close()
	defer b.Close()
	ctx := context.Background()
	if err := a.AppendSessionEvent(ctx, SessionEvent{TenantID: "t", SessionID: "s", IdempotencyKey: "fence", Type: "session.lease.acquired", FencingToken: 2}); err != nil {
		t.Fatal(err)
	}
	if err := b.PutMemory(ctx, MemoryRecord{TenantID: "t", SessionID: "s", Key: "k", Value: "stale", FencingToken: 1}); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale memory: %v", err)
	}
	if err := b.AppendSessionEvent(ctx, SessionEvent{TenantID: "t", SessionID: "s", IdempotencyKey: "stale", Type: "message", FencingToken: 1}); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale event: %v", err)
	}
	item := Artifact{TenantID: "t", SessionID: "s", Name: "reply", ContentRef: "session-event://t/s/output", FencingToken: 1}
	if _, err := b.PutArtifact(ctx, item); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale artifact: %v", err)
	}
	item.FencingToken = 2
	if _, err := a.PutArtifact(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := a.PutMemory(ctx, MemoryRecord{TenantID: "t", SessionID: "s", Key: "k", Value: "current", FencingToken: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PutKnowledge(ctx, KnowledgeRecord{TenantID: "t", AgentAppID: "app", Source: "doc", Content: "knowledge"}); err != nil {
		t.Fatal(err)
	}
	artifacts, err := b.ListArtifacts(ctx, "t", "s")
	if err != nil || len(artifacts) != 1 {
		t.Fatalf("artifacts=%v err=%v", artifacts, err)
	}
	memory, err := b.ListMemory(ctx, "t", "s")
	if err != nil || len(memory) != 1 || memory[0].Value != "current" {
		t.Fatalf("memory=%v err=%v", memory, err)
	}
	knowledge, err := b.ListKnowledge(ctx, "t", "app")
	if err != nil || len(knowledge) != 1 {
		t.Fatalf("knowledge=%v err=%v", knowledge, err)
	}
	other, _ := b.ListKnowledge(ctx, "other", "app")
	if len(other) != 0 {
		t.Fatal("cross-tenant knowledge")
	}
}

func TestMigrationChecksActualTargetAndSourceChanges(t *testing.T) {
	ctx := context.Background()
	source, target := NewInMemoryStore(), NewInMemoryStore()
	put := func(store DataStore, session string) {
		t.Helper()
		if err := store.PutMemory(ctx, MemoryRecord{TenantID: "t", SessionID: session, Key: "k", Value: "v"}); err != nil {
			t.Fatal(err)
		}
	}
	put(source, "source")
	report, err := MigrateRedisToSQL(ctx, source, target, MigrationOptions{TenantID: "t", DryRun: true})
	if err != nil || report.DestinationCount != 0 || report.Matched {
		t.Fatalf("dry=%+v err=%v", report, err)
	}
	put(target, "extra")
	report, err = MigrateRedisToSQL(ctx, source, target, MigrationOptions{TenantID: "t"})
	if err == nil || report.Status != "failed" {
		t.Fatal("extra destination session accepted")
	}
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	target = NewInMemoryStore()
	report, err = MigrateRedisToSQL(ctx, source, target, MigrationOptions{TenantID: "t", BatchSize: 1, CheckpointPath: checkpoint, Progress: func(MigrationReport) { put(source, "new-session") }})
	if err == nil || report.Status != "failed" {
		t.Fatal("source mutation accepted")
	}
	if _, err := os.Stat(checkpoint); err != nil {
		t.Fatal("failed migration lost checkpoint", err)
	}
}

func TestMigrationCopiesRichRecordsAndMemoryOnlySQLSessions(t *testing.T) {
	source, err := NewSQLiteStore(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target, err := NewSQLiteStore(filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	ctx := context.Background()
	if err := source.PutMemory(ctx, MemoryRecord{TenantID: "t", SessionID: "memory-only", Key: "k", Value: "v"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PutKnowledge(ctx, KnowledgeRecord{TenantID: "t", AgentAppID: "app", Source: "doc", Content: "content"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PutArtifact(ctx, Artifact{TenantID: "t", SessionID: "s", Name: "a", ContentRef: "session-event://t/s/output"}); err != nil {
		t.Fatal(err)
	}
	result, err := MigrateRedisToSQL(ctx, source, target, MigrationOptions{TenantID: "t"})
	if err != nil || !result.Matched || result.DestinationCount != 3 {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestCompositeRoutesAndPersistsIndependentKinds(t *testing.T) {
	server := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "metadata.db")
	config := backendSelection{Backend: "redis", Address: server.Addr(), Memory: BackendEndpoint{"sqlite", path}, Knowledge: BackendEndpoint{"sqlite", path}, Artifact: BackendEndpoint{"sqlite", path}}
	a, err := newBackendStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDataStore(a)
	ctx := context.Background()
	if err := a.AppendSessionEvent(ctx, SessionEvent{TenantID: "t", SessionID: "s", IdempotencyKey: "fence", Type: "session.lease.acquired", FencingToken: 3}); err != nil {
		t.Fatal(err)
	}
	if err := a.PutMemory(ctx, MemoryRecord{TenantID: "t", SessionID: "s", Key: "k", Value: "persisted", FencingToken: 3}); err != nil {
		t.Fatal(err)
	}
	if err := a.PutMemory(ctx, MemoryRecord{TenantID: "t", SessionID: "s", Key: "k", Value: "stale", FencingToken: 2}); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("composite stale write: %v", err)
	}
	b, err := newBackendStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDataStore(b)
	memory, err := b.ListMemory(ctx, "t", "s")
	if err != nil || len(memory) != 1 || memory[0].Value != "persisted" {
		t.Fatalf("memory=%v %v", memory, err)
	}
	redis := NewRedisStore(server.Addr())
	defer redis.Close()
	raw, _ := redis.ListMemory(ctx, "t", "s")
	if len(raw) != 0 {
		t.Fatal("Memory ignored route")
	}
}

func TestAutomaticSummaryPersistsCheckpointAndReplays(t *testing.T) {
	store := NewInMemoryStore()
	handler := NewAdminHandler(nil, DevelopmentIdentity{})
	defer handler.Close()
	ctx := context.Background()
	for _, e := range []SessionEvent{{TenantID: "t", SessionID: "s", IdempotencyKey: "in", Type: "message.input", Payload: []byte(`{"input":"hello"}`)}, {TenantID: "t", SessionID: "s", IdempotencyKey: "out", Type: "message.completed", Payload: []byte(`{"output":"world"}`)}} {
		if err := store.AppendSessionEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	options := chatRunOptions{tenant: TenantContext{TenantID: "t"}, sessionID: "s", appID: "app", requestID: "r"}
	for i := 0; i < 2; i++ {
		if err := handler.updateSessionSummary(ctx, store, options); err != nil {
			t.Fatal(err)
		}
	}
	state, err := store.GetSessionState(ctx, "t", "s")
	if err != nil || state.EventCount != 3 || state.SummarySourceSequence != 2 || !strings.Contains(state.Summary, "world") {
		t.Fatalf("state=%+v %v", state, err)
	}
}

func TestObjectPublicationVerifiesContentAndTenant(t *testing.T) {
	base := NewInMemoryStore()
	s := &compositeStore{DataStore: base, artifacts: base, objects: artifactmemory.NewService()}
	ctx := context.Background()
	if err := base.AppendSessionEvent(ctx, SessionEvent{TenantID: "t", SessionID: "s", IdempotencyKey: "out", Type: "message.completed", Payload: []byte(`{"output":"published content"}`)}); err != nil {
		t.Fatal(err)
	}
	item := Artifact{TenantID: "t", SessionID: "s", Name: "reply", ContentRef: "session-event://t/s/out"}
	saved, err := s.PutArtifact(ctx, item)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.PutArtifact(ctx, item)
	if err != nil || second.ContentRef != saved.ContentRef {
		t.Fatal("publication retry changed reference", err)
	}
	content, err := s.LoadArtifactContent(ctx, "t", saved.ID)
	if err != nil || string(content) != "published content" {
		t.Fatalf("content=%s %v", content, err)
	}
	if _, err := s.LoadArtifactContent(ctx, "other", saved.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-tenant content", err)
	}
}

type fixtureEmbedder struct{}

func (fixtureEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	return []float64{1, 0}, nil
}
func (fixtureEmbedder) GetDimensions() int { return 2 }
func (e fixtureEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := e.GetEmbedding(ctx, text)
	return v, nil, err
}
func TestVectorRebuildAndTenantFiltering(t *testing.T) {
	base := NewInMemoryStore()
	s := &compositeStore{DataStore: base, knowledge: base, embedder: fixtureEmbedder{}, vectorConfig: VectorBackendConfig{Backend: "inmemory", Dimensions: 2, Generation: "v1"}, vectors: map[string]vectorstore.VectorStore{}}
	ctx := context.Background()
	for _, tenant := range []string{"a", "b"} {
		if _, err := base.PutKnowledge(ctx, KnowledgeRecord{TenantID: tenant, AgentAppID: "app", Source: "doc", Content: tenant + " secret"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ReindexKnowledge(ctx, s, "a"); err != nil {
		t.Fatal(err)
	}
	records, err := s.SearchKnowledge(ctx, "a", "app", "secret")
	if err != nil || len(records) != 1 || records[0].TenantID != "a" {
		t.Fatalf("records=%v %v", records, err)
	}
}
func TestSharedAuditStoreVisibleAcrossCenters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.db")
	a, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	first, second := NewGovernanceCenter(), NewGovernanceCenter()
	first.ConfigureAuditStore(a)
	second.ConfigureAuditStore(b)
	event := AuditEvent{TenantID: "t", ID: "one", Decision: "allowed", OccurredAt: time.Now().UTC()}
	if err := first.Record(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	events, err := second.QueryAuditEvents(context.Background(), AuditQuery{TenantID: "t"})
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%v %v", events, err)
	}
	other, err := second.QueryAuditEvents(context.Background(), AuditQuery{TenantID: "other"})
	if err != nil || len(other) != 0 {
		t.Fatal("cross-tenant audit", err)
	}
	encoded, _ := json.Marshal(events)
	if !strings.Contains(string(encoded), "allowed") {
		t.Fatal("audit content missing")
	}
}

func TestMigrationLockBlocksTenantTrafficAndRedisWrites(t *testing.T) {
	server := miniredis.RunT(t)
	control := NewInMemoryControlPlane()
	handler := NewAdminHandler(control, DevelopmentIdentity{})
	defer handler.Close()
	selection := backendSelection{Backend: "redis", Address: server.Addr()}
	if err := control.saveBackendSelection(context.Background(), "tenant-a", selection); err != nil {
		t.Fatal(err)
	}
	if err := control.beginBackendMigration(context.Background(), "tenant-a", "migration-one", selection); err != nil {
		t.Fatal(err)
	}
	if _, _, err := handler.acquireStore(context.Background(), "tenant-a"); !errors.Is(err, ErrTenantMigrating) {
		t.Fatalf("tenant traffic was not blocked: %v", err)
	}
	redisStore := NewRedisStore(server.Addr())
	defer redisStore.Close()
	token, err := redisStore.freezeMigration(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := redisStore.PutMemory(context.Background(), MemoryRecord{TenantID: "tenant-a", SessionID: "session", Key: "key", Value: "value"}); !errors.Is(err, ErrTenantMigrating) {
		t.Fatalf("Redis accepted a write during migration: %v", err)
	}
	if err := redisStore.unfreezeMigration(context.Background(), "tenant-a", token); err != nil {
		t.Fatal(err)
	}
	if err := control.finishBackendMigration(context.Background(), "tenant-a", "migration-one", nil); err != nil {
		t.Fatal(err)
	}
	store, release, err := handler.acquireStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if store == nil {
		t.Fatal("tenant store was not restored")
	}
}

func TestExternalMemoryUsesTenantSessionScope(t *testing.T) {
	ingestedCh := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			var ingested map[string]any
			if err := json.NewDecoder(r.Body).Decode(&ingested); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			ingestedCh <- ingested
			_, _ = w.Write([]byte(`{"results":[{"id":"created","memory":"saved"}]}`))
		case http.MethodGet:
			if r.URL.Query().Get("user_id") != "tenant-a:session-a" {
				t.Fatalf("external Memory user scope = %q", r.URL.Query().Get("user_id"))
			}
			_, _ = w.Write([]byte(`{"results":[{"id":"external-one","memory":"external fact","metadata":{"trpc_app_name":"tenant-a:memory"},"created_at":"2026-01-01T00:00:00Z"}]}`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	base := NewInMemoryStore()
	store := &compositeStore{DataStore: base, memory: base, artifacts: base, knowledge: base, owned: []DataStore{base}}
	if err := store.configureExternalMemory(ExternalMemoryConfig{Host: server.URL, SelfHosted: true}); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.PutMemory(context.Background(), MemoryRecord{TenantID: "tenant-a", SessionID: "session-a", Key: "name", Value: "Ada"}); err != nil {
		t.Fatal(err)
	}
	stored, err := base.ListMemory(context.Background(), "tenant-a", "session-a")
	if err != nil || len(stored) != 1 {
		t.Fatalf("authoritative Memory = %#v, err = %v", stored, err)
	}
	converted := frameworkSession(stored[0].TenantID, "memory", stored[0].SessionID, []SessionEvent{{ID: stored[0].ID, Type: "message.input", Payload: []byte(`{"input":"name=Ada"}`), OccurredAt: stored[0].UpdatedAt}})
	if len(converted.Events) != 1 || len(converted.Events[0].Choices) != 1 {
		t.Fatalf("upstream Memory session = %#v", converted)
	}
	var ingested map[string]any
	select {
	case ingested = <-ingestedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("external Memory ingest was not observed")
	}
	metadata, _ := ingested["metadata"].(map[string]any)
	if ingested["user_id"] != "tenant-a:session-a" || metadata["trpc_app_name"] != "tenant-a:memory" {
		t.Fatalf("external Memory ingest scope = %#v", ingested)
	}
	items, err := store.ContextMemory(context.Background(), "tenant-a", "session-a")
	if err != nil || len(items) != 2 || items[1].TenantID != "tenant-a" || items[1].Value != "external fact" {
		t.Fatalf("external Memory items = %#v, err = %v", items, err)
	}
}
