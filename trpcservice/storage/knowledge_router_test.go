package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
)

func TestKnowledgeRouterInMemorySearchAndScope(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{
		ID: "tutorial-knowledge", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "active", Version: 1,
		Config: json.RawMessage(`{"dimensions":64,"max_results":5}`),
	})
	data.Revisions[0].KnowledgeConfig = json.RawMessage(`{
        "enabled":true,"max_results":3,"chunk_size":20,"chunk_overlap":5,
        "embedding":{"provider":"hash","dimensions":64}
    }`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	repository := controlplane.NewMemoryRepository(data)
	router, err := NewKnowledgeRouter(repository, secret.StaticStore{})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	scope := runtimecontext.TutorialScope()
	revision := data.Revisions[0]
	chunks, err := router.UpsertDocument(context.Background(), scope, revision, KnowledgeDocument{
		ID: "handbook", Name: "Handbook", Content: "refund policy allows returns within thirty days",
	})
	if err != nil || chunks < 2 {
		t.Fatalf("chunks=%d err=%v", chunks, err)
	}
	kb, enabled, err := router.KnowledgeForRevision(context.Background(), scope, revision)
	if err != nil || !enabled {
		t.Fatalf("enabled=%t err=%v", enabled, err)
	}
	result, err := kb.Search(context.Background(), &knowledge.SearchRequest{Query: "refund policy"})
	if err != nil || result.Document == nil || !strings.Contains(result.Text, "refund") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for _, item := range result.Documents {
		if item.Document.Metadata["tenant_id"] != "tutorial-tenant" ||
			item.Document.Metadata["app_id"] != "tutorial-app" {
			t.Fatalf("unscoped document=%+v", item.Document)
		}
	}
}

func TestHashEmbedderDeterministic(t *testing.T) {
	embedder := NewHashEmbedder(32)
	first, _ := embedder.GetEmbedding(context.Background(), "same text")
	second, _ := embedder.GetEmbedding(context.Background(), "same text")
	if len(first) != 32 || len(second) != 32 {
		t.Fatalf("dimensions=%d/%d", len(first), len(second))
	}
	for index := range first {
		if first[index] != second[index] {
			t.Fatal("hash embedding is not deterministic")
		}
	}
}

func TestKnowledgeRouterMigrationDualWrite(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	now := time.Now().UTC()
	source := controlplane.BackendBinding{
		ID: "knowledge-source", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "active",
		Config: json.RawMessage(`{"dimensions":32}`), Version: 1,
	}
	data.BackendBindings = append(data.BackendBindings, source)
	data.Revisions[0].KnowledgeConfig = json.RawMessage(`{
        "enabled":true,"embedding":{"provider":"hash","dimensions":32}
    }`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	repository := controlplane.NewMemoryRepository(data)
	target := controlplane.BackendBinding{
		ID: "knowledge-target", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "migration_target",
		Config: json.RawMessage(`{"dimensions":32}`), Version: 1,
	}
	if err := repository.CreateBackendBinding(context.Background(), target); err != nil {
		t.Fatalf("create target: %v", err)
	}
	if err := repository.CreateBackendMigration(context.Background(), controlplane.BackendMigration{
		ID: "knowledge-migration", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "knowledge", SourceBindingID: source.ID, TargetBindingID: target.ID,
		State: controlplane.MigrationDualWrite, Checkpoint: json.RawMessage(`{}`),
		Verification: json.RawMessage(`{}`), Version: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create migration: %v", err)
	}
	router, _ := NewKnowledgeRouter(repository, secret.StaticStore{})
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	scope := runtimecontext.TutorialScope()
	revision := data.Revisions[0]
	if _, err := router.UpsertDocument(context.Background(), scope, revision, KnowledgeDocument{
		ID: "migration-doc", Content: "dual written knowledge",
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	config, _ := parseRevisionKnowledgeConfig(revision.KnowledgeConfig)
	targetHandle, err := router.cachedHandle(context.Background(), scope, revision, config, target)
	if err != nil {
		t.Fatalf("target handle: %v", err)
	}
	result, err := targetHandle.knowledge.Search(
		context.Background(), &knowledge.SearchRequest{Query: "dual written knowledge"},
	)
	if err != nil || result.Document == nil {
		t.Fatalf("target result=%+v err=%v", result, err)
	}
}
