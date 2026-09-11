package storage

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestKnowledgeIngestQueueValidatesRequestsBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	queue := &PostgresKnowledgeIngestQueue{}
	base := KnowledgeIngestRequest{
		TenantID: "tenant-a", AppCode: "support", DocumentID: "doc-1",
		Name: "客服手册", Filename: "support.md", Data: []byte("content"),
		Backend: config.BackendConfig{Driver: "pgvector"},
	}
	invalid := []KnowledgeIngestRequest{
		{},
		func() KnowledgeIngestRequest { value := base; value.TenantID = ""; return value }(),
		func() KnowledgeIngestRequest { value := base; value.AppCode = ""; return value }(),
		func() KnowledgeIngestRequest { value := base; value.DocumentID = ""; return value }(),
		func() KnowledgeIngestRequest { value := base; value.Name = ""; return value }(),
		func() KnowledgeIngestRequest { value := base; value.Filename = ""; return value }(),
		func() KnowledgeIngestRequest { value := base; value.Data = nil; return value }(),
		func() KnowledgeIngestRequest { value := base; value.ChunkSize = -1; return value }(),
		func() KnowledgeIngestRequest { value := base; value.ChunkSize = 100; value.Overlap = 100; return value }(),
		func() KnowledgeIngestRequest { value := base; value.Backend.Driver = "unknown"; return value }(),
		func() KnowledgeIngestRequest {
			value := base
			value.Backend = config.BackendConfig{Driver: "qdrant"}
			return value
		}(),
		func() KnowledgeIngestRequest {
			value := base
			value.Backend = config.BackendConfig{Driver: "elasticsearch"}
			return value
		}(),
	}
	for index, request := range invalid {
		if _, err := queue.EnqueueKnowledgeIngest(context.Background(), request); err == nil {
			t.Fatalf("invalid request %d was accepted", index)
		}
	}
	for _, backend := range []config.BackendConfig{
		{Driver: "pgvector"},
		{Driver: "qdrant", ConnectionRef: "env:QDRANT_CONFIG"},
		{Driver: "elasticsearch", ConnectionRef: "env:ELASTICSEARCH_CONFIG"},
	} {
		request := base
		request.Backend = backend
		if err := validateKnowledgeIngestRequest(request); err != nil {
			t.Fatalf("validateKnowledgeIngestRequest(%s) = %v", backend.Driver, err)
		}
	}
	if err := queue.CancelKnowledgeIngest(context.Background(), "", "support", "doc"); err == nil {
		t.Fatal("CancelKnowledgeIngest() accepted missing tenant")
	}
	if _, _, err := queue.ClaimKnowledgeIngest(context.Background(), "", time.Minute); err == nil {
		t.Fatal("ClaimKnowledgeIngest() accepted missing owner")
	}
	if _, _, err := queue.ClaimKnowledgeIngest(context.Background(), "worker", 0); err == nil {
		t.Fatal("ClaimKnowledgeIngest() accepted zero lease")
	}
	if err := queue.CompleteKnowledgeIngest(context.Background(), KnowledgeIngestCompletion{}); err == nil {
		t.Fatal("CompleteKnowledgeIngest() accepted empty completion")
	}
	if _, err := queue.FailKnowledgeIngest(context.Background(), "job", "worker", "failed", 0); err == nil {
		t.Fatal("FailKnowledgeIngest() accepted zero attempts")
	}
	if _, err := queue.ListKnowledgeDocumentSources(context.Background(), "", "support"); err == nil {
		t.Fatal("ListKnowledgeDocumentSources() accepted missing tenant")
	}
	if err := queue.SaveKnowledgeDocumentSnapshot(context.Background(), "tenant-a", "support", "doc", []byte("not json")); err == nil {
		t.Fatal("SaveKnowledgeDocumentSnapshot() accepted invalid JSON")
	}
	if err := queue.SaveKnowledgeDocumentSnapshot(context.Background(), "", "support", "doc", []byte(`[]`)); err == nil {
		t.Fatal("SaveKnowledgeDocumentSnapshot() accepted missing tenant")
	}
	if got := truncateIngestError("  short error  "); got != "short error" {
		t.Fatalf("truncateIngestError(short) = %q", got)
	}
	long := make([]byte, 600)
	for index := range long {
		long[index] = 'x'
	}
	if got := truncateIngestError(string(long)); len(got) != 512 {
		t.Fatalf("truncateIngestError(long) length = %d", len(got))
	}
}

func TestKnowledgeMigrationStoreValidatesConstructionAndOperation(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresKnowledgeMigrationStore(nil); err == nil {
		t.Fatal("NewPostgresKnowledgeMigrationStore(nil) error = nil")
	}
	store := &PostgresKnowledgeMigrationStore{}
	if _, err := store.CreateKnowledgeMigration(context.Background(), KnowledgeMigrationStatus{}); err == nil {
		t.Fatal("CreateKnowledgeMigration() accepted empty status")
	}
	if err := store.WithKnowledgeMigrationLock(context.Background(), "tenant-a", "support", nil); err == nil {
		t.Fatal("WithKnowledgeMigrationLock() accepted nil operation")
	}
}
