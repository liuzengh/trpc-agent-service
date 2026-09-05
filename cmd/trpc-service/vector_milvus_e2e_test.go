//go:build integration

package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"

	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/milvus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/retrieval"
)

// TestProductionRuntimeVectorLocalMilvusEndToEnd exercises the production
// assembly with an explicitly enabled local Milvus backend, a real PostgreSQL
// task/memory state and the real Redis lease path. The embedder is the
// deterministic test embedder injected through the assembly seam; it never
// becomes a production default.
func TestProductionRuntimeVectorLocalMilvusEndToEnd(t *testing.T) {
	address := os.Getenv("P106DP_MILVUS_ADDRESS")
	if address == "" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("P106DP_MILVUS_ADDRESS and TEST_DATABASE_URL are required")
	}
	f := newProductionTestFixture(t)
	vectorEnabledEnv(t)
	t.Setenv("MILVUS_COLLECTION", "p106dp_e2e_"+f.schema)
	t.Setenv("MILVUS_ENDPOINT", address)
	t.Setenv("MILVUS_USERNAME", "root")
	t.Setenv("MILVUS_PASSWORD", os.Getenv("P106DP_MILVUS_PASSWORD"))
	t.Setenv("VECTOR_WORKER_ENABLED", "true")
	t.Setenv("RETRIEVAL_ENABLED", "true")
	t.Setenv("VECTOR_REDIS_URL", os.Getenv("TEST_REDIS_URL"))
	embedder, err := vector.NewDeterministicEmbedder("test-model", "v1", 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	backendConfig := vector.BackendConfig{Mode: "milvus", EndpointRef: "env:MILVUS_ENDPOINT", Collection: "p106dp_e2e_" + f.schema, CredentialRef: "env://MILVUS_PASSWORD", Model: "test-model", ModelVersion: "v1", Dimension: 8, SchemaVersion: "schema-v1", Metric: "cosine", MaxInputBytes: vector.MaxContentBytes, MaxMetadataBytes: vector.MaxMetadataBytes, MaxTopK: vector.MaxTopK, OperationTimeout: 8 * time.Second, ReadinessTimeout: 12 * time.Second}
	if err = milvus.ProvisionTestCollection(context.Background(), backendConfig, address, milvus.Credentials{Username: "root", Password: os.Getenv("P106DP_MILVUS_PASSWORD")}); err != nil {
		t.Fatal(err)
	}
	runtimeValue, err := assembleProductionWithDependencies(context.Background(), productionTestResponder{factory: &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}}, productionAssemblyDependencies{
		agentFactory:   &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)},
		VectorEmbedder: embedder,
	})
	if err != nil {
		t.Fatal(err)
	}
	seedProductionRows(t, f, runtimeValue.pool)
	if err = runtimeValue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := runtimeValue.Stop(stopCtx); err != nil {
			t.Logf("runtime stop failed: %v", err)
		}
	})
	tc := vectorFixtureTenant(f.tenantID)
	ctx := tenant.WithContext(context.Background(), tc)
	if err = runtimeValue.vector.memory.Put(ctx, tc, memory.Memory{TenantID: f.tenantID, ID: "mem-e2e", Scope: memory.ScopeUser, ScopeID: "scope-user", Kind: "fact", Content: "end to end content"}); err != nil {
		t.Fatal(err)
	}
	waitVectorTaskSucceeded(t, runtimeValue.pool, f.schema, f.tenantID, "mem-e2e", 45*time.Second)
	results, err := runtimeValue.vector.retrieval.Retrieve(ctx, tc, retrieval.Request{Query: "end to end content", TopK: 3, MinScore: 0.9})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].MemoryID != "mem-e2e" || results[0].Content != "end to end content" {
		t.Fatalf("unexpected retrieval results: %+v", results)
	}
	// Update: the new version task runs and the new content is returned.
	if err = runtimeValue.vector.memory.Put(ctx, tc, memory.Memory{TenantID: f.tenantID, ID: "mem-e2e", Scope: memory.ScopeUser, ScopeID: "scope-user", Kind: "fact", Content: "end to end content v2"}); err != nil {
		t.Fatal(err)
	}
	waitVectorTaskSucceeded(t, runtimeValue.pool, f.schema, f.tenantID, "mem-e2e", 45*time.Second)
	results, err = runtimeValue.vector.retrieval.Retrieve(ctx, tc, retrieval.Request{Query: "end to end content v2", TopK: 3, MinScore: 0.9})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Content != "end to end content v2" {
		t.Fatalf("unexpected updated results: %+v", results)
	}
	// Tombstone: the delete task runs and retrieval must not return the row.
	if err = runtimeValue.vector.memory.Delete(ctx, tc, "mem-e2e"); err != nil {
		t.Fatal(err)
	}
	waitVectorTaskSucceeded(t, runtimeValue.pool, f.schema, f.tenantID, "mem-e2e", 45*time.Second)
	results, err = runtimeValue.vector.retrieval.Retrieve(ctx, tc, retrieval.Request{Query: "end to end content v2", TopK: 3, MinScore: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("tombstoned memory still retrievable: %+v", results)
	}
}
