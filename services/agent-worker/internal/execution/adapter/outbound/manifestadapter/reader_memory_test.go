package manifestadapter

import (
	"context"
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"testing"
)

func TestReaderMemoryBuildsFixedBackendAndToolSelection(t *testing.T) {
	reader, route, _, _ := readerDataInput(t, func(c map[string]any) {
		b := datav1.Snapshot{SchemaVersion: "v1", TenantID: c["tenant_id"].(string), BackendID: "memory-pg", BackendRevision: 3, Kind: datav1.PostgreSQL, Adapter: "managed-postgres-v1", Isolation: datav1.MemoryIsolation, Limits: datav1.Limits{TimeoutMS: 5000, MaxConcurrency: 2, MaxBytes: 1 << 20}, PostgreSQL: &datav1.PostgresTarget{Host: "memory.internal", Port: 5432, Database: "agent_platform", Username: "memory_runtime", SSLMode: "require"}}
		d, err := b.Digest()
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(b)
		var backend map[string]any
		json.Unmarshal(raw, &backend)
		readerDataNode(c)["memory"] = map[string]any{"resource": "memory", "tools": []string{"memory_add", "memory_load"}, "preload_limit": -1}
		resources := c["resources"].(map[string]any)
		resources["models"].(map[string]any)["primary"].(map[string]any)["capabilities"] = []string{"chat", "tool_call"}
		resources["storage"].(map[string]any)["memory"] = map[string]any{"kind": "managed_memory", "adapter_version": "managed-memory-v1", "backend": backend, "credential": map[string]any{"credential_id": "crd_22222222222222222222222222222222", "purpose": "dsn_password", "audience_digest": d}}
		c["storage_roles"].(map[string]any)["memory"] = "memory"
		e := c["execution"].(map[string]any)
		e["allowed_endpoint_hosts"] = append(e["allowed_endpoint_hosts"].([]any), "memory.internal")
	})
	p, err := reader.Resolve(context.Background(), route)
	if err != nil {
		t.Fatal(err)
	}
	if p.Memory == nil || p.Memory.AgentID == "" || p.Memory.Backend.PostgreSQL.Host != "memory.internal" || p.Memory.Backend.BackendRevision != 3 || p.Memory.PreloadLimit != -1 || len(p.Memory.Tools) != 2 || p.Memory.Credential.Purpose != "dsn_password" || len(p.Uses()) != 3 {
		t.Fatalf("wrong fixed memory projection: %+v", p.Memory)
	}
	p.Memory.Backend.PostgreSQL.Host = "changed"
	p.Memory.Tools[0] = "changed"
	next, err := reader.Resolve(context.Background(), route)
	if err != nil || next.Memory.Backend.PostgreSQL.Host != "memory.internal" || next.Memory.Tools[0] != "memory_add" {
		t.Fatalf("projection aliases mutable plan: %v", err)
	}
}
