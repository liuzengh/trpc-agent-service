package deploymentv1

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"testing"
)

func workerMemoryFixture(t *testing.T) ManifestContent {
	c := workerSummaryFixture(t)
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: c.TenantID, BackendID: "pg-memory", BackendRevision: 1, Kind: datav1.PostgreSQL, Adapter: "managed-postgres-v1", Isolation: datav1.MemoryIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 4, MaxBytes: 1 << 20}, PostgreSQL: &datav1.PostgresTarget{Host: "memory.internal", Port: 5432, Database: "agent_platform", Username: "memory_runtime", SSLMode: "require"}}
	digest, err := b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	c.StorageRoles["memory"] = "memory"
	c.Resources.Storage["memory"] = ManifestStorageResource{Kind: "managed_memory", AdapterVersion: "managed-memory-v1", Backend: &b, Credential: CredentialUse{CredentialID: "crd_memory", Purpose: "dsn_password", AudienceDigest: digest}}
	n := c.AgentPlan.Nodes[c.AgentPlan.Root]
	n.Memory = &ManifestMemory{Resource: "memory", Tools: []string{"memory_add", "memory_update", "memory_delete", "memory_clear", "memory_search", "memory_load"}}
	c.AgentPlan.Nodes[c.AgentPlan.Root] = n
	m := c.Resources.Models[n.ModelResource]
	m.Capabilities = append(m.Capabilities, "tool_call")
	c.Resources.Models[n.ModelResource] = m
	c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "memory.internal")
	return c
}
func TestWorkerV1MemoryExplicitFixedPostgresClosure(t *testing.T) {
	c := workerMemoryFixture(t)
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*ManifestContent){
		"implicit": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			n.Memory = nil
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
		},
		"unbound":      func(c *ManifestContent) { delete(c.StorageRoles, "memory") },
		"wrong tenant": func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.TenantID = "other" },
		"wrong role": func(c *ManifestContent) {
			c.Resources.Storage["memory"].Backend.PostgreSQL.Username = "session_runtime"
		},
		"wrong scope": func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Isolation = datav1.SessionIsolation },
		"missing credential": func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Credential = CredentialUse{}
			c.Resources.Storage["memory"] = r
		},
		"different target": func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.PostgreSQL.Host = "other.internal" },
		"no tools capability": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			m := c.Resources.Models[n.ModelResource]
			m.Capabilities = []string{"chat"}
			c.Resources.Models[n.ModelResource] = m
		},
		"unknown tool": func(c *ManifestContent) { c.AgentPlan.Nodes[c.AgentPlan.Root].Memory.Tools = []string{"other"} },
		"no budget":    func(c *ManifestContent) { c.Execution.MaxToolCalls = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			c := workerMemoryFixture(t)
			change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("expanded memory closure accepted")
			}
		})
	}
}
