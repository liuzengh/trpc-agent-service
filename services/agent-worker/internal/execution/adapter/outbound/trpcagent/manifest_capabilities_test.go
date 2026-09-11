package trpcagent

import (
	"errors"
	"strconv"
	"testing"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func manifestCapabilityFixture() protocol.ManifestContent {
	pg := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant", BackendID: "pg", BackendRevision: 1, Kind: datav1.PostgreSQL, Adapter: "managed-postgres-v1", Isolation: datav1.MemoryIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 8, MaxBytes: 1024}, PostgreSQL: &datav1.PostgresTarget{Host: "db.test", Port: 5432, Database: "mem", Username: "worker", SSLMode: "require"}}
	s3 := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant", BackendID: "s3", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: pg.Limits, S3: &datav1.S3Target{Endpoint: "https://s3.test", Bucket: "artifacts", Region: "test", Versioning: "disabled"}}
	q := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant", BackendID: "qdrant", BackendRevision: 1, Kind: datav1.Qdrant, Adapter: "managed-qdrant-v1", Isolation: "tenant-profile-resource-v1", Limits: pg.Limits, Qdrant: &datav1.QdrantTarget{Endpoint: "https://qdrant.test", Collection: "docs", VectorName: "text", Dimensions: 8, Distance: "cosine"}}
	return protocol.ManifestContent{TenantID: "tenant", AgentPlan: protocol.AgentPlan{Root: "assistant", Nodes: map[string]protocol.ManifestNode{"assistant": {Kind: "llm", ModelResource: "primary"}}}, StorageRoles: map[string]string{"session": "session", "memory": "memory", "artifact": "artifact"}, Resources: protocol.ManifestResources{
		Models:    map[string]protocol.ManifestModelResource{"primary": {Capabilities: []string{"chat"}}},
		Storage:   map[string]protocol.ManifestStorageResource{"session": {Kind: "postgres_state", AdapterVersion: "postgres-state-v1"}, "memory": {Kind: "managed_memory", AdapterVersion: "managed-memory-v1", Backend: &pg}, "artifact": {Kind: "managed_artifact", AdapterVersion: "managed-artifact-v1", MetadataContract: protocol.ArtifactMetadataContract, Backend: &s3}},
		Knowledge: map[string]protocol.ManifestKnowledgeResource{"docs": {Kind: "managed_knowledge", AdapterVersion: "managed-knowledge-v1", Backend: &q, Embedding: protocol.ManifestEmbeddingResource{Model: "embed", BaseURL: "https://embed.test", Dimensions: 8}}},
	}}
}
func manifestCapabilityEnable(c *protocol.ManifestContent) {
	limit := int64(-1)
	yes := true
	c.Runtime = &protocol.ManifestRuntime{Summary: &protocol.ManifestSummary{Enabled: true, ModelResource: "primary", EventThreshold: 5}}
	node := c.AgentPlan.Nodes["assistant"]
	node.Memory = &protocol.ManifestMemory{Resource: "memory", Tools: []string{memory.SearchToolName, memory.AddToolName}, PreloadLimit: &limit}
	node.Artifact = &protocol.ManifestArtifact{Enabled: true, Resource: "artifact"}
	node.AddSessionSummary = &yes
	node.KnowledgeResources = []string{"docs"}
	c.AgentPlan.Nodes["assistant"] = node
}
func manifestCapabilityServices() CapabilityServices {
	return CapabilityServices{Memory: &capabilityMemory{tools: []tool.Tool{namedCapabilityTool(memory.AddToolName), namedCapabilityTool(memory.SearchToolName)}}, Artifact: &capabilityArtifact{}, Knowledge: &capabilityKnowledge{}}
}

func TestManifestCapabilityDefaultAndSelectedOptions(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			c := manifestCapabilityFixture()
			services := manifestCapabilityServices()
			if enabled {
				manifestCapabilityEnable(&c)
			}
			opts, err := BuildManifestCapabilityOptions(c, "assistant", services)
			if err != nil {
				t.Fatal(err)
			}
			a := applyCapabilityAgent(opts.Agent)
			if enabled {
				if len(a.Tools) != 2 || a.Tools[0].Declaration().Name != memory.SearchToolName || a.Tools[1].Declaration().Name != memory.AddToolName || a.PreloadMemory != -1 || !a.AddSessionSummary || a.Knowledge != services.Knowledge {
					t.Fatal("Manifest selection lost")
				}
				observeCapabilityRunner(t, opts.Runner, services.Memory, services.Artifact)
			} else {
				if len(a.Tools) != 0 || a.PreloadMemory != 0 || a.AddSessionSummary || a.Knowledge != nil {
					t.Fatal("available resource/service enabled implicitly")
				}
				observeCapabilityRunner(t, opts.Runner, nil, nil)
			}
		})
	}
}
func TestManifestCapabilityRejectsInvalidBindingsAtomically(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.ManifestContent)
	}{
		{"missing node", func(c *protocol.ManifestContent) { delete(c.AgentPlan.Nodes, "assistant") }},
		{"non llm", func(c *protocol.ManifestContent) {
			n := c.AgentPlan.Nodes["assistant"]
			n.Kind = "sequence"
			c.AgentPlan.Nodes["assistant"] = n
		}},
		{"memory missing", func(c *protocol.ManifestContent) { delete(c.Resources.Storage, "memory") }},
		{"memory role", func(c *protocol.ManifestContent) { c.StorageRoles["memory"] = "session" }},
		{"memory tenant", func(c *protocol.ManifestContent) { c.Resources.Storage["memory"].Backend.TenantID = "other" }},
		{"memory isolation", func(c *protocol.ManifestContent) {
			c.Resources.Storage["memory"].Backend.Isolation = datav1.SessionIsolation
		}},
		{"disabled memory", func(c *protocol.ManifestContent) {
			n := c.AgentPlan.Nodes["assistant"]
			n.Memory.Tools = nil
			n.Memory.PreloadLimit = nil
		}},
		{"unsupported tool", func(c *protocol.ManifestContent) { c.AgentPlan.Nodes["assistant"].Memory.Tools = []string{"other"} }},
		{"duplicate tool", func(c *protocol.ManifestContent) {
			c.AgentPlan.Nodes["assistant"].Memory.Tools = []string{memory.AddToolName, memory.AddToolName}
		}},
		{"preload unsafe JSON integer", func(c *protocol.ManifestContent) {
			*c.AgentPlan.Nodes["assistant"].Memory.PreloadLimit = 9007199254740992
		}},
		{"preload invalid", func(c *protocol.ManifestContent) { *c.AgentPlan.Nodes["assistant"].Memory.PreloadLimit = -2 }},
		{"artifact disabled", func(c *protocol.ManifestContent) { c.AgentPlan.Nodes["assistant"].Artifact.Enabled = false }},
		{"artifact metadata", func(c *protocol.ManifestContent) {
			s := c.Resources.Storage["artifact"]
			s.MetadataContract = "other"
			c.Resources.Storage["artifact"] = s
		}},
		{"artifact role", func(c *protocol.ManifestContent) { c.StorageRoles["artifact"] = "memory" }},
		{"artifact tenant", func(c *protocol.ManifestContent) { c.Resources.Storage["artifact"].Backend.TenantID = "other" }},
		{"runtime missing", func(c *protocol.ManifestContent) { c.Runtime = nil }},
		{"summary missing", func(c *protocol.ManifestContent) { c.Runtime.Summary = nil }},
		{"summary disabled", func(c *protocol.ManifestContent) { c.Runtime.Summary.Enabled = false }},
		{"summary model missing", func(c *protocol.ManifestContent) { c.Runtime.Summary.ModelResource = "other" }},
		{"summary model capability", func(c *protocol.ManifestContent) {
			c.Resources.Models["primary"] = protocol.ManifestModelResource{Capabilities: []string{"embedding"}}
		}},
		{"summary threshold", func(c *protocol.ManifestContent) { c.Runtime.Summary.EventThreshold = 0 }},
		{"summary session missing", func(c *protocol.ManifestContent) { delete(c.StorageRoles, "session") }},
		{"summary consumption false", func(c *protocol.ManifestContent) { *c.AgentPlan.Nodes["assistant"].AddSessionSummary = false }},
		{"knowledge missing", func(c *protocol.ManifestContent) { delete(c.Resources.Knowledge, "docs") }},
		{"knowledge multi", func(c *protocol.ManifestContent) {
			n := c.AgentPlan.Nodes["assistant"]
			n.KnowledgeResources = []string{"docs", "second"}
			c.AgentPlan.Nodes["assistant"] = n
		}},
		{"knowledge tenant", func(c *protocol.ManifestContent) { c.Resources.Knowledge["docs"].Backend.TenantID = "other" }},
		{"knowledge dimension", func(c *protocol.ManifestContent) { c.Resources.Knowledge["docs"].Backend.Qdrant.Dimensions = 9 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := manifestCapabilityFixture()
			manifestCapabilityEnable(&c)
			tc.change(&c)
			opts, err := BuildManifestCapabilityOptions(c, "assistant", manifestCapabilityServices())
			if !errors.Is(err, ErrManifestCapabilities) || opts.Agent != nil || opts.Runner != nil {
				t.Fatalf("expected atomic rejection got %+v %v", opts, err)
			}
		})
	}
}
func TestManifestCapabilityMissingFinalServices(t *testing.T) {
	for _, role := range []string{"memory", "artifact", "knowledge"} {
		t.Run(role, func(t *testing.T) {
			c := manifestCapabilityFixture()
			manifestCapabilityEnable(&c)
			services := manifestCapabilityServices()
			switch role {
			case "memory":
				services.Memory = nil
			case "artifact":
				services.Artifact = nil
			case "knowledge":
				services.Knowledge = nil
			}
			opts, err := BuildManifestCapabilityOptions(c, "assistant", services)
			if !errors.Is(err, ErrManifestCapabilities) || opts.Agent != nil || opts.Runner != nil {
				t.Fatal("missing final service accepted")
			}
		})
	}
}
func TestManifestCapabilityPreloadNativeRange(t *testing.T) {
	c := manifestCapabilityFixture()
	n := c.AgentPlan.Nodes["assistant"]
	limit := int64(9007199254740991)
	n.Memory = &protocol.ManifestMemory{Resource: "memory", Tools: []string{}, PreloadLimit: &limit}
	c.AgentPlan.Nodes["assistant"] = n
	opts, err := BuildManifestCapabilityOptions(c, "assistant", manifestCapabilityServices())
	if strconv.IntSize == 32 {
		if !errors.Is(err, ErrManifestCapabilities) {
			t.Fatal("native overflow accepted")
		}
		return
	}
	if err != nil || int64(applyCapabilityAgent(opts.Agent).PreloadMemory) != limit {
		t.Fatal("implicit cap", err)
	}
}

func TestManifestCapabilityRedisAndManagedSession(t *testing.T) {
	c := manifestCapabilityFixture()
	manifestCapabilityEnable(&c)
	redis := c.Resources.Storage["memory"].Backend.Clone()
	redis.Kind = datav1.Redis
	redis.Adapter = "managed-redis-v1"
	redis.PostgreSQL = nil
	redis.Redis = &datav1.RedisTarget{Host: "redis.test", Port: 6379, Database: 1, Username: "worker", TLS: true}
	resource := c.Resources.Storage["memory"]
	resource.Backend = &redis
	c.Resources.Storage["memory"] = resource
	session := redis.Clone()
	session.Isolation = datav1.SessionIsolation
	c.Resources.Storage["session"] = protocol.ManifestStorageResource{Kind: "managed_session", AdapterVersion: "managed-session-v1", Backend: &session}
	if _, err := BuildManifestCapabilityOptions(c, "assistant", manifestCapabilityServices()); err != nil {
		t.Fatal(err)
	}
	session.Isolation = datav1.MemoryIsolation
	if _, err := BuildManifestCapabilityOptions(c, "assistant", manifestCapabilityServices()); !errors.Is(err, ErrManifestCapabilities) {
		t.Fatal("Memory backend accepted as Summary Session", err)
	}
}
func TestManifestCapabilitySelectsOnlyNamedNode(t *testing.T) {
	c := manifestCapabilityFixture()
	manifestCapabilityEnable(&c)
	c.AgentPlan.Nodes["plain"] = protocol.ManifestNode{Kind: "llm", ModelResource: "primary"}
	options, err := BuildManifestCapabilityOptions(c, "plain", manifestCapabilityServices())
	if err != nil {
		t.Fatal(err)
	}
	a := applyCapabilityAgent(options.Agent)
	if len(a.Tools) != 0 || a.Knowledge != nil || a.AddSessionSummary || a.PreloadMemory != 0 {
		t.Fatal("sibling or global summary implicitly enabled consumption")
	}
	observeCapabilityRunner(t, options.Runner, nil, nil)
}
