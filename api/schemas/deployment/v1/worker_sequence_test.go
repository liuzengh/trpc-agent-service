package deploymentv1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
)

func workerSequenceFixture(t *testing.T) ManifestContent {
	t.Helper()
	c := workerFixture(t)
	leaf := c.AgentPlan.Nodes[c.AgentPlan.Root]
	c.AgentPlan = AgentPlan{Root: "root", Nodes: map[string]ManifestNode{
		"root":   {Kind: "sequence", Children: []string{"first", "nested"}},
		"nested": {Kind: "sequence", Children: []string{"second"}},
		"first":  leaf, "second": leaf,
	}}
	return c
}

func TestWorkerSequenceOrderedTree(t *testing.T) {
	for _, nested := range []bool{false, true} {
		c := workerSequenceFixture(t)
		if !nested {
			delete(c.AgentPlan.Nodes, "nested")
			c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: []string{"second", "first"}}
		}
		before, _ := json.Marshal(c)
		if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
			t.Fatalf("nested=%v: %v", nested, err)
		}
		after, _ := json.Marshal(c)
		if !bytes.Equal(before, after) {
			t.Fatal("gate mutated ordered children or resources")
		}
		decoded, err := DecodeManifestContent(after)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(decoded.AgentPlan.Nodes["root"].Children, c.AgentPlan.Nodes["root"].Children) {
			t.Fatal("codec reordered children")
		}
		if err = ValidateWorkerV1(decoded, c.PlatformContract.Digest); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWorkerSequenceTreeBoundaries(t *testing.T) {
	for _, total := range []int{128, 129} {
		c := workerFixture(t)
		leaf := c.AgentPlan.Nodes[c.AgentPlan.Root]
		c.AgentPlan = AgentPlan{Root: "root", Nodes: map[string]ManifestNode{}}
		children := []string{"nested"}
		nested := []string{}
		for i := 0; i < 63; i++ {
			id := fmt.Sprintf("a%d", i)
			children = append(children, id)
			c.AgentPlan.Nodes[id] = leaf
		}
		for i := 0; i < total-65; i++ {
			id := fmt.Sprintf("b%d", i)
			nested = append(nested, id)
			c.AgentPlan.Nodes[id] = leaf
		}
		c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: children}
		c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "sequence", Children: nested}
		err := ValidateWorkerV1(c, c.PlatformContract.Digest)
		if (err == nil) != (total == 128) {
			t.Fatalf("nodes=%d: %v", total, err)
		}
	}
	for _, depth := range []int{16, 17} {
		c := workerFixture(t)
		leaf := c.AgentPlan.Nodes[c.AgentPlan.Root]
		c.AgentPlan = AgentPlan{Root: "n1", Nodes: map[string]ManifestNode{}}
		for i := 1; i < depth; i++ {
			c.AgentPlan.Nodes[fmt.Sprintf("n%d", i)] = ManifestNode{Kind: "sequence", Children: []string{fmt.Sprintf("n%d", i+1)}}
		}
		c.AgentPlan.Nodes[fmt.Sprintf("n%d", depth)] = leaf
		err := ValidateWorkerV1(c, c.PlatformContract.Digest)
		if (err == nil) != (depth == 16) {
			t.Fatalf("depth=%d: %v", depth, err)
		}
	}
	for _, count := range []int{1, 64, 65} {
		c := workerFixture(t)
		leaf := c.AgentPlan.Nodes[c.AgentPlan.Root]
		c.AgentPlan = AgentPlan{Root: "root", Nodes: map[string]ManifestNode{}}
		children := []string{}
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("leaf%d", i)
			c.AgentPlan.Nodes[id] = leaf
			children = append(children, id)
		}
		c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: children}
		err := ValidateWorkerV1(c, c.PlatformContract.Digest)
		if (err == nil) != (count <= 64) {
			t.Fatalf("children=%d: %v", count, err)
		}
	}
}

func TestWorkerSequenceRejectsNonTreeAndInactiveOptions(t *testing.T) {
	yes := true
	for name, change := range map[string]func(*ManifestContent){
		"missing root": func(c *ManifestContent) { c.AgentPlan.Root = "missing" },
		"empty plan":   func(c *ManifestContent) { c.AgentPlan.Nodes = nil },
		"unreachable":  func(c *ManifestContent) { c.AgentPlan.Nodes["orphan"] = c.AgentPlan.Nodes["first"] },
		"cycle": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "sequence", Children: []string{"root"}}
		},
		"two parents": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "sequence", Children: []string{"first", "second"}}
		},
		"duplicate children": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "sequence", Children: []string{"second", "second"}}
		},
		"missing child": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "sequence", Children: []string{"missing"}}
		},
		"empty sequence": func(c *ManifestContent) { c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "sequence"} },
		"terminal parallel": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "parallel", Children: []string{"second"}}
		},
		"loop without explicit bound": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "loop", Body: "second", MaxIterations: 0}
		},
		"llm children": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["first"]
			n.Children = []string{"second"}
			c.AgentPlan.Nodes["first"] = n
		},
		"llm body": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["second"]
			n.Body = "first"
			c.AgentPlan.Nodes["second"] = n
		},
		"llm iterations": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["second"]
			n.MaxIterations = 1
			c.AgentPlan.Nodes["second"] = n
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := workerSequenceFixture(t)
			change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("invalid tree accepted")
			}
		})
	}
	for name, change := range map[string]func(*ManifestNode){
		"instruction": func(n *ManifestNode) { n.Instruction = "hidden" }, "model": func(n *ManifestNode) { n.ModelResource = "primary" },
		"tools": func(n *ManifestNode) { n.ToolResources = []string{"x"} }, "knowledge": func(n *ManifestNode) { n.KnowledgeResources = []string{"x"} },
		"callables": func(n *ManifestNode) { n.CallableEntries = []string{"tools/x"} }, "generation": func(n *ManifestNode) { n.Generation = &Generation{} },
		"memory": func(n *ManifestNode) { n.Memory = &ManifestMemory{} }, "artifact": func(n *ManifestNode) { n.Artifact = &ManifestArtifact{} },
		"summary": func(n *ManifestNode) { n.AddSessionSummary = &yes }, "body": func(n *ManifestNode) { n.Body = "second" }, "iterations": func(n *ManifestNode) { n.MaxIterations = 1 },
	} {
		t.Run("sequence/"+name, func(t *testing.T) {
			c := workerSequenceFixture(t)
			n := c.AgentPlan.Nodes["nested"]
			change(&n)
			c.AgentPlan.Nodes["nested"] = n
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("inactive sequence options accepted")
			}
		})
	}
}

func workerSequenceAllResources(t *testing.T) ManifestContent {
	t.Helper()
	c := workerArtifactFixture(t)
	first := c.AgentPlan.Nodes[c.AgentPlan.Root]
	second := first
	second.Memory = nil
	second.Artifact = nil
	second.AddSessionSummary = nil
	second.ModelResource = "second_model"
	model := c.Resources.Models[first.ModelResource]
	model.BaseURL = "https://second-model.example.test/v1"
	model.Credential = CredentialUse{CredentialID: "second-model-key", Purpose: "api_key", AudienceDigest: CredentialAudienceDigest(model.Kind, model.BaseURL)}
	c.Resources.Models[second.ModelResource] = model
	c.ResolvedRequirements.Models[second.ModelResource] = second.ModelResource
	c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "second-model.example.test")
	summary := model
	summary.BaseURL = "https://summary.example.test/v1"
	summary.Capabilities = []string{"chat"}
	summary.Credential = CredentialUse{CredentialID: "summary-key", Purpose: "api_key", AudienceDigest: CredentialAudienceDigest(summary.Kind, summary.BaseURL)}
	c.Runtime.Summary.ModelResource = "summarizer"
	c.Resources.Models["summarizer"] = summary
	c.ResolvedRequirements.Models["summarizer"] = "summarizer"
	c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "summary.example.test")
	c.Resources.Tools = map[string]ManifestToolResource{}
	c.ResolvedRequirements.Tools = map[string]string{}
	c.Resources.Knowledge = map[string]ManifestKnowledgeResource{}
	c.ResolvedRequirements.Knowledge = map[string]string{}
	for i, n := range []*ManifestNode{&first, &second} {
		key := fmt.Sprintf("tool%d", i)
		endpoint := fmt.Sprintf("https://tool%d.example.test/mcp", i)
		u := CredentialUse{CredentialID: key + "-key", Purpose: "bearer_token", AudienceDigest: CredentialAudienceDigest("mcp_streamable_http", endpoint, "bearer")}
		c.Resources.Tools[key] = ManifestToolResource{Kind: "mcp_streamable_http", AdapterVersion: "mcp-web-search-v1", ServerURL: endpoint, ToolsetName: "selected", ToolName: "lookup", Capability: "mcp.search", Auth: ManifestToolAuth{Kind: "bearer", Credential: &u}}
		c.ResolvedRequirements.Tools[key] = key
		n.ToolResources = []string{key}
		n.CallableEntries = []string{"tools/" + key}
		c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, fmt.Sprintf("tool%d.example.test", i))
		key = fmt.Sprintf("docs%d", i)
		host := fmt.Sprintf("qdrant%d.example.test", i)
		b := datav1.Snapshot{SchemaVersion: "v1", TenantID: c.TenantID, BackendID: key, BackendRevision: 1, Kind: datav1.Qdrant, Adapter: "managed-qdrant-v1", Isolation: "tenant-profile-resource-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, Qdrant: &datav1.QdrantTarget{Endpoint: "https://" + host, Collection: key, VectorName: "content", Dimensions: 3, Distance: "cosine"}}
		digest, err := b.Digest()
		if err != nil {
			t.Fatal(err)
		}
		q := CredentialUse{CredentialID: key + "-key", Purpose: "qdrant_api_key", AudienceDigest: digest}
		base := fmt.Sprintf("https://embedding%d.example.test/v1", i)
		c.Resources.Knowledge[key] = ManifestKnowledgeResource{Kind: "managed_knowledge", AdapterVersion: "managed-knowledge-v1", Backend: &b, Credential: &q, Capability: "knowledge.search", Embedding: ManifestEmbeddingResource{Model: "embed", BaseURL: base, Dimensions: 3, Credential: CredentialUse{CredentialID: key + "-embedding-key", Purpose: "embedding_api_key", AudienceDigest: CredentialAudienceDigest("managed_knowledge", base)}}}
		c.ResolvedRequirements.Knowledge[key] = key
		n.KnowledgeResources = []string{key}
		n.CallableEntries = append(n.CallableEntries, "knowledge/"+key)
		c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, host, fmt.Sprintf("embedding%d.example.test", i))
	}
	c.AgentPlan = AgentPlan{Root: "root", Nodes: map[string]ManifestNode{"root": {Kind: "sequence", Children: []string{"first", "second"}}, "first": first, "second": second}}
	return c
}

func TestWorkerSequenceWholeTreeResourceClosure(t *testing.T) {
	c := workerSequenceAllResources(t)
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	// A second node may explicitly share the one global Memory/Artifact role.
	second := c.AgentPlan.Nodes["second"]
	second.Memory = c.AgentPlan.Nodes["first"].Memory
	second.Artifact = c.AgentPlan.Nodes["first"].Artifact
	c.AgentPlan.Nodes["second"] = second
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal("shared explicit storage", err)
	}
	for name, change := range map[string]func(*ManifestContent){
		"second model missing":  func(c *ManifestContent) { delete(c.Resources.Models, "second_model") },
		"summary model missing": func(c *ManifestContent) { delete(c.Resources.Models, "summarizer") },
		"summary model closure": func(c *ManifestContent) { delete(c.ResolvedRequirements.Models, "summarizer") },
		"extra model":           func(c *ManifestContent) { c.Resources.Models["extra"] = c.Resources.Models["second_model"] },
		"second model lacks tools": func(c *ManifestContent) {
			m := c.Resources.Models["second_model"]
			m.Capabilities = []string{"chat"}
			c.Resources.Models["second_model"] = m
		},
		"second model audience": func(c *ManifestContent) {
			m := c.Resources.Models["second_model"]
			m.Credential.AudienceDigest = "wrong"
			c.Resources.Models["second_model"] = m
		},
		"second model credential alias": func(c *ManifestContent) {
			m := c.Resources.Models["second_model"]
			m.Credential.CredentialID = c.Resources.Models["summarizer"].Credential.CredentialID
			c.Resources.Models["second_model"] = m
		},
		"second tool missing":      func(c *ManifestContent) { delete(c.Resources.Tools, "tool1") },
		"second tool binding":      func(c *ManifestContent) { c.ResolvedRequirements.Tools["tool1"] = "tool0" },
		"extra tool":               func(c *ManifestContent) { c.Resources.Tools["extra"] = c.Resources.Tools["tool0"] },
		"second tool credential":   func(c *ManifestContent) { c.Resources.Tools["tool1"].Auth.Credential.Purpose = "api_key" },
		"second knowledge missing": func(c *ManifestContent) { delete(c.Resources.Knowledge, "docs1") },
		"second knowledge binding": func(c *ManifestContent) { c.ResolvedRequirements.Knowledge["docs1"] = "docs0" },
		"extra knowledge":          func(c *ManifestContent) { c.Resources.Knowledge["extra"] = c.Resources.Knowledge["docs0"] },
		"second knowledge tenant":  func(c *ManifestContent) { c.Resources.Knowledge["docs1"].Backend.TenantID = "other" },
		"second embedding audience": func(c *ManifestContent) {
			k := c.Resources.Knowledge["docs1"]
			k.Embedding.Credential.AudienceDigest = "wrong"
			c.Resources.Knowledge["docs1"] = k
		},
		"missing endpoint": func(c *ManifestContent) {
			c.Execution.AllowedEndpointHosts = slices.DeleteFunc(c.Execution.AllowedEndpointHosts, func(s string) bool { return s == "tool1.example.test" })
		},
		"extra endpoint": func(c *ManifestContent) {
			c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "other.example")
		},
		"unused storage": func(c *ManifestContent) { c.Resources.Storage["extra"] = c.Resources.Storage["memory"] },
	} {
		t.Run(name, func(t *testing.T) {
			c := workerSequenceAllResources(t)
			change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("whole-tree closure widened")
			}
		})
	}
	for name, change := range map[string]func(*ManifestNode){
		"duplicate tool": func(n *ManifestNode) { n.ToolResources = append(n.ToolResources, n.ToolResources[0]) },
		"second knowledge per node": func(n *ManifestNode) {
			n.KnowledgeResources = append(n.KnowledgeResources, "docs0")
			n.CallableEntries = append(n.CallableEntries, "knowledge/docs0")
		},
		"wrong callable":           func(n *ManifestNode) { n.CallableEntries = []string{"tools/tool0", "knowledge/docs1"} },
		"generation":               func(n *ManifestNode) { v := int64(999999); n.Generation = &Generation{MaxOutputTokens: &v} },
		"other memory role":        func(n *ManifestNode) { n.Memory = &ManifestMemory{Resource: "other", Tools: []string{"memory_load"}} },
		"other artifact role":      func(n *ManifestNode) { n.Artifact = &ManifestArtifact{Resource: "other", Enabled: true} },
		"disabled summary consume": func(n *ManifestNode) { no := false; n.AddSessionSummary = &no },
	} {
		t.Run("second/"+name, func(t *testing.T) {
			c := workerSequenceAllResources(t)
			n := c.AgentPlan.Nodes["second"]
			change(&n)
			c.AgentPlan.Nodes["second"] = n
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("second node authority not checked")
			}
		})
	}
}

func TestWorkerSequenceSharedAuthorityIsOneGlobalResource(t *testing.T) {
	c := workerSequenceAllResources(t)
	first, second := c.AgentPlan.Nodes["first"], c.AgentPlan.Nodes["second"]
	second.ToolResources = append([]string(nil), first.ToolResources...)
	second.KnowledgeResources = append([]string(nil), first.KnowledgeResources...)
	second.CallableEntries = append([]string(nil), first.CallableEntries...)
	c.AgentPlan.Nodes["second"] = second
	delete(c.Resources.Tools, "tool1")
	delete(c.ResolvedRequirements.Tools, "tool1")
	delete(c.Resources.Knowledge, "docs1")
	delete(c.ResolvedRequirements.Knowledge, "docs1")
	c.Execution.AllowedEndpointHosts = slices.DeleteFunc(c.Execution.AllowedEndpointHosts, func(host string) bool {
		return slices.Contains([]string{"tool1.example.test", "qdrant1.example.test", "embedding1.example.test"}, host)
	})
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal("shared resource was treated as per-leaf extra", err)
	}
	// The same model used by a plain leaf and a tool-enabled leaf must retain the
	// stronger capability requirement regardless of visitation order.
	primary := first.ModelResource
	second.ModelResource = primary
	first.ToolResources = nil
	first.KnowledgeResources = nil
	first.CallableEntries = nil
	first.Memory = nil
	first.Artifact = nil
	c.AgentPlan.Nodes["first"], c.AgentPlan.Nodes["second"] = first, second
	delete(c.Resources.Models, "second_model")
	delete(c.ResolvedRequirements.Models, "second_model")
	delete(c.StorageRoles, "memory")
	delete(c.StorageRoles, "artifact")
	delete(c.Resources.Storage, "memory")
	delete(c.Resources.Storage, "artifact")
	c.Execution.AllowedEndpointHosts = slices.DeleteFunc(c.Execution.AllowedEndpointHosts, func(host string) bool {
		return slices.Contains([]string{"second-model.example.test", "memory.internal", "s3.internal"}, host)
	})
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	model := c.Resources.Models[primary]
	model.Capabilities = []string{"chat"}
	c.Resources.Models[primary] = model
	if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
		t.Fatal("shared model lost second leaf tool_call requirement")
	}
}
