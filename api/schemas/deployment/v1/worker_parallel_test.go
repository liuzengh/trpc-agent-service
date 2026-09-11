package deploymentv1

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func workerParallelFixture(t *testing.T) ManifestContent {
	t.Helper()
	c := workerSequenceAllResources(t)
	// The older semantic-only fixture uses symbolic credential IDs. This test
	// also crosses the public JSON codec, so give every credential a valid,
	// distinct fixture identity and avoid its inherited duplicate capability.
	credentialID := func(key string) string {
		sum := sha256.Sum256([]byte(key))
		return fmt.Sprintf("crd_%x", sum[:16])
	}
	for key, model := range c.Resources.Models {
		model.Credential.CredentialID = credentialID("models/" + key)
		model.Capabilities = slices.Clone(model.Capabilities)
		slices.Sort(model.Capabilities)
		model.Capabilities = slices.Compact(model.Capabilities)
		c.Resources.Models[key] = model
	}
	for key, tool := range c.Resources.Tools {
		tool.Auth.Credential.CredentialID = credentialID("tools/" + key)
	}
	for key, knowledge := range c.Resources.Knowledge {
		knowledge.Credential.CredentialID = credentialID("knowledge/" + key)
		knowledge.Embedding.Credential.CredentialID = credentialID("embedding/" + key)
		c.Resources.Knowledge[key] = knowledge
	}
	memory := c.Resources.Storage["memory"]
	memory.Credential.CredentialID = credentialID("storage/memory")
	c.Resources.Storage["memory"] = memory
	final := c.AgentPlan.Nodes["second"]
	final.ToolResources, final.KnowledgeResources, final.CallableEntries = []string{}, []string{}, []string{}
	c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: []string{"fanout", "final"}}
	c.AgentPlan.Nodes["fanout"] = ManifestNode{Kind: "parallel", Children: []string{"nested", "first"}}
	c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "parallel", Children: []string{"second"}}
	c.AgentPlan.Nodes["final"] = final
	return c
}

func TestWorkerParallelOrderedTreeAndGlobalClosure(t *testing.T) {
	c := workerParallelFixture(t)
	before, _ := json.Marshal(c)
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(c)
	if !bytes.Equal(before, after) {
		t.Fatal("gate rewrote source tree or resource authority")
	}
	decoded, err := DecodeManifestContent(after)
	if err != nil {
		var document any
		_ = json.Unmarshal(after, &document)
		validators, _ := manifestValidators()
		t.Fatalf("decode: %v; schema: %v", err, validators.content.Validate(document))
	}
	if err = ValidateWorkerV1(decoded, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(decoded.AgentPlan.Nodes["fanout"].Children, []string{"nested", "first"}) || !slices.Equal(decoded.AgentPlan.Nodes["root"].Children, []string{"fanout", "final"}) {
		t.Fatal("parallel or explicit successor order changed")
	}
	leaves, err := workerV1LLMNodes(decoded.AgentPlan)
	if err != nil || len(leaves) != 3 || leaves[0].ModelResource != c.AgentPlan.Nodes["second"].ModelResource || leaves[1].ModelResource != c.AgentPlan.Nodes["first"].ModelResource || len(leaves[2].ToolResources) != 0 {
		t.Fatal("node traversal or final authority changed", err, leaves)
	}
	if len(decoded.Resources.Models) != 3 || len(decoded.Resources.Tools) != 2 || len(decoded.Resources.Knowledge) != 2 || len(decoded.Resources.Storage) != 3 || decoded.Runtime.Summary.ModelResource != "summarizer" {
		t.Fatal("all-leaf, Summary and explicit data capability union changed")
	}
}

func TestWorkerParallelRequiresExplicitTerminalLLM(t *testing.T) {
	for name, change := range map[string]func(*ManifestContent){
		"root": func(c *ManifestContent) {
			c.AgentPlan.Root = "fanout"
			delete(c.AgentPlan.Nodes, "root")
			delete(c.AgentPlan.Nodes, "final")
		},
		"sequence tail": func(c *ManifestContent) {
			c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: []string{"final", "fanout"}}
		},
		"nested sequence tail": func(c *ManifestContent) {
			c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: []string{"final", "wrapper"}}
			c.AgentPlan.Nodes["wrapper"] = ManifestNode{Kind: "sequence", Children: []string{"fanout"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := workerParallelFixture(t)
			change(&c)
			before, _ := json.Marshal(c)
			err := ValidateWorkerV1(c, c.PlatformContract.Digest)
			if !errors.Is(err, ErrUnsupportedWorkerManifest) || !strings.Contains(err.Error(), `terminal parallel node "fanout" requires an explicit successor llm in sequence`) {
				t.Fatalf("missing actionable terminal diagnostic: %v", err)
			}
			after, _ := json.Marshal(c)
			if !bytes.Equal(before, after) {
				t.Fatal("gate silently injected summary leaf or rewrote order")
			}
		})
	}
	// A nested sequence can provide the explicit final leaf. Parallel children
	// themselves need not all terminate in an LLM: only the overall Final path
	// has this rule, independently of full-tree structure validation.
	c := workerParallelFixture(t)
	c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: []string{"fanout", "tail"}}
	c.AgentPlan.Nodes["tail"] = ManifestNode{Kind: "sequence", Children: []string{"final"}}
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerParallelBoundsAndSingleParent(t *testing.T) {
	for _, count := range []int{0, 1, 64, 65} {
		c := workerFixture(t)
		leaf := c.AgentPlan.Nodes[c.AgentPlan.Root]
		c.AgentPlan = AgentPlan{Root: "root", Nodes: map[string]ManifestNode{"root": {Kind: "sequence", Children: []string{"fanout", "final"}}, "final": leaf}}
		children := []string{}
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("p%d", i)
			children = append(children, id)
			c.AgentPlan.Nodes[id] = leaf
		}
		c.AgentPlan.Nodes["fanout"] = ManifestNode{Kind: "parallel", Children: children}
		if err := ValidateWorkerV1(c, c.PlatformContract.Digest); (err == nil) != (count >= 1 && count <= 64) {
			t.Fatalf("parallel children %d: %v", count, err)
		}
	}
	for _, depth := range []int{16, 17} {
		c := workerFixture(t)
		leaf := c.AgentPlan.Nodes[c.AgentPlan.Root]
		c.AgentPlan = AgentPlan{Root: "n1", Nodes: map[string]ManifestNode{"n1": {Kind: "sequence", Children: []string{"n2", "final"}}, "final": leaf}}
		for i := 2; i < depth; i++ {
			c.AgentPlan.Nodes[fmt.Sprintf("n%d", i)] = ManifestNode{Kind: "parallel", Children: []string{fmt.Sprintf("n%d", i+1)}}
		}
		c.AgentPlan.Nodes[fmt.Sprintf("n%d", depth)] = leaf
		if err := ValidateWorkerV1(c, c.PlatformContract.Digest); (err == nil) != (depth == 16) {
			t.Fatalf("parallel depth %d: %v", depth, err)
		}
	}
	for _, total := range []int{128, 129} {
		c := workerFixture(t)
		leaf := c.AgentPlan.Nodes[c.AgentPlan.Root]
		c.AgentPlan = AgentPlan{Root: "root", Nodes: map[string]ManifestNode{"root": {Kind: "sequence", Children: []string{"fanout", "final"}}, "final": leaf}}
		children, nested := []string{"nested"}, []string{}
		for i := 0; i < 63; i++ {
			id := fmt.Sprintf("a%d", i)
			children = append(children, id)
			c.AgentPlan.Nodes[id] = leaf
		}
		for i := 0; i < total-67; i++ {
			id := fmt.Sprintf("b%d", i)
			nested = append(nested, id)
			c.AgentPlan.Nodes[id] = leaf
		}
		c.AgentPlan.Nodes["fanout"] = ManifestNode{Kind: "parallel", Children: children}
		c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "parallel", Children: nested}
		if len(c.AgentPlan.Nodes) != total {
			t.Fatal("invalid test node count", len(c.AgentPlan.Nodes), total)
		}
		if err := ValidateWorkerV1(c, c.PlatformContract.Digest); (err == nil) != (total == 128) {
			t.Fatalf("parallel nodes %d: %v", total, err)
		}
	}
	for name, change := range map[string]func(*ManifestContent){
		"duplicate": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "parallel", Children: []string{"second", "second"}}
		},
		"shared parent": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "parallel", Children: []string{"second", "first"}}
		},
		"cycle": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "parallel", Children: []string{"fanout"}}
		},
		"missing":     func(c *ManifestContent) { delete(c.AgentPlan.Nodes, "second") },
		"unreachable": func(c *ManifestContent) { c.AgentPlan.Nodes["orphan"] = c.AgentPlan.Nodes["second"] },
		"loop without explicit bound": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "loop", Body: "second", MaxIterations: 0}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := workerParallelFixture(t)
			change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("invalid nonterminal branch accepted")
			}
		})
	}
}

func TestWorkerParallelRejectsInactiveOptionsAndBranchAuthority(t *testing.T) {
	yes := true
	for name, change := range map[string]func(*ManifestNode){
		"instruction": func(n *ManifestNode) { n.Instruction = "hidden" }, "model": func(n *ManifestNode) { n.ModelResource = "primary" },
		"tools": func(n *ManifestNode) { n.ToolResources = []string{} }, "knowledge": func(n *ManifestNode) { n.KnowledgeResources = []string{} },
		"callables": func(n *ManifestNode) { n.CallableEntries = []string{} }, "generation": func(n *ManifestNode) { n.Generation = &Generation{} },
		"memory": func(n *ManifestNode) { n.Memory = &ManifestMemory{} }, "artifact": func(n *ManifestNode) { n.Artifact = &ManifestArtifact{} },
		"summary": func(n *ManifestNode) { n.AddSessionSummary = &yes }, "body": func(n *ManifestNode) { n.Body = "first" }, "iterations": func(n *ManifestNode) { n.MaxIterations = 1 },
	} {
		t.Run("parallel/"+name, func(t *testing.T) {
			c := workerParallelFixture(t)
			n := c.AgentPlan.Nodes["fanout"]
			change(&n)
			c.AgentPlan.Nodes["fanout"] = n
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("parallel carried hidden llm authority")
			}
		})
	}
	for name, change := range map[string]func(*ManifestContent){
		"branch model missing": func(c *ManifestContent) { delete(c.Resources.Models, c.AgentPlan.Nodes["first"].ModelResource) },
		"branch model tool_call": func(c *ManifestContent) {
			key := c.AgentPlan.Nodes["second"].ModelResource
			m := c.Resources.Models[key]
			m.Capabilities = []string{"chat"}
			c.Resources.Models[key] = m
		},
		"sibling callable leak": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["second"]
			n.CallableEntries = append(n.CallableEntries, "tools/tool0")
			c.AgentPlan.Nodes["second"] = n
		},
		"branch second knowledge": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["second"]
			n.KnowledgeResources = append(n.KnowledgeResources, "docs0")
			c.AgentPlan.Nodes["second"] = n
		},
		"branch credential audience": func(c *ManifestContent) {
			key := c.AgentPlan.Nodes["second"].ModelResource
			m := c.Resources.Models[key]
			m.Credential.AudienceDigest = c.Resources.Models["summarizer"].Credential.AudienceDigest
			c.Resources.Models[key] = m
		},
		"missing summary": func(c *ManifestContent) { delete(c.Resources.Models, "summarizer") },
		"extra endpoint": func(c *ManifestContent) {
			c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "unselected.example.test")
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := workerParallelFixture(t)
			change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("invalid parallel branch authority accepted")
			}
		})
	}
}
