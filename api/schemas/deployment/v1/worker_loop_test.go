package deploymentv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func workerLoopFixture(t *testing.T) ManifestContent {
	t.Helper()
	c := workerParallelFixture(t)
	c.AgentPlan.Nodes["cycle"] = ManifestNode{Kind: "loop", Body: c.AgentPlan.Root, MaxIterations: 2}
	c.AgentPlan.Root = "cycle"
	return c
}

func TestWorkerLoopExplicitBoundedBodyAndResourceClosure(t *testing.T) {
	for _, max := range []int{1, 2, 32} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			c := workerLoopFixture(t)
			n := c.AgentPlan.Nodes["cycle"]
			n.MaxIterations = int64(max)
			c.AgentPlan.Nodes["cycle"] = n
			before, _ := json.Marshal(c)
			if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(c)
			if !bytes.Equal(before, after) {
				t.Fatal("gate rewrote loop or imposed an implicit iteration policy")
			}
			decoded, err := DecodeManifestContent(after)
			if err != nil || ValidateWorkerV1(decoded, c.PlatformContract.Digest) != nil {
				t.Fatal("existing immutable loop codec rejected valid content", err)
			}
			if got := decoded.AgentPlan.Nodes["cycle"]; got.Body != "root" || got.MaxIterations != int64(max) || got.Kind != "loop" {
				t.Fatal("explicit body or iteration field drifted", got)
			}
			leaves, err := workerV1LLMNodes(decoded.AgentPlan)
			if err != nil || len(leaves) != 3 || len(decoded.AgentPlan.Nodes) != 7 {
				t.Fatal("loop expanded the static tree or resource closure", err, len(leaves))
			}
			if len(decoded.Resources.Models) != 3 || len(decoded.Resources.Tools) != 2 || len(decoded.Resources.Knowledge) != 2 || len(decoded.Resources.Storage) != 3 || decoded.Runtime.Summary.ModelResource != "summarizer" {
				t.Fatal("loop changed all-leaf/Summary/global data authority")
			}
		})
	}
}

func TestWorkerLoopFinalPathTraversesBody(t *testing.T) {
	// Root loop with a single LLM body needs no hidden post-loop LLM.
	c := workerFixture(t)
	c.AgentPlan.Nodes["cycle"] = ManifestNode{Kind: "loop", Body: c.AgentPlan.Root, MaxIterations: 2}
	c.AgentPlan.Root = "cycle"
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	// A parallel body is valid when the whole plan has an explicit successor.
	c = workerParallelFixture(t)
	c.AgentPlan.Nodes["cycle"] = ManifestNode{Kind: "loop", Body: "fanout", MaxIterations: 2}
	c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: []string{"cycle", "final"}}
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*ManifestContent){
		"parallel body": func(c *ManifestContent) {
			c.AgentPlan.Nodes["cycle"] = ManifestNode{Kind: "loop", Body: "fanout", MaxIterations: 2}
			delete(c.AgentPlan.Nodes, "root")
			delete(c.AgentPlan.Nodes, "final")
		},
		"sequence tail parallel through loop": func(c *ManifestContent) {
			c.AgentPlan.Nodes["root"] = ManifestNode{Kind: "sequence", Children: []string{"final", "fanout"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := workerLoopFixture(t)
			change(&c)
			err := ValidateWorkerV1(c, c.PlatformContract.Digest)
			if !errors.Is(err, ErrUnsupportedWorkerManifest) || !strings.Contains(err.Error(), `terminal parallel node "fanout" requires an explicit successor llm in sequence`) {
				t.Fatal("terminal loop body selected an arbitrary parallel result", err)
			}
		})
	}
}

func TestWorkerLoopBoundsAndSingleParent(t *testing.T) {
	for _, depth := range []int{16, 17} {
		c := workerFixture(t)
		leaf := c.AgentPlan.Nodes[c.AgentPlan.Root]
		c.AgentPlan = AgentPlan{Root: "n1", Nodes: map[string]ManifestNode{}}
		for i := 1; i < depth; i++ {
			c.AgentPlan.Nodes[fmt.Sprintf("n%d", i)] = ManifestNode{Kind: "loop", Body: fmt.Sprintf("n%d", i+1), MaxIterations: 2}
		}
		c.AgentPlan.Nodes[fmt.Sprintf("n%d", depth)] = leaf
		if err := ValidateWorkerV1(c, c.PlatformContract.Digest); (err == nil) != (depth == 16) {
			t.Fatalf("loop depth=%d: %v", depth, err)
		}
	}
	for name, change := range map[string]func(*ManifestContent){
		"zero bound": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["cycle"]
			n.MaxIterations = 0
			c.AgentPlan.Nodes["cycle"] = n
		},
		"negative bound": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["cycle"]
			n.MaxIterations = -1
			c.AgentPlan.Nodes["cycle"] = n
		},
		"over bound": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["cycle"]
			n.MaxIterations = 33
			c.AgentPlan.Nodes["cycle"] = n
		},
		"empty body": func(c *ManifestContent) { n := c.AgentPlan.Nodes["cycle"]; n.Body = ""; c.AgentPlan.Nodes["cycle"] = n },
		"missing body": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["cycle"]
			n.Body = "missing"
			c.AgentPlan.Nodes["cycle"] = n
		},
		"self cycle": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["cycle"]
			n.Body = "cycle"
			c.AgentPlan.Nodes["cycle"] = n
		},
		"ancestor cycle": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "loop", Body: "cycle", MaxIterations: 2}
		},
		"body has two parents": func(c *ManifestContent) {
			c.AgentPlan.Nodes["nested"] = ManifestNode{Kind: "loop", Body: "first", MaxIterations: 2}
		},
		"orphan": func(c *ManifestContent) { c.AgentPlan.Nodes["orphan"] = c.AgentPlan.Nodes["first"] },
	} {
		t.Run(name, func(t *testing.T) {
			c := workerLoopFixture(t)
			change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("invalid loop structure accepted")
			}
		})
	}
}

func TestWorkerLoopRejectsInactiveFieldsAndInvalidBodyAuthority(t *testing.T) {
	yes := true
	for name, change := range map[string]func(*ManifestNode){
		"children":    func(n *ManifestNode) { n.Children = []string{} },
		"instruction": func(n *ManifestNode) { n.Instruction = "hidden" }, "model": func(n *ManifestNode) { n.ModelResource = "primary" },
		"tools": func(n *ManifestNode) { n.ToolResources = []string{} }, "knowledge": func(n *ManifestNode) { n.KnowledgeResources = []string{} },
		"callables": func(n *ManifestNode) { n.CallableEntries = []string{} }, "generation": func(n *ManifestNode) { n.Generation = &Generation{} },
		"memory": func(n *ManifestNode) { n.Memory = &ManifestMemory{} }, "artifact": func(n *ManifestNode) { n.Artifact = &ManifestArtifact{} },
		"summary": func(n *ManifestNode) { n.AddSessionSummary = &yes },
	} {
		t.Run(name, func(t *testing.T) {
			c := workerLoopFixture(t)
			n := c.AgentPlan.Nodes["cycle"]
			change(&n)
			c.AgentPlan.Nodes["cycle"] = n
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("loop carried inactive authority")
			}
		})
	}
	for name, change := range map[string]func(*ManifestContent){
		"body model missing":  func(c *ManifestContent) { delete(c.Resources.Models, c.AgentPlan.Nodes["first"].ModelResource) },
		"body Summary absent": func(c *ManifestContent) { delete(c.Resources.Models, "summarizer") },
		"sibling tool leak": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["second"]
			n.CallableEntries = append(n.CallableEntries, "tools/tool0")
			c.AgentPlan.Nodes["second"] = n
		},
		"multiple Knowledge": func(c *ManifestContent) {
			n := c.AgentPlan.Nodes["second"]
			n.KnowledgeResources = append(n.KnowledgeResources, "docs0")
			c.AgentPlan.Nodes["second"] = n
		},
		"body credential collision": func(c *ManifestContent) {
			n := c.Resources.Models["second_model"]
			n.Credential.CredentialID = c.Resources.Models["primary"].Credential.CredentialID
			c.Resources.Models["second_model"] = n
		},
		"extra endpoint": func(c *ManifestContent) {
			c.Execution.AllowedEndpointHosts = append(c.Execution.AllowedEndpointHosts, "unselected.example.test")
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := workerLoopFixture(t)
			change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("invalid body authority accepted")
			}
		})
	}
}
