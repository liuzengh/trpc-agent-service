package manifestadapter

import (
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	manifest "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
	"slices"
)

// projectSequence runs only after full immutable/tree validation. Traversal
// follows declared children order; map order never chooses execution or Final.
func projectSequence(c protocol.ManifestContent, publication manifest.Publication) domain.Plan {
	nodes := map[string]domain.NodePlan{}
	knowledges := map[string]domain.KnowledgePlan{}
	tools := map[string]domain.ToolPlan{}
	var p domain.Plan
	first := true
	var visit func(string)
	visit = func(id string) {
		n := c.AgentPlan.Nodes[id]
		if n.Kind == "loop" {
			nodes[id] = domain.NodePlan{Kind: n.Kind, Body: n.Body, MaxIterations: n.MaxIterations}
			visit(n.Body)
			return
		}
		if n.Kind == "sequence" || n.Kind == "parallel" {
			nodes[id] = domain.NodePlan{Kind: n.Kind, Children: append([]string(nil), n.Children...)}
			for _, child := range n.Children {
				visit(child)
			}
			return
		}
		leaf := projectLLM(c, id, publication)
		if first {
			p = leaf
			p.Tools = nil
			p.Knowledge = nil
			p.Memory = nil
			p.Artifact = nil
			first = false
		}
		node := domain.NodePlan{Kind: "llm", Instruction: leaf.Instruction, ModelEndpoint: leaf.ModelEndpoint, ModelName: leaf.ModelName, ModelCredential: leaf.ModelCredential, Temperature: leaf.Temperature, MaxOutputTokens: leaf.NodeMaxOutputTokens, ToolResources: append([]string(nil), n.ToolResources...), Artifact: n.Artifact != nil, AddSessionSummary: n.AddSessionSummary != nil && *n.AddSessionSummary}
		if n.Workspace != nil {
			node.Workspace = &domain.WorkspacePlan{ExecutorResource: n.Workspace.ExecutorResource, Tools: append([]string(nil), n.Workspace.Tools...)}
		}
		for _, t := range leaf.Tools {
			tools[t.Resource] = t
		}
		if leaf.Knowledge != nil {
			node.KnowledgeResource = leaf.Knowledge.Resource
			knowledges[leaf.Knowledge.Resource] = *leaf.Knowledge
		}
		if leaf.Artifact != nil {
			p.Artifact = leaf.Artifact
		}
		if leaf.Memory != nil {
			node.Memory = &domain.NodeMemoryPlan{Tools: append([]string(nil), leaf.Memory.Tools...), PreloadLimit: leaf.Memory.PreloadLimit}
			if p.Memory == nil {
				copy := *leaf.Memory
				copy.Tools = nil
				copy.PreloadLimit = 0
				p.Memory = &copy
			}
			for _, name := range leaf.Memory.Tools {
				if !slices.Contains(p.Memory.Tools, name) {
					p.Memory.Tools = append(p.Memory.Tools, name)
				}
			}
		}
		nodes[id] = node
	}
	visit(c.AgentPlan.Root)
	p.NodeID = c.AgentPlan.Root
	p.Executors = make(map[string]domain.ExecutorPlan, len(c.Resources.Executors))
	for key, r := range c.Resources.Executors {
		p.Executors[key] = domain.ExecutorPlan{Kind: r.Kind, AdapterVersion: r.AdapterVersion}
	}
	p.Nodes = nodes
	p.Knowledges = knowledges
	keys := make([]string, 0, len(tools))
	for key := range tools {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		p.Tools = append(p.Tools, tools[key])
	}
	if p.Memory != nil {
		slices.Sort(p.Memory.Tools)
	}
	return p
}
