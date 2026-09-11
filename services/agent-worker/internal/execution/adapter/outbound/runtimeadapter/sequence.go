package runtimeadapter

import (
	"context"
	"net/url"
	"slices"
	"strings"

	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/knowledgestore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func cloneSequencePlan(p domain.Plan) domain.Plan {
	if p.Executors != nil {
		fixed := make(map[string]domain.ExecutorPlan, len(p.Executors))
		for k, v := range p.Executors {
			fixed[k] = v
		}
		p.Executors = fixed
	}
	if p.Nodes != nil {
		original := p.Nodes
		p.Nodes = make(map[string]domain.NodePlan, len(original))
		for id, n := range original {
			n.Children = append([]string(nil), n.Children...)
			n.ToolResources = append([]string(nil), n.ToolResources...)
			if n.Temperature != nil {
				v := *n.Temperature
				n.Temperature = &v
			}
			if n.MaxOutputTokens != nil {
				v := *n.MaxOutputTokens
				n.MaxOutputTokens = &v
			}
			if n.Workspace != nil {
				v := *n.Workspace
				v.Tools = append([]string(nil), v.Tools...)
				n.Workspace = &v
			}
			if n.Memory != nil {
				v := *n.Memory
				v.Tools = append([]string(nil), v.Tools...)
				n.Memory = &v
			}
			p.Nodes[id] = n
		}
	}
	if p.Knowledges != nil {
		original := p.Knowledges
		p.Knowledges = make(map[string]domain.KnowledgePlan, len(original))
		for key, k := range original {
			k.Backend = k.Backend.Clone()
			p.Knowledges[key] = k
		}
	}
	return p
}

func validateSequencePlan(p domain.Plan) error {
	if err := validateWorkspacePlan(p); err != nil {
		return err
	}
	if len(p.Nodes) == 0 {
		if len(p.Knowledges) > 0 {
			return application.ErrManifestInvalid
		}
		return nil
	}
	if len(p.Nodes) > 128 || p.Knowledge != nil {
		return application.ErrManifestInvalid
	}
	seen := map[string]bool{}
	usedTools := map[string]bool{}
	usedKnowledge := map[string]bool{}
	memoryNames := map[string]bool{}
	memoryUsed, artifactUsed := false, false
	var visit func(string, int) bool
	visit = func(id string, depth int) bool {
		n, ok := p.Nodes[id]
		if !ok || seen[id] || depth > 16 || !mcpResourcePattern.MatchString(id) {
			return false
		}
		seen[id] = true
		if n.Kind == "sequence" || n.Kind == "parallel" || n.Kind == "loop" {
			if n.Instruction != "" || n.ModelName != "" || n.ModelEndpoint != "" || n.ModelCredential != (domain.CredentialUse{}) || n.Temperature != nil || n.MaxOutputTokens != nil || len(n.ToolResources) > 0 || n.KnowledgeResource != "" || n.Memory != nil || n.Artifact || n.AddSessionSummary || n.Workspace != nil {
				return false
			}
			if n.Kind == "loop" {
				return len(n.Children) == 0 && n.Body != "" && n.MaxIterations >= 1 && n.MaxIterations <= 32 && visit(n.Body, depth+1)
			}
			if len(n.Children) < 1 || len(n.Children) > 64 || n.Body != "" || n.MaxIterations != 0 {
				return false
			}
			for _, child := range n.Children {
				if !visit(child, depth+1) {
					return false
				}
			}
			return true
		}
		if n.Kind != "llm" || len(n.Children) > 0 || n.Body != "" || n.MaxIterations != 0 || strings.TrimSpace(n.ModelName) == "" {
			return false
		}
		ep, err := url.Parse(n.ModelEndpoint)
		if err != nil || (ep.Scheme != "http" && ep.Scheme != "https") || ep.Hostname() == "" || ep.User != nil || ep.RawQuery != "" || ep.ForceQuery || strings.Contains(n.ModelEndpoint, "#") || n.ModelCredential.CredentialID == "" || n.ModelCredential.Purpose != "api_key" || n.ModelCredential.AudienceDigest != protocol.CredentialAudienceDigest("openai_compatible", n.ModelEndpoint) {
			return false
		}
		if n.MaxOutputTokens != nil && (*n.MaxOutputTokens < 1 || *n.MaxOutputTokens > p.MaxOutputTokens) {
			return false
		}
		if n.Temperature != nil && (*n.Temperature < 0 || *n.Temperature > 2) {
			return false
		}
		localTools := map[string]bool{}
		for _, key := range n.ToolResources {
			if localTools[key] {
				return false
			}
			localTools[key] = true
			usedTools[key] = true
		}
		if n.KnowledgeResource != "" {
			if _, ok := p.Knowledges[n.KnowledgeResource]; !ok {
				return false
			}
			usedKnowledge[n.KnowledgeResource] = true
		}
		if n.Memory != nil {
			if p.Memory == nil {
				return false
			}
			memoryUsed = true
			limit := int64(n.Memory.PreloadLimit)
			cfg := protocol.ManifestMemory{Resource: "memory", Tools: n.Memory.Tools, PreloadLimit: &limit}
			if cfg.Validate() != nil {
				return false
			}
			for _, name := range n.Memory.Tools {
				memoryNames[name] = true
			}
		}
		if n.Artifact {
			if p.Artifact == nil {
				return false
			}
			artifactUsed = true
		}
		if n.AddSessionSummary && p.Summary == nil {
			return false
		}
		return true
	}
	if !visit(p.NodeID, 1) || len(seen) != len(p.Nodes) || len(usedTools) != len(p.Tools) || len(usedKnowledge) != len(p.Knowledges) || memoryUsed != (p.Memory != nil) || artifactUsed != (p.Artifact != nil) {
		return application.ErrManifestInvalid
	}
	// Parallel produces branch events, not a semantic root answer. Only an
	// explicit subsequent LLM on the ordered terminal path may supply Final.
	terminal := p.NodeID
	for {
		n := p.Nodes[terminal]
		if n.Kind == "loop" {
			terminal = n.Body
			continue
		}
		if n.Kind == "sequence" {
			terminal = n.Children[len(n.Children)-1]
			continue
		}
		break
	}
	if p.Nodes[terminal].Kind != "llm" {
		return application.ErrManifestInvalid
	}
	for _, t := range p.Tools {
		if !usedTools[t.Resource] {
			return application.ErrManifestInvalid
		}
	}
	if p.Memory != nil {
		if len(memoryNames) != len(p.Memory.Tools) {
			return application.ErrManifestInvalid
		}
		for _, name := range p.Memory.Tools {
			if !memoryNames[name] {
				return application.ErrManifestInvalid
			}
		}
	}
	for key, k := range p.Knowledges {
		if key != k.Resource {
			return application.ErrManifestInvalid
		}
		copy := p
		copy.Knowledge = &k
		if err := validateKnowledgePlan(copy); err != nil {
			return err
		}
	}
	return nil
}

func (f *Factory) prepareSequenceKnowledge(ctx context.Context, p domain.Plan, batch map[domain.CredentialUse]string) (map[string]*knowledgestore.Store, error) {
	stores := map[string]*knowledgestore.Store{}
	keys := make([]string, 0, len(p.Knowledges))
	for key := range p.Knowledges {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		k := p.Knowledges[key]
		copy := p
		copy.Knowledge = &k
		s, err := f.prepareKnowledge(ctx, copy, batch)
		if err != nil {
			for _, opened := range stores {
				opened.Close()
			}
			return nil, err
		}
		stores[key] = s
	}
	return stores, nil
}

func (a *attempt) populateSequence(req *trpcagent.Request) error {
	if len(a.plan.Nodes) == 0 {
		return nil
	}
	p := a.plan
	req.Nodes = make(map[string]trpcagent.NodeConfig, len(p.Nodes))
	tools := map[string]trpcagent.MCPToolConfig{}
	for i, t := range p.Tools {
		tools[t.Resource] = trpcagent.MCPToolConfig{Resource: t.Resource, Capability: t.Capability, Tool: a.mcpServices[i].Tool()}
	}
	for id, n := range p.Nodes {
		out := trpcagent.NodeConfig{Kind: n.Kind, Body: n.Body, MaxIterations: n.MaxIterations, Children: append([]string(nil), n.Children...), Instruction: n.Instruction, Model: trpcagent.Model{Endpoint: n.ModelEndpoint, Name: n.ModelName, APIKey: a.nodeModelKeys[id], Temperature: n.Temperature, MaxOutputTokens: n.MaxOutputTokens}, Artifact: n.Artifact, AddSessionSummary: n.AddSessionSummary}
		if n.Workspace != nil {
			out.WorkspaceTools = append([]string(nil), n.Workspace.Tools...)
		}
		for _, key := range n.ToolResources {
			out.Tools = append(out.Tools, tools[key])
		}
		if n.KnowledgeResource != "" {
			s := a.sequenceKnowledge[n.KnowledgeResource]
			if s == nil {
				return application.ErrRuntimeFailed
			}
			out.Knowledge = &trpcagent.KnowledgeConfig{Resource: n.KnowledgeResource, Service: s}
		}
		if n.Memory != nil {
			out.Memory = &trpcagent.MemorySelection{Tools: append([]string(nil), n.Memory.Tools...), PreloadLimit: n.Memory.PreloadLimit}
		}
		req.Nodes[id] = out
	}
	req.Tools = nil
	req.Knowledge = nil
	return nil
}
