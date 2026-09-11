package trpcagent

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/chainagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/cycleagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/parallelagent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// executionNodes preserves the old single-LLM request while requiring explicit
// leaf selections in a graph. Each node occurs once: SDK author/branch identity
// must not ambiguously refer to multiple positions in the execution tree.
func executionNodes(req Request) (map[string]NodeConfig, string, error) {
	nodes := req.Nodes
	if len(nodes) == 0 {
		leaf := NodeConfig{Kind: "llm", Instruction: req.Instruction, Model: req.Model, Tools: req.Tools, Knowledge: req.Knowledge, Artifact: req.Artifact != nil, AddSessionSummary: req.Summary != nil && req.Summary.AddSessionSummary}
		if req.Memory != nil {
			leaf.Memory = &MemorySelection{Tools: req.Memory.Tools, PreloadLimit: req.Memory.PreloadLimit}
		}
		nodes = map[string]NodeConfig{req.NodeID: leaf}
	}
	if len(nodes) > 128 {
		return nil, "", errors.New("execution node count exceeds schema limit")
	}
	seen := map[string]bool{}
	var visit func(string, int) error
	visit = func(id string, depth int) error {
		if depth > 16 {
			return errors.New("execution depth exceeds schema limit")
		}
		n, ok := nodes[id]
		if !ok || seen[id] || strings.TrimSpace(id) == "" || strings.Contains(id, "/") {
			return errors.New("invalid execution tree identity")
		}
		seen[id] = true
		switch n.Kind {
		case "loop":
			if n.Body == "" || n.MaxIterations < 1 || n.MaxIterations > 32 || len(n.Children) != 0 || compositeHasLeafConfig(n) {
				return errors.New("invalid loop configuration")
			}
			if err := visit(n.Body, depth+1); err != nil {
				return err
			}
		case "sequence", "parallel":
			if n.Body != "" || n.MaxIterations != 0 || compositeHasLeafConfig(n) || len(n.Children) > 64 || len(n.Children) == 0 {
				return errors.New("invalid sequence configuration")
			}
			for _, child := range n.Children {
				if err := visit(child, depth+1); err != nil {
					return err
				}
			}
		case "llm":
			if len(n.Children) > 0 || n.Body != "" || n.MaxIterations != 0 {
				return errors.New("LLM cannot have sequence children")
			}
			if _, err := nodeOutputLimit(n.Model, req.MaxOutputTokens); err != nil {
				return err
			}
			if err := validateWorkspaceSelection(req, n); err != nil {
				return err
			}
			if n.Memory != nil && req.Memory == nil || n.Artifact && req.Artifact == nil || n.AddSessionSummary && req.Summary == nil {
				return errors.New("node capability service missing")
			}
		default:
			return errors.New("unsupported execution node kind")
		}
		return nil
	}
	if err := visit(req.NodeID, 1); err != nil {
		return nil, "", err
	}
	if len(seen) != len(nodes) {
		return nil, "", errors.New("unreachable execution node")
	}
	terminal, err := terminalLeaf(nodes, req.NodeID)
	return nodes, terminal, err
}
func nodeOutputLimit(m Model, published int64) (int64, error) {
	ep, err := url.Parse(m.Endpoint)
	if err != nil || (ep.Scheme != "http" && ep.Scheme != "https") || ep.Host == "" || ep.User != nil || ep.RawQuery != "" || ep.ForceQuery || strings.Contains(m.Endpoint, "#") || strings.TrimSpace(m.Name) == "" {
		return 0, errors.New("invalid model endpoint or name")
	}
	limit := published
	if m.MaxOutputTokens != nil {
		limit = *m.MaxOutputTokens
	}
	if limit <= 0 || limit > published || int64(int(limit)) != limit {
		return 0, errors.New("invalid published per-response output limit")
	}
	if m.Temperature != nil && (math.IsNaN(*m.Temperature) || *m.Temperature < 0 || *m.Temperature > 2) {
		return 0, errors.New("invalid temperature")
	}
	return limit, nil
}

type executionAssembly struct {
	workspace     *WorkspaceAssembly
	root          agent.Agent
	runnerOptions []runner.Option
	models        map[string]*modelTransport
	tools         map[string]*memoryToolState
	artifacts     []*artifactTools
	knowledge     []*tracedKnowledge
	memory        *MemoryAttempt
	mcp           *mcpState
	transports    []*http.Transport
	calls         atomic.Int64
	lifecycle     sync.WaitGroup
}

func (a *executionAssembly) close() {
	for _, t := range a.transports {
		t.CloseIdleConnections()
	}
	if a.memory != nil {
		_ = a.memory.Close()
	}
}
func (a *executionAssembly) failure() error {
	for _, s := range a.knowledge {
		if s.failed.Load() {
			return ErrKnowledge
		}
	}
	for _, s := range a.artifacts {
		if s.failed.Load() {
			return ErrArtifact
		}
	}
	for _, s := range a.tools {
		if s.approvalFailed.Load() {
			return ErrApproval
		}
		if s.failed.Load() {
			return ErrMemoryTool
		}
	}
	return nil
}
func (e Executor) assemble(ctx context.Context, req Request, nodes map[string]NodeConfig, local *overlay) (out *executionAssembly, err error) {
	a := &executionAssembly{models: map[string]*modelTransport{}, tools: map[string]*memoryToolState{}, mcp: &mcpState{}, runnerOptions: []runner.Option{runner.WithSessionService(local), runner.WithMemoryService(nil)}}
	defer func() {
		if err != nil {
			a.close()
		}
	}()
	var memoryService memory.Service
	if req.Memory != nil {
		if req.Memory.BoundKey.AppName != req.TenantID {
			return nil, ErrMemoryScope
		}
		a.memory, err = NewMemoryAttempt(ctx, memory.UserKey{AppName: local.key.AppName, UserID: local.key.UserID}, req.Memory.BoundKey, req.Memory.Entries, req.Memory.BaseRevision)
		if err != nil {
			return nil, err
		}
		memoryService, err = TraceMemoryService(a.memory, e.Tracer)
		if err != nil {
			return nil, err
		}
		a.runnerOptions = append(a.runnerOptions, runner.WithMemoryService(memoryService))
	}
	if req.Workspace != nil {
		var service artifact.Service
		if req.Artifact != nil {
			service = req.Artifact.Service
		}
		a.workspace, err = NewWorkspaceAssembly(req.Workspace.ExecTool, req.Workspace.SaveArtifactTool, service)
		if err != nil {
			return nil, err
		}
		if req.Artifact != nil {
			v := *req.Artifact
			v.Service = a.workspace.Service()
			req.Artifact = &v
		}
	}
	if req.Artifact != nil {
		if nilCapabilityService(req.Artifact.Service) || req.Artifact.MaxBytes < 1 {
			return nil, ErrArtifact
		}
		a.runnerOptions = append(a.runnerOptions, runner.WithArtifactService(req.Artifact.Service))
	}
	var build func(string) (agent.Agent, error)
	build = func(id string) (agent.Agent, error) {
		n := nodes[id]
		if n.Kind == "loop" {
			body, err := build(n.Body)
			if err != nil {
				return nil, err
			}
			return isolatedChain{Agent: cycleagent.New(id, cycleagent.WithSubAgents([]agent.Agent{body}), cycleagent.WithMaxIterations(int(n.MaxIterations))), lifecycle: &a.lifecycle}, nil
		}
		if n.Kind == "sequence" || n.Kind == "parallel" {
			children := make([]agent.Agent, 0, len(n.Children))
			for _, child := range n.Children {
				v, err := build(child)
				if err != nil {
					return nil, err
				}
				children = append(children, v)
			}
			if n.Kind == "parallel" {
				return isolatedChain{Agent: parallelagent.New(id, parallelagent.WithSubAgents(children)), lifecycle: &a.lifecycle}, nil
			}
			return isolatedChain{Agent: chainagent.New(id, chainagent.WithSubAgents(children)), lifecycle: &a.lifecycle}, nil
		}
		limit, _ := nodeOutputLimit(n.Model, req.MaxOutputTokens)
		transport := http.DefaultTransport.(*http.Transport).Clone()
		a.transports = append(a.transports, transport)
		state := &modelTransport{base: transport}
		a.models[id] = state
		m := newFixedModel(n.Model, limit, state)
		bindings := map[string]string{}
		names := []string{}
		var ordinary []tool.Tool
		approvalTools := map[string]approvalTool{}
		for _, t := range n.Tools {
			if nilCapabilityService(t.Tool) || t.Tool.Declaration() == nil {
				return nil, ErrMCP
			}
			alias := mcpToolAlias(t.Resource)
			if _, ok := bindings[alias]; ok {
				return nil, ErrCallable
			}
			bindings[alias] = "tools/" + t.Resource
			ordinary = append(ordinary, boundMCPTool{CallableTool: t.Tool, name: alias, state: a.mcp})
			names = append(names, alias)
			if t.Capability == approvalCapability {
				approvalTools[alias] = approvalTool{resource: t.Resource, capability: t.Capability}
			}
		}
		if n.Knowledge != nil {
			bindings[sdkKnowledgeName] = "knowledge/" + n.Knowledge.Resource
		}
		if len(bindings) > 0 {
			var err error
			m, err = WrapCallableModel(m, bindings)
			if err != nil {
				return nil, err
			}
		}
		max := int(limit)
		options := []llmagent.Option{llmagent.WithModel(m), llmagent.WithInstruction(n.Instruction), llmagent.WithGenerationConfig(model.GenerationConfig{MaxTokens: &max, Temperature: n.Model.Temperature, Stream: true}), llmagent.WithEnableCodeExecutionResponseProcessor(false), llmagent.WithCodeExecutor(nil), llmagent.WithPreloadMemory(0), llmagent.WithAddSessionSummary(n.AddSessionSummary), llmagent.WithSyncSummaryIntraRun(false), llmagent.WithMaxHistoryRuns(0), llmagent.WithPreserveSameBranch(true)}
		for _, name := range n.WorkspaceTools {
			if a.workspace == nil {
				return nil, errors.New("workspace service missing")
			}
			t, err := a.workspace.Tool(name)
			if err != nil {
				return nil, err
			}
			ordinary = append(ordinary, t)
			names = append(names, name)
		}
		if len(ordinary) > 0 {
			options = append(options, llmagent.WithTools(ordinary))
		}
		cfg := CapabilityConfig{AddSessionSummary: n.AddSessionSummary}
		services := CapabilityServices{}
		if n.Memory != nil {
			cfg.MemoryTools = n.Memory.Tools
			cfg.MemoryPreloadLimit = n.Memory.PreloadLimit
			services.Memory = memoryService
			names = append(names, n.Memory.Tools...)
		}
		if n.Artifact {
			s := &artifactTools{tracer: e.Tracer, maxBytes: req.Artifact.MaxBytes}
			a.artifacts = append(a.artifacts, s)
			cfg.Artifact = true
			services.Artifact = req.Artifact.Service
			services.ArtifactTools = s.tools()
			names = append(names, ArtifactToolNames...)
		}
		if n.Knowledge != nil {
			if nilCapabilityService(n.Knowledge.Service) {
				return nil, ErrKnowledge
			}
			s := &tracedKnowledge{service: n.Knowledge.Service, tracer: e.Tracer}
			a.knowledge = append(a.knowledge, s)
			cfg.Knowledge = true
			services.Knowledge = s
			names = append(names, sdkKnowledgeName)
		}
		capabilityOptions, err := BuildCapabilityOptions(cfg, services)
		if err != nil {
			return nil, err
		}
		// Runner services are global; applying a disabled leaf's Runner options would
		// erase services required by another leaf. Only leaf Agent options are used.
		options = append(options, capabilityOptions.Agent...)
		if len(names) > 0 {
			if req.MaxToolCalls < 1 {
				return nil, ErrMemoryTool
			}
			ts := newMemoryToolState(names, req.MaxToolCalls, e.Tracer)
			ts.sharedCalls = &a.calls
			if len(approvalTools) > 0 {
				if e.Approvals == nil {
					return nil, ErrApproval
				}
				ts.approval = &approvalState{store: e.Approvals, tenantID: req.TenantID, runID: req.RunID, attemptID: req.AttemptID, nodeID: id, tools: approvalTools}
			}
			a.tools[id] = ts
			options = append(options, llmagent.WithToolCallbacks(ts.callbacks()))
		}
		return llmagent.New(id, options...), nil
	}
	a.root, err = build(req.NodeID)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// SDK v1.11.2 composite agents write Agent/AgentName in its run goroutine while
// Runner concurrently reads its original invocation for diagnostics. A public
// clone isolates that mutable execution identity; Session and append notices
// remain shared. Preserve Branch so this wrapper adds no history-filter level.
type isolatedChain struct {
	agent.Agent
	lifecycle *sync.WaitGroup
}

func (a isolatedChain) Run(ctx context.Context, inv *agent.Invocation) (<-chan *event.Event, error) {
	owned := inv.Clone(agent.WithInvocationAgent(a.Agent), agent.WithInvocationBranch(inv.Branch))
	if a.lifecycle == nil {
		return a.Agent.Run(agent.NewInvocationContext(ctx, owned), owned)
	}
	a.lifecycle.Add(1)
	upstream, err := a.Agent.Run(agent.NewInvocationContext(ctx, owned), owned)
	if err != nil {
		a.lifecycle.Done()
		return nil, err
	}
	downstream := make(chan *event.Event)
	go func() {
		defer a.lifecycle.Done()
		defer close(downstream)
		for evt := range upstream {
			select {
			case downstream <- evt:
			case <-ctx.Done():
			}
		}
	}()
	return downstream, nil
}

// Parallel arrival order never defines the business result. The root's final
// sequence path must finish at an explicit LLM outside the parallel block.
func terminalLeaf(nodes map[string]NodeConfig, id string) (string, error) {
	n := nodes[id]
	switch n.Kind {
	case "llm":
		return id, nil
	case "loop":
		return terminalLeaf(nodes, n.Body)
	case "sequence":
		return terminalLeaf(nodes, n.Children[len(n.Children)-1])
	default:
		return "", errors.New("parallel requires an explicit following LLM final")
	}
}

// Runner may close its root stream immediately after a branch error, before
// Parallel's sibling streams finish. Wait for the existing SDK composite streams
// to close before releasing shared Attempt services; never add another scheduler.
func (a *executionAssembly) wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() { a.lifecycle.Wait(); close(done) }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func compositeHasLeafConfig(n NodeConfig) bool {
	return len(n.WorkspaceTools) > 0 || n.Instruction != "" || n.Model.Endpoint != "" || n.Model.Name != "" || n.Model.APIKey != "" || n.Model.Temperature != nil || n.Model.MaxOutputTokens != nil || n.Memory != nil || n.Artifact || n.Knowledge != nil || len(n.Tools) > 0 || n.AddSessionSummary
}

func validateWorkspaceSelection(req Request, n NodeConfig) error {
	if len(n.WorkspaceTools) == 0 {
		return nil
	}
	if req.Workspace == nil || len(n.WorkspaceTools) > 2 {
		return errors.New("workspace service missing")
	}
	seen := map[string]bool{}
	for _, name := range n.WorkspaceTools {
		if seen[name] {
			return errors.New("duplicate workspace tool")
		}
		seen[name] = true
		switch name {
		case "workspace_exec":
			if nilCapabilityService(req.Workspace.ExecTool) {
				return errors.New("workspace execution missing")
			}
		case "workspace_save_artifact":
			if !n.Artifact || req.Artifact == nil || nilCapabilityService(req.Workspace.SaveArtifactTool) {
				return errors.New("workspace save requires node artifact")
			}
		default:
			return errors.New("unknown workspace tool")
		}
	}
	return nil
}
