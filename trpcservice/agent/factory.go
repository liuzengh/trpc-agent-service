// Package agent builds tenant-scoped tRPC-Agent-Go agents from immutable
// ExecutionProfile snapshots.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	agentcondition "github.com/liuzengh/trpc-agent-service/trpcservice/agent/condition"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/chainagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/cycleagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/graphagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/parallelagent"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	upstreamknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/skill"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const maxCompositionDepth = 32

const platformToolVersion int64 = 1

var memoryToolRefs = []governance.VersionedRef{
	{ID: agentmemory.AddToolName, Version: platformToolVersion},
	{ID: agentmemory.UpdateToolName, Version: platformToolVersion},
	{ID: agentmemory.DeleteToolName, Version: platformToolVersion},
	{ID: agentmemory.ClearToolName, Version: platformToolVersion},
	{ID: agentmemory.SearchToolName, Version: platformToolVersion},
	{ID: agentmemory.LoadToolName, Version: platformToolVersion},
}

var knowledgeToolRef = governance.VersionedRef{ID: "knowledge_search", Version: platformToolVersion}

type ModelResolver interface {
	ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error)
}

type ToolResolver interface {
	ResolveTools(context.Context, string, []profile.VersionedRef) ([]tool.Tool, error)
}

type SkillResolver interface {
	RepositoryProvider(context.Context, string, []profile.SkillRef) (skill.RepositoryProvider, error)
}

type KnowledgeResolver interface {
	ResolveKnowledge(context.Context, string, string, []profile.VersionedRef, int64) (upstreamknowledge.Knowledge, error)
}

type CheckpointResolver interface {
	ResolveCheckpointSaver(context.Context, string, string, int64) (graph.CheckpointSaver, error)
}

type Callbacks struct {
	Agent *agentcore.Callbacks
	Model *model.Callbacks
	Tool  *tool.Callbacks
}

type Factory struct {
	// Capabilities is the code-owned admission registry used to turn an
	// immutable profile into a RuntimePlan. A zero value selects the reviewed
	// default registry so existing process wiring remains compatible.
	Capabilities  CapabilityRegistry
	Profiles      profile.ExecutionProfileResolver
	Models        ModelResolver
	Tools         ToolResolver
	Skills        SkillResolver
	Knowledge     KnowledgeResolver
	Conditions    agentcondition.Resolver
	Memory        agentmemory.Service
	Checkpoints   CheckpointResolver
	Callbacks     Callbacks
	Policies      governance.Repository
	Confirmations governance.ConfirmationCoordinator
	ToolResults   messaging.ToolResultStore
	Telemetry     telemetry.Provider
}

func (f Factory) Build(ctx context.Context, snapshot profile.ExecutionProfileSnapshot) (agentcore.Agent, error) {
	value, _, err := f.BuildWithPlugins(ctx, snapshot)
	return value, err
}

// BuildWithPlugins returns the immutable root agent and its reviewed
// runner-scoped plugins. Plugins are materialized only from the root revision:
// a child agent must not silently alter the runner that hosts the whole graph.
func (f Factory) BuildWithPlugins(ctx context.Context, snapshot profile.ExecutionProfileSnapshot) (agentcore.Agent, []plugin.Plugin, error) {
	if f.Profiles == nil || f.Models == nil {
		return nil, nil, runtime.ErrCapabilityUnsupported
	}
	plan, err := f.compileRuntimePlan(snapshot)
	if err != nil {
		return nil, nil, err
	}
	pluginPlan, err := CompilePluginPlan(snapshot.PluginRefs)
	if err != nil {
		return nil, nil, err
	}
	buildOptions := factoryBuildOptions{}
	if pluginPlan.ToolSearch {
		// A single compiled surface is supplied both to llmagent (for execution)
		// and to the official runner plugin (for deferred discovery). This makes
		// tool_search a presentation layer over the existing guarded surface,
		// rather than a second resolver or authorization path.
		tools, compileErr := f.toolSurfaceCompiler().Compile(ctx, plan)
		if compileErr != nil {
			return nil, nil, compileErr
		}
		if len(tools) == 0 {
			return nil, nil, runtime.ErrCapabilityUnsupported
		}
		buildOptions.rootTools = tools
		buildOptions.rootToolsResolved = true
		buildOptions.deferRootTools = true
	}
	buildOptions.enableAwaitUserReply = pluginPlan.AwaitUserReply
	value, _, err := f.build(ctx, plan, snapshot.Key.AgentAppID, 0, false,
		make(map[profile.ExecutionProfileKey]bool), buildOptions)
	if err != nil {
		return nil, nil, err
	}
	plugins, err := BuildPlugins(snapshot.PluginRefs, buildOptions.rootTools)
	if err != nil {
		return nil, nil, err
	}
	return value, plugins, nil
}

// factoryBuildOptions is intentionally private to one Factory build. Only the
// root LLM can receive a runner-scoped deferred Tool Surface; recursive child
// builds always receive its zero value.
type factoryBuildOptions struct {
	rootTools            []tool.Tool
	rootToolsResolved    bool
	deferRootTools       bool
	enableAwaitUserReply bool
}

func (f Factory) compileRuntimePlan(snapshot profile.ExecutionProfileSnapshot) (RuntimePlan, error) {
	registry := f.Capabilities
	if len(registry.descriptors) == 0 {
		registry = DefaultCapabilityRegistry()
	}
	plan, err := registry.CompileRuntimePlan(snapshot)
	if err != nil {
		return RuntimePlan{}, err
	}
	if f.Memory != nil {
		if !registry.supports(CapabilityMemory) {
			return RuntimePlan{}, runtime.ErrCapabilityUnsupported
		}
		plan.Capabilities[CapabilityMemory] = registry.descriptors[CapabilityMemory]
		plan.ToolSurface.UsesMemory = true
	}
	return plan, nil
}

func (f Factory) toolSurfaceCompiler() ToolSurfaceCompiler {
	return ToolSurfaceCompiler{Tools: f.Tools, Knowledge: f.Knowledge, Memory: f.Memory,
		Policies: f.Policies, Confirmations: f.Confirmations, ToolResults: f.ToolResults, Telemetry: f.Telemetry}
}

func (f Factory) build(ctx context.Context, plan RuntimePlan, name string, depth int,
	insideGraph bool, path map[profile.ExecutionProfileKey]bool, buildOptions factoryBuildOptions,
) (agentcore.Agent, bool, error) {
	snapshot := plan.Snapshot
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if depth != 0 && len(snapshot.PluginRefs) != 0 {
		return nil, false, runtime.ErrCapabilityUnsupported
	}
	if depth > maxCompositionDepth || path[snapshot.Key] {
		return nil, false, runtime.ErrInvariantViolation
	}
	path[snapshot.Key] = true
	defer delete(path, snapshot.Key)

	if snapshot.AgentKind == agentapp.AgentKindLLM {
		value, ask, err := f.buildLLM(ctx, plan, name, insideGraph, buildOptions)
		return value, ask, err
	}
	subAgents := make([]agentcore.Agent, 0, len(snapshot.AgentSpec.Nodes))
	requiresConfirmation := false
	for _, node := range snapshot.AgentSpec.Nodes {
		if node.FailurePolicy != agentapp.FailurePolicyFailFast {
			return nil, false, runtime.ErrCapabilityUnsupported
		}
		key := profile.ExecutionProfileKey{
			TenantID:         snapshot.Key.TenantID,
			TenantVersion:    snapshot.Key.TenantVersion,
			AgentAppID:       node.AgentRef.AgentAppID,
			AgentAppRevision: node.AgentRef.Revision,
			ContentDigest:    node.AgentRef.ContentDigest,
			ConfigVersion:    snapshot.Key.ConfigVersion,
			PolicyVersion:    snapshot.Key.PolicyVersion,
		}
		var child profile.ExecutionProfileSnapshot
		var err error
		if resolver, ok := f.Profiles.(profile.ChildExecutionProfileResolver); ok {
			child, err = resolver.ResolveChild(ctx, snapshot, node)
		} else {
			child, err = f.Profiles.Resolve(ctx, key)
		}
		if err != nil {
			return nil, false, err
		}
		childPlan, err := f.compileRuntimePlan(child)
		if err != nil {
			return nil, false, err
		}
		built, childRequiresConfirmation, err := f.build(ctx, childPlan, node.Key, depth+1,
			insideGraph || snapshot.AgentKind == agentapp.AgentKindGraph, path, factoryBuildOptions{})
		if err != nil {
			return nil, false, err
		}
		subAgents = append(subAgents, built)
		requiresConfirmation = requiresConfirmation || childRequiresConfirmation
	}

	switch snapshot.AgentKind {
	case agentapp.AgentKindChain:
		return chainagent.New(name, chainagent.WithSubAgents(subAgents), chainagent.WithAgentCallbacks(f.Callbacks.Agent)), requiresConfirmation, nil
	case agentapp.AgentKindParallel:
		if snapshot.AgentSpec.MaxConcurrency < len(subAgents) {
			return nil, false, runtime.ErrCapabilityUnsupported
		}
		return parallelagent.New(name, parallelagent.WithSubAgents(subAgents), parallelagent.WithAgentCallbacks(f.Callbacks.Agent)), requiresConfirmation, nil
	case agentapp.AgentKindCycle:
		return cycleagent.New(name, cycleagent.WithSubAgents(subAgents), cycleagent.WithMaxIterations(snapshot.AgentSpec.MaxIterations), cycleagent.WithAgentCallbacks(f.Callbacks.Agent)), requiresConfirmation, nil
	case agentapp.AgentKindGraph:
		value, err := f.buildGraph(ctx, plan, name, subAgents, requiresConfirmation)
		return value, requiresConfirmation, err
	default:
		return nil, false, runtime.ErrCapabilityUnsupported
	}
}

func (f Factory) buildLLM(ctx context.Context, plan RuntimePlan, name string, insideGraph bool,
	buildOptions factoryBuildOptions,
) (agentcore.Agent, bool, error) {
	snapshot := plan.Snapshot
	refs := append([]profile.VersionedRef{snapshot.ModelProfileRef}, snapshot.FallbackModelRefs...)
	models := make([]model.Model, 0, len(refs))
	for _, ref := range refs {
		resolved, err := f.Models.ResolveModel(ctx, snapshot.Key.TenantID, ref)
		if err != nil {
			return nil, false, err
		}
		models = append(models, instrumentModel(f.Telemetry, budgetedModel{inner: resolved}))
	}
	resolvedModel := models[0]
	if len(models) > 1 {
		resolvedModel = newFailoverModel(refs, models)
	}
	modelCallbacks := f.Callbacks.Model
	toolCallbacks := f.Callbacks.Tool
	if telemetry.Enabled(f.Telemetry) {
		modelCallbacks = withTelemetryModelCallbacks(modelCallbacks, f.Telemetry)
		toolCallbacks = withTelemetryToolCallbacks(toolCallbacks, f.Telemetry)
	}
	options := []llmagent.Option{
		llmagent.WithDescription(snapshot.Description),
		llmagent.WithInstruction(snapshot.Instruction),
		llmagent.WithGlobalInstruction(snapshot.GlobalInstruction),
		llmagent.WithModel(resolvedModel),
		llmagent.WithAgentCallbacks(f.Callbacks.Agent),
		llmagent.WithModelCallbacks(modelCallbacks),
		llmagent.WithToolCallbacks(toolCallbacks),
	}
	if buildOptions.enableAwaitUserReply {
		options = append(options, llmagent.WithAwaitUserReplyTool(true))
	}
	if len(snapshot.GenerationConfig) != 0 {
		config, err := decodeGenerationConfig(snapshot.GenerationConfig)
		if err != nil {
			return nil, false, err
		}
		options = append(options, llmagent.WithGenerationConfig(config))
	}
	tools := buildOptions.rootTools
	if !buildOptions.rootToolsResolved {
		var err error
		tools, err = f.toolSurfaceCompiler().Compile(ctx, plan)
		if err != nil {
			return nil, false, err
		}
	}
	if len(tools) != 0 {
		options = append(options, llmagent.WithTools(tools))
	}
	if buildOptions.deferRootTools {
		// Toolsearch injects only tools that the session has explicitly loaded
		// into the model request. WithTools retains the same callables in the
		// execution surface; WithToolFilter hides their initial declarations and
		// cannot weaken their policy/budget/telemetry wrappers.
		options = append(options, llmagent.WithToolFilter(func(context.Context, tool.Tool) bool { return false }))
	}
	if len(snapshot.SkillRefs) != 0 {
		if f.Skills == nil {
			return nil, false, runtime.ErrCapabilityUnsupported
		}
		provider, err := f.Skills.RepositoryProvider(ctx, snapshot.Key.TenantID, snapshot.SkillRefs)
		if err != nil {
			return nil, false, err
		}
		// Explicitly select the SDK's knowledge-only profile. Leaving this
		// implicit lets trpc-agent-go convenience-wire a local code executor
		// when Skills are configured, which would violate the service's
		// tenant-scoped execution and artifact governance boundary.
		options = append(options,
			llmagent.WithSkillRepositoryProvider(provider),
			llmagent.WithSkillScopeMode(skill.SkillScopeApp),
			llmagent.WithSkillFilter(revisionSkillFilter(snapshot.SkillRefs)),
			llmagent.WithSkillToolProfile(llmagent.SkillToolProfileKnowledgeOnly),
		)
	}
	value := agentcore.Agent(llmagent.New(name, options...))
	askTools, err := f.graphAskTools(ctx, plan)
	if err != nil {
		return nil, false, err
	}
	if insideGraph && len(askTools) != 0 {
		value, err = newGraphConfirmationAgent(value, askTools)
		if err != nil {
			return nil, false, err
		}
	}
	return value, len(askTools) != 0, nil
}

// revisionSkillFilter is a second, SDK-native visibility boundary for Skill
// summaries. The repository resolver mounts only the referenced immutable
// package roots, while this filter ensures a repository implementation cannot
// make an extra skill visible during a run. Names are validated against the
// published skill IDs before a provider is accepted.
func revisionSkillFilter(refs []profile.SkillRef) skill.VisibilityFilter {
	allowed := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		allowed[ref.ID] = struct{}{}
	}
	return func(_ context.Context, summary skill.Summary) bool {
		_, ok := allowed[summary.Name]
		return ok
	}
}

func rejectReservedToolRefs(refs []profile.VersionedRef) error {
	for _, ref := range refs {
		if ref.ID == knowledgeToolRef.ID || ref.ID == agentapp.PluginToolSearch || ref.ID == agentapp.PluginAwaitUserReply || ref.ID == "call_tool" {
			return runtime.ErrCapabilityUnsupported
		}
		for _, reserved := range memoryToolRefs {
			if ref.ID == reserved.ID {
				return runtime.ErrCapabilityUnsupported
			}
		}
	}
	return nil
}

func (f Factory) graphAskTools(ctx context.Context, plan RuntimePlan) (map[string]struct{}, error) {
	snapshot := plan.Snapshot
	result := make(map[string]struct{})
	refs := make([]governance.VersionedRef, 0, len(snapshot.ToolRefs)+len(memoryToolRefs)+1)
	refs = append(refs, governanceRefs(plan.ToolSurface.RevisionRefs)...)
	if plan.ToolSurface.UsesMemory {
		memoryRefs, err := refsForMemoryTools(f.Memory.Tools())
		if err != nil {
			return nil, err
		}
		refs = append(refs, memoryRefs...)
	}
	if len(plan.ToolSurface.KnowledgeRefs) != 0 {
		refs = append(refs, knowledgeToolRef)
	}
	if len(refs) == 0 {
		return result, nil
	}
	if f.Policies == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	policy, err := f.Policies.GetPolicy(ctx, snapshot.Key.TenantID, snapshot.Key.PolicyVersion)
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if governance.ToolDecision(policy, ref).Action == governance.ActionAsk {
			// Continuations intentionally resolve only revision-declared tools:
			// a platform memory/knowledge tool needs the exact originating
			// runtime plan, which a confirmation record does not persist yet.
			// Refuse this combination rather than publishing a confirmation
			// that cannot be replayed safely after a Worker restart.
			if ref == knowledgeToolRef || isMemoryToolRef(ref) {
				return nil, runtime.ErrCapabilityUnsupported
			}
			result[ref.ID] = struct{}{}
		}
	}
	return result, nil
}

// ResolveConfirmedTool returns one exact, revision-pinned callable behind the same
// non-bypassable policy/grant wrapper used by normal agent construction.
func (f Factory) ResolveConfirmedTool(ctx context.Context, tenantID string, ref profile.VersionedRef) (tool.CallableTool, error) {
	if f.Tools == nil || f.Policies == nil || f.Confirmations == nil || f.ToolResults == nil || tenantID == "" || ref.ID == "" || ref.Version < 1 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	values, err := f.Tools.ResolveTools(ctx, tenantID, []profile.VersionedRef{ref})
	if err != nil {
		return nil, err
	}
	guarded, err := servicetool.GuardCallablesWithConfirmation(f.Policies, f.Confirmations, f.ToolResults,
		[]governance.VersionedRef{{ID: ref.ID, Version: ref.Version}}, values)
	if err != nil {
		return nil, err
	}
	callable, ok := guarded[0].(tool.CallableTool)
	if !ok {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if telemetry.Enabled(f.Telemetry) {
		return instrumentedCallable{inner: callable, provider: f.Telemetry}, nil
	}
	return callable, nil
}

func (f Factory) buildGraph(ctx context.Context, plan RuntimePlan, name string, subAgents []agentcore.Agent,
	requiresConfirmation bool,
) (agentcore.Agent, error) {
	snapshot := plan.Snapshot
	stateGraph := graph.NewStateGraph(graph.NewStateSchema())
	outgoing := make(map[string]bool, len(snapshot.AgentSpec.Nodes))
	for _, node := range snapshot.AgentSpec.Nodes {
		stateGraph.AddAgentNode(node.Key)
	}
	stateGraph.SetEntryPoint(snapshot.AgentSpec.EntryNode)
	if hasConditionalGraphEdges(snapshot.AgentSpec.Edges) && !plan.Uses(CapabilityGraphCondition) {
		return nil, runtime.ErrCapabilityUnsupported
	}
	conditional, err := compileGraphConditions(ctx, snapshot.Key.TenantID, snapshot.AgentSpec.Edges, f.Conditions)
	if err != nil {
		return nil, err
	}
	for _, edge := range snapshot.AgentSpec.Edges {
		if edge.ConditionRef == nil {
			stateGraph.AddEdge(edge.From, edge.To)
			outgoing[edge.From] = true
		}
	}
	for _, value := range conditional {
		condition := value.condition
		pathMap := value.pathMap
		stateGraph.AddConditionalEdges(value.from, func(condition agentcondition.Selector, paths map[string]string) graph.ConditionalFunc {
			return func(conditionCtx context.Context, state graph.State) (string, error) {
				branch, selectErr := condition.Select(conditionCtx, state)
				if selectErr != nil {
					return "", selectErr
				}
				if _, exists := paths[branch]; !exists {
					return "", runtime.ErrInvariantViolation
				}
				return branch, nil
			}
		}(condition, pathMap), pathMap)
		outgoing[value.from] = true
	}
	for _, node := range snapshot.AgentSpec.Nodes {
		if !outgoing[node.Key] {
			stateGraph.AddEdge(node.Key, graph.End)
		}
	}
	compiled, err := stateGraph.Compile()
	if err != nil {
		return nil, fmt.Errorf("compile graph: %w", err)
	}
	options := []graphagent.Option{
		graphagent.WithDescription(snapshot.Description),
		graphagent.WithSubAgents(subAgents),
		graphagent.WithMaxConcurrency(snapshot.AgentSpec.MaxConcurrency),
		graphagent.WithAgentCallbacks(f.Callbacks.Agent),
	}
	if requiresConfirmation && !snapshot.AgentSpec.Checkpoint.Required {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if snapshot.AgentSpec.Checkpoint.Required {
		if !plan.Uses(CapabilityCheckpoint) {
			return nil, runtime.ErrCapabilityUnsupported
		}
		if f.Checkpoints == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		saver, err := f.Checkpoints.ResolveCheckpointSaver(ctx, snapshot.Key.TenantID, snapshot.AgentSpec.Checkpoint.Namespace, snapshot.Key.ConfigVersion)
		if err != nil {
			return nil, err
		}
		if saver == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		options = append(options, graphagent.WithCheckpointSaver(saver))
	}
	return graphagent.New(name, compiled, options...)
}

type compiledGraphCondition struct {
	from      string
	condition agentcondition.Selector
	pathMap   map[string]string
}

// compileGraphConditions turns Revision-declared branch tokens into the exact
// path map accepted by tRPC-Agent-Go StateGraph. It repeats control-plane
// validation because custom profile resolvers and stale cache entries must not
// be able to assemble a graph with mixed or dynamic routing.
func compileGraphConditions(ctx context.Context, tenantID string, edges []agentapp.AgentEdgeSpecV1,
	resolver agentcondition.Resolver,
) ([]compiledGraphCondition, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	type source struct {
		ref     profile.VersionedRef
		pathMap map[string]string
	}
	sources := make(map[string]*source)
	unconditional := make(map[string]struct{})
	order := make([]string, 0, len(edges))
	for _, edge := range edges {
		if edge.From == "" || edge.To == "" {
			return nil, runtime.ErrInvariantViolation
		}
		if edge.ConditionRef == nil {
			if edge.Branch != "" {
				return nil, runtime.ErrInvariantViolation
			}
			if _, exists := sources[edge.From]; exists {
				return nil, runtime.ErrInvariantViolation
			}
			unconditional[edge.From] = struct{}{}
			continue
		}
		ref := profile.VersionedRef{ID: edge.ConditionRef.ID, Version: edge.ConditionRef.Version,
			ContentDigest: edge.ConditionRef.ContentDigest}
		if ref.ID == "" || ref.Version < 1 || ref.ContentDigest != "" || edge.ConditionRef.Required || edge.Branch == "" {
			return nil, runtime.ErrInvariantViolation
		}
		if _, exists := unconditional[edge.From]; exists {
			return nil, runtime.ErrInvariantViolation
		}
		value, exists := sources[edge.From]
		if !exists {
			value = &source{ref: ref, pathMap: make(map[string]string)}
			sources[edge.From] = value
			order = append(order, edge.From)
		} else if value.ref != ref {
			return nil, runtime.ErrInvariantViolation
		}
		if _, exists := value.pathMap[edge.Branch]; exists {
			return nil, runtime.ErrInvariantViolation
		}
		value.pathMap[edge.Branch] = edge.To
	}
	if len(sources) != 0 && resolver == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	result := make([]compiledGraphCondition, 0, len(order))
	for _, from := range order {
		value := sources[from]
		selector, err := resolver.ResolveGraphCondition(ctx, tenantID, value.ref)
		if err != nil || selector == nil {
			if err != nil {
				return nil, err
			}
			return nil, runtime.ErrCapabilityUnsupported
		}
		if !conditionPathMapMatches(selector.Branches(), value.pathMap) {
			return nil, runtime.ErrInvariantViolation
		}
		paths := make(map[string]string, len(value.pathMap))
		for branch, target := range value.pathMap {
			paths[branch] = target
		}
		result = append(result, compiledGraphCondition{from: from, condition: selector, pathMap: paths})
	}
	return result, nil
}

func conditionPathMapMatches(branches []string, paths map[string]string) bool {
	if len(branches) == 0 || len(branches) != len(paths) {
		return false
	}
	seen := make(map[string]struct{}, len(branches))
	for _, branch := range branches {
		if branch == "" {
			return false
		}
		if _, exists := seen[branch]; exists {
			return false
		}
		seen[branch] = struct{}{}
		if paths[branch] == "" {
			return false
		}
	}
	return true
}

func decodeGenerationConfig(value profile.GenerationConfigV1) (model.GenerationConfig, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return model.GenerationConfig{}, runtime.ErrInvariantViolation
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config model.GenerationConfig
	if err := decoder.Decode(&config); err != nil {
		return model.GenerationConfig{}, fmt.Errorf("%w: generation config: %v", runtime.ErrInvariantViolation, err)
	}
	return config, nil
}
