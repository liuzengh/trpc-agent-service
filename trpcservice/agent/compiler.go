package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/modelops"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"golang.org/x/sync/singleflight"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

var environmentNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

// Compiler turns an immutable control-plane revision into a runnable Agent.
type Compiler interface {
	Compile(ctx context.Context, scope runtimecontext.Scope) (agentcore.Agent, error)
}

// RunPolicyProvider supplies request-scoped governance options.
type RunPolicyProvider interface {
	RunPolicyOptions(
		ctx context.Context,
		input ChatInput,
	) ([]agentcore.RunOption, error)
}

type UsagePricing struct {
	PromptPerMillion     float64
	CompletionPerMillion float64
}

func (p UsagePricing) Cost(promptTokens int, completionTokens int) float64 {
	return float64(promptTokens)/1_000_000*p.PromptPerMillion +
		float64(completionTokens)/1_000_000*p.CompletionPerMillion
}

type UsagePricingProvider interface {
	UsagePricing(ctx context.Context, scope runtimecontext.Scope) (UsagePricing, error)
}

type KnowledgeProvider interface {
	KnowledgeForRevision(
		ctx context.Context,
		scope runtimecontext.Scope,
		revision controlplane.AgentRevision,
	) (knowledge.Knowledge, bool, error)
}

// StaticCompiler preserves the dependency-free tutorial and focused tests.
type StaticCompiler struct {
	agent agentcore.Agent
}

// NewStaticCompiler returns the same immutable Agent for every valid scope.
func NewStaticCompiler(agentInstance agentcore.Agent) (*StaticCompiler, error) {
	if agentInstance == nil {
		return nil, fmt.Errorf("static compiler agent is required")
	}
	return &StaticCompiler{agent: agentInstance}, nil
}

func (c *StaticCompiler) Compile(
	_ context.Context,
	scope runtimecontext.Scope,
) (agentcore.Agent, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return c.agent, nil
}

// RevisionCompiler compiles and caches published Agent revisions.
type RevisionCompiler struct {
	repository    controlplane.Repository
	startupModel  model.Model
	defaultStream bool
	mu            sync.RWMutex
	cache         map[string]agentcore.Agent
	group         singleflight.Group
	toolCatalog   *platformtool.Catalog
	auditWriter   audit.Writer
	approvals     approval.Repository
	knowledge     KnowledgeProvider
	toolJournal   toolexec.Journal
	secrets       secret.Store
	modelBudget   *tenant.Guard
	skills        *platformskill.Registry
}

type RevisionCompilerOption func(*RevisionCompiler)

func WithSkills(registry *platformskill.Registry) RevisionCompilerOption {
	return func(c *RevisionCompiler) { c.skills = registry }
}

func WithModelBudget(guard *tenant.Guard) RevisionCompilerOption {
	return func(c *RevisionCompiler) { c.modelBudget = guard }
}

// ModelForRevision also resolves background models through the same tenant
// secret grants, revision pricing and per-call budget guard as foreground runs.
func (c *RevisionCompiler) ModelForRevision(ctx context.Context, revision controlplane.AgentRevision, purpose string) (model.Model, error) {
	selected, err := c.buildRevisionModel(ctx, revision.TenantID, revision.ModelConfig)
	if err != nil {
		return nil, err
	}
	if c.modelBudget == nil {
		return selected, nil
	}
	cfg, err := parseRevisionModelConfig(revision.ModelConfig)
	if err != nil {
		return nil, err
	}
	return modelops.New(selected, c.modelBudget, modelops.Options{TenantID: revision.TenantID, AppID: revision.AppID, Purpose: purpose, PromptPerMillion: cfg.PromptCostPerMillion, CompletionPerMillion: cfg.CompletionCostPerMillion, MaxPromptTokens: cfg.MaxPromptTokens, MaxCompletionTokens: cfg.MaxCompletionTokens, Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second})
}

func WithSecretStore(store secret.Store) RevisionCompilerOption {
	return func(c *RevisionCompiler) { c.secrets = store }
}

func WithToolCatalog(catalog *platformtool.Catalog) RevisionCompilerOption {
	return func(compiler *RevisionCompiler) {
		compiler.toolCatalog = catalog
	}
}

func WithAuditWriter(writer audit.Writer) RevisionCompilerOption {
	return func(compiler *RevisionCompiler) {
		compiler.auditWriter = writer
	}
}

func WithApprovalRepository(repository approval.Repository) RevisionCompilerOption {
	return func(compiler *RevisionCompiler) {
		compiler.approvals = repository
	}
}

func WithKnowledgeProvider(provider KnowledgeProvider) RevisionCompilerOption {
	return func(compiler *RevisionCompiler) {
		compiler.knowledge = provider
	}
}

func WithToolExecutionJournal(journal toolexec.Journal) RevisionCompilerOption {
	return func(compiler *RevisionCompiler) {
		compiler.toolJournal = journal
	}
}

// NewRevisionCompiler creates a compiler. Revisions with model source
// "startup_env" reuse startupModel; other revisions build their own adapter.
func NewRevisionCompiler(
	repository controlplane.Repository,
	startupModel model.Model,
	defaultStream bool,
	opts ...RevisionCompilerOption,
) (*RevisionCompiler, error) {
	if repository == nil {
		return nil, fmt.Errorf("revision compiler repository is required")
	}
	if startupModel == nil {
		return nil, fmt.Errorf("revision compiler startup model is required")
	}
	compiler := &RevisionCompiler{
		repository:    repository,
		startupModel:  startupModel,
		defaultStream: defaultStream,
		cache:         make(map[string]agentcore.Agent),
		secrets:       secret.EnvStore{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(compiler)
		}
	}
	return compiler, nil
}

// Compile validates revision scope, then returns a cached immutable Agent.
func (c *RevisionCompiler) Compile(
	ctx context.Context,
	scope runtimecontext.Scope,
) (agentcore.Agent, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	revision, err := c.repository.GetRevision(ctx, scope.TenantID, scope.RevisionID)
	if err != nil {
		return nil, fmt.Errorf("load Agent revision: %w", err)
	}
	if revision.AppID != scope.AppID || revision.TenantID != scope.TenantID {
		return nil, fmt.Errorf("agent revision scope mismatch")
	}
	if runtimecontext.IsDebugExecution(ctx) {
		// Preview IDs are short-lived and user-created. Do not retain their
		// prompt/model objects forever in the published-revision cache.
		return c.compileRevision(ctx, scope, revision)
	}
	cacheKey := scope.TenantID + "\x00" + revision.ID + "\x00" + revision.Checksum
	c.mu.RLock()
	cached := c.cache[cacheKey]
	c.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}
	value, err, _ := c.group.Do(cacheKey, func() (any, error) {
		c.mu.RLock()
		existing := c.cache[cacheKey]
		c.mu.RUnlock()
		if existing != nil {
			return existing, nil
		}
		compiled, compileErr := c.compileRevision(ctx, scope, revision)
		if compileErr != nil {
			return nil, compileErr
		}
		c.mu.Lock()
		c.cache[cacheKey] = compiled
		c.mu.Unlock()
		return compiled, nil
	})
	if err != nil {
		return nil, err
	}
	compiled, ok := value.(agentcore.Agent)
	if !ok || compiled == nil {
		return nil, fmt.Errorf("compiled revision returned invalid Agent")
	}
	return compiled, nil
}

// Invalidate removes every cached checksum variant of one revision.
func (c *RevisionCompiler) Invalidate(tenantID string, revisionID string) {
	prefix := tenantID + "\x00" + revisionID + "\x00"
	c.mu.Lock()
	for key := range c.cache {
		if strings.HasPrefix(key, prefix) {
			delete(c.cache, key)
		}
	}
	c.mu.Unlock()
}

type revisionAgentConfig struct {
	Skills            []platformskill.Ref          `json:"skills,omitempty"`
	MCPServers        []platformtool.MCPServerSpec `json:"mcp_servers,omitempty"`
	Name              string                       `json:"name"`
	Description       string                       `json:"description"`
	Instruction       string                       `json:"instruction"`
	Stream            *bool                        `json:"stream,omitempty"`
	PreloadMemory     int                          `json:"preload_memory,omitempty"`
	SummaryEveryTurns int                          `json:"summary_every_turns,omitempty"`
}

type revisionModelConfig struct {
	MaxPromptTokens          int     `json:"max_prompt_tokens,omitempty"`
	MaxCompletionTokens      int     `json:"max_completion_tokens,omitempty"`
	TimeoutSeconds           int     `json:"timeout_seconds,omitempty"`
	Source                   string  `json:"source"`
	Provider                 string  `json:"provider"`
	Name                     string  `json:"name"`
	BaseURL                  string  `json:"base_url"`
	APIKeyEnv                string  `json:"api_key_env"`
	APIKeyRef                string  `json:"api_key_ref,omitempty"`
	PromptCostPerMillion     float64 `json:"prompt_cost_per_million,omitempty"`
	CompletionCostPerMillion float64 `json:"completion_cost_per_million,omitempty"`
}

func (c *RevisionCompiler) compileRevision(
	ctx context.Context,
	scope runtimecontext.Scope,
	revision controlplane.AgentRevision,
) (agentcore.Agent, error) {
	if err := governance.ValidateMemoryPolicy(revision.AgentConfig, revision.MemoryConfig); err != nil {
		return nil, err
	}
	agentConfig, err := parseRevisionAgentConfig(revision)
	if err != nil {
		return nil, err
	}
	selectedModel, err := c.ModelForRevision(ctx, revision, "chat")
	if err != nil {
		return nil, err
	}
	stream := c.defaultStream
	if agentConfig.Stream != nil {
		stream = *agentConfig.Stream
	}
	policy, err := governance.ParseToolPolicy(revision.ToolPolicy)
	if err != nil {
		return nil, err
	}
	refs, err := c.skills.Validate(scope.TenantID, revision.AgentConfig, policy.AllowedTools)
	if err != nil {
		return nil, err
	}
	servers, err := platformtool.ParseMCPServers(revision.AgentConfig)
	if err != nil {
		return nil, err
	}
	if runtimecontext.IsDebugExecution(ctx) {
		policy, err = c.debugPolicy(ctx, scope.TenantID, policy, servers)
		if err != nil {
			return nil, err
		}
	}
	var tools []agenttool.Tool
	if len(policy.AllowedTools) > 0 {
		if c.toolCatalog == nil {
			return nil, fmt.Errorf("agent revision declares tools but no tool catalog is configured")
		}
		tools, err = c.toolCatalog.Resolve(platformskill.LocalTools(refs, platformtool.MCPLocalTools(servers, policy.AllowedTools)))
		if err != nil {
			return nil, err
		}
	}
	remoteTools, err := platformtool.BuildMCPTools(ctx, c.secrets, scope, servers, policy.AllowedTools)
	if err != nil {
		return nil, err
	}
	tools = append(tools, remoteTools...)
	agentOptions := []llmagent.Option{
		llmagent.WithModel(selectedModel),
		llmagent.WithDescription(agentConfig.Description),
		llmagent.WithInstruction(agentConfig.Instruction),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: stream}),
		llmagent.WithTools(tools),
	}
	if len(refs) > 0 {
		repo, err := c.skills.RepositoryFor(scope.TenantID, refs)
		if err != nil {
			return nil, err
		}
		// Explicitly prevent the framework's automatic host executor fallback.
		agentOptions = append(agentOptions, llmagent.WithSkills(repo),
			llmagent.WithSkillToolProfile(llmagent.SkillToolProfileKnowledgeOnly),
			llmagent.WithAllowedSkillTools("skill_load"),
			llmagent.WithSkillsDirectoryHints(false), llmagent.WithSkillsFilePathHints(false))
	}
	modelCallbacks, err := governance.BuildModelCallbacks(revision.GuardrailConfig)
	if err != nil {
		return nil, err
	}
	if modelCallbacks != nil {
		agentOptions = append(agentOptions, llmagent.WithModelCallbacks(modelCallbacks))
	}
	if toolCallbacks := toolexec.NewCallbacks(
		c.toolJournal, c.auditWriter, revision.ID,
	); toolCallbacks != nil {
		agentOptions = append(agentOptions, llmagent.WithToolCallbacks(toolCallbacks))
	}
	if agentConfig.PreloadMemory > 0 {
		agentOptions = append(agentOptions, llmagent.WithPreloadMemory(agentConfig.PreloadMemory))
	}
	if c.knowledge != nil {
		knowledgeBase, enabled, err := c.knowledge.KnowledgeForRevision(ctx, scope, revision)
		if err != nil {
			return nil, fmt.Errorf("build revision knowledge: %w", err)
		}
		if enabled {
			agentOptions = append(agentOptions, llmagent.WithKnowledge(knowledgeBase))
		}
	}
	return llmagent.New(agentConfig.Name, agentOptions...), nil
}

func revisionKnowledgeEnabled(raw json.RawMessage) bool {
	var config struct {
		Enabled bool `json:"enabled"`
	}
	return json.Unmarshal(raw, &config) == nil && config.Enabled
}

func (c *RevisionCompiler) RunPolicyOptions(
	ctx context.Context,
	input ChatInput,
) ([]agentcore.RunOption, error) {
	revision, err := c.repository.GetRevision(ctx, input.Scope.TenantID, input.Scope.RevisionID)
	if err != nil {
		return nil, fmt.Errorf("load revision tool policy: %w", err)
	}
	policy, err := governance.ParseToolPolicy(revision.ToolPolicy)
	if err != nil {
		return nil, err
	}
	memoryPolicy, err := governance.ParseMemoryPolicy(revision.MemoryConfig)
	if err != nil {
		return nil, err
	}
	policy.AllowedTools = governance.ScopeMemoryTools(policy.AllowedTools, memoryPolicy, input.ChatType)
	if revisionKnowledgeEnabled(revision.KnowledgeConfig) {
		policy.AllowedTools = append(policy.AllowedTools, "knowledge_search")
	}
	servers, err := platformtool.ParseMCPServers(revision.AgentConfig)
	if err != nil {
		return nil, err
	}
	dangerous, err := platformtool.MCPDangerousTools(ctx, c.secrets, input.Scope.TenantID, servers)
	if err != nil {
		return nil, err
	}
	policy.DangerousTools = append(policy.DangerousTools, dangerous...)
	if runtimecontext.IsDebugExecution(ctx) {
		policy, err = c.debugPolicy(ctx, input.Scope.TenantID, policy, servers)
		if err != nil {
			return nil, err
		}
	}
	for _, name := range policy.AllowedTools {
		if c.toolCatalog.RequiresApproval(name) {
			policy.DangerousTools = append(policy.DangerousTools, name)
		}
	}
	var recorder governance.DecisionRecorder
	if c.auditWriter != nil || c.approvals != nil || c.toolJournal != nil {
		recorder = func(ctx context.Context, decision governance.ToolDecision) error {
			approvalID := ""
			if decision.Action == "ask" && c.approvals != nil {
				record, err := c.approvals.Request(ctx, approval.Request{
					TenantID:         input.Scope.TenantID,
					AppID:            input.Scope.AppID,
					RevisionID:       input.Scope.RevisionID,
					ChannelBindingID: input.Scope.ChannelBindingID,
					RequestID:        input.RequestID,
					MessageID:        input.MessageID,
					UserID:           input.UserID,
					SessionID:        input.SessionID,
					ToolCallID:       decision.ToolCallID,
					ToolName:         decision.ToolName,
					ArgumentsHash:    decision.ArgumentsHash,
					ResumeText:       input.Text,
					ReplyTarget:      input.ReplyTarget,
				})
				if err != nil {
					return fmt.Errorf("create tool approval: %w", err)
				}
				approvalID = record.ApprovalID
			}
			if c.auditWriter == nil {
				return nil
			}
			return c.auditWriter.Record(ctx, audit.Event{
				TenantID:         input.Scope.TenantID,
				Channel:          input.Scope.ChannelType,
				ChannelBindingID: input.Scope.ChannelBindingID,
				UserID:           input.UserID,
				SessionID:        input.SessionID,
				MessageID:        input.MessageID,
				RequestID:        input.RequestID,
				TraceID:          audit.TraceID(ctx),
				RevisionID:       input.Scope.RevisionID,
				ToolName:         decision.ToolName,
				Decision:         "tool_" + decision.Action,
				Details: map[string]any{
					"code":           decision.Code,
					"calls_used":     decision.CallsUsed,
					"call_limit":     decision.CallLimit,
					"tool_call_id":   decision.ToolCallID,
					"reason":         decision.Reason,
					"arguments_hash": decision.ArgumentsHash,
					"approval_id":    approvalID,
				},
			})
		}
	}
	// Run the reservation last, after permission audit succeeds. BeforeTool
	// callbacks run too early to distinguish an allowed call from an ask/deny.
	reserve := func(ctx context.Context, decision governance.ToolDecision) error {
		if decision.Action != "allow" || c.toolJournal == nil {
			return nil
		}
		return toolexec.StartAuthorized(ctx, c.toolJournal, toolexec.Execution{
			RevisionID: revision.ID, ToolCallID: decision.ToolCallID,
			ToolName: decision.ToolName, ArgumentsHash: decision.ArgumentsHash,
		}, c.toolCatalog.IsManagedSideEffect(decision.ToolName))
	}
	return governance.RunOptionsForCaller(
		policy,
		governance.Caller{UserID: input.UserID, ChatType: input.ChatType},
		input.ApprovedTools,
		input.ApprovedToolCalls,
		recorder,
		reserve,
	), nil
}

func (c *RevisionCompiler) UsagePricing(
	ctx context.Context,
	scope runtimecontext.Scope,
) (UsagePricing, error) {
	revision, err := c.repository.GetRevision(ctx, scope.TenantID, scope.RevisionID)
	if err != nil {
		return UsagePricing{}, fmt.Errorf("load revision usage pricing: %w", err)
	}
	modelConfig, err := parseRevisionModelConfig(revision.ModelConfig)
	if err != nil {
		return UsagePricing{}, err
	}
	return UsagePricing{
		PromptPerMillion:     modelConfig.PromptCostPerMillion,
		CompletionPerMillion: modelConfig.CompletionCostPerMillion,
	}, nil
}

func (c *RevisionCompiler) buildRevisionModel(ctx context.Context, tenantID string, raw json.RawMessage) (model.Model, error) {
	if err := ValidateRevisionModelConfig(raw); err != nil {
		return nil, err
	}
	modelConfig, err := parseRevisionModelConfig(raw)
	if err != nil {
		return nil, err
	}
	modelConfig.Source = strings.ToLower(strings.TrimSpace(modelConfig.Source))
	if modelConfig.Source == "" || modelConfig.Source == "startup_env" {
		return c.startupModel, nil
	}
	if modelConfig.Source != "revision" {
		return nil, fmt.Errorf("unsupported revision model source %q", modelConfig.Source)
	}
	ref := modelConfig.APIKeyRef
	if modelConfig.APIKeyEnv != "" {
		if ref != "" || !environmentNamePattern.MatchString(modelConfig.APIKeyEnv) {
			return nil, fmt.Errorf("revision model requires only one valid api_key_ref or api_key_env")
		}
		ref = "env://" + modelConfig.APIKeyEnv
	}
	if ref == "" || c.secrets == nil {
		return nil, secret.ErrForbidden
	}
	apiKey, err := c.secrets.Resolve(ctx, tenantID, secret.Model, ref)
	if err != nil {
		return nil, fmt.Errorf("resolve revision model credential: %w", err)
	}
	return BuildModel(config.ModelConfig{
		Provider: strings.ToLower(strings.TrimSpace(modelConfig.Provider)),
		Name:     strings.TrimSpace(modelConfig.Name),
		BaseURL:  strings.TrimSpace(modelConfig.BaseURL),
		APIKey:   apiKey,
	})
}

func parseRevisionModelConfig(raw json.RawMessage) (revisionModelConfig, error) {
	var modelConfig revisionModelConfig
	if err := decodeStrictJSON(raw, &modelConfig); err != nil {
		return revisionModelConfig{}, fmt.Errorf("decode revision model config: %w", err)
	}
	if modelConfig.PromptCostPerMillion < 0 || modelConfig.CompletionCostPerMillion < 0 || modelConfig.PromptCostPerMillion > 1_000_000 || modelConfig.CompletionCostPerMillion > 1_000_000 {
		return revisionModelConfig{}, fmt.Errorf("model token prices must not be negative")
	}
	if modelConfig.MaxPromptTokens < 0 || modelConfig.MaxPromptTokens > 1_000_000 || modelConfig.MaxCompletionTokens < 0 || modelConfig.MaxCompletionTokens > 131072 || modelConfig.TimeoutSeconds < 0 || modelConfig.TimeoutSeconds > 300 {
		return revisionModelConfig{}, fmt.Errorf("invalid per-call model limits")
	}
	return modelConfig, nil
}

func decodeStrictJSON(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("configuration must contain one JSON value")
	}
	return nil
}

var _ Compiler = (*StaticCompiler)(nil)
var _ Compiler = (*RevisionCompiler)(nil)
var _ RunPolicyProvider = (*RevisionCompiler)(nil)
var _ UsagePricingProvider = (*RevisionCompiler)(nil)
