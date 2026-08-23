// Package agent hosts tenant-specific agents built on tRPC-Agent-Go.
package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/governance"
	"github.com/DocJlm/trpc-agent-service/trpcservice/secrets"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	agentgo "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

type Request struct {
	Tenant    tenant.Tenant
	UserID    string
	SessionID string
	Content   string
	TraceID   string
}

type Result struct {
	Content          string
	Model            string
	ToolCalls        []string
	PromptTokens     int
	CompletionTokens int
	CostUSD          float64
}

type Engine interface {
	Run(context.Context, Request) (Result, error)
	Close() error
}

// EchoEngine is deterministic and is used by tests and offline demos.
type EchoEngine struct{}

func (EchoEngine) Run(_ context.Context, req Request) (Result, error) {
	if strings.TrimSpace(req.Content) == "" {
		return Result{}, errors.New("agent input is empty")
	}
	return Result{Content: "echo: " + req.Content, Model: "fake"}, nil
}

func (EchoEngine) Close() error { return nil }

type runtime struct {
	runner   runner.Runner
	session  session.Service
	model    string
	key      string
	tenantID string
	agentID  string
	active   int
	retiring bool
}

// TRPCEngine builds one runner per immutable tenant/version/model profile.
// tRPC-Agent-Go owns short-term dialogue state while the platform inbox,
// session events and reply outbox remain the durable system of record.
type TRPCEngine struct {
	secrets      secrets.Provider
	mu           sync.Mutex
	runners      map[string]*runtime
	now          func() time.Time
	redisURL     string
	postgresDSN  string
	modelFactory func(tenant.ModelProfile, string) model.Model
	governance   governance.Controller
}

type EngineOption func(*TRPCEngine)

func WithRedisURL(value string) EngineOption {
	return func(engine *TRPCEngine) { engine.redisURL = value }
}

func WithPostgresDSN(value string) EngineOption {
	return func(engine *TRPCEngine) { engine.postgresDSN = value }
}

func WithGovernance(controller governance.Controller) EngineOption {
	return func(engine *TRPCEngine) { engine.governance = controller }
}

func withModelFactory(factory func(tenant.ModelProfile, string) model.Model) EngineOption {
	return func(engine *TRPCEngine) { engine.modelFactory = factory }
}

func NewTRPCEngine(provider secrets.Provider, options ...EngineOption) *TRPCEngine {
	engine := &TRPCEngine{
		secrets: provider,
		runners: make(map[string]*runtime),
		now:     time.Now,
	}
	for _, option := range options {
		option(engine)
	}
	if engine.governance == nil {
		engine.governance = governance.NewMemoryController()
	}
	return engine
}

func (e *TRPCEngine) Run(ctx context.Context, req Request) (Result, error) {
	if strings.TrimSpace(req.Content) == "" {
		return Result{}, errors.New("agent input is empty")
	}
	if req.TraceID == "" {
		req.TraceID = uuid.NewString()
	}
	rt, err := e.runtime(ctx, req.Tenant)
	if err != nil {
		return Result{}, err
	}
	defer e.releaseRuntime(rt)
	timeout := 45 * time.Second
	if req.Tenant.Model.Timeout != "" {
		parsed, parseErr := time.ParseDuration(req.Tenant.Model.Timeout)
		if parseErr != nil {
			return Result{}, fmt.Errorf("parse agent timeout: %w", parseErr)
		}
		timeout = parsed
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	runCtx = context.WithValue(runCtx, requestIDContextKey{}, req.TraceID)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		_ = e.governance.Cancel(cleanupCtx, req.Tenant, req.TraceID)
	}()
	events, err := rt.runner.Run(
		runCtx,
		req.UserID,
		req.SessionID,
		model.NewUserMessage(req.Content),
		agentgo.WithRequestID(req.TraceID),
	)
	if err != nil {
		return Result{}, fmt.Errorf("start tRPC agent runner: %w", err)
	}

	result := Result{Model: rt.model}
	var streamed strings.Builder
	for event := range events {
		if event == nil || event.Response == nil {
			continue
		}
		if event.Response.Error != nil {
			return Result{}, fmt.Errorf("model response: %s", event.Response.Error.Message)
		}
		if event.Response.Usage != nil {
			result.PromptTokens = event.Response.Usage.PromptTokens
			result.CompletionTokens = event.Response.Usage.CompletionTokens
		}
		for _, choice := range event.Response.Choices {
			for _, call := range choice.Message.ToolCalls {
				result.ToolCalls = appendUnique(result.ToolCalls, call.Function.Name)
			}
			for _, call := range choice.Delta.ToolCalls {
				result.ToolCalls = appendUnique(result.ToolCalls, call.Function.Name)
			}
			if event.Response.IsPartial {
				streamed.WriteString(choice.Delta.Content)
				continue
			}
			if choice.Message.Content != "" {
				result.Content = choice.Message.Content
			}
		}
	}
	if result.Content == "" {
		result.Content = streamed.String()
	}
	if strings.TrimSpace(result.Content) == "" {
		return Result{}, errors.New("agent returned no text response")
	}
	result.CostUSD = governance.CostUSD(req.Tenant.Budget, result.PromptTokens, result.CompletionTokens)
	return result, nil
}

func (e *TRPCEngine) runtime(ctx context.Context, t tenant.Tenant) (*runtime, error) {
	key := strings.Join([]string{t.ID, t.Agent.ID, t.Agent.Version, fmt.Sprint(t.RuntimeRevision), t.Model.Model, t.Model.BaseURL}, "|")
	e.mu.Lock()
	if rt := e.runners[key]; rt != nil {
		rt.active++
		e.mu.Unlock()
		return rt, nil
	}
	if e.secrets == nil {
		e.mu.Unlock()
		return nil, errors.New("secret provider is not configured")
	}
	values, err := e.secrets.Resolve(ctx, t.Model.APIKeyRef)
	if err != nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("resolve model credential: %w", err)
	}
	if len(values) != 1 {
		e.mu.Unlock()
		return nil, fmt.Errorf("model credential must contain exactly one value, got %d", len(values))
	}
	modelName := t.Model.Model
	if modelName == "" {
		modelName = "deepseek-v4-flash"
	}
	baseURL := t.Model.BaseURL
	if baseURL == "" {
		baseURL = "https://api.deepseek.com"
	}
	var llm model.Model
	if e.modelFactory != nil {
		llm = e.modelFactory(t.Model, values[0])
	} else {
		llm = openai.New(modelName, openai.WithAPIKey(values[0]), openai.WithBaseURL(baseURL))
	}
	sessionService, err := e.sessionService(t)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	tools := e.allowedTools(t)
	agentCallbacks := e.agentCallbacks(t)
	toolCallbacks := e.toolCallbacks(t)
	maxOutputTokens := t.Budget.MaxOutputTokens
	if maxOutputTokens <= 0 {
		maxOutputTokens = 2048
	}
	ag := llmagent.New(
		t.Agent.ID,
		llmagent.WithDescription("A tenant-isolated IM assistant"),
		llmagent.WithInstruction(t.Agent.Instruction),
		llmagent.WithModel(llm),
		llmagent.WithTools(tools),
		llmagent.WithAgentCallbacks(agentCallbacks),
		llmagent.WithToolCallbacks(toolCallbacks),
		llmagent.WithGenerationConfig(model.GenerationConfig{MaxTokens: &maxOutputTokens, Stream: true}),
	)
	rt := &runtime{
		runner: runner.NewRunner(
			t.ID+":"+t.Agent.ID,
			ag,
			runner.WithSessionService(sessionService),
		),
		session: sessionService,
		model:   modelName,
		key:     key, tenantID: t.ID, agentID: t.Agent.ID, active: 1,
	}
	e.runners[key] = rt
	var stale []*runtime
	for oldKey, old := range e.runners {
		if oldKey == key || old.tenantID != t.ID || old.agentID != t.Agent.ID {
			continue
		}
		old.retiring = true
		if old.active == 0 {
			delete(e.runners, oldKey)
			stale = append(stale, old)
		}
	}
	e.mu.Unlock()
	_ = closeRuntimes(stale)
	return rt, nil
}

func (e *TRPCEngine) releaseRuntime(rt *runtime) {
	if rt == nil {
		return
	}
	e.mu.Lock()
	if rt.active > 0 {
		rt.active--
	}
	shouldClose := rt.retiring && rt.active == 0
	if shouldClose {
		delete(e.runners, rt.key)
	}
	e.mu.Unlock()
	if shouldClose {
		_ = closeRuntimes([]*runtime{rt})
	}
}

func closeRuntimes(items []*runtime) error {
	var errs []error
	for _, rt := range items {
		if err := rt.runner.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := rt.session.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type requestIDContextKey struct{}

func requestID(ctx context.Context, fallback string) string {
	if value, ok := ctx.Value(requestIDContextKey{}).(string); ok && value != "" {
		return value
	}
	return fallback
}

func (e *TRPCEngine) agentCallbacks(t tenant.Tenant) *agentgo.Callbacks {
	callbacks := agentgo.NewCallbacks()
	callbacks.RegisterBeforeAgent(agentgo.BeforeAgentCallbackStructured(func(ctx context.Context, args *agentgo.BeforeAgentArgs) (*agentgo.BeforeAgentResult, error) {
		if args == nil || args.Invocation == nil {
			return nil, errors.New("agent callback invocation is missing")
		}
		ctx, span := otel.Tracer("trpc-agent-service/agent").Start(ctx, "agent.governance")
		id := requestID(ctx, args.Invocation.InvocationID)
		if err := e.governance.Reserve(ctx, t, id, args.Invocation.Message.Content); err != nil {
			span.RecordError(err)
			span.End()
			return nil, err
		}
		return &agentgo.BeforeAgentResult{Context: ctx}, nil
	}))
	callbacks.RegisterAfterAgent(agentgo.AfterAgentCallbackStructured(func(ctx context.Context, args *agentgo.AfterAgentArgs) (*agentgo.AfterAgentResult, error) {
		var promptTokens, completionTokens int
		fallback := ""
		if args != nil && args.Invocation != nil {
			fallback = args.Invocation.InvocationID
		}
		if args != nil && args.FullResponseEvent != nil && args.FullResponseEvent.Response != nil && args.FullResponseEvent.Response.Usage != nil {
			promptTokens = args.FullResponseEvent.Response.Usage.PromptTokens
			completionTokens = args.FullResponseEvent.Response.Usage.CompletionTokens
		}
		_, err := e.governance.Finalize(ctx, t, requestID(ctx, fallback), promptTokens, completionTokens)
		span := oteltrace.SpanFromContext(ctx)
		if args != nil && args.Error != nil {
			span.RecordError(args.Error)
		}
		if err != nil {
			span.RecordError(err)
		}
		span.End()
		return &agentgo.AfterAgentResult{Context: ctx}, err
	}))
	return callbacks
}

func (e *TRPCEngine) toolCallbacks(t tenant.Tenant) *tool.Callbacks {
	callbacks := tool.NewCallbacks()
	callbacks.RegisterBeforeTool(tool.BeforeToolCallbackStructured(func(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
		if args == nil {
			return nil, errors.New("tool callback arguments are missing")
		}
		ctx, span := otel.Tracer("trpc-agent-service/tool").Start(ctx, "tool."+args.ToolName)
		if !t.AllowsTool(args.ToolName) {
			err := fmt.Errorf("%w: %s", governance.ErrToolDenied, args.ToolName)
			span.RecordError(err)
			span.End()
			return nil, err
		}
		return &tool.BeforeToolResult{Context: ctx}, nil
	}))
	callbacks.RegisterAfterTool(tool.AfterToolCallbackStructured(func(ctx context.Context, args *tool.AfterToolArgs) (*tool.AfterToolResult, error) {
		span := oteltrace.SpanFromContext(ctx)
		if args != nil && args.Error != nil {
			span.RecordError(args.Error)
		}
		span.End()
		return &tool.AfterToolResult{Context: ctx}, nil
	}))
	return callbacks
}

func (e *TRPCEngine) sessionService(t tenant.Tenant) (session.Service, error) {
	switch strings.ToLower(t.Backend.Session) {
	case "redis":
		if e.redisURL == "" {
			return nil, fmt.Errorf("tenant %s selects redis session but redis is not configured", t.ID)
		}
		redisURL := e.redisURL
		if !strings.Contains(redisURL, "://") {
			redisURL = "redis://" + redisURL
		}
		service, err := sessionredis.NewService(
			sessionredis.WithRedisClientURL(redisURL),
			sessionredis.WithKeyPrefix("trpc:session:"+t.ID+":"),
			sessionredis.WithSessionEventLimit(1000),
		)
		if err != nil {
			return nil, fmt.Errorf("create redis session service: %w", err)
		}
		return service, nil
	case "postgres", "postgresql":
		if e.postgresDSN == "" {
			return nil, fmt.Errorf("tenant %s selects postgres session but postgres is not configured", t.ID)
		}
		service, err := sessionpostgres.NewService(
			sessionpostgres.WithPostgresClientDSN(e.postgresDSN),
			sessionpostgres.WithSessionEventLimit(1000),
		)
		if err != nil {
			return nil, fmt.Errorf("create postgres session service: %w", err)
		}
		return service, nil
	case "", "memory", "inmemory":
		return inmemory.NewSessionService(), nil
	default:
		return nil, fmt.Errorf("unsupported session backend %q", t.Backend.Session)
	}
}

func (e *TRPCEngine) allowedTools(t tenant.Tenant) []tool.Tool {
	if !t.AllowsTool("get_server_time") {
		return nil
	}
	type input struct{}
	type output struct {
		RFC3339 string `json:"rfc3339" jsonschema:"description=Current server time in RFC3339 format"`
		Unix    int64  `json:"unix" jsonschema:"description=Current Unix timestamp"`
	}
	clock := function.NewFunctionTool(
		func(context.Context, input) (output, error) {
			now := e.now()
			return output{RFC3339: now.Format(time.RFC3339), Unix: now.Unix()}, nil
		},
		function.WithName("get_server_time"),
		function.WithDescription("Return the current time of the Agent server."),
	)
	return []tool.Tool{clock}
}

func (e *TRPCEngine) Close() error {
	e.mu.Lock()
	items := make([]*runtime, 0, len(e.runners))
	for _, rt := range e.runners {
		items = append(items, rt)
	}
	e.runners = make(map[string]*runtime)
	e.mu.Unlock()
	return closeRuntimes(items)
}

func appendUnique(items []string, value string) []string {
	if value == "" {
		return items
	}
	for _, item := range items {
		if item == value {
			return items
		}
	}
	return append(items, value)
}
