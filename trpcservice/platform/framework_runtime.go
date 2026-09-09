package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	openaimodel "trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type RuntimeEvent struct {
	Type string
	Data map[string]string
}

type StreamingRunnerAdapter interface {
	RunnerAdapter
	RunEvents(context.Context, RunnerRequest) (<-chan RuntimeEvent, error)
	Close() error
}

type AgentFactory func(context.Context, DeploymentVersion) (frameworkagent.Agent, error)

type ModelProviderProfile struct {
	ID       string
	BaseURL  string
	APIKey   string
	Model    string
	Protocol string
}

const (
	ModelProtocolChatCompletions = "chat_completions"
	ModelProtocolResponses       = "responses"
)

// OpenAICompatibleAgentFactory keeps provider credentials server-owned while
// allowing immutable Deployment Versions to select an approved model.
func OpenAICompatibleAgentFactory(profile ModelProviderProfile) AgentFactory {
	return func(_ context.Context, version DeploymentVersion) (frameworkagent.Agent, error) {
		provider, _ := version.Config["provider_profile"].(string)
		if provider != profile.ID || profile.ID == "" || profile.BaseURL == "" || profile.APIKey == "" {
			return nil, errors.New("model_provider_unavailable")
		}
		modelName, _ := version.Config["model"].(string)
		if modelName == "" {
			modelName = profile.Model
		}
		if modelName == "" {
			return nil, errors.New("model_not_configured")
		}
		var configuredModel model.Model
		switch profile.Protocol {
		case "", ModelProtocolChatCompletions:
			configuredModel = openaimodel.New(modelName, openaimodel.WithBaseURL(profile.BaseURL), openaimodel.WithAPIKey(profile.APIKey))
		case ModelProtocolResponses:
			configuredModel = newResponsesModel(modelName, profile.BaseURL, profile.APIKey)
		default:
			return nil, errors.New("model_protocol_unsupported")
		}
		options := []llmagent.Option{llmagent.WithModel(configuredModel)}
		if instruction, _ := version.Config["prompt"].(string); instruction != "" {
			options = append(options, llmagent.WithInstruction(instruction))
		}
		if raw, ok := version.Config["generation_config"]; ok {
			encoded, err := json.Marshal(raw)
			if err != nil {
				return nil, errors.New("generation_config_invalid")
			}
			var generation model.GenerationConfig
			if err := json.Unmarshal(encoded, &generation); err != nil {
				return nil, errors.New("generation_config_invalid")
			}
			options = append(options, llmagent.WithGenerationConfig(generation))
		}
		return llmagent.New(version.AgentAppID, options...), nil
	}
}

type governanceReplayContextKey struct{}
type governanceAdmissionContextKey struct{}
type governanceExternalCompletionContextKey struct{}
type governanceBufferContextKey struct{}

func DefaultAgentFactory() AgentFactory {
	return func(_ context.Context, version DeploymentVersion) (frameworkagent.Agent, error) {
		if toolName, _ := version.Config["deterministic_tool_call"].(string); toolName != "" {
			declared := false
			for _, candidate := range configStrings(version.Config, "tools") {
				if candidate == toolName {
					declared = true
					break
				}
			}
			if !declared {
				return nil, fmt.Errorf("deterministic Tool %q is not declared", toolName)
			}
			delay, err := deterministicToolDelay(version.Config["deterministic_tool_delay_ms"])
			if err != nil {
				return nil, err
			}
			return serviceagent.NewDeterministicToolAgentWithDelay(version.AgentAppID, toolName, delay), nil
		}
		delay, err := deterministicFixtureDelay(version.Config["deterministic_response_delay_ms"], "response")
		if err != nil {
			return nil, err
		}
		return serviceagent.NewDeterministicAgentWithDelay(version.AgentAppID, delay), nil
	}
}

func deterministicToolDelay(value any) (time.Duration, error) {
	return deterministicFixtureDelay(value, "Tool")
}

func deterministicFixtureDelay(value any, kind string) (time.Duration, error) {
	if value == nil {
		return 0, nil
	}
	var milliseconds int64
	switch typed := value.(type) {
	case int:
		milliseconds = int64(typed)
	case int64:
		milliseconds = typed
	case float64:
		if typed != float64(int64(typed)) {
			return 0, fmt.Errorf("deterministic %s delay is invalid", kind)
		}
		milliseconds = int64(typed)
	default:
		return 0, fmt.Errorf("deterministic %s delay is invalid", kind)
	}
	if milliseconds < 0 || milliseconds > 30000 {
		return 0, fmt.Errorf("deterministic %s delay is invalid", kind)
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

type FrameworkRunnerAdapter struct {
	resolve        func(context.Context, DeploymentVersionRef) (DeploymentVersion, bool, error)
	factory        AgentFactory
	mu             sync.Mutex
	runners        map[string]frameworkrunner.Runner
	runs           map[string]map[uint64]context.CancelFunc
	nextRun        uint64
	closed         bool
	governance     *GovernanceCenter
	toolGovernance ToolGovernance
	runWG          sync.WaitGroup
}

func NewFrameworkRunnerAdapter(resolve func(context.Context, DeploymentVersionRef) (DeploymentVersion, bool, error), factory AgentFactory) *FrameworkRunnerAdapter {
	if factory == nil {
		factory = DefaultAgentFactory()
	}
	return &FrameworkRunnerAdapter{resolve: resolve, factory: factory, runners: make(map[string]frameworkrunner.Runner), runs: make(map[string]map[uint64]context.CancelFunc)}
}

func (a *FrameworkRunnerAdapter) runner(ctx context.Context, ref DeploymentVersionRef) (frameworkrunner.Runner, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, errors.New("framework_runtime_closed")
	}
	version, ok, err := a.resolve(ctx, ref)
	if err != nil {
		return nil, err
	}
	if !ok || version.ID != ref.VersionID || version.TenantID != ref.TenantID {
		return nil, errors.New("deployment_version_not_found")
	}
	if !version.Active {
		return nil, errors.New("deployment_version_inactive")
	}
	cacheKey := versionRefKey(ref)
	if runner := a.runners[cacheKey]; runner != nil {
		return runner, nil
	}
	agent, err := a.factory(ctx, version)
	if err != nil {
		return nil, fmt.Errorf("agent_factory: %w", err)
	}
	options := []frameworkrunner.Option{}
	if a.governance != nil || a.toolGovernance != nil {
		options = append(options, frameworkrunner.WithPlugins(&governanceRuntimePlugin{center: a.governance, tools: a.toolGovernance}))
	}
	runner := frameworkrunner.NewRunner(version.AgentAppID, agent, options...)
	a.runners[cacheKey] = runner
	return runner, nil
}

type ToolGovernance interface {
	AuthorizeTool(context.Context, GovernanceRequest, string, string, []byte) error
	CompleteTool(context.Context, GovernanceRequest, string, string, error) error
}

func (a *FrameworkRunnerAdapter) SetToolGovernance(governance ToolGovernance) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.toolGovernance = governance
}

func (a *FrameworkRunnerAdapter) SetGovernance(center *GovernanceCenter) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.governance = center
}

type governanceRuntimePlugin struct {
	center *GovernanceCenter
	tools  ToolGovernance
}

type modelCallTiming struct {
	started time.Time
	once    sync.Once
}

type modelCallTimingContextKey struct{}

func (p *governanceRuntimePlugin) Name() string { return "platform-governance" }

func (p *governanceRuntimePlugin) Register(registry *plugin.Registry) {
	registry.BeforeModel(func(ctx context.Context, _ *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		if _, ok := RunnerIdentityFromContext(ctx); !ok || p.center == nil {
			return nil, nil
		}
		timing := &modelCallTiming{started: p.center.now().UTC()}
		return &model.BeforeModelResult{Context: context.WithValue(ctx, modelCallTimingContextKey{}, timing)}, nil
	})
	registry.AfterModel(func(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		request, ok := RunnerIdentityFromContext(ctx)
		timing, timed := ctx.Value(modelCallTimingContextKey{}).(*modelCallTiming)
		if !ok || !timed || p.center == nil || args != nil && args.Response != nil && args.Response.IsPartial && args.Error == nil {
			return nil, nil
		}
		var recordErr error
		timing.once.Do(func() {
			var callErr error
			if args != nil {
				callErr = args.Error
			}
			recordErr = p.center.RecordModelCall(runnerGovernanceRequest(request, nil, nil, ""), request.TraceID, p.center.now().UTC().Sub(timing.started), callErr)
		})
		if recordErr != nil {
			return nil, &GovernanceError{Code: "audit_unavailable", TraceID: request.TraceID}
		}
		return nil, nil
	})
	registry.BeforeAgent(func(ctx context.Context, args *frameworkagent.BeforeAgentArgs) (*frameworkagent.BeforeAgentResult, error) {
		if request, ok := RunnerIdentityFromContext(ctx); ok && p.center != nil {
			if _, admitted := ctx.Value(governanceAdmissionContextKey{}).(bool); !admitted {
				input := ""
				if args != nil && args.Invocation != nil {
					input = args.Invocation.Message.Content
				}
				result, err := p.center.Evaluate(ctx, runnerGovernanceRequest(request, nil, nil, input))
				if err != nil {
					return nil, err
				}
				if args != nil && args.Invocation != nil {
					args.Invocation.Message.Content = result.Input
				}
				return &frameworkagent.BeforeAgentResult{Context: context.WithValue(ctx, governanceAdmissionContextKey{}, true)}, nil
			}
			if err := p.center.RecordSpan(runnerGovernanceRequest(request, nil, nil, ""), request.TraceID, "plugin.before_agent", "ok"); err != nil {
				return nil, &GovernanceError{Code: "audit_unavailable", TraceID: request.TraceID}
			}
		}
		return nil, nil
	})
	registry.OnEvent(func(ctx context.Context, _ *frameworkagent.Invocation, upstream *event.Event) (*event.Event, error) {
		request, ok := RunnerIdentityFromContext(ctx)
		if !ok || p.center == nil || upstream == nil || upstream.Response == nil {
			return upstream, nil
		}
		updated := upstream.Clone()
		// Runner completion notices are keyed by the original event ID. Clone
		// generates a new ID, so preserve it or completion-aware Agents time out.
		updated.ID = upstream.ID
		buffer := p.center.RequiresBufferedOutput(request.TenantID, request.AppID)
		for index := range updated.Response.Choices {
			choice := &updated.Response.Choices[index]
			if updated.Response.IsPartial {
				if buffer {
					if _, adapterBuffering := ctx.Value(governanceBufferContextKey{}).(bool); adapterBuffering {
						continue
					}
				}
				choice.Delta.Content, _ = p.center.FilterOutput(request.TenantID, request.AppID, choice.Delta.Content)
				continue
			}
			choice.Message.Content, _ = p.center.FilterOutput(request.TenantID, request.AppID, choice.Message.Content)
		}
		return updated, nil
	})
	registry.AfterAgent(func(ctx context.Context, _ *frameworkagent.AfterAgentArgs) (*frameworkagent.AfterAgentResult, error) {
		if request, ok := RunnerIdentityFromContext(ctx); ok && p.center != nil {
			if err := p.center.RecordSpan(runnerGovernanceRequest(request, nil, nil, ""), request.TraceID, "plugin.after_agent", "ok"); err != nil {
				return nil, &GovernanceError{Code: "audit_unavailable", TraceID: request.TraceID}
			}
		}
		return nil, nil
	})
	registry.BeforeTool(func(ctx context.Context, args *frameworktool.BeforeToolArgs) (*frameworktool.BeforeToolResult, error) {
		request, ok := RunnerIdentityFromContext(ctx)
		if !ok || (p.center == nil && p.tools == nil) {
			return nil, nil
		}
		governance := p.tools
		if governance == nil {
			governance = p.center
		}
		err := governance.AuthorizeTool(ctx, runnerGovernanceRequest(request, nil, nil, ""), request.TraceID, args.ToolName, args.Arguments)
		if p.center != nil {
			if _, replayEnabled := ctx.Value(governanceReplayContextKey{}).(bool); replayEnabled && IsGovernanceError(err, "confirmation_consumed") {
				if replay, ok := p.center.ToolReplayResult(request.TenantID, request.RequestID, args.ToolName); ok {
					return &frameworktool.BeforeToolResult{CustomResult: replay}, nil
				}
			}
		}
		return nil, err
	})
	registry.AfterTool(func(ctx context.Context, args *frameworktool.AfterToolArgs) (*frameworktool.AfterToolResult, error) {
		request, ok := RunnerIdentityFromContext(ctx)
		if !ok || (p.center == nil && p.tools == nil) {
			return nil, nil
		}
		governance := p.tools
		if governance == nil {
			governance = p.center
		}
		err := governance.CompleteTool(ctx, runnerGovernanceRequest(request, nil, nil, ""), request.TraceID, args.ToolName, args.Error)
		return nil, err
	})
}

func (a *FrameworkRunnerAdapter) Run(ctx context.Context, request RunnerRequest) (RunnerResponse, error) {
	events, err := a.RunEvents(ctx, request)
	if err != nil {
		return RunnerResponse{}, err
	}
	var output string
	var usageTokens int64
	var usageKnown bool
	for runtimeEvent := range events {
		if tokens, known := runtimeUsage(runtimeEvent.Data); known {
			usageTokens, usageKnown = tokens, true
		}
		if runtimeEvent.Type == "message.delta" {
			output += runtimeEvent.Data["delta"]
		}
		if runtimeEvent.Type == "message.completed" && runtimeEvent.Data["output"] != "" {
			output = runtimeEvent.Data["output"]
		}
		if runtimeEvent.Type == "run.failed" {
			return RunnerResponse{}, errors.New(runtimeEvent.Data["error"])
		}
		if runtimeEvent.Type == "run.cancelled" {
			return RunnerResponse{}, context.Canceled
		}
	}
	return RunnerResponse{Output: output, UsageTokens: usageTokens, UsageKnown: usageKnown}, nil
}

func (a *FrameworkRunnerAdapter) RunEvents(ctx context.Context, request RunnerRequest) (<-chan RuntimeEvent, error) {
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return nil, errors.New("framework_runtime_closed")
	}
	ref := DeploymentVersionRef{TenantID: request.TenantID, VersionID: request.VersionID}
	version, ok, err := a.resolve(ctx, ref)
	if err != nil {
		return nil, err
	}
	if !ok || version.TenantID != request.TenantID || version.AgentAppID != request.AppID || version.DeploymentID != request.DeploymentID {
		return nil, errors.New("deployment_version_scope_mismatch")
	}
	governanceOwned := false
	externallyCompleted, _ := ctx.Value(governanceExternalCompletionContextKey{}).(bool)
	if a.governance != nil {
		admission, err := a.governance.Evaluate(ctx, runnerGovernanceRequest(request, configStrings(version.Config, "tools"), configStrings(version.Config, "mcp"), request.Input))
		if err != nil {
			return nil, err
		}
		request.Input = admission.Input
		request.TraceID = admission.TraceID
		request.PolicyRevision = admission.PolicyRevision
		governanceOwned = !externallyCompleted && admission.ExecutionActive
	}
	runner, err := a.runner(ctx, ref)
	if err != nil {
		if governanceOwned {
			completeGovernance(ctx, a.governance, runnerGovernanceCompletion(request, "", 0, false, "runner_failed", false))
		}
		return nil, err
	}
	bufferOutput := a.governance != nil && a.governance.RequiresBufferedOutput(request.TenantID, request.AppID)
	runnerContext := context.WithValue(withRunnerIdentity(ctx, request), governanceReplayContextKey{}, true)
	runnerContext = context.WithValue(runnerContext, governanceAdmissionContextKey{}, true)
	runnerContext = context.WithValue(runnerContext, governanceBufferContextKey{}, bufferOutput)
	runCtx, cancel := context.WithCancel(runnerContext)
	runID, registered := a.registerRun(ref, cancel)
	if !registered {
		cancel()
		if governanceOwned {
			completeGovernance(ctx, a.governance, runnerGovernanceCompletion(request, "", 0, false, "runner_failed", false))
		}
		return nil, errors.New("framework_runtime_closed")
	}
	// Platform Session Events and Summary are the shared source of conversation
	// context. Give the upstream runner a request-scoped session so its default
	// in-memory session service cannot make behavior depend on which Worker was
	// selected or retain duplicate history after a cross-node retry.
	upstreamSessionID := request.SessionID + ":" + request.RequestID
	upstream, err := runner.Run(runCtx, request.UserID, upstreamSessionID, model.NewUserMessage(request.Input), frameworkagent.WithRequestID(request.RequestID), frameworkagent.WithExecutionTraceEnabled(true))
	if err != nil {
		a.unregisterRun(ref, runID)
		cancel()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			results := make(chan RuntimeEvent, 1)
			results <- RuntimeEvent{Type: "run.cancelled", Data: runtimeEventData(request, map[string]string{"error": "run cancelled"})}
			close(results)
			if governanceOwned {
				completeGovernance(ctx, a.governance, runnerGovernanceCompletion(request, "", 0, false, "cancelled", true))
			}
			return results, nil
		}
		if governanceOwned {
			completeGovernance(ctx, a.governance, runnerGovernanceCompletion(request, "", 0, false, "runner_failed", false))
		}
		return nil, err
	}
	results := make(chan RuntimeEvent, 4)
	go func() {
		defer close(results)
		defer a.unregisterRun(ref, runID)
		defer cancel()
		var rawOutput string
		var sawContent, sawCompleted bool
		var usageTokens int64
		var usageKnown bool
		errorType := ""
		cancelled := false
		awaitingConfirmation := false
		defer func() {
			if governanceOwned && !awaitingConfirmation {
				completeGovernance(ctx, a.governance, runnerGovernanceCompletion(request, rawOutput, usageTokens, usageKnown, errorType, cancelled))
			}
		}()
		for {
			select {
			case <-runCtx.Done():
				cancelled, errorType = true, "cancelled"
				a.emitCancelled(results, RuntimeEvent{Type: "run.cancelled", Data: runtimeEventData(request, map[string]string{"error": "run cancelled"})})
				return
			case upstreamEvent, ok := <-upstream:
				if !ok {
					if bufferOutput && sawContent {
						output := rawOutput
						if a.governance != nil {
							output, _ = a.governance.FilterOutput(request.TenantID, request.AppID, output)
						}
						data := runtimeEventData(request, map[string]string{"delta": output, "output": output})
						addRuntimeUsage(data, usageTokens, usageKnown)
						if !a.emit(runCtx, results, RuntimeEvent{Type: "message.delta", Data: data}) {
							cancelled, errorType = true, "cancelled"
							return
						}
						if !a.emit(runCtx, results, RuntimeEvent{Type: "message.completed", Data: data}) {
							cancelled, errorType = true, "cancelled"
							return
						}
					}
					data := runtimeEventData(request, nil)
					addRuntimeUsage(data, usageTokens, usageKnown)
					a.emit(runCtx, results, RuntimeEvent{Type: "run.completed", Data: data})
					return
				}
				if upstreamEvent == nil || upstreamEvent.Response == nil {
					continue
				}
				if tokens, known := eventUsage(upstreamEvent); known {
					usageTokens, usageKnown = tokens, true
				}
				if upstreamEvent.IsError() {
					errorText := upstreamEvent.Response.Error.Error()
					log.Printf("framework runtime event failed: %s", errorText)
					if strings.Contains(errorText, "confirmation_required") {
						awaitingConfirmation = true
					} else {
						errorType = "runner_failed"
					}
					data := runtimeEventData(request, map[string]string{"error": errorText})
					addRuntimeUsage(data, usageTokens, usageKnown)
					a.emit(runCtx, results, RuntimeEvent{Type: "run.failed", Data: data})
					cancelAndDrainRunnerEvents(cancel, upstream)
					return
				}
				content := eventContent(upstreamEvent)
				if content == "" {
					continue
				}
				sawContent = true
				if upstreamEvent.Response.IsPartial {
					if !sawCompleted {
						rawOutput += content
					}
				} else {
					rawOutput, sawCompleted = content, true
				}
				// The upstream function-call processor surfaces BeforeTool plugin
				// failures as a model-visible completed message. Stop here so a
				// pending confirmation cannot be mistaken for a successful run.
				if !upstreamEvent.Response.IsPartial && strings.HasPrefix(content, "tool callback error:") {
					if strings.Contains(content, "confirmation_required") {
						awaitingConfirmation = true
					} else {
						errorType = "runner_failed"
					}
					if !a.emit(runCtx, results, RuntimeEvent{Type: "run.failed", Data: runtimeEventData(request, map[string]string{"error": content})}) {
						cancelled, errorType = true, "cancelled"
						return
					}
					// Cancellation is the ownership boundary. A non-cooperative
					// upstream must not keep an unbounded drain goroutine alive.
					cancelAndDrainRunnerEvents(cancel, upstream)
					return
				}
				eventType := "message.delta"
				if !upstreamEvent.Response.IsPartial {
					eventType = "message.completed"
				}
				if bufferOutput {
					continue
				}
				if a.governance != nil {
					content, _ = a.governance.FilterOutput(request.TenantID, request.AppID, content)
				}
				data := runtimeEventData(request, map[string]string{"delta": content, "output": content})
				addRuntimeUsage(data, usageTokens, usageKnown)
				if !a.emit(runCtx, results, RuntimeEvent{Type: eventType, Data: data}) {
					cancelled, errorType = true, "cancelled"
					return
				}
			}
		}
	}()
	return results, nil
}

type runnerIdentityKey struct{}

func withRunnerIdentity(ctx context.Context, request RunnerRequest) context.Context {
	return context.WithValue(ctx, runnerIdentityKey{}, request)
}

const runnerEventDrainTimeout = 2 * time.Second

func cancelAndDrainRunnerEvents(cancel context.CancelFunc, upstream <-chan *event.Event) {
	cancel()
	timer := time.NewTimer(runnerEventDrainTimeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-upstream:
			if !ok {
				return
			}
		case <-timer.C:
			return
		}
	}
}

func RunnerIdentityFromContext(ctx context.Context) (RunnerRequest, bool) {
	request, ok := ctx.Value(runnerIdentityKey{}).(RunnerRequest)
	return request, ok
}

func runtimeEventData(request RunnerRequest, values map[string]string) map[string]string {
	data := map[string]string{
		"tenant_id": request.TenantID, "app_id": request.AppID, "deployment_id": request.DeploymentID,
		"version_id": request.VersionID, "session_id": request.SessionID, "request_id": request.RequestID, "trace_id": request.TraceID,
		"traceparent": request.TraceParent,
		"user_id":     request.UserID, "channel": request.Channel, "external_subject": request.ExternalSubject,
	}
	for key, value := range values {
		data[key] = value
	}
	return data
}

func addRuntimeUsage(data map[string]string, tokens int64, known bool) {
	if !known {
		return
	}
	data["usage_known"] = "true"
	data["usage_tokens"] = strconv.FormatInt(tokens, 10)
}

func eventUsage(upstream *event.Event) (int64, bool) {
	if upstream == nil {
		return 0, false
	}
	if upstream.ExecutionTrace != nil && upstream.ExecutionTrace.Usage != nil {
		return int64(upstream.ExecutionTrace.Usage.TotalTokens), true
	}
	if upstream.Response != nil && upstream.Response.Usage != nil {
		return int64(upstream.Response.Usage.TotalTokens), true
	}
	return 0, false
}

func runnerGovernanceRequest(request RunnerRequest, requiredTools, requiredMCP []string, input string) GovernanceRequest {
	if input == "" {
		input = request.Input
	}
	return GovernanceRequest{
		TenantID: request.TenantID, AgentAppID: request.AppID, UserID: request.UserID,
		SessionID: request.SessionID, RequestID: request.RequestID, Channel: request.Channel,
		ProviderAccount: request.ProviderAccount, ConversationType: request.ConversationType,
		ExternalSubject: request.ExternalSubject, Input: input, RequiredTools: requiredTools,
		RequiredMCP: requiredMCP, PolicyRevision: request.PolicyRevision,
	}
}

func runnerGovernanceCompletion(request RunnerRequest, output string, tokens int64, usageKnown bool, errorType string, cancelled bool) GovernanceCompletion {
	return GovernanceCompletion{
		TenantID: request.TenantID, AgentAppID: request.AppID, RequestID: request.RequestID,
		UserID: request.UserID, SessionID: request.SessionID, Channel: request.Channel,
		ExternalSubject: request.ExternalSubject, Output: output, Tokens: tokens,
		NoUsage: !usageKnown, ErrorType: errorType, Cancelled: cancelled,
	}
}

func (a *FrameworkRunnerAdapter) registerRun(ref DeploymentVersionRef, cancel context.CancelFunc) (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return 0, false
	}
	a.nextRun++
	a.runWG.Add(1)
	key := versionRefKey(ref)
	if a.runs[key] == nil {
		a.runs[key] = make(map[uint64]context.CancelFunc)
	}
	a.runs[key][a.nextRun] = cancel
	return a.nextRun, true
}

func (a *FrameworkRunnerAdapter) unregisterRun(ref DeploymentVersionRef, runID uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := versionRefKey(ref)
	if runs := a.runs[key]; runs != nil {
		if _, ok := runs[runID]; !ok {
			return
		}
		delete(runs, runID)
		a.runWG.Done()
		if len(runs) == 0 {
			delete(a.runs, key)
		}
	}
}

func (a *FrameworkRunnerAdapter) RetireVersion(ref DeploymentVersionRef) error {
	a.mu.Lock()
	key := versionRefKey(ref)
	for _, cancel := range a.runs[key] {
		cancel()
	}
	runner := a.runners[key]
	delete(a.runners, key)
	a.mu.Unlock()
	if runner == nil {
		return nil
	}
	return runner.Close()
}

func (a *FrameworkRunnerAdapter) emit(ctx context.Context, results chan<- RuntimeEvent, value RuntimeEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case results <- value:
		return true
	}
}

func (a *FrameworkRunnerAdapter) emitCancelled(results chan<- RuntimeEvent, value RuntimeEvent) {
	select {
	case results <- value:
	default:
	}
}

func (a *FrameworkRunnerAdapter) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	for _, runs := range a.runs {
		for _, cancel := range runs {
			cancel()
		}
	}
	runners := make([]frameworkrunner.Runner, 0, len(a.runners))
	for _, runner := range a.runners {
		runners = append(runners, runner)
	}
	a.mu.Unlock()
	var firstErr error
	for _, runner := range runners {
		if err := runner.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	a.runWG.Wait()
	return firstErr
}

func eventContent(upstreamEvent *event.Event) string {
	if upstreamEvent.Response == nil || len(upstreamEvent.Response.Choices) == 0 {
		return ""
	}
	choice := upstreamEvent.Response.Choices[0]
	if upstreamEvent.Response.IsPartial {
		return choice.Delta.Content
	}
	return choice.Message.Content
}
