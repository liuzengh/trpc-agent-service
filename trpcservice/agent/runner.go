package agent

import (
	"context"
	"errors"
	"sync"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// RunnerInput is the dependency-free execution contract consumed by the
// external-agent adapter. Runtime builds it from its complete execution plan;
// this package owns the tRPC-Agent-Go materialization after that boundary.
type RunnerInput struct {
	Tenant  tenant.Tenant
	Agent   LLMAgentFactoryInput
	Model   modelprofile.ModelFactoryInput
	Storage backend.StorageFactoryInput
}

func llmAgentOptions(input LLMAgentFactoryInput, model trpcmodel.Model, toolSets ...[]trpctool.Tool) []llmagent.Option {
	options := []llmagent.Option{
		llmagent.WithDescription(input.Description),
		llmagent.WithInstruction(input.Instruction),
		llmagent.WithGlobalInstruction(input.GlobalInstruction),
		llmagent.WithModel(model),
		llmagent.WithGenerationConfig(toTRPCGenerationConfig(input.Generation)),
		llmagent.WithMaxLLMCalls(input.Runtime.MaxLLMCalls),
		llmagent.WithMaxToolIterations(input.Runtime.MaxToolCalls),
		llmagent.WithEnableParallelTools(input.Runtime.EnableParallelTools),
		llmagent.WithToolConcurrencyConfig(trpctool.ConcurrencyConfig{MaxConcurrency: input.Runtime.MaxParallelTools}),
	}
	if len(toolSets) > 0 && len(toolSets[0]) > 0 {
		options = append(options, llmagent.WithTools(toolSets[0]))
	}
	return options
}

type callbackStateKey struct{}
type usageObserverContextKey struct{}

// UsageObserver receives one terminal provider usage sample for each model
// call. It is carried by execution context so cached Runners can be reused
// without sharing accounting state between requests.
type UsageObserver func(context.Context, runtimebudget.Usage)

// WithUsageObserver attaches per-execution usage accounting to a context.
func WithUsageObserver(ctx context.Context, observer UsageObserver) context.Context {
	if ctx == nil || observer == nil {
		return ctx
	}
	return context.WithValue(ctx, usageObserverContextKey{}, observer)
}

func usageObserverFromContext(ctx context.Context) UsageObserver {
	if ctx == nil {
		return nil
	}
	observer, _ := ctx.Value(usageObserverContextKey{}).(UsageObserver)
	return observer
}

type callbackState struct {
	finishSpan func(error)
	started    time.Time
	ctx        context.Context
	catalog    metrics.Catalog
	labels     map[string]string
	mu         sync.Mutex
	once       sync.Once
	usage      *trpcmodel.Usage
}

var errModelResponseIncomplete = errors.New("model response stream incomplete")

func (state *callbackState) observe(response *trpcmodel.Response) {
	if state == nil || response == nil || response.Usage == nil {
		return
	}
	usage := *response.Usage
	state.mu.Lock()
	state.usage = &usage
	state.mu.Unlock()
}

func (state *callbackState) finish(err error) {
	if state == nil {
		return
	}
	state.once.Do(func() {
		state.mu.Lock()
		usage := state.usage
		state.mu.Unlock()
		if usage != nil {
			labels := make(map[string]string, len(state.labels))
			for key, value := range state.labels {
				if key != "operation" && key != "status" && key != "error_class" {
					labels[key] = value
				}
			}
			_ = state.catalog.Tokens(state.ctx, int64(usage.PromptTokens), labels)
			_ = state.catalog.Tokens(state.ctx, int64(usage.CompletionTokens), labels)
			if observer := usageObserverFromContext(state.ctx); observer != nil {
				observer(state.ctx, runtimebudget.Usage{InputTokens: int64(usage.PromptTokens), OutputTokens: int64(usage.CompletionTokens)})
			}
		}
		if state.finishSpan != nil {
			state.finishSpan(err)
		}
		_ = state.catalog.Operation(state.ctx, state.started, state.labels, err)
	})
}

func stateFromContext(ctx context.Context) *callbackState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(callbackStateKey{}).(*callbackState)
	return state
}

func isTerminalModelResponse(response *trpcmodel.Response) bool {
	return response != nil && (response.Error != nil || response.Done || !response.IsPartial)
}

func modelResponseError(response *trpcmodel.Response) error {
	if response != nil && response.Error != nil {
		if errors.Is(response.Error, context.Canceled) {
			return context.Canceled
		}
		if errors.Is(response.Error, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		return errModelResponse
	}
	return nil
}

var errModelResponse = errors.New("model response error")

// telemetryModel closes model spans when a provider returns a function-level
// error or a response stream ends without a terminal response. The framework's
// callbacks still own usage extraction; this wrapper only supplies the missing
// terminal signal for streaming and pre-channel failures.
type telemetryModel struct{ delegate trpcmodel.Model }

func wrapTelemetryModel(delegate trpcmodel.Model) trpcmodel.Model {
	if delegate == nil {
		return nil
	}
	if iter, ok := delegate.(trpcmodel.IterModel); ok {
		return telemetryIterModel{telemetryModel: telemetryModel{delegate: delegate}, iter: iter}
	}
	return telemetryModel{delegate: delegate}
}

func (model telemetryModel) Info() trpcmodel.Info { return model.delegate.Info() }

func (model telemetryModel) GenerateContent(ctx context.Context, request *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	responses, err := model.delegate.GenerateContent(ctx, request)
	state := stateFromContext(ctx)
	if err != nil {
		state.finish(err)
		return nil, err
	}
	if responses == nil {
		state.finish(errModelResponseIncomplete)
		return nil, errModelResponseIncomplete
	}
	out := make(chan *trpcmodel.Response)
	go func() {
		defer close(out)
		terminal := false
		for {
			select {
			case <-ctx.Done():
				if !terminal {
					state.finish(ctx.Err())
				}
				return
			case response, ok := <-responses:
				if !ok {
					if !terminal {
						state.finish(errModelResponseIncomplete)
					}
					return
				}
				if response != nil {
					state.observe(response)
					if isTerminalModelResponse(response) {
						terminal = true
						state.finish(modelResponseError(response))
					}
				}
				select {
				case out <- response:
				case <-ctx.Done():
					if !terminal {
						state.finish(ctx.Err())
					}
					return
				}
			}
		}
	}()
	return out, nil
}

type telemetryIterModel struct {
	telemetryModel
	iter trpcmodel.IterModel
}

func (model telemetryIterModel) GenerateContentIter(ctx context.Context, request *trpcmodel.Request) (trpcmodel.Seq[*trpcmodel.Response], error) {
	seq, err := model.iter.GenerateContentIter(ctx, request)
	state := stateFromContext(ctx)
	if err != nil {
		state.finish(err)
		return nil, err
	}
	if seq == nil {
		state.finish(errModelResponseIncomplete)
		return nil, errModelResponseIncomplete
	}
	return func(yield func(*trpcmodel.Response) bool) {
		terminal := false
		seq(func(response *trpcmodel.Response) bool {
			if response != nil {
				state.observe(response)
				if isTerminalModelResponse(response) {
					terminal = true
					state.finish(modelResponseError(response))
				}
			}
			return yield(response)
		})
		if !terminal {
			if err := ctx.Err(); err != nil {
				state.finish(err)
			} else {
				state.finish(errModelResponseIncomplete)
			}
		}
	}, nil
}

type modelRetryBinder interface {
	WithModelRetryCallbacks(context.Context, func(context.Context, *trpcmodel.Request) (context.Context, *trpcmodel.Response, error), func(context.Context, *trpcmodel.Request, *trpcmodel.Response) (context.Context, error)) context.Context
}

func (model telemetryModel) WithModelRetryCallbacks(ctx context.Context, before func(context.Context, *trpcmodel.Request) (context.Context, *trpcmodel.Response, error), after func(context.Context, *trpcmodel.Request, *trpcmodel.Response) (context.Context, error)) context.Context {
	binder, ok := model.delegate.(modelRetryBinder)
	if !ok {
		return ctx
	}
	return binder.WithModelRetryCallbacks(ctx, before, after)
}

func telemetryOptions(provider observability.Provider, providerName, modelFamily string) []llmagent.Option {
	catalog := metrics.New(provider)
	modelCallbacks := trpcmodel.NewCallbacks().RegisterBeforeModel(func(ctx context.Context, _ *trpcmodel.BeforeModelArgs) (*trpcmodel.BeforeModelResult, error) {
		started := time.Now()
		next, _, finish := observability.StartOperation(ctx, provider, observability.OperationModelCall, "model")
		labels := map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": providerName, "model_family": modelFamily}
		startedLabels := make(map[string]string, len(labels)+1)
		for key, value := range labels {
			startedLabels[key] = value
		}
		startedLabels["status"] = "started"
		_ = catalog.Request(next, startedLabels)
		state := &callbackState{finishSpan: finish, started: started, ctx: next, catalog: catalog, labels: labels}
		return &trpcmodel.BeforeModelResult{Context: context.WithValue(next, callbackStateKey{}, state)}, nil
	}).RegisterAfterModel(func(ctx context.Context, args *trpcmodel.AfterModelArgs) (*trpcmodel.AfterModelResult, error) {
		state := stateFromContext(ctx)
		if state == nil {
			return nil, nil
		}
		if args == nil {
			state.finish(errModelResponseIncomplete)
			return nil, nil
		}
		state.observe(args.Response)
		if args.Response == nil || isTerminalModelResponse(args.Response) || args.Error != nil {
			err := args.Error
			if err == nil && args.Response != nil {
				err = modelResponseError(args.Response)
			}
			if err == nil && args.Response == nil {
				err = errModelResponseIncomplete
			}
			state.finish(err)
		}
		return nil, nil
	})
	toolCallbacks := trpctool.NewCallbacks().RegisterBeforeTool(func(ctx context.Context, _ *trpctool.BeforeToolArgs) (*trpctool.BeforeToolResult, error) {
		started := time.Now()
		next, _, finish := observability.StartOperation(ctx, provider, observability.OperationToolCall, "tool")
		labels := map[string]string{"component": "tool", "operation": observability.OperationToolCall}
		startedLabels := map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "started"}
		_ = catalog.Request(next, startedLabels)
		state := &callbackState{finishSpan: finish, started: started, ctx: next, catalog: catalog, labels: labels}
		return &trpctool.BeforeToolResult{Context: context.WithValue(next, callbackStateKey{}, state)}, nil
	}).RegisterAfterTool(func(ctx context.Context, args *trpctool.AfterToolArgs) (*trpctool.AfterToolResult, error) {
		state := stateFromContext(ctx)
		if state == nil {
			return nil, nil
		}
		if args == nil {
			state.finish(errModelResponseIncomplete)
		} else {
			state.finish(args.Error)
		}
		return nil, nil
	})
	return []llmagent.Option{llmagent.WithModelCallbacks(modelCallbacks), llmagent.WithToolCallbacks(toolCallbacks)}
}

// policyRunner preserves the published Agent runtime policy at the Runner
// boundary. Caller-provided options are applied first; the fixed policy is
// appended last so an execution cannot silently extend its control-plane limits.
type policyRunner struct {
	delegate     trpcrunner.Runner
	capabilities *storagefactory.CapabilitySet
	runOptions   []trpcagent.RunOption
}

func (runner *policyRunner) Run(ctx context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	allOptions := make([]trpcagent.RunOption, 0, len(options)+len(runner.runOptions))
	allOptions = append(allOptions, options...)
	allOptions = append(allOptions, runner.runOptions...)
	return runner.delegate.Run(ctx, userID, sessionID, message, allOptions...)
}

func (runner *policyRunner) Close() error {
	if runner == nil {
		return nil
	}
	return errors.Join(runner.delegate.Close(), runner.capabilities.Close())
}

func toTRPCGenerationConfig(configuration appmodel.GenerationConfig) trpcmodel.GenerationConfig {
	return trpcmodel.GenerationConfig{
		Temperature: configuration.Temperature,
		TopP:        configuration.TopP,
		MaxTokens:   configuration.MaxOutputTokens,
	}
}
