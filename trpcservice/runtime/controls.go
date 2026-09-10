package runtime

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/model"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type tokenBudget struct {
	tokenLimit int
	costLimit  float64
	usedTokens int
	usedCost   float64
	reserved   bool
	unmetered  bool
	estimate   func(int, int) (float64, bool)
	mu         sync.Mutex
}

func newBudgetCallbacks(policy tenant.BudgetPolicy, estimates ...func(int, int) (float64, bool)) *model.Callbacks {
	if policy.MaxTokensPerExecution <= 0 && policy.MaxCostPerExecution <= 0 {
		return nil
	}
	var estimate func(int, int) (float64, bool)
	if len(estimates) > 0 {
		estimate = estimates[0]
	}
	budget := &tokenBudget{
		tokenLimit: policy.MaxTokensPerExecution,
		costLimit:  policy.MaxCostPerExecution,
		estimate:   estimate,
	}
	callbacks := model.NewCallbacks()
	callbacks.RegisterBeforeModel(func(_ context.Context, _ *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		budget.mu.Lock()
		// Reserve the entire remaining budget while a model call is in
		// flight. Usage is only known in AfterModel, so allowing another
		// concurrent call here could let both calls pass before either one
		// charges its tokens.
		depleted := budget.unmetered || budget.reserved ||
			(budget.tokenLimit > 0 && budget.usedTokens >= budget.tokenLimit) ||
			(budget.costLimit > 0 && budget.usedCost >= budget.costLimit)
		if !depleted {
			budget.reserved = true
		}
		budget.mu.Unlock()
		if depleted {
			return nil, tenant.ErrBudgetExceeded
		}
		return nil, nil
	})
	callbacks.RegisterAfterModel(func(_ context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		// The framework invokes AfterModel for every streaming chunk. Keep the
		// reservation until the non-partial terminal response carries usage.
		if isStreamingModelResponse(args) {
			return nil, nil
		}
		// Preserve the model/provider failure. A failed call without usage is
		// not the same as a successful call whose usage cannot be trusted.
		if args != nil && (args.Error != nil || (args.Response != nil && args.Response.Error != nil)) {
			total, metered := modelResponseUsage(args)
			cost, costMetered := modelResponseCost(args, budget.estimate)
			budget.mu.Lock()
			budget.reserved = false
			if metered {
				budget.usedTokens += total
			} else {
				// A failed model call without trusted usage is still an unknown
				// spend. Keep the execution fail-closed so a retry cannot create
				// unbounded unmetered model calls.
				budget.unmetered = true
			}
			if budget.costLimit > 0 {
				if costMetered {
					budget.usedCost += cost
				} else {
					budget.unmetered = true
				}
			}
			budget.mu.Unlock()
			return nil, nil
		}
		total, metered := modelResponseUsage(args)
		cost, costMetered := modelResponseCost(args, budget.estimate)
		budget.mu.Lock()
		budget.reserved = false
		if !metered || (budget.costLimit > 0 && !costMetered) {
			// A successful model call without trusted usage must not leave the
			// budget available for unlimited follow-up calls.
			budget.unmetered = true
			budget.mu.Unlock()
			return nil, tenant.ErrBudgetExceeded
		}
		budget.usedTokens += total
		if budget.costLimit > 0 {
			budget.usedCost += cost
		}
		depleted := (budget.tokenLimit > 0 && budget.usedTokens > budget.tokenLimit) ||
			(budget.costLimit > 0 && budget.usedCost > budget.costLimit)
		budget.mu.Unlock()
		if depleted {
			return nil, tenant.ErrBudgetExceeded
		}
		return nil, nil
	})
	return callbacks
}

func isStreamingModelResponse(args *model.AfterModelArgs) bool {
	return args != nil && args.Response != nil && args.Response.IsPartial &&
		args.Error == nil && args.Response.Error == nil
}

func modelResponseUsage(args *model.AfterModelArgs) (int, bool) {
	if args == nil || args.Response == nil || args.Response.Usage == nil {
		return 0, false
	}
	usage := args.Response.Usage
	if usage.PromptTokens < 0 || usage.CompletionTokens < 0 || usage.TotalTokens < 0 {
		return 0, false
	}
	total := usage.TotalTokens
	if total <= 0 {
		total = usage.PromptTokens + usage.CompletionTokens
	}
	return total, total > 0
}

func modelResponseCost(args *model.AfterModelArgs, estimate func(int, int) (float64, bool)) (float64, bool) {
	if args == nil || args.Response == nil || args.Response.Usage == nil || estimate == nil {
		return 0, false
	}
	usage := args.Response.Usage
	if usage.PromptTokens < 0 || usage.CompletionTokens < 0 {
		return 0, false
	}
	if usage.TotalTokens > 0 && usage.PromptTokens == 0 && usage.CompletionTokens == 0 {
		return 0, false
	}
	return estimate(usage.PromptTokens, usage.CompletionTokens)
}

func modelCostEstimator(metrics *platformmetrics.Recorder, provider, modelName string) func(int, int) (float64, bool) {
	if metrics == nil {
		return nil
	}
	return func(inputTokens, outputTokens int) (float64, bool) {
		cost := metrics.EstimateCost(provider, modelName, inputTokens, outputTokens)
		if cost == nil {
			return 0, false
		}
		return *cost, true
	}
}

type modelSpanState struct {
	span     trace.Span
	started  time.Time
	finished bool
	mu       sync.Mutex
}

type modelSpanStateKey struct{}

// addModelObservabilityCallbacks adds one metadata-only model span and one
// per-model-call metric. The observation callback is prepended to AfterModel
// so budget rejection cannot leave the model span open.
func addModelObservabilityCallbacks(
	callbacks *model.Callbacks,
	exec worker.Execution,
	metrics *platformmetrics.Recorder,
) *model.Callbacks {
	if callbacks == nil {
		callbacks = model.NewCallbacks()
	}
	callbacks.RegisterBeforeModel(func(ctx context.Context, _ *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		modelCtx, span := platformtelemetry.StartSpan(ctx, "model.execute",
			attribute.String("tenant_id", exec.Tenant.TenantID),
			attribute.String("app_id", exec.Tenant.AppID),
			attribute.String("config_version", exec.Tenant.ConfigVersion),
			attribute.String("request_id", exec.RequestID),
			attribute.String("channel", exec.Tenant.Channel),
			attribute.String("provider", exec.Config.Model.Provider),
			attribute.String("model", exec.Config.Model.Model),
		)
		return &model.BeforeModelResult{
			Context: context.WithValue(modelCtx, modelSpanStateKey{}, &modelSpanState{
				span:    span,
				started: time.Now(),
			}),
		}, nil
	})
	finish := model.AfterModelCallbackStructured(func(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		if isStreamingModelResponse(args) {
			return nil, nil
		}
		if ctx == nil {
			ctx = context.Background()
		}
		state, _ := ctx.Value(modelSpanStateKey{}).(*modelSpanState)
		if state == nil {
			return nil, nil
		}
		state.mu.Lock()
		if state.finished {
			state.mu.Unlock()
			return nil, nil
		}
		state.finished = true
		state.mu.Unlock()

		errType := ""
		if args == nil || args.Error != nil || args.Response == nil || args.Response.Error != nil {
			errType = "model"
			platformtelemetry.MarkError(state.span, errType, errors.New("model call failed"))
		}
		state.span.End()
		inputTokens, outputTokens := 0, 0
		if args != nil && args.Response != nil && args.Response.Usage != nil {
			inputTokens = args.Response.Usage.PromptTokens
			outputTokens = args.Response.Usage.CompletionTokens
		}
		if metrics != nil {
			metrics.RecordModel(ctx, platformmetrics.Labels{
				TenantID:      exec.Tenant.TenantID,
				AppID:         exec.Tenant.AppID,
				ConfigVersion: exec.Tenant.ConfigVersion,
				Channel:       exec.Tenant.Channel,
				Result:        resultForError(errType),
				ErrorType:     errType,
			}, exec.Config.Model.Provider, exec.Config.Model.Model, time.Since(state.started), inputTokens, outputTokens)
		}
		return nil, nil
	})
	callbacks.AfterModel = append([]model.AfterModelCallbackStructured{finish}, callbacks.AfterModel...)
	return callbacks
}

type toolSpanState struct {
	span  trace.Span
	start time.Time
}

type toolSpanStateKey struct{}

func (r *Runtime) toolCallbacks(exec worker.Execution) *frameworktool.Callbacks {
	callbacks := frameworktool.NewCallbacks()
	callbacks.RegisterBeforeTool(func(ctx context.Context, args *frameworktool.BeforeToolArgs) (*frameworktool.BeforeToolResult, error) {
		name := ""
		toolCallID := ""
		var arguments []byte
		if args != nil {
			name = args.ToolName
			toolCallID = args.ToolCallID
			arguments = args.Arguments
		}
		toolCtx, span := platformtelemetry.StartSpan(ctx, "tool.execute",
			attribute.String("tenant_id", exec.Tenant.TenantID),
			attribute.String("app_id", exec.Tenant.AppID),
			attribute.String("config_version", exec.Tenant.ConfigVersion),
			attribute.String("request_id", exec.RequestID),
			attribute.String("tool.name", name),
		)
		toolCtx = platformtool.WithIdempotencyKey(toolCtx, platformtool.StableIdempotencyKey(
			exec.Tenant.TenantID,
			exec.Tenant.AppID,
			exec.RequestID,
			toolCallID,
			name,
			arguments,
		))
		return &frameworktool.BeforeToolResult{
			Context: context.WithValue(toolCtx, toolSpanStateKey{}, toolSpanState{
				span:  span,
				start: time.Now(),
			}),
		}, nil
	})
	callbacks.RegisterAfterTool(func(ctx context.Context, args *frameworktool.AfterToolArgs) (*frameworktool.AfterToolResult, error) {
		if ctx == nil {
			ctx = context.Background()
		}
		state, _ := ctx.Value(toolSpanStateKey{}).(toolSpanState)
		started := state.start
		if started.IsZero() {
			started = time.Now()
		}
		if state.span != nil {
			if args != nil && args.Error != nil {
				platformtelemetry.MarkError(state.span, "tool", args.Error)
			}
			state.span.End()
		}
		name := ""
		if args != nil {
			name = args.ToolName
		}
		errType := ""
		var classifiedErr error
		if args != nil && args.Error != nil {
			safety := platformtool.SafetySideEffect
			if r.tools != nil {
				if resolved, ok := r.tools.Safety(name); ok {
					safety = resolved
				}
			}
			failureClass := platformtool.ClassifyFailure(safety, args.Error)
			errType = string(failureClass)
			classifiedErr = classifyToolExecutionFailure(failureClass, args.Error)
		}
		if r.metrics != nil {
			r.metrics.RecordTool(ctx, platformmetrics.Labels{
				TenantID:      exec.Tenant.TenantID,
				AppID:         exec.Tenant.AppID,
				ConfigVersion: exec.Tenant.ConfigVersion,
				Channel:       exec.Tenant.Channel,
				Result:        resultForError(errType),
			}, time.Since(started), errType)
		}
		if exec.Config.Audit.Enabled && exec.Config.Audit.RecordToolDecisions {
			eventType := platformaudit.ToolCompleted
			decision := "completed"
			if errType != "" {
				eventType = platformaudit.ToolFailed
				decision = "failed"
			}
			r.recordAudit(ctx, exec, platformaudit.Event{
				ToolName:  name,
				Decision:  decision,
				Latency:   time.Since(started),
				ErrorType: errType,
				EventType: eventType,
			})
		}
		if classifiedErr != nil {
			return nil, classifiedErr
		}
		return nil, nil
	})
	return callbacks
}

func classifyToolExecutionFailure(class platformtool.FailureClass, err error) error {
	switch class {
	case platformtool.FailureInfrastructureRetryable:
		return worker.NewRetryableExecutionError(err)
	case platformtool.FailureSideEffectResultUncertain:
		return worker.NewSideEffectUncertainError(err)
	case platformtool.FailurePermanent, platformtool.FailureSideEffectResultKnown:
		return worker.NewPermanentExecutionError(err)
	default:
		// Unknown or missing classifications fail closed at the execution
		// boundary. A malformed provider classification must never turn into
		// an automatic replay of a possibly mutating Tool.
		return worker.NewPermanentExecutionError(errors.New("tool failure classification is invalid"))
	}
}

func (r *Runtime) recordAudit(ctx context.Context, exec worker.Execution, event platformaudit.Event) {
	if r.audit == nil {
		return
	}
	if event.TenantID == "" {
		event.TenantID = exec.Tenant.TenantID
	}
	if event.AppID == "" {
		event.AppID = exec.Tenant.AppID
	}
	if event.Channel == "" {
		event.Channel = exec.Tenant.Channel
	}
	if event.UserID == "" {
		event.UserID = exec.Tenant.UserID
	}
	if event.SessionID == "" {
		event.SessionID = exec.Tenant.SessionID
	}
	if event.AgentName == "" {
		event.AgentName = runtimeAgentName
	}
	if event.TraceID == "" {
		event.TraceID = exec.Tenant.TraceID
	}
	if event.RequestID == "" {
		event.RequestID = exec.RequestID
	}
	if event.ConfigVersion == "" {
		event.ConfigVersion = exec.Tenant.ConfigVersion
	}
	if exec.Config.Audit.RedactPII {
		event = platformaudit.RedactEvent(event)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := r.audit.Record(writeCtx, event); err != nil {
		log.Printf("audit write failed tenant=%s app=%s event=%s: %s", event.TenantID, event.AppID, event.EventType, platformlog.SafeError(err))
		if r.metrics != nil {
			r.metrics.RecordAuditFailure(ctx, platformmetrics.Labels{
				TenantID: event.TenantID,
				AppID:    event.AppID,
				Channel:  event.Channel,
			})
		}
	}
}

func resultForError(errorType string) string {
	if errorType == "" {
		return "success"
	}
	return "error"
}
