package agent

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var errModelResponse = errors.New("model returned an error response")

type modelSpanKey struct{}
type toolSpanKey struct{}

type callbackSpan struct {
	finish func(error)
}

// withTelemetryModelCallbacks appends the service callback after user
// before-hooks (so rejected/custom requests do not create a model span) and
// prepends the after-hook (so it closes the span before user post-processing).
func withTelemetryModelCallbacks(existing *model.Callbacks, provider telemetry.Provider) *model.Callbacks {
	callbacks := existing.Clone()
	if callbacks == nil {
		callbacks = model.NewCallbacks()
	}
	callbacks.BeforeModel = append(callbacks.BeforeModel, func(ctx context.Context, args *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		if args == nil || args.Request == nil {
			return nil, nil
		}
		child, finish := startCallbackOperation(ctx, provider, telemetry.OperationModelGenerate)
		return &model.BeforeModelResult{Context: context.WithValue(child, modelSpanKey{}, callbackSpan{finish: finish})}, nil
	})
	callbacks.AfterModel = append([]model.AfterModelCallbackStructured{func(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		if ctx != nil {
			if state, ok := ctx.Value(modelSpanKey{}).(callbackSpan); ok && state.finish != nil {
				var err error
				if args != nil {
					err = args.Error
				}
				state.finish(err)
				return &model.AfterModelResult{Context: ctx}, nil
			}
		}
		return nil, nil
	}}, callbacks.AfterModel...)
	return callbacks
}

// withTelemetryToolCallbacks follows the same ordering as model callbacks.
// The callable wrapper below closes function-level errors when the upstream
// flow does not reach AfterTool (for example a transport error or permission
// short-circuit).
func withTelemetryToolCallbacks(existing *tool.Callbacks, provider telemetry.Provider) *tool.Callbacks {
	callbacks := existing.Clone()
	if callbacks == nil {
		callbacks = tool.NewCallbacks()
	}
	callbacks.BeforeTool = append(callbacks.BeforeTool, func(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
		if args == nil || args.ToolName == "" {
			return nil, nil
		}
		child, finish := startCallbackOperation(ctx, provider, telemetry.OperationToolExecute)
		return &tool.BeforeToolResult{Context: context.WithValue(child, toolSpanKey{}, callbackSpan{finish: finish})}, nil
	})
	callbacks.AfterTool = append([]tool.AfterToolCallbackStructured{func(ctx context.Context, args *tool.AfterToolArgs) (*tool.AfterToolResult, error) {
		if ctx != nil {
			if state, ok := ctx.Value(toolSpanKey{}).(callbackSpan); ok && state.finish != nil {
				var err error
				if args != nil {
					err = args.Error
				}
				state.finish(err)
				return &tool.AfterToolResult{Context: ctx}, nil
			}
		}
		return nil, nil
	}}, callbacks.AfterTool...)
	return callbacks
}

func startCallbackOperation(ctx context.Context, provider telemetry.Provider, operation telemetry.Operation) (context.Context, func(error)) {
	return telemetry.StartOperation(ctx, provider, telemetry.EffectiveTraceParent(ctx, ""), operation,
		telemetry.ComponentAttribute(telemetry.ComponentWorker))
}

type instrumentedModel struct {
	inner    model.Model
	provider telemetry.Provider
}

// budgetedModel is always installed, including when tracing is disabled. The
// controller itself is opt-in via the Worker execution context, so ordinary
// callers retain their existing behavior.
type budgetedModel struct{ inner model.Model }

func (m budgetedModel) Info() model.Info { return m.inner.Info() }

func (m budgetedModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := runtime.ConsumeLLMCall(ctx); err != nil {
		return nil, err
	}
	return m.inner.GenerateContent(ctx, request)
}

func (m instrumentedModel) Info() model.Info { return m.inner.Info() }

func (m instrumentedModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	finish := callbackFinish(ctx, modelSpanKey{})
	if finish == nil {
		var child context.Context
		child, finish = startCallbackOperation(ctx, m.provider, telemetry.OperationModelGenerate)
		ctx = child
	}
	responses, err := m.inner.GenerateContent(ctx, request)
	if err != nil {
		finish(err)
		return nil, err
	}
	if responses == nil {
		finish(errModelResponse)
		return nil, errModelResponse
	}
	out := make(chan *model.Response, 8)
	go func() {
		var responseErr error
		defer func() {
			finish(responseErr)
			close(out)
		}()
		for {
			select {
			case response, ok := <-responses:
				if !ok {
					return
				}
				if response != nil && response.Error != nil {
					responseErr = errModelResponse
				}
				select {
				case out <- response:
				case <-ctx.Done():
					if responseErr == nil {
						responseErr = ctx.Err()
					}
					return
				}
			case <-ctx.Done():
				responseErr = ctx.Err()
				return
			}
		}
	}()
	return out, nil
}

func callbackFinish(ctx context.Context, key any) func(error) {
	if ctx == nil {
		return nil
	}
	state, ok := ctx.Value(key).(callbackSpan)
	if !ok {
		return nil
	}
	return state.finish
}

// FinishToolOperation closes the service tool span when the upstream flow
// short-circuits before invoking AfterTool (for example a permission deny).
// It returns false when no service callback span is attached.
func FinishToolOperation(ctx context.Context, err error) bool {
	finish := callbackFinish(ctx, toolSpanKey{})
	if finish == nil {
		return false
	}
	finish(err)
	return true
}

type instrumentedCallable struct {
	inner    tool.CallableTool
	provider telemetry.Provider
}

type budgetedCallable struct{ inner tool.CallableTool }

func (t budgetedCallable) Declaration() *tool.Declaration { return t.inner.Declaration() }

func (t budgetedCallable) Call(ctx context.Context, arguments []byte) (any, error) {
	release, err := runtime.BeginToolCall(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return t.inner.Call(ctx, arguments)
}

func (t budgetedCallable) GovernanceToolRef() governance.VersionedRef {
	if versioned, ok := t.inner.(governance.VersionedTool); ok {
		return versioned.GovernanceToolRef()
	}
	return governance.VersionedRef{}
}

func (t instrumentedCallable) Declaration() *tool.Declaration { return t.inner.Declaration() }

func (t instrumentedCallable) Call(ctx context.Context, arguments []byte) (any, error) {
	finish := callbackFinish(ctx, toolSpanKey{})
	if finish == nil {
		var child context.Context
		child, finish = startCallbackOperation(ctx, t.provider, telemetry.OperationToolExecute)
		ctx = child
	}
	result, err := t.inner.Call(ctx, arguments)
	finish(err)
	return result, err
}

func (t instrumentedCallable) GovernanceToolRef() governance.VersionedRef {
	if versioned, ok := t.inner.(governance.VersionedTool); ok {
		return versioned.GovernanceToolRef()
	}
	return governance.VersionedRef{}
}

func instrumentModel(provider telemetry.Provider, value model.Model) model.Model {
	if !telemetry.Enabled(provider) || value == nil {
		return value
	}
	return instrumentedModel{inner: value, provider: provider}
}

func instrumentCallables(provider telemetry.Provider, values []tool.Tool) []tool.Tool {
	if !telemetry.Enabled(provider) {
		return values
	}
	result := make([]tool.Tool, len(values))
	for index, value := range values {
		callable, ok := value.(tool.CallableTool)
		if !ok || callable == nil {
			result[index] = value
			continue
		}
		result[index] = instrumentedCallable{inner: callable, provider: provider}
	}
	return result
}

func budgetCallables(values []tool.Tool) []tool.Tool {
	result := make([]tool.Tool, len(values))
	for index, value := range values {
		callable, ok := value.(tool.CallableTool)
		if !ok || callable == nil {
			result[index] = value
			continue
		}
		result[index] = budgetedCallable{inner: callable}
	}
	return result
}

var _ model.Model = instrumentedModel{}
var _ model.Model = budgetedModel{}
var _ tool.CallableTool = instrumentedCallable{}
var _ tool.CallableTool = budgetedCallable{}
var _ governance.VersionedTool = instrumentedCallable{}
var _ governance.VersionedTool = budgetedCallable{}
