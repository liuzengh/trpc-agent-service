package agent

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	frameworkevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
)

type tenantContext = tenant.TenantContext

type frameworkRunner interface {
	Run(context.Context, string, string, model.Message, ...frameworkagent.RunOption) (<-chan *frameworkevent.Event, error)
	Stop(context.Context, string) error
	Close() error
}

type frameworkRunnerFactory func(context.Context, AgentSpec, Provider) (frameworkRunner, error)
type frameworkRunnerFactoryWithDeps func(context.Context, tenant.TenantContext, AgentSpec, Provider, ToolInvoker, *executionState) (frameworkRunner, error)

var defaultFrameworkRunnerFactory frameworkRunnerFactory = newFrameworkRunner
var defaultToolFrameworkRunnerFactory frameworkRunnerFactoryWithDeps = newFrameworkRunnerWithDeps

type execution struct {
	events                 <-chan *frameworkevent.Event
	done                   <-chan struct{}
	pumpCancel             context.CancelFunc
	frameworkErr           error
	pumpCompleted          atomic.Bool
	frameworkChannelClosed atomic.Bool
}

func newExecution(events <-chan *frameworkevent.Event, runErr error) *execution {
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	pumped := make(chan *frameworkevent.Event, 16)
	done := make(chan struct{})
	exec := &execution{
		events:       pumped,
		done:         done,
		pumpCancel:   pumpCancel,
		frameworkErr: runErr,
	}
	go func() {
		defer func() {
			exec.pumpCompleted.Store(true)
			close(done)
			close(pumped)
		}()
		if events == nil {
			return
		}
		for {
			select {
			case <-pumpCtx.Done():
				return
			case eventValue, ok := <-events:
				if !ok {
					exec.frameworkChannelClosed.Store(true)
					return
				}
				select {
				case pumped <- eventValue:
				case <-pumpCtx.Done():
					return
				}
			}
		}
	}()
	return exec
}

func (e *execution) cancelPump() {
	if e != nil && e.pumpCancel != nil {
		e.pumpCancel()
	}
}

func (e *execution) waitPump(timeout time.Duration) bool {
	if e == nil {
		return false
	}
	if e.pumpCompleted.Load() {
		return true
	}
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-e.done:
		return true
	case <-timer.C:
		return e.pumpCompleted.Load()
	}
}

func (e *execution) frameworkError() error {
	if e == nil {
		return nil
	}
	return e.frameworkErr
}

func (r *agentRuntimeImpl) Run(ctx context.Context, input AgentInput) (result AgentResult, runErr error) {
	if r == nil || r.deps.ProviderFactory == nil {
		return AgentResult{}, errors.New("agent: runtime is not configured")
	}
	if err := ctx.Err(); err != nil {
		return AgentResult{}, err
	}
	if err := validateInput(r.tc, r.spec, input); err != nil {
		return AgentResult{}, err
	}
	if err := providerContextCheck(ctx); err != nil {
		return AgentResult{}, err
	}

	provider, err := r.deps.ProviderFactory.Build(ctx, r.tc, r.spec)
	if err != nil {
		return AgentResult{}, fmt.Errorf("%w: build provider: %w", ErrProviderFailure, err)
	}
	if provider == nil {
		return AgentResult{}, fmt.Errorf("%w: provider factory returned nil provider", ErrProviderFailure)
	}
	state := &executionState{}
	var fw frameworkRunner
	var exec *execution
	if len(r.spec.Tools) > 0 && r.deps.ToolInvoker != nil {
		fw, err = defaultToolFrameworkRunnerFactory(ctx, r.tc, r.spec, provider, r.deps.ToolInvoker, state)
	} else {
		fw, err = defaultFrameworkRunnerFactory(ctx, r.spec, provider)
	}
	if err != nil {
		return AgentResult{}, fmt.Errorf("%w: construct runner: %w", ErrFrameworkFailure, err)
	}
	defer func() {
		if exec != nil {
			exec.cancelPump()
			if !exec.waitPump(r.deps.StopTimeout) {
				runErr = errors.Join(runErr, ErrProducerIncomplete)
			}
		}
		if closeErr := fw.Close(); closeErr != nil {
			closeErr = fmt.Errorf("%w: close: %w", ErrFrameworkFailure, closeErr)
			runErr = errors.Join(runErr, closeErr)
		}
	}()

	messages := append(cloneMessages(input.History), input.Input)
	frameworkMessages := make([]model.Message, 0, len(messages)+1)
	if strings.TrimSpace(r.spec.SystemPrompt) != "" {
		frameworkMessages = append(frameworkMessages, model.Message{Role: model.RoleSystem, Content: r.spec.SystemPrompt})
	}
	for _, message := range messages {
		role, roleErr := frameworkRole(message.Role)
		if roleErr != nil {
			return AgentResult{}, roleErr
		}
		frameworkMessages = append(frameworkMessages, model.Message{Role: role, Content: message.Content})
	}

	requestID := r.tc.RequestID
	if requestID == "" {
		requestID = input.Input.ID
	}
	frameworkEvents, frameworkErr := frameworkrunner.RunWithMessages(ctx, fw, r.tc.InternalUser, r.tc.SessionID, frameworkMessages, frameworkagent.WithRequestID(requestID))
	exec = newExecution(frameworkEvents, frameworkErr)
	result, drainErr := drainExecution(ctx, exec, r.deps.DrainTimeout, r.deps.StopTimeout, func(stopCtx context.Context) error {
		return fw.Stop(stopCtx, requestID)
	})
	if drainErr != nil {
		return result, drainErr
	}
	if !exec.pumpCompleted.Load() || !exec.frameworkChannelClosed.Load() {
		return result, combineExecutionErrors(exec, fmt.Errorf("%w: event pump did not observe framework completion", ErrProducerIncomplete))
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	if err := classifyFrameworkError(exec.frameworkError()); err != nil {
		return result, err
	}
	providerErr, toolEvents := state.snapshot()
	appendToolEvents(&result, toolEvents)
	if providerErr != nil {
		return result, providerErr
	}
	if err := classifyResultEvents(result); err != nil {
		return result, err
	}
	if err := validateResult(result); err != nil {
		return result, err
	}
	return result, nil
}

func validateInput(tc tenant.TenantContext, spec AgentSpec, input AgentInput) error {
	if err := input.TenantContext.Validate(); err != nil {
		return fmt.Errorf("%w: tenant context: %v", ErrInvalidInput, err)
	}
	if err := validateSpec(input.Agent); err != nil {
		return err
	}
	if !reflect.DeepEqual(input.Agent, spec) {
		return fmt.Errorf("%w: runtime input agent changed", ErrTenantMismatch)
	}
	if input.TenantContext.TenantID != spec.TenantID || input.TenantContext.AgentAppID != spec.AgentAppID || input.TenantContext.ConfigVersion != spec.Version {
		return fmt.Errorf("%w: runtime input does not match runtime configuration", ErrTenantMismatch)
	}
	if !reflect.DeepEqual(input.TenantContext, tc) {
		return fmt.Errorf("%w: runtime context changed", ErrTenantMismatch)
	}
	if strings.TrimSpace(input.Input.Content) == "" {
		return fmt.Errorf("%w: input content is required", ErrInvalidInput)
	}
	return nil
}

func providerContextCheck(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func frameworkRole(role string) (model.Role, error) {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "", "user":
		return model.RoleUser, nil
	case "system":
		return model.RoleSystem, nil
	case "assistant":
		return model.RoleAssistant, nil
	case "tool":
		return model.RoleTool, nil
	default:
		return "", fmt.Errorf("%w: unsupported message role %q", ErrInvalidInput, role)
	}
}

func drainEvents(ctx context.Context, events <-chan *frameworkevent.Event, timeout time.Duration) (AgentResult, error) {
	return drainEventsWithStop(ctx, events, timeout, timeout, nil)
}

func drainEventsWithStop(ctx context.Context, events <-chan *frameworkevent.Event, drainTimeout, stopTimeout time.Duration, stop func(context.Context) error) (AgentResult, error) {
	return drainExecution(ctx, newExecution(events, nil), drainTimeout, stopTimeout, stop)
}

func drainExecution(ctx context.Context, exec *execution, drainTimeout, stopTimeout time.Duration, stop func(context.Context) error) (AgentResult, error) {
	if exec == nil || exec.events == nil {
		return AgentResult{}, fmt.Errorf("%w: runner returned nil event channel", ErrFrameworkFailure)
	}
	if drainTimeout <= 0 {
		drainTimeout = 2 * time.Second
	}
	if stopTimeout <= 0 {
		stopTimeout = 500 * time.Millisecond
	}
	result := AgentResult{}
	var sequence int64
	var cancelErr error
	drainTimer := time.NewTimer(drainTimeout)
	defer drainTimer.Stop()
	var drainC <-chan time.Time = drainTimer.C
	stopped := false
	ctxDone := ctx.Done()
	for {
		select {
		case eventValue, ok := <-exec.events:
			if !ok {
				if !exec.waitPump(stopTimeout) || !exec.frameworkChannelClosed.Load() {
					return result, combineExecutionErrors(exec, cancelErr, ErrProducerIncomplete)
				}
				if cancelErr != nil {
					return result, combineExecutionErrors(exec, cancelErr)
				}
				return result, nil
			}
			if eventValue == nil {
				continue
			}
			sequence++
			converted := convertEvent(sequence, eventValue)
			result.Events = append(result.Events, converted)
			if eventValue.Response != nil {
				if eventValue.Response.Error != nil {
					result.FinishType = eventValue.Response.Error.Type
				}
				if len(eventValue.Response.Choices) > 0 {
					choice := eventValue.Response.Choices[0]
					content := choice.Message.Content
					role := choice.Message.Role
					if content == "" {
						content = choice.Delta.Content
						role = choice.Delta.Role
					}
					if role == "" || role == model.RoleAssistant {
						result.Text += content
					}
					if choice.FinishReason != nil {
						result.FinishType = *choice.FinishReason
					}
				}
				if eventValue.Response.Usage != nil {
					result.Usage.InputTokens += int64(eventValue.Response.Usage.PromptTokens)
					result.Usage.OutputTokens += int64(eventValue.Response.Usage.CompletionTokens)
				}
			}
		case <-ctxDone:
			if !stopped {
				stopped = true
				ctxDone = nil
				cancelErr = ctx.Err()
				if !drainTimer.Stop() {
					select {
					case <-drainTimer.C:
					default:
					}
				}
				drainTimer.Reset(drainTimeout)
				stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
				if stop != nil {
					stopErr := make(chan error, 1)
					go func() { stopErr <- stop(stopCtx) }()
					select {
					case err := <-stopErr:
						if err != nil {
							cancelErr = errors.Join(cancelErr, fmt.Errorf("%w: stop: %w", ErrFrameworkFailure, err))
						}
					case <-stopCtx.Done():
						cancelErr = errors.Join(cancelErr, fmt.Errorf("%w: stop timeout: %w", ErrFrameworkFailure, stopCtx.Err()))
					}
				}
				cancel()
				drainC = drainTimer.C
			}
		case <-drainC:
			exec.cancelPump()
			exec.waitPump(stopTimeout)
			return result, combineExecutionErrors(exec, cancelErr, fmt.Errorf("%w after %s", ErrDrainTimeout, drainTimeout), incompleteError(exec))
		}
	}
}

func combineExecutionErrors(exec *execution, primary ...error) error {
	errs := make([]error, 0, len(primary)+1)
	for _, err := range primary {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if exec != nil {
		if frameworkErr := classifyFrameworkError(exec.frameworkError()); frameworkErr != nil {
			errs = append(errs, frameworkErr)
		}
	}
	return errors.Join(errs...)
}

func incompleteError(exec *execution) error {
	if exec != nil && exec.pumpCompleted.Load() && exec.frameworkChannelClosed.Load() {
		return nil
	}
	return ErrProducerIncomplete
}

func appendToolEvents(result *AgentResult, events []RunnerEvent) {
	if result == nil {
		return
	}
	sequence := int64(len(result.Events))
	for _, eventValue := range events {
		sequence++
		eventValue.Sequence = sequence
		result.Events = append(result.Events, eventValue)
	}
}

func convertEvent(sequence int64, value *frameworkevent.Event) RunnerEvent {
	out := RunnerEvent{Sequence: sequence, Role: value.Author, Metadata: map[string]string{}}
	if value.Response != nil {
		out.Type = value.Response.Object
		out.ErrorType = ""
		if value.Response.Error != nil {
			out.ErrorType = value.Response.Error.Type
		}
		if len(value.Response.Choices) > 0 {
			choice := value.Response.Choices[0]
			out.Content = choice.Message.Content
			if out.Content == "" {
				out.Content = choice.Delta.Content
			}
			out.Role = string(choice.Message.Role)
		}
	}
	return out
}

func classifyFrameworkError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrFrameworkFailure, err)
}

func classifyResultEvents(result AgentResult) error {
	for _, eventValue := range result.Events {
		if eventValue.ErrorType == "" {
			continue
		}
		return fmt.Errorf("%w: %w", ErrProviderFailure, &ProviderResponseError{Type: eventValue.ErrorType, Message: eventValue.ErrorType})
	}
	return nil
}

func validateResult(result AgentResult) error {
	if strings.TrimSpace(result.Text) == "" {
		return fmt.Errorf("%w: runner returned empty output", ErrFrameworkFailure)
	}
	return nil
}

type managedFrameworkRunner struct {
	runner frameworkrunner.Runner
}

func (r managedFrameworkRunner) Run(ctx context.Context, userID, sessionID string, message model.Message, opts ...frameworkagent.RunOption) (<-chan *frameworkevent.Event, error) {
	return r.runner.Run(ctx, userID, sessionID, message, opts...)
}

func (r managedFrameworkRunner) Stop(_ context.Context, requestID string) error {
	managed, ok := r.runner.(frameworkrunner.ManagedRunner)
	if !ok {
		return errors.New("runner does not expose cancellation")
	}
	if !managed.Cancel(requestID) {
		return frameworkrunner.ErrRunNotFound
	}
	return nil
}

func (r managedFrameworkRunner) Close() error { return r.runner.Close() }

func newFrameworkRunner(ctx context.Context, spec AgentSpec, provider Provider) (frameworkRunner, error) {
	return newFrameworkRunnerWithDeps(ctx, tenant.TenantContext{TenantID: spec.TenantID, AgentAppID: spec.AgentAppID, ConfigVersion: spec.Version}, spec, provider, nil, nil)
}

func newFrameworkRunnerWithDeps(_ context.Context, tc tenant.TenantContext, spec AgentSpec, provider Provider, invoker ToolInvoker, state *executionState) (frameworkRunner, error) {
	modelAdapter := &providerModel{provider: provider, name: spec.ModelProvider, state: state}
	opts := []llmagent.Option{llmagent.WithModel(modelAdapter), llmagent.WithInstruction(spec.SystemPrompt)}
	if tools := newFrameworkTools(spec, invoker, tc, state); len(tools) > 0 {
		opts = append(opts, llmagent.WithTools(tools))
	}
	ag := llmagent.New(spec.Name, opts...)
	return managedFrameworkRunner{runner: frameworkrunner.NewRunner(spec.TenantID, ag)}, nil
}

type providerModel struct {
	provider Provider
	name     string
	state    *executionState
}

func (m *providerModel) Info() model.Info { return model.Info{Name: m.name} }

func (m *providerModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	input := make([]Message, 0, len(request.Messages))
	for _, message := range request.Messages {
		converted := Message{Role: string(message.Role), Content: message.Content, ToolID: message.ToolID, ToolName: message.ToolName}
		for _, call := range message.ToolCalls {
			converted.ToolCalls = append(converted.ToolCalls, ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: string(call.Function.Arguments)})
		}
		input = append(input, converted)
	}
	toolSpecs := make([]ToolSpec, 0, len(request.Tools))
	names := make([]string, 0, len(request.Tools))
	for name := range request.Tools {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		declaration := request.Tools[name].Declaration()
		if declaration == nil {
			continue
		}
		toolSpecs = append(toolSpecs, ToolSpec{Name: declaration.Name, Description: declaration.Description})
	}
	out := make(chan *model.Response, 1)
	go func() {
		defer close(out)
		response, err := m.provider.Complete(ctx, ProviderRequest{Messages: input, Tools: toolSpecs})
		if err != nil {
			if m.state != nil {
				m.state.recordError(err)
			}
			if ctx.Err() != nil {
				return
			}
			out <- &model.Response{Object: model.ObjectTypeError, Done: true, Error: model.ResponseErrorFromError(err, model.ErrorTypeAPIError)}
			return
		}
		finish := response.FinishType
		if finish == "" {
			finish = "stop"
		}
		message := model.Message{Role: model.RoleAssistant, Content: response.Text}
		for _, call := range response.ToolCalls {
			message.ToolCalls = append(message.ToolCalls, model.ToolCall{Type: "function", ID: call.ID, Function: model.FunctionDefinitionParam{Name: call.Name, Arguments: []byte(call.Arguments)}})
		}
		out <- &model.Response{Object: model.ObjectTypeChatCompletion, Done: true, Choices: []model.Choice{{Message: message, FinishReason: &finish}}, Usage: &model.Usage{PromptTokens: int(response.InputTokens), CompletionTokens: int(response.OutputTokens), TotalTokens: int(response.InputTokens + response.OutputTokens)}}
	}()
	return out, nil
}

var _ frameworkrunner.Runner = (frameworkRunner)(nil)
