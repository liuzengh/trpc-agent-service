package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"

	platformmetrics "github.com/Violet2314/trpc-agent-service/trpcservice/metrics"
	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrBudgetDenied means tenant rate or token limits deny a run.
	ErrBudgetDenied = errors.New("tenant budget denied")
	// ErrPermissionDenied means one or more IM users cannot invoke the app.
	ErrPermissionDenied = errors.New("IM user permission denied")
)

// ModelFactory resolves a model without exposing its API key to callers.
type ModelFactory interface {
	Model(context.Context, tenant.ModelConfig) (model.Model, error)
}

// ToolProvider returns all platform tools available before tenant filtering.
type ToolProvider interface {
	Tools(context.Context, tenant.Snapshot) ([]agenttool.Tool, error)
}

// Governor enforces tenant permissions and quotas around one run.
type Governor interface {
	Authorize(context.Context, tenant.Snapshot, []storage.UserEvent) error
	RecordUsage(context.Context, string, int) error
}

// Redactor removes secrets and tenant-specific sensitive values from output.
type Redactor interface {
	Redact(tenant.Snapshot, string) string
}

// DefaultExecutor assembles a request-scoped LLMAgent and Runner.
type DefaultExecutor struct {
	backends     storage.Factory
	models       ModelFactory
	tools        ToolProvider
	governor     Governor
	redactor     Redactor
	confirmation ConfirmationGate
	metrics      platformmetrics.Recorder
}

// ExecutorOption configures optional Worker behavior.
type ExecutorOption func(*DefaultExecutor)

// WithConfirmationGate enables dangerous-tool confirmation.
func WithConfirmationGate(gate ConfirmationGate) ExecutorOption {
	return func(executor *DefaultExecutor) {
		executor.confirmation = gate
	}
}

// WithMetrics enables Worker model/tool metrics.
func WithMetrics(recorder platformmetrics.Recorder) ExecutorOption {
	return func(executor *DefaultExecutor) {
		executor.metrics = recorder
	}
}

// NewExecutor constructs a Worker executor.
func NewExecutor(
	backends storage.Factory,
	models ModelFactory,
	tools ToolProvider,
	governor Governor,
	redactor Redactor,
	options ...ExecutorOption,
) (*DefaultExecutor, error) {
	if backends == nil || models == nil || tools == nil {
		return nil, errors.New("Worker backend, model, and tool factories are required")
	}
	if governor == nil {
		governor = NopGovernor{}
	}
	if redactor == nil {
		redactor = NewPolicyRedactor()
	}
	executor := &DefaultExecutor{
		backends: backends,
		models:   models,
		tools:    tools,
		governor: governor,
		redactor: redactor,
	}
	for _, option := range options {
		if option != nil {
			option(executor)
		}
	}
	return executor, nil
}

// Execute authorizes, assembles, and starts one debounced Agent turn.
func (e *DefaultExecutor) Execute(
	ctx context.Context,
	snapshot tenant.Snapshot,
	sessionID string,
	messages []storage.UserEvent,
) (<-chan Event, error) {
	if sessionID == "" || len(messages) == 0 {
		return nil, errors.New("Worker session ID and messages are required")
	}
	if err := e.governor.Authorize(ctx, snapshot, messages); err != nil {
		if errors.Is(err, ErrBudgetDenied) || errors.Is(err, ErrPermissionDenied) {
			return deniedEvents(err), nil
		}
		return nil, fmt.Errorf("authorize Worker execution: %w", err)
	}

	sessionService, err := e.backends.SessionService(snapshot.App)
	if err != nil {
		return nil, fmt.Errorf("create session service: %w", err)
	}
	if sessionService == nil {
		return nil, errors.New("create session service: backend returned nil")
	}
	memoryBackend, err := e.backends.MemoryBackend(snapshot.App)
	if err != nil {
		return nil, fmt.Errorf("create memory service: %w", err)
	}
	platformTools, err := e.tools.Tools(ctx, snapshot)
	if err != nil {
		return nil, fmt.Errorf("resolve platform tools: %w", err)
	}
	platformTools = append(platformTools, memoryBackend.Tools...)
	if memoryBackend.Service != nil {
		platformTools = append(platformTools, memoryBackend.Service.Tools()...)
	}

	// The framework filter is a soft constraint, so tools are pre-filtered
	// before registration and filtered again per invocation.
	include := agenttool.NewIncludeToolNamesFilter(snapshot.App.Tools...)
	visibleTools := agenttool.FilterTools(ctx, platformTools, include)
	if e.confirmation != nil {
		confirmationEvents, handled, err := e.confirmation.Resolve(
			ctx, snapshot, sessionID, messages, visibleTools,
		)
		if err != nil {
			return nil, fmt.Errorf("resolve dangerous tool confirmation: %w", err)
		}
		if handled {
			return confirmationEvents, nil
		}
	}
	modelInstance, err := e.models.Model(ctx, snapshot.App.Model)
	if err != nil {
		return nil, fmt.Errorf("create model: %w", err)
	}
	agentOptions := []llmagent.Option{
		llmagent.WithModel(modelInstance),
		llmagent.WithInstruction("You are a helpful enterprise assistant. Follow tenant tool and data boundaries."),
		llmagent.WithTools(visibleTools),
		llmagent.WithToolFilter(include),
	}
	if e.confirmation != nil {
		agentOptions = append(agentOptions, llmagent.WithToolCallbacks(e.confirmation.Callbacks(snapshot)))
	}
	agentInstance := llmagent.New(snapshot.App.AppName, agentOptions...)

	runnerOptions := []runner.Option{runner.WithSessionService(sessionService)}
	if memoryBackend.Service != nil {
		runnerOptions = append(runnerOptions, runner.WithMemoryService(memoryBackend.Service))
	}
	if memoryBackend.Ingestor != nil {
		runnerOptions = append(runnerOptions, runner.WithSessionIngestor(memoryBackend.Ingestor))
	}
	run := runner.NewRunner(snapshot.App.AppName, agentInstance, runnerOptions...)

	modelMessages, userID := rewriteMessages(sessionID, messages)
	raw, err := run.Run(
		ctx,
		userID,
		sessionID,
		modelMessages[len(modelMessages)-1],
		agent.WithUserMessageRewriter(func(
			context.Context,
			*agent.UserMessageRewriteArgs,
		) ([]model.Message, error) {
			return append([]model.Message(nil), modelMessages...), nil
		}),
		agent.WithToolFilter(include),
	)
	if err != nil {
		_ = run.Close()
		return nil, fmt.Errorf("start Agent run: %w", err)
	}

	output := make(chan Event)
	go e.projectEvents(ctx, snapshot, run, raw, output, time.Now())
	return output, nil
}

func (e *DefaultExecutor) projectEvents(
	ctx context.Context,
	snapshot tenant.Snapshot,
	run runner.Runner,
	raw <-chan *event.Event,
	output chan<- Event,
	started time.Time,
) {
	defer close(output)
	defer run.Close()

	seenPartial := make(map[string]bool)
	seenUsage := make(map[string]bool)
	doneSent := false
	errorSent := false
	for source := range raw {
		if source == nil {
			continue
		}
		if source.Response != nil && source.Response.Usage != nil && !seenUsage[source.Response.ID] {
			seenUsage[source.Response.ID] = true
			tokens := source.Response.Usage.TotalTokens
			_ = e.governor.RecordUsage(ctx, snapshot.Tenant.ID, tokens)
			if e.metrics != nil {
				e.metrics.ObserveModel(
					snapshot.Tenant.ID,
					snapshot.App.Model.Model,
					time.Since(started),
					tokens,
				)
			}
			if !emitWorkerEvent(ctx, output, Event{Type: "usage", UsageTokens: tokens}) {
				return
			}
		}
		if source.IsRunnerCompletion() {
			if source.Response != nil && source.Response.Error != nil && !errorSent {
				errorSent = true
				if !emitWorkerEvent(ctx, output, Event{
					Type:  "error",
					Error: e.redactor.Redact(snapshot, source.Response.Error.Message),
				}) {
					return
				}
			}
			if !doneSent {
				doneSent = true
				if !emitWorkerEvent(ctx, output, Event{Type: "done"}) {
					return
				}
			}
			continue
		}
		if source.Response == nil {
			continue
		}
		if source.Response.Error != nil {
			errorSent = true
			if !emitWorkerEvent(ctx, output, Event{
				Type:  "error",
				Error: e.redactor.Redact(snapshot, source.Response.Error.Message),
			}) {
				return
			}
			continue
		}
		if source.Object == model.ObjectTypeToolResponse {
			for _, choice := range source.Choices {
				if e.metrics != nil {
					e.metrics.ObserveTool(
						snapshot.Tenant.ID,
						choice.Message.ToolName,
						time.Since(started),
						false,
					)
				}
				if !emitWorkerEvent(ctx, output, Event{
					Type:     "tool_result",
					ToolName: choice.Message.ToolName,
					Text:     e.redactor.Redact(snapshot, choice.Message.Content),
				}) {
					return
				}
			}
			continue
		}
		if source.Response.IsToolCallResponse() {
			for _, choice := range source.Choices {
				for _, call := range choice.Message.ToolCalls {
					if !emitWorkerEvent(ctx, output, Event{
						Type:     "tool_call",
						ToolName: call.Function.Name,
						Text:     e.redactor.Redact(snapshot, string(call.Function.Arguments)),
					}) {
						return
					}
				}
			}
			continue
		}
		if source.IsPartial {
			seenPartial[source.Response.ID] = true
			for _, choice := range source.Choices {
				if choice.Delta.Content != "" && !emitWorkerEvent(ctx, output, Event{
					Type: "text_delta",
					Text: e.redactor.Redact(snapshot, choice.Delta.Content),
				}) {
					return
				}
			}
			continue
		}
		if !seenPartial[source.Response.ID] {
			for _, choice := range source.Choices {
				if choice.Message.Content != "" && !emitWorkerEvent(ctx, output, Event{
					Type: "text_delta",
					Text: e.redactor.Redact(snapshot, choice.Message.Content),
				}) {
					return
				}
			}
		}
	}
	if !doneSent {
		_ = emitWorkerEvent(ctx, output, Event{Type: "done"})
	}
}

func rewriteMessages(sessionID string, events []storage.UserEvent) ([]model.Message, string) {
	groupID, isGroup := groupIDFromSession(sessionID)
	messages := make([]model.Message, 0, len(events))
	for _, source := range events {
		content := source.Text
		if isGroup {
			content = fmt.Sprintf("[%s] %s", source.SenderID, source.Text)
		}
		messages = append(messages, model.NewUserMessage(content))
	}
	if isGroup {
		// Framework identity is (AppName, UserID, SessionID), so group UserID
		// must be stable instead of using each sender.
		return messages, "group:" + groupID
	}
	return messages, events[len(events)-1].SenderID
}

func groupIDFromSession(sessionID string) (string, bool) {
	const marker = ":group:"
	_, groupID, found := strings.Cut(sessionID, marker)
	return groupID, found && groupID != ""
}

func deniedEvents(err error) <-chan Event {
	output := make(chan Event, 2)
	message := "request denied by tenant policy"
	if errors.Is(err, ErrBudgetDenied) {
		message = "tenant request quota or token budget exceeded"
	}
	if errors.Is(err, ErrPermissionDenied) {
		message = "user is not allowed to invoke this Agent"
	}
	output <- Event{Type: "error", Error: message}
	output <- Event{Type: "done"}
	close(output)
	return output
}

func emitWorkerEvent(ctx context.Context, output chan<- Event, value Event) bool {
	select {
	case output <- value:
		return true
	case <-ctx.Done():
		return false
	}
}
