package gateway

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/execution"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
)

type executionForwardState struct {
	terminalErr          error
	terminalEventType    audit.EventType
	terminalErrorType    string
	terminalErrorEmitted bool
	terminalSeen         bool
	reply                strings.Builder
}

func (state *executionForwardState) observe(event execution.Event) {
	switch event.Type {
	case execution.EventMessage:
		state.reply.WriteString(event.Text)
	case execution.EventError:
		state.terminalErr = event.Err
		if IsContextCancellation(state.terminalErr) {
			state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
			return
		}
		if state.terminalErr == nil {
			state.terminalErr = ErrExecution
		}
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionFailed, string(audit.ErrorUnavailable)
	}
	if event.Done {
		state.terminalSeen = true
	}
	if !event.Done || state.terminalErr != nil {
		return
	}
	switch event.Status {
	case "error":
		state.terminalErr = ErrExecution
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionFailed, string(audit.ErrorUnavailable)
	case "canceled":
		state.terminalErr = context.Canceled
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
	case "deadline_exceeded":
		state.terminalErr = context.DeadlineExceeded
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
	default:
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionCompleted, ""
	}
}

func (state *executionForwardState) skip(event execution.Event) bool {
	return event.Type == execution.EventDone || (event.Type == execution.EventError && IsContextCancellation(event.Err))
}

func (state *executionForwardState) markSendFailure(ctx context.Context) {
	state.terminalErr = ctx.Err()
	if state.terminalErr == nil {
		state.terminalErr = context.Canceled
	}
	state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
	state.terminalErrorEmitted = false
}

func (state *executionForwardState) ensureTerminal(ctx context.Context) {
	if state.terminalSeen || state.terminalErr != nil {
		return
	}
	if ctx.Err() != nil {
		state.terminalErr = ctx.Err()
		state.terminalEventType, state.terminalErrorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
		return
	}
	state.terminalEventType, state.terminalErrorType = audit.EventExecutionCompleted, ""
}

func (dispatcher *Dispatcher) forwardExecution(ctx context.Context, run *dispatchExecution) {
	defer close(run.output)

	state := executionForwardState{}
	for event := range run.executionEvents {
		state.observe(event)
		if state.skip(event) {
			continue
		}
		mapped := mapExecutionEvent(event)
		if mapped.Type != DispatchEventMessage && mapped.Type != DispatchEventStatus && mapped.Type != DispatchEventError {
			continue
		}
		if mapped.Type == DispatchEventError {
			state.terminalErrorEmitted = true
		}
		if !sendDispatchEvent(ctx, run.output, mapped) {
			state.markSendFailure(ctx)
			break
		}
	}
	state.ensureTerminal(ctx)
	mediaIntents := []servicetool.ReplyIntent(nil)
	if run.mediaReplies != nil {
		mediaIntents = run.mediaReplies.Intents()
	}
	terminalErr := dispatcher.finalizeForward(ctx, run, &state, mediaIntents)
	run.finishForwardOutput(ctx, terminalErr, state.terminalErrorEmitted)
	if terminalErr != nil {
		class := observability.ErrorClass(terminalErr)
		run.span.SetAttributes(observability.Attribute{Key: "error_class", Value: class})
		run.span.SetStatus(observability.StatusError, class)
		run.span.RecordError(terminalErr)
	} else {
		run.span.SetStatus(observability.StatusOK, "")
	}
	_ = dispatcher.metrics.Operation(ctx, run.started, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch}, terminalErr)
	logDispatchFailure(run.metadata.principal, run.metadata.requestID, run.metadata.traceID, terminalErr)
	run.span.End()
}

func mapExecutionEvent(event execution.Event) DispatchEvent {
	result := DispatchEvent{Type: DispatchEventType(event.Type), RequestID: event.RequestID, TraceID: event.TraceID, Text: event.Text, Status: event.Status, Done: event.Done}
	if event.Type == execution.EventError {
		if IsContextCancellation(event.Err) {
			result.Error = ErrExecutionCanceled.Error()
		} else {
			result.Error = ErrExecution.Error()
		}
	}
	return result
}

func (run *dispatchExecution) finishForwardOutput(ctx context.Context, terminalErr error, terminalErrorEmitted bool) {
	if IsContextCancellation(terminalErr) {
		if !terminalErrorEmitted {
			trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventError, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Error: ErrExecutionCanceled.Error()})
		}
		trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventDone, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Status: cancellationStatus(ctx), Done: true})
		return
	}
	if terminalErr != nil {
		if !terminalErrorEmitted {
			errorText := ErrExecution.Error()
			if errors.Is(terminalErr, ErrAuditWriteFailed) {
				errorText = ErrAuditWriteFailed.Error()
			} else if errors.Is(terminalErr, runtimebudget.ErrExceeded) {
				errorText = runtimebudget.ErrExceeded.Error()
			} else if errors.Is(terminalErr, runtimebudget.ErrCostUnavailable) {
				errorText = runtimebudget.ErrCostUnavailable.Error()
			}
			trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventError, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Error: errorText})
		}
		trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventDone, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Status: "error", Done: true})
		return
	}
	trySendDispatchEvent(run.output, DispatchEvent{Type: DispatchEventDone, RequestID: run.metadata.requestID, TraceID: run.metadata.traceID, Status: "complete", Done: true})
}

func (dispatcher *Dispatcher) finalizeForward(ctx context.Context, run *dispatchExecution, state *executionForwardState, mediaReplies []servicetool.ReplyIntent) error {
	terminalErr := state.terminalErr
	eventType := state.terminalEventType
	errorType := state.terminalErrorType
	if eventType == "" {
		eventType = audit.EventExecutionCompleted
		if terminalErr != nil {
			eventType = audit.EventExecutionFailed
			if IsContextCancellation(terminalErr) {
				eventType = audit.EventExecutionCanceled
			}
		}
	}
	if errorType == "" {
		errorType = terminalAuditError(terminalErr)
	}
	if budgetErr := dispatcher.settleBudget(detachedCorrelationContext(ctx, run.metadata.requestID, run.metadata.traceID), run, eventType); budgetErr != nil && (terminalErr == nil || IsContextCancellation(terminalErr)) {
		terminalErr = budgetErr
		eventType = audit.EventExecutionFailed
		errorType = terminalAuditError(budgetErr)
	}
	if run.auditUsage != nil {
		run.auditUsage.ExecutionResult = executionAuditResult(eventType)
	}
	if dispatcher.handoffStore != nil {
		result := audit.ResultSuccess
		if terminalErr != nil {
			result = audit.ResultFailure
			if IsContextCancellation(terminalErr) {
				result = audit.ResultCanceled
			}
		}
		if _, err := dispatcher.handoffStore.Finalize(detachedCorrelationContext(ctx, run.metadata.requestID, run.metadata.traceID), audit.ExecutionHandoff{TenantID: run.metadata.principal.TenantID(), HandoffID: audit.NewEventID(run.metadata.requestID, "handoff"), State: audit.HandoffFinalized, Result: result, ErrorType: errorType}); err != nil && (terminalErr == nil || IsContextCancellation(terminalErr)) {
			terminalErr = auditWriteFailure()
		}
	}
	if err := dispatcher.finalizeExecutionAudit(ctx, run, eventType, errorType); err != nil && (terminalErr == nil || IsContextCancellation(terminalErr)) {
		terminalErr = auditWriteFailure()
	}
	dispatcher.finishDurable(ctx, run.metadata, run.durable, terminalErr, state.reply.String(), mediaReplies)
	return terminalErr
}

func (dispatcher *Dispatcher) finalizeExecutionAudit(ctx context.Context, run *dispatchExecution, eventType audit.EventType, errorType string) error {
	if run.auditFinalized {
		return nil
	}
	err := dispatcher.writeExecutionAuditWithCost(detachedCorrelationContext(ctx, run.metadata.requestID, run.metadata.traceID), run.metadata, eventType, errorType, run.auditUsage)
	if err == nil {
		run.auditFinalized = true
	}
	return err
}

func auditWriteFailure() error {
	return errors.Join(ErrExecution, ErrAuditWriteFailed)
}

func terminalAuditError(err error) string {
	if err == nil {
		return ""
	}
	if isBudgetRejection(err) {
		return string(audit.ErrorBudget)
	}
	if IsContextCancellation(err) {
		return string(audit.ErrorCanceled)
	}
	return string(audit.ErrorUnavailable)
}

func (dispatcher *Dispatcher) writeExecutionAudit(ctx context.Context, metadata dispatchMetadata, eventType audit.EventType, errorType string) error {
	return dispatcher.writeExecutionAuditRevision(ctx, metadata, eventType, errorType, nil)
}

func (dispatcher *Dispatcher) writeExecutionAuditRevision(ctx context.Context, metadata dispatchMetadata, eventType audit.EventType, errorType string, revision *int64) error {
	return dispatcher.writeExecutionAuditRevisionWithCost(ctx, metadata, eventType, errorType, revision, nil)
}

func (dispatcher *Dispatcher) writeExecutionAuditWithCost(ctx context.Context, metadata dispatchMetadata, eventType audit.EventType, errorType string, cost *audit.Usage) error {
	return dispatcher.writeExecutionAuditRevisionWithCost(ctx, metadata, eventType, errorType, nil, cost)
}

func (dispatcher *Dispatcher) writeExecutionAuditRevisionWithCost(ctx context.Context, metadata dispatchMetadata, eventType audit.EventType, errorType string, revision *int64, cost *audit.Usage) error {
	if dispatcher.auditWriter == nil {
		return nil
	}
	channel := string(metadata.principal.Kind())
	if target, ok := metadata.principal.RoutingTarget(); ok {
		channel = string(target.Channel)
	}
	event := audit.Event{SchemaVersion: audit.SchemaVersion, EventID: audit.NewEventID(metadata.requestID, string(eventType)), EventType: eventType, TenantID: metadata.principal.TenantID(), Channel: channel, UserID: metadata.message.ExternalUserID, SessionID: metadata.identity.SessionID, AgentAppID: metadata.principal.AppID(), Revision: revision, ModelProfileID: metadata.modelProfileID, ErrorType: errorType, Cost: cost, RequestID: metadata.requestID, TraceID: metadata.traceID, ActorType: string(metadata.principal.Kind()), ActorID: metadata.principal.SubjectID(), OccurredAt: time.Now().UTC()}
	if _, err := dispatcher.auditWriter.Append(ctx, event); err != nil {
		return err
	}
	return nil
}

func executionAuditResult(eventType audit.EventType) audit.ExecutionResult {
	switch eventType {
	case audit.EventExecutionCanceled, audit.EventExecutionTimedOut:
		return audit.ResultCanceled
	case audit.EventExecutionFailed, audit.EventExecutionFallback:
		return audit.ResultFailure
	default:
		return audit.ResultSuccess
	}
}

func cancellationStatus(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "canceled"
}

func sendDispatchEvent(ctx context.Context, output chan<- DispatchEvent, event DispatchEvent) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	default:
	}
	select {
	case output <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func trySendDispatchEvent(output chan<- DispatchEvent, event DispatchEvent) {
	select {
	case output <- event:
	default:
	}
}
