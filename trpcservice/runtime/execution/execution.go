// Package execution owns one runtime Runner invocation and its stream
// lifecycle. It does not know about Gateway protocols or durable replies.
package execution

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	serviceagent "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrInvalid reports invalid execution configuration or input.
	ErrInvalid = errors.New("invalid runtime execution")
	// ErrNotReady reports that the Coordinator is nil or has no registry.
	ErrNotReady = errors.New("runtime execution is not ready")
	// ErrExecution is the stable, redacted result of a Runner failure.
	ErrExecution = errors.New("execution failed")
)

const (
	defaultDrainTimeout = 250 * time.Millisecond

	terminalPending uint32 = iota
	terminalCanceled
)

// EventType identifies the protocol-neutral execution event surface.
type EventType string

const (
	// EventMessage contains assistant text emitted by the Runner.
	EventMessage EventType = "message"
	// EventStatus contains non-terminal execution progress.
	EventStatus EventType = "status"
	// EventError contains a redacted execution failure or cancellation cause.
	EventError EventType = "error"
	// EventDone identifies the terminal event for one execution stream.
	EventDone EventType = "done"
)

// Event is the protocol-neutral result of one Runner event. Err is never a
// provider error: non-cancellation Runner failures are reduced to
// ErrExecution before they cross this boundary.
type Event struct {
	Type      EventType
	RequestID string
	TraceID   string
	Text      string
	Status    string
	Err       error
	Done      bool
}

// Request contains the fixed runtime plan and user input for one invocation.
// The plan is validated again by the Runner registry before acquisition.
type Request struct {
	Plan      runtime.ExecutionPlan
	Identity  tenant.RunnerIdentity
	Message   serviceagent.Message
	RequestID string
	TraceID   string
}

// Registry is the runner capability consumed by Coordinator. The interface is
// owned here so runtime execution can be tested without depending on Gateway.
type Registry interface {
	Ready() bool
	Acquire(context.Context, runtime.ExecutionPlan) (*runtimerunner.RunnerLease, error)
}

// Config configures one runtime execution coordinator.
type Config struct {
	Registry      Registry
	DrainTimeout  time.Duration
	Observability observability.Provider
}

// Coordinator acquires one Runner lease, invokes the Runner, maps its events,
// and releases the lease after the returned stream reaches a terminal state.
type Coordinator struct {
	registry     Registry
	drainTimeout time.Duration
	telemetry    observability.Provider
	metrics      metrics.Catalog
}

// executionStream contains the resources owned by one asynchronous Runner
// invocation. The context remains an explicit argument to keep cancellation
// ownership visible at the forwarding boundary.
type executionStream struct {
	request      Request
	runnerEvents <-chan serviceagent.RunnerEvent
	lease        *runtimerunner.RunnerLease
	output       chan<- Event
	finishRunner func(error)
	started      time.Time
}

// NewCoordinator validates and creates a runtime execution coordinator.
func NewCoordinator(config Config) (*Coordinator, error) {
	if config.Registry == nil {
		return nil, fmt.Errorf("%w: runner registry is required", ErrInvalid)
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = defaultDrainTimeout
	}
	if config.DrainTimeout < 0 {
		return nil, fmt.Errorf("%w: drain timeout cannot be negative", ErrInvalid)
	}
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	return &Coordinator{
		registry:     config.Registry,
		drainTimeout: config.DrainTimeout,
		telemetry:    config.Observability,
		metrics:      metrics.New(config.Observability),
	}, nil
}

// Ready reports whether the coordinator has a usable Runner registry.
func (coordinator *Coordinator) Ready() bool {
	return coordinator != nil && coordinator.registry != nil && coordinator.registry.Ready()
}

// Execute starts one Runner invocation and returns its protocol-neutral event
// stream. The stream owns the acquired Runner lease until it closes.
func (coordinator *Coordinator) Execute(ctx context.Context, request Request) (<-chan Event, error) {
	if coordinator == nil || coordinator.registry == nil {
		return nil, ErrNotReady
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.RequestID == "" {
		return nil, fmt.Errorf("%w: request ID is required", ErrInvalid)
	}
	if request.Identity.UserID == "" || request.Identity.SessionID == "" {
		return nil, fmt.Errorf("%w: runner identity is required", ErrInvalid)
	}
	if _, err := request.Plan.CacheKey(); err != nil {
		return nil, fmt.Errorf("%w: execution plan: %w", ErrInvalid, err)
	}

	lease, err := coordinator.registry.Acquire(ctx, request.Plan)
	if err != nil {
		return nil, err
	}
	runnerValue := lease.Runner()
	if runnerValue == nil {
		_ = lease.Release()
		return nil, runtimerunner.ErrRunnerUnavailable
	}
	_ = coordinator.metrics.Lease(ctx, 1, map[string]string{"component": "runner", "status": "active"})
	runnerCtx, _, finishRunner := observability.StartOperation(observability.WithCorrelation(ctx, request.RequestID, request.TraceID), coordinator.telemetry, observability.OperationRunnerExecution, "runner")
	started := time.Now()
	_ = coordinator.metrics.Request(runnerCtx, map[string]string{"component": "runner", "operation": observability.OperationRunnerExecution, "status": "started"})
	runnerEvents, err := serviceagent.Invoke(runnerCtx, runnerValue, serviceagent.Invocation{
		UserID: request.Identity.UserID, SessionID: request.Identity.SessionID,
		Message: request.Message, RequestID: request.RequestID,
	}, coordinator.drainTimeout)
	if err != nil {
		err = normalizeRunError(err)
		finishRunner(err)
		_ = coordinator.metrics.Operation(runnerCtx, started, map[string]string{"component": "runner", "operation": observability.OperationRunnerExecution}, err)
		_ = lease.Release()
		_ = coordinator.metrics.Lease(ctx, -1, map[string]string{"component": "runner", "status": "active"})
		return nil, err
	}
	if runnerEvents == nil {
		err = ErrExecution
		finishRunner(err)
		_ = coordinator.metrics.Operation(runnerCtx, started, map[string]string{"component": "runner", "operation": observability.OperationRunnerExecution}, err)
		_ = lease.Release()
		_ = coordinator.metrics.Lease(ctx, -1, map[string]string{"component": "runner", "status": "active"})
		return nil, err
	}

	output := make(chan Event, 32)
	_ = coordinator.metrics.Active(ctx, 1, map[string]string{"component": "runner"})
	go coordinator.forward(runnerCtx, executionStream{
		request: request, runnerEvents: runnerEvents, lease: lease, output: output,
		finishRunner: finishRunner, started: started,
	})
	return output, nil
}

func (coordinator *Coordinator) forward(ctx context.Context, stream executionStream) {
	defer close(stream.output)

	var terminalState atomic.Uint32
	terminalState.Store(terminalPending)
	cancelWatchDone := make(chan struct{})
	defer close(cancelWatchDone)
	go func() {
		select {
		case <-ctx.Done():
			terminalState.CompareAndSwap(terminalPending, terminalCanceled)
		case <-cancelWatchDone:
		}
	}()

	var terminalErr error
	terminalCommitted := false
	defer func() {
		if !terminalCommitted {
			if coordinator.canceled(ctx, &terminalState) {
				terminalErr = cancellationError(ctx)
				coordinator.emitCancellation(stream.output, stream.request, terminalErr)
			} else {
				coordinator.emitDone(stream.output, stream.request, "complete")
			}
		}
		// The Agent adapter owns the upstream Event channel, but the runtime
		// still owns this normalized stream until it is closed. Drain it before
		// releasing the lease so invalidation or registry shutdown cannot close
		// an external Runner while its adapter is finishing source cleanup.
		coordinator.drain(stream.runnerEvents)
		stream.finishRunner(terminalErr)
		_ = coordinator.metrics.Operation(ctx, stream.started, map[string]string{"component": "runner", "operation": observability.OperationRunnerExecution}, terminalErr)
		_ = stream.lease.Release()
		_ = coordinator.metrics.Lease(ctx, -1, map[string]string{"component": "runner", "status": "active"})
		_ = coordinator.metrics.Active(ctx, -1, map[string]string{"component": "runner"})
	}()

	for {
		if coordinator.canceled(ctx, &terminalState) {
			terminalErr = cancellationError(ctx)
			coordinator.emitCancellation(stream.output, stream.request, terminalErr)
			terminalCommitted = true
			return
		}
		select {
		case event, ok := <-stream.runnerEvents:
			if coordinator.canceled(ctx, &terminalState) {
				terminalErr = cancellationError(ctx)
				coordinator.emitCancellation(stream.output, stream.request, terminalErr)
				terminalCommitted = true
				return
			}
			if !ok {
				if coordinator.canceled(ctx, &terminalState) {
					terminalErr = cancellationError(ctx)
					coordinator.emitCancellation(stream.output, stream.request, terminalErr)
				} else {
					coordinator.emitDone(stream.output, stream.request, "complete")
				}
				terminalCommitted = true
				return
			}
			mapped, done := mapRunnerEvent(event, stream.request.RequestID, stream.request.TraceID)
			var terminalEvent Event
			hasTerminalEvent := false
			for _, item := range mapped {
				if coordinator.canceled(ctx, &terminalState) {
					terminalErr = cancellationError(ctx)
					coordinator.emitCancellation(stream.output, stream.request, terminalErr)
					terminalCommitted = true
					return
				}
				if item.Type == EventError {
					terminalErr = item.Err
				}
				if item.Type == EventDone {
					terminalEvent = item
					hasTerminalEvent = true
					continue
				}
				if !sendEvent(ctx, stream.output, item) {
					terminalErr = cancellationError(ctx)
					coordinator.emitCancellation(stream.output, stream.request, terminalErr)
					terminalCommitted = true
					return
				}
			}
			if done {
				if coordinator.canceled(ctx, &terminalState) {
					terminalErr = cancellationError(ctx)
					coordinator.emitCancellation(stream.output, stream.request, terminalErr)
					terminalCommitted = true
					return
				}
				if hasTerminalEvent {
					trySend(stream.output, terminalEvent)
				}
				terminalCommitted = true
				return
			}
		case <-ctx.Done():
			terminalErr = cancellationError(ctx)
			coordinator.emitCancellation(stream.output, stream.request, terminalErr)
			terminalCommitted = true
			return
		}
	}
}

func (coordinator *Coordinator) drain(events <-chan serviceagent.RunnerEvent) {
	if coordinator == nil || events == nil || coordinator.drainTimeout <= 0 {
		return
	}
	timer := time.NewTimer(coordinator.drainTimeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-timer.C:
			return
		}
	}
}

func (coordinator *Coordinator) canceled(ctx context.Context, state *atomic.Uint32) bool {
	return state.Load() == terminalCanceled || ctx.Err() != nil
}

func (coordinator *Coordinator) emitCancellation(output chan<- Event, request Request, err error) {
	trySend(output, Event{Type: EventError, RequestID: request.RequestID, TraceID: request.TraceID, Err: err})
	trySend(output, Event{Type: EventDone, RequestID: request.RequestID, TraceID: request.TraceID, Status: cancellationStatus(err), Done: true})
}

func (coordinator *Coordinator) emitDone(output chan<- Event, request Request, status string) {
	trySend(output, Event{Type: EventDone, RequestID: request.RequestID, TraceID: request.TraceID, Status: status, Done: true})
}

func normalizeRunError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrExecution
}

func cancellationError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return context.Canceled
}

func cancellationStatus(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "canceled"
}

func mapRunnerEvent(event serviceagent.RunnerEvent, requestID, traceID string) ([]Event, bool) {
	if event.Type == "" {
		return []Event{{Type: EventStatus, RequestID: requestID, TraceID: traceID, Status: "progress"}}, false
	}
	result := Event{Type: EventType(event.Type), RequestID: requestID, TraceID: traceID, Text: event.Text, Status: event.Status, Err: event.Err, Done: event.Done}
	switch event.Type {
	case serviceagent.RunnerEventMessage:
		result.Type = EventMessage
	case serviceagent.RunnerEventStatus:
		result.Type = EventStatus
	case serviceagent.RunnerEventError:
		result.Type = EventError
		result.Err = ErrExecution
	case serviceagent.RunnerEventDone:
		result.Type = EventDone
		result.Done = true
	}
	if result.Type == EventError && !errors.Is(result.Err, context.Canceled) && !errors.Is(result.Err, context.DeadlineExceeded) {
		result.Err = ErrExecution
	}
	return []Event{result}, result.Type == EventDone || result.Done
}

func sendEvent(ctx context.Context, output chan<- Event, event Event) bool {
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

func trySend(output chan<- Event, event Event) {
	select {
	case output <- event:
	default:
	}
}
