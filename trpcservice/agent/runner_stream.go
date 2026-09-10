package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

var (
	// ErrRunnerExecution is the redacted result of an external Runner event
	// that reports a provider failure.
	ErrRunnerExecution = errors.New("agent runner execution failed")
)

// RunnerEventType identifies the service-owned event surface emitted by an
// external Agent Runner invocation.
type RunnerEventType string

const (
	// RunnerEventMessage contains assistant text emitted by the Runner.
	RunnerEventMessage RunnerEventType = "message"
	// RunnerEventStatus contains non-terminal execution progress.
	RunnerEventStatus RunnerEventType = "status"
	// RunnerEventError contains a redacted execution failure.
	RunnerEventError RunnerEventType = "error"
	// RunnerEventDone identifies a terminal Runner invocation.
	RunnerEventDone RunnerEventType = "done"
)

// RunnerEvent is the normalized event surface between the Agent adapter and
// service runtime scheduling. It never exposes an upstream event or provider
// error.
type RunnerEvent struct {
	Type   RunnerEventType
	Text   string
	Status string
	Err    error
	Done   bool
}

// Invocation contains the identity and message needed for one Agent Runner
// invocation. RequestID is forwarded as the upstream request option.
type Invocation struct {
	UserID    string
	SessionID string
	Message   Message
	RequestID string
}

// Invoke starts one external Agent Runner invocation and normalizes its event
// stream. The returned channel is closed after a terminal event, source
// closure, or bounded cancellation drain. The caller owns the returned stream
// consumption; the Runner lifecycle remains owned by the runtime registry.
func Invoke(ctx context.Context, runner Runner, request Invocation, drainTimeout time.Duration) (<-chan RunnerEvent, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if runner == nil {
		return nil, fmt.Errorf("%w: runner is required", ErrInvalid)
	}
	if request.UserID == "" || request.SessionID == "" {
		return nil, fmt.Errorf("%w: runner identity is required", ErrInvalid)
	}
	if request.RequestID == "" {
		return nil, fmt.Errorf("%w: request ID is required", ErrInvalid)
	}
	runnerEvents, err := runner.Run(ctx, request.UserID, request.SessionID, request.Message, trpcagent.WithRequestID(request.RequestID))
	if err != nil {
		return nil, err
	}
	if runnerEvents == nil {
		return nil, fmt.Errorf("%w: runner event stream is nil", ErrInvalid)
	}
	output := make(chan RunnerEvent, 32)
	go forwardRunnerEvents(ctx, runnerEvents, output, drainTimeout)
	return output, nil
}

func forwardRunnerEvents(ctx context.Context, source <-chan *trpcevent.Event, output chan<- RunnerEvent, drainTimeout time.Duration) {
	defer close(output)
	for {
		select {
		case <-ctx.Done():
			drainRunnerEvents(source, drainTimeout)
			return
		case event, ok := <-source:
			if !ok {
				return
			}
			mapped, terminal := mapExternalRunnerEvent(event)
			for _, item := range mapped {
				if !sendRunnerEvent(ctx, output, item) {
					drainRunnerEvents(source, drainTimeout)
					return
				}
			}
			if terminal {
				drainRunnerEvents(source, drainTimeout)
				return
			}
		}
	}
}

func mapExternalRunnerEvent(event *trpcevent.Event) ([]RunnerEvent, bool) {
	if event == nil || event.Response == nil {
		return []RunnerEvent{{Type: RunnerEventStatus, Status: "progress"}}, false
	}
	response := event.Response
	if response.Error != nil {
		return []RunnerEvent{
			{Type: RunnerEventError, Err: ErrRunnerExecution},
			{Type: RunnerEventDone, Status: "error", Done: true},
		}, true
	}
	text := externalResponseText(response)
	result := make([]RunnerEvent, 0, 2)
	if text != "" {
		result = append(result, RunnerEvent{Type: RunnerEventMessage, Text: text})
	}
	if response.Done {
		result = append(result, RunnerEvent{Type: RunnerEventDone, Status: "complete", Done: true})
		return result, true
	}
	if len(result) == 0 {
		status := "progress"
		if response.IsPartial {
			status = "partial"
		}
		result = append(result, RunnerEvent{Type: RunnerEventStatus, Status: status})
	}
	return result, false
}

func externalResponseText(response *trpcmodel.Response) string {
	if response == nil {
		return ""
	}
	var builder strings.Builder
	for _, choice := range response.Choices {
		text := choice.Delta.Content
		if text == "" {
			text = choice.Message.Content
		}
		if text != "" {
			builder.WriteString(text)
		}
	}
	return builder.String()
}

func sendRunnerEvent(ctx context.Context, output chan<- RunnerEvent, event RunnerEvent) bool {
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

func drainRunnerEvents(events <-chan *trpcevent.Event, timeout time.Duration) {
	if events == nil || timeout <= 0 {
		return
	}
	timer := time.NewTimer(timeout)
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
