package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-a2a-go/protocol"
	"trpc.group/trpc-go/trpc-a2a-go/taskmanager"
)

// A2ATaskStore is the durable authority required by the A2A façade.
type A2ATaskStore interface {
	ExecutionReader
	RequestCancel(context.Context, CancelRequest) (CancelResult, error)
}

// DurableA2ATaskManager implements the public A2A TaskManager contract without
// process-local task, subscriber, session, or push-notification state.
type DurableA2ATaskManager struct {
	Submitter    RunSubmitter
	Tasks        A2ATaskStore
	Events       SharedEventStore
	Readiness    interface{ Ready() bool }
	PollInterval time.Duration
	ReplayLimit  int
}

func (m *DurableA2ATaskManager) valid() bool {
	return m != nil && m.Submitter.Inbox != nil && m.Submitter.Payloads != nil && m.Submitter.Dispatcher != nil &&
		m.Submitter.PayloadKeyVersion > 0 && m.Tasks != nil && m.Events != nil && m.Readiness != nil
}

func (m *DurableA2ATaskManager) OnSendMessage(ctx context.Context, request protocol.SendMessageParams) (*protocol.MessageResult, error) {
	handle, trusted, err := m.submit(ctx, request)
	if err != nil {
		return nil, err
	}
	if request.Configuration != nil && request.Configuration.Blocking != nil && *request.Configuration.Blocking {
		if err := m.waitTerminal(ctx, trusted, handle.RequestID); err != nil {
			return nil, err
		}
	}
	task, err := m.task(ctx, trusted, handle.RequestID)
	if err != nil {
		return nil, err
	}
	return &protocol.MessageResult{Result: task}, nil
}

func (m *DurableA2ATaskManager) OnSendMessageStream(ctx context.Context, request protocol.SendMessageParams) (<-chan protocol.StreamingMessageEvent, error) {
	handle, trusted, err := m.submit(ctx, request)
	if err != nil {
		return nil, err
	}
	return m.stream(ctx, trusted, handle.RequestID)
}

func (m *DurableA2ATaskManager) OnGetTask(ctx context.Context, params protocol.TaskQueryParams) (*protocol.Task, error) {
	trusted, err := m.trusted(ctx, false, true, false)
	if err != nil {
		return nil, err
	}
	return m.task(ctx, trusted, params.ID)
}

func (m *DurableA2ATaskManager) OnCancelTask(ctx context.Context, params protocol.TaskIDParams) (*protocol.Task, error) {
	trusted, err := m.trusted(ctx, false, false, true)
	if err != nil {
		return nil, err
	}
	current, err := m.Tasks.GetExecution(ctx, ExecutionKey{TenantID: trusted.Tenant.TenantID, RequestID: params.ID})
	if err != nil {
		return nil, a2aTaskError(params.ID, err)
	}
	if err := validateA2AExecution(current, trusted, params.ID); err != nil {
		return nil, err
	}
	state := a2aTaskState(current.Outcome)
	if current.Outcome.Terminal() {
		return nil, taskmanager.ErrTaskNotCancelable(params.ID, state)
	}
	if _, err := m.Tasks.RequestCancel(ctx, CancelRequest{TenantID: trusted.Tenant.TenantID, RequestID: params.ID,
		ExpectedVersion: current.Version, ActorID: trusted.PrincipalID, ReasonCode: "a2a_requested", TraceParent: trusted.TraceParent}); err != nil {
		return nil, a2aTaskError(params.ID, err)
	}
	return m.task(ctx, trusted, params.ID)
}

func (*DurableA2ATaskManager) OnPushNotificationSet(context.Context, protocol.TaskPushNotificationConfig) (*protocol.TaskPushNotificationConfig, error) {
	return nil, taskmanager.ErrPushNotificationNotSupported()
}

func (*DurableA2ATaskManager) OnPushNotificationGet(context.Context, protocol.TaskIDParams) (*protocol.TaskPushNotificationConfig, error) {
	return nil, taskmanager.ErrPushNotificationNotSupported()
}

func (m *DurableA2ATaskManager) OnResubscribe(ctx context.Context, params protocol.TaskIDParams) (<-chan protocol.StreamingMessageEvent, error) {
	trusted, err := m.trusted(ctx, false, true, false)
	if err != nil {
		return nil, err
	}
	return m.stream(ctx, trusted, params.ID)
}

func (m *DurableA2ATaskManager) submit(ctx context.Context, request protocol.SendMessageParams) (ExecutionHandle, ServerInvocationContext, error) {
	trusted, err := m.trusted(ctx, true, false, false)
	if err != nil {
		return ExecutionHandle{}, ServerInvocationContext{}, err
	}
	text, err := a2aInputText(request)
	if err != nil {
		return ExecutionHandle{}, ServerInvocationContext{}, err
	}
	handle, err := m.Submitter.Submit(ctx, RunSubmission{Tenant: trusted.Tenant, UserID: trusted.UserID,
		SessionID: trusted.SessionID, IdempotencyKey: trusted.IdempotencyKey, Protocol: "a2a", Text: text, TraceParent: trusted.TraceParent})
	return handle, trusted, err
}

func (m *DurableA2ATaskManager) trusted(ctx context.Context, run, read, cancel bool) (ServerInvocationContext, error) {
	if !m.valid() {
		return ServerInvocationContext{}, runtime.ErrCapabilityUnsupported
	}
	trusted, ok := ServerInvocationFromContext(ctx)
	if !ok || trusted.Protocol != "a2a" || trusted.Tenant.Validate() != nil || trusted.PrincipalID == "" ||
		trusted.Tenant.SubjectID != trusted.PrincipalID || trusted.UserID == "" || trusted.SessionID == "" || trusted.IdempotencyKey == "" {
		return ServerInvocationContext{}, ErrUnauthenticated
	}
	if (run && !trusted.CanRun) || (read && !trusted.CanRead) || (cancel && !trusted.CanCancel) {
		return ServerInvocationContext{}, ErrForbidden
	}
	if run && !m.Readiness.Ready() {
		return ServerInvocationContext{}, runtime.ErrBackendUnavailable
	}
	return trusted, nil
}

func a2aInputText(request protocol.SendMessageParams) (string, error) {
	if request.Message.Role != protocol.MessageRoleUser || request.Message.TaskID != nil || len(request.Message.ReferenceTaskIDs) != 0 || len(request.Message.Parts) == 0 {
		return "", runtime.ErrInvalidEnvelope
	}
	if request.Configuration != nil {
		if request.Configuration.PushNotificationConfig != nil {
			return "", taskmanager.ErrPushNotificationNotSupported()
		}
		for _, mode := range request.Configuration.AcceptedOutputModes {
			if mode != "text" && mode != "text/plain" {
				return "", taskmanager.ErrContentTypeNotSupported(mode)
			}
		}
	}
	var parts []string
	for _, part := range request.Message.Parts {
		textPart, ok := part.(*protocol.TextPart)
		if !ok {
			if value, valueOK := part.(protocol.TextPart); valueOK {
				textPart = &value
			} else {
				return "", taskmanager.ErrContentTypeNotSupported(part.GetKind())
			}
		}
		value := strings.TrimSpace(textPart.Text)
		if value == "" {
			return "", runtime.ErrInvalidEnvelope
		}
		parts = append(parts, value)
	}
	return strings.Join(parts, "\n"), nil
}

func (m *DurableA2ATaskManager) task(ctx context.Context, trusted ServerInvocationContext, requestID string) (*protocol.Task, error) {
	if requestID == "" {
		return nil, taskmanager.ErrTaskNotFound(requestID)
	}
	status, err := m.Tasks.GetExecution(ctx, ExecutionKey{TenantID: trusted.Tenant.TenantID, RequestID: requestID})
	if err != nil {
		return nil, a2aTaskError(requestID, err)
	}
	if err := validateA2AExecution(status, trusted, requestID); err != nil {
		return nil, err
	}
	task := &protocol.Task{ID: requestID, ContextID: status.Envelope.SessionID, Kind: protocol.KindTask,
		Status:   protocol.TaskStatus{State: a2aTaskState(status.Outcome), Timestamp: status.Envelope.CreatedAt.UTC().Format(time.RFC3339Nano)},
		Metadata: map[string]interface{}{"durableRequestId": requestID, "version": status.Version, "cancelRequested": status.CancelRequested}}
	if status.Outcome == runtime.OutcomeSucceeded {
		page, replayErr := m.Events.Replay(ctx, ExecutionKey{TenantID: trusted.Tenant.TenantID, RequestID: requestID}, 0, normalizeReplayLimit(m.ReplayLimit))
		if replayErr != nil {
			return nil, replayErr
		}
		for _, shared := range page.Events {
			if shared.Type == "message.completed" {
				content, decodeErr := a2aResultContent(shared.Data)
				if decodeErr != nil {
					return nil, decodeErr
				}
				task.Artifacts = append(task.Artifacts, a2aArtifact(requestID, content))
			}
		}
		if len(task.Artifacts) == 0 {
			return nil, runtime.ErrBackendUnavailable
		}
	}
	return task, nil
}

func (m *DurableA2ATaskManager) waitTerminal(ctx context.Context, trusted ServerInvocationContext, requestID string) error {
	interval := m.pollInterval()
	for {
		status, err := m.Tasks.GetExecution(ctx, ExecutionKey{TenantID: trusted.Tenant.TenantID, RequestID: requestID})
		if err != nil {
			return a2aTaskError(requestID, err)
		}
		if err := validateA2AExecution(status, trusted, requestID); err != nil {
			return err
		}
		if status.Outcome.Terminal() {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *DurableA2ATaskManager) stream(ctx context.Context, trusted ServerInvocationContext, requestID string) (<-chan protocol.StreamingMessageEvent, error) {
	initial, err := m.task(ctx, trusted, requestID)
	if err != nil {
		return nil, err
	}
	out := make(chan protocol.StreamingMessageEvent, 4)
	go m.replay(ctx, trusted, requestID, initial, out)
	return out, nil
}

func (m *DurableA2ATaskManager) replay(ctx context.Context, trusted ServerInvocationContext, requestID string, initial *protocol.Task, out chan<- protocol.StreamingMessageEvent) {
	defer close(out)
	if !sendA2AEvent(ctx, out, protocol.StreamingMessageEvent{Result: initial}) {
		return
	}
	after := uint64(0)
	for {
		page, err := m.Events.Replay(ctx, ExecutionKey{TenantID: trusted.Tenant.TenantID, RequestID: requestID}, after, normalizeReplayLimit(m.ReplayLimit))
		if err != nil {
			return
		}
		for _, shared := range page.Events {
			if shared.TenantID != trusted.Tenant.TenantID || shared.RequestID != requestID || shared.Sequence != after+1 {
				return
			}
			value, convertErr := m.a2aStreamEvent(ctx, trusted, requestID, shared)
			if convertErr != nil || !sendA2AEvent(ctx, out, value) {
				return
			}
			after = shared.Sequence
		}
		if page.Terminal {
			return
		}
		timer := time.NewTimer(m.pollInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (m *DurableA2ATaskManager) a2aStreamEvent(ctx context.Context, trusted ServerInvocationContext, requestID string, shared SharedEvent) (protocol.StreamingMessageEvent, error) {
	switch shared.Type {
	case "message.completed":
		content, err := a2aResultContent(shared.Data)
		if err != nil {
			return protocol.StreamingMessageEvent{}, err
		}
		task, err := m.task(ctx, trusted, requestID)
		if err != nil {
			return protocol.StreamingMessageEvent{}, err
		}
		artifact := a2aArtifact(requestID, content)
		update := protocol.NewTaskArtifactUpdateEvent(requestID, task.ContextID, artifact, true)
		return protocol.StreamingMessageEvent{Result: &update}, nil
	case "run.completed", "run.failed", "run.denied":
	default:
		return protocol.StreamingMessageEvent{}, runtime.ErrInvariantViolation
	}
	task, err := m.task(ctx, trusted, requestID)
	if err != nil {
		return protocol.StreamingMessageEvent{}, err
	}
	update := protocol.NewTaskStatusUpdateEvent(requestID, task.ContextID, task.Status, task.Status.State == protocol.TaskStateCompleted || task.Status.State == protocol.TaskStateCanceled || task.Status.State == protocol.TaskStateFailed || task.Status.State == protocol.TaskStateRejected)
	return protocol.StreamingMessageEvent{Result: &update}, nil
}

func (m *DurableA2ATaskManager) pollInterval() time.Duration {
	if m.PollInterval <= 0 {
		return time.Second
	}
	return m.PollInterval
}

func sendA2AEvent(ctx context.Context, out chan<- protocol.StreamingMessageEvent, value protocol.StreamingMessageEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- value:
		return true
	}
}

func a2aTaskState(outcome runtime.Outcome) protocol.TaskState {
	switch outcome {
	case runtime.OutcomePending, runtime.OutcomeQueued:
		return protocol.TaskStateSubmitted
	case runtime.OutcomeRunning:
		return protocol.TaskStateWorking
	case runtime.OutcomeBlocked, runtime.OutcomeWaitingConfirmation:
		return protocol.TaskStateInputRequired
	case runtime.OutcomeSucceeded:
		return protocol.TaskStateCompleted
	case runtime.OutcomeCancelled:
		return protocol.TaskStateCanceled
	case runtime.OutcomeDenied, runtime.OutcomeConfirmationDenied:
		return protocol.TaskStateRejected
	case runtime.OutcomeFailed, runtime.OutcomeConfirmationTimeout:
		return protocol.TaskStateFailed
	default:
		return protocol.TaskStateUnknown
	}
}

func a2aArtifact(requestID, content string) protocol.Artifact {
	return protocol.Artifact{ArtifactID: "result-" + requestID, Parts: []protocol.Part{&protocol.TextPart{Kind: protocol.KindText, Text: content}},
		Metadata: map[string]interface{}{"durableRequestId": requestID}}
}

func a2aResultContent(data json.RawMessage) (string, error) {
	var value struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(data, &value); err != nil || strings.TrimSpace(value.Content) == "" {
		return "", runtime.ErrInvariantViolation
	}
	return value.Content, nil
}

func a2aTaskError(taskID string, err error) error {
	if errors.Is(err, runtime.ErrNotFound) {
		return taskmanager.ErrTaskNotFound(taskID)
	}
	return err
}

func validateA2AExecution(status ExecutionStatus, trusted ServerInvocationContext, requestID string) error {
	envelope := status.Envelope
	if envelope.TenantID != trusted.Tenant.TenantID || envelope.RequestID != requestID ||
		envelope.AgentAppID != trusted.Tenant.AgentAppID || envelope.UserID != trusted.UserID || envelope.SessionID != trusted.SessionID {
		return ErrForbidden
	}
	if envelope.CreatedAt.IsZero() {
		return runtime.ErrInvariantViolation
	}
	return nil
}

var _ taskmanager.TaskManager = (*DurableA2ATaskManager)(nil)
