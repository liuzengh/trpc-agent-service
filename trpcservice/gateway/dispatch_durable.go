package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"github.com/google/uuid"
)

func (dispatcher *Dispatcher) reserveHandoff(ctx context.Context, metadata dispatchMetadata) error {
	if dispatcher.handoffStore == nil {
		return nil
	}
	_, err := dispatcher.handoffStore.Reserve(ctx, audit.ExecutionHandoff{
		TenantID: metadata.principal.TenantID(), HandoffID: audit.NewEventID(metadata.requestID, "handoff"),
		RequestID: metadata.requestID, TraceID: metadata.traceID, EventID: audit.NewEventID(metadata.requestID, string(audit.EventExecutionStarted)), State: audit.HandoffPending,
	})
	return err
}

func normalizeDispatchRequest(ctx context.Context, request DispatchRequest) (InboundMessage, string, string, error) {
	if ctx == nil {
		return InboundMessage{}, "", "", fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return InboundMessage{}, "", "", err
	}
	if err := request.Principal.Validate(); err != nil {
		return InboundMessage{}, "", "", ErrUnauthenticated
	}
	message, err := request.Message.Normalize()
	if err != nil {
		return InboundMessage{}, "", "", err
	}
	requestID, err := normalizeCorrelationID(request.RequestID, true)
	if err != nil {
		return InboundMessage{}, "", "", err
	}
	traceID, err := normalizeCorrelationID(request.TraceID, false)
	if err != nil {
		return InboundMessage{}, "", "", err
	}
	return message, requestID, traceID, nil
}

func detachedCorrelationContext(parent context.Context, requestID, traceID string) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return observability.WithCorrelation(context.WithoutCancel(parent), requestID, traceID)
}

func (dispatcher *Dispatcher) claimInbound(ctx context.Context, metadata dispatchMetadata) (result *durableExecution, err error) {
	return dispatcher.claimInboundWithLease(ctx, metadata, durableInboundLeaseForRuntime(appmodel.DefaultRuntimePolicy()))
}

func (dispatcher *Dispatcher) claimInboundWithLease(ctx context.Context, metadata dispatchMetadata, leaseDuration time.Duration) (result *durableExecution, err error) {
	if dispatcher.runtimeStore == nil || metadata.principal.Kind() != PrincipalChannel {
		return nil, nil
	}
	if leaseDuration <= 0 {
		leaseDuration = durableInboundLeaseForRuntime(appmodel.DefaultRuntimePolicy())
	}
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, dispatcher.telemetry, observability.OperationStorageOperation, "storage")
	_ = dispatcher.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "status": "started"})
	defer func() {
		finish(err)
		_ = dispatcher.metrics.Operation(operationCtx, started, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation}, err)
		status := "success"
		if err != nil {
			status = observability.ErrorClass(err)
			if status == "" {
				status = "error"
			}
		}
		_ = dispatcher.metrics.BackendDuration(operationCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "status": status, "error_class": observability.ErrorClass(err)})
	}()
	ctx = operationCtx
	target, ok := metadata.principal.RoutingTarget()
	if !ok || metadata.message.ExternalMessageID == "" || len([]rune(metadata.message.ExternalMessageID)) > maxDurableExternalMessageIDRunes {
		return nil, fmt.Errorf("%w: durable Channel messages require an external message ID", ErrInvalid)
	}
	store := dispatcher.runtimeStore
	replyTarget, err := replyTarget(target, metadata.message)
	if err != nil {
		return nil, err
	}
	if err := ensureInboundSession(ctx, store, metadata.principal.TenantID(), metadata.identity.SessionID); err != nil {
		return nil, err
	}
	event, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{
		TenantID: metadata.principal.TenantID(), EventID: uuid.NewString(), SessionID: metadata.identity.SessionID,
		BindingID: target.BindingID, ExternalMessageID: metadata.message.ExternalMessageID,
		IdempotencyKey: metadata.message.ExternalMessageID,
		ReplyTarget:    replyTarget,
	})
	if err != nil {
		return nil, err
	}
	owner := "gateway-" + uuid.NewString()
	event, err = prepareInboundEvent(ctx, store, inboundEventPreparation{tenantID: metadata.principal.TenantID(), event: event, duplicate: duplicate, owner: owner})
	if err != nil {
		return nil, err
	}
	from := event.Status
	running, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{
		TenantID: metadata.principal.TenantID(), EventID: event.EventID, From: from,
		To: runtimestorage.EventRunning, Owner: owner, LeaseDuration: leaseDuration,
	})
	if err != nil {
		if duplicate && errors.Is(err, runtimestorage.ErrConflict) {
			return nil, ErrDuplicateMessage
		}
		return nil, err
	}
	return &durableExecution{store: store, tenantID: metadata.principal.TenantID(), eventID: event.EventID, owner: owner, fencingToken: running.FencingToken, replyTarget: event.ReplyTarget}, nil
}

func durableInboundLeaseForRuntime(policy appmodel.RuntimePolicy) time.Duration {
	seconds := policy.ExecutionTimeoutSeconds
	if seconds <= 0 {
		seconds = appmodel.DefaultRuntimePolicy().ExecutionTimeoutSeconds
	}
	return time.Duration(seconds)*time.Second + durableInboundLeaseGrace
}

func replyTarget(target channels.RoutingTarget, message InboundMessage) (runtimestorage.ReplyTarget, error) {
	reply := runtimestorage.ReplyTarget{BindingID: target.BindingID, ConversationKind: string(message.ConversationKind), ThreadID: message.ExternalThreadID}
	switch message.ConversationKind {
	case channels.ConversationDirect:
		reply.ReceiverID = message.ExternalPeerID
	case channels.ConversationGroup:
		reply.ReceiverID = message.ExternalChatID
	default:
		return runtimestorage.ReplyTarget{}, fmt.Errorf("%w: reply conversation kind is invalid", ErrInvalid)
	}
	if err := runtimestorage.ValidateReplyTarget(reply); err != nil {
		return runtimestorage.ReplyTarget{}, fmt.Errorf("%w: reply target is invalid", ErrInvalid)
	}
	return reply, nil
}

func ensureInboundSession(ctx context.Context, store sessionstorage.SessionStateStore, tenantID, sessionID string) error {
	if _, err := store.GetSession(ctx, tenantID, sessionID); err == nil {
		return nil
	} else if !errors.Is(err, runtimestorage.ErrNotFound) {
		return err
	}
	_, err := store.CreateSession(ctx, tenantID, sessionID, nil)
	if errors.Is(err, runtimestorage.ErrDuplicate) {
		return nil
	}
	return err
}

type inboundEventPreparation struct {
	tenantID  string
	event     runtimestorage.MessageEvent
	duplicate bool
	owner     string
}

func prepareInboundEvent(ctx context.Context, store runtimestorage.MessageStore, preparation inboundEventPreparation) (runtimestorage.MessageEvent, error) {
	if !preparation.duplicate {
		return preparation.event, nil
	}
	event := preparation.event
	if event.Status == runtimestorage.EventRunning && (event.LeaseExpiresAt == nil || event.LeaseExpiresAt.After(time.Now().UTC())) {
		return runtimestorage.MessageEvent{}, ErrDuplicateMessage
	}
	if event.Status == runtimestorage.EventRunning {
		if _, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: preparation.tenantID, EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: preparation.owner}); err != nil {
			return runtimestorage.MessageEvent{}, ErrDuplicateMessage
		}
		event.Status = runtimestorage.EventExecutionReconciling
	}
	if event.Status != runtimestorage.EventReceived && event.Status != runtimestorage.EventExecutionReconciling {
		return runtimestorage.MessageEvent{}, ErrDuplicateMessage
	}
	return event, nil
}

func (dispatcher *Dispatcher) failDurable(durable *durableExecution, cause error) {
	if durable == nil {
		return
	}
	to := runtimestorage.EventFailed
	_ = dispatcher.observeStorage(context.Background(), func(operationCtx context.Context) error {
		_, err := durable.store.TransitionMessage(operationCtx, runtimestorage.MessageTransition{
			TenantID: durable.tenantID, EventID: durable.eventID, From: runtimestorage.EventRunning,
			To: to, Owner: durable.owner, FencingToken: durable.fencingToken,
		})
		return err
	})
}

func (dispatcher *Dispatcher) finishDurable(ctx context.Context, metadata dispatchMetadata, durable *durableExecution, terminalErr error, reply string, mediaReplies []servicetool.ReplyIntent) {
	if durable == nil {
		return
	}
	durableCtx := detachedCorrelationContext(ctx, metadata.requestID, metadata.traceID)
	if terminalErr != nil && !IsContextCancellation(terminalErr) {
		reply = durableFailureFallbackReply
		mediaReplies = nil
	}
	segments := 0
	replyID := ""
	if dispatcher.materializer != nil {
		input := outbox.MaterializeInput{TenantID: durable.tenantID, EventID: durable.eventID, ReplyID: durable.eventID, RequestID: metadata.requestID, TraceID: metadata.traceID, TraceParent: observability.TraceParentFromContext(durableCtx), ReplyTarget: durable.replyTarget}
		if terminalErr == nil && len(mediaReplies) > 0 {
			input.Segments = mediaReplySegments(mediaReplies)
		} else {
			input.Payload = reply
		}
		if strings.TrimSpace(input.Payload) != "" || len(input.Segments) > 0 {
			var err error
			segments, err = dispatcher.materializer.Materialize(durableCtx, input)
			if err != nil {
				terminalErr = err
				if len(mediaReplies) > 0 {
					fallback := outbox.MaterializeInput{TenantID: durable.tenantID, EventID: durable.eventID, ReplyID: durable.eventID, RequestID: metadata.requestID, TraceID: metadata.traceID, TraceParent: observability.TraceParentFromContext(durableCtx), Payload: durableFailureFallbackReply, ReplyTarget: durable.replyTarget}
					segments, err = dispatcher.materializer.Materialize(durableCtx, fallback)
					if err == nil {
						replyID = durable.eventID
					}
				}
			} else {
				replyID = durable.eventID
			}
		}
	}
	to := runtimestorage.EventCompleted
	if terminalErr != nil && replyID == "" {
		to = runtimestorage.EventFailed
	}
	_ = dispatcher.observeStorage(durableCtx, func(operationCtx context.Context) error {
		_, err := durable.store.TransitionMessage(operationCtx, runtimestorage.MessageTransition{
			TenantID: durable.tenantID, EventID: durable.eventID, From: runtimestorage.EventRunning,
			To: to, Owner: durable.owner, FencingToken: durable.fencingToken, ReplyID: replyID, SegmentCount: segments,
		})
		return err
	})
}

func mediaReplySegments(intents []servicetool.ReplyIntent) []outbox.ReplySegment {
	segments := make([]outbox.ReplySegment, 0, len(intents))
	for _, intent := range intents {
		segments = append(segments, outbox.ReplySegment{Kind: intent.Kind, Payload: intent.Payload, Attachment: intent.Attachment, Fallback: intent.Fallback})
	}
	return segments
}

func (dispatcher *Dispatcher) observeStorage(ctx context.Context, operation func(context.Context) error) error {
	if dispatcher == nil || operation == nil {
		return ErrInvalid
	}
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, dispatcher.telemetry, observability.OperationStorageOperation, "storage")
	_ = dispatcher.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other", "status": "started"})
	err := operation(operationCtx)
	finish(err)
	labels := map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other"}
	_ = dispatcher.metrics.Operation(operationCtx, started, labels, err)
	status := "success"
	if err != nil {
		status = observability.ErrorClass(err)
		if status == "" {
			status = "error"
		}
	}
	_ = dispatcher.metrics.BackendDuration(operationCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "provider": "other", "status": status, "error_class": observability.ErrorClass(err)})
	return err
}
