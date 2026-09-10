package gateway

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	serviceagent "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/execution"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"github.com/google/uuid"
)

var (
	// ErrExecution is the stable, redacted execution failure exposed by
	// Dispatch. Provider messages and stack traces never cross this boundary.
	ErrExecution = errors.New("execution failed")
	// ErrExecutionCanceled is the stable cancellation result for a Dispatch
	// stream after its Runner events have been drained.
	ErrExecutionCanceled = errors.New("execution canceled")
	// ErrAuditWriteFailed is the stable redacted failure when a mandatory audit
	// lifecycle fact cannot be durably written.
	ErrAuditWriteFailed = errors.New("audit_write_failed")
)

const (
	defaultDispatchDrainTimeout      = 250 * time.Millisecond
	durableInboundLeaseGrace         = 30 * time.Second
	durableFailureFallbackReply      = "An error occurred during execution. Please contact the service provider."
	maxDurableExternalMessageIDRunes = 512
)

// DispatchEventType identifies the protocol-neutral event surface consumed by
// JSON and SSE adapters.
type DispatchEventType string

const (
	// DispatchEventMessage identifies an inbound message dispatch event.
	DispatchEventMessage DispatchEventType = "message"
	// DispatchEventStatus identifies a dispatch status event.
	DispatchEventStatus DispatchEventType = "status"
	// DispatchEventError identifies a dispatch failure event.
	DispatchEventError DispatchEventType = "error"
	// DispatchEventDone identifies a completed dispatch event.
	DispatchEventDone DispatchEventType = "done"
)

// DispatchRequest is the trusted input to the protocol-neutral execution
// boundary. Principal fields are never reconstructed from Message.
type DispatchRequest struct {
	Principal Principal
	Message   InboundMessage
	RequestID string
	TraceID   string
	// Accepted is notified after the durable channel claim and handoff reserve
	// succeed, before runner execution begins. It is optional for API callers.
	Accepted chan<- struct{}
}

// DispatchEvent is a redacted event safe for a protocol adapter. It contains
// no Plan, repository object, Secret, provider response, or raw error.
type DispatchEvent struct {
	Type      DispatchEventType `json:"type"`
	RequestID string            `json:"request_id"`
	TraceID   string            `json:"trace_id,omitempty"`
	Text      string            `json:"text,omitempty"`
	Status    string            `json:"status,omitempty"`
	Error     string            `json:"error,omitempty"`
	Done      bool              `json:"done"`
}

// DispatchService is the protocol-neutral contract implemented by Dispatcher.
type DispatchService interface {
	Dispatch(context.Context, DispatchRequest) (<-chan DispatchEvent, error)
}

// DispatchConfig configures the Resolver and Runtime Execution boundary.
type DispatchConfig struct {
	Resolver      *PlanResolver
	Registry      *runtimerunner.RunnerRegistry
	DrainTimeout  time.Duration
	Observability observability.Provider
	// SessionStore is the session-state capability used by durable dispatch.
	SessionStore sessionstorage.SessionStateStore
	// MessageStore is the inbound message lifecycle capability used by durable
	// dispatch.
	MessageStore runtimestorage.MessageStore
	// ReplyBatchStore is the atomic reply materialization capability used when a
	// materializer is constructed by the dispatcher.
	ReplyBatchStore runtimestorage.ReplyBatchEnqueuer
	Materializer    *outbox.Materializer
	// AuditWriter receives mandatory execution lifecycle facts. It is optional
	// for compatibility with deployments that have not enabled audit storage.
	AuditWriter audit.Writer
	// HandoffStore durably reserves and finalizes execution audit facts.
	HandoffStore audit.HandoffStore
	// Attachments loads verified tenant-owned media only when an inbound message
	// contains attachment references. Text-only dispatches remain independent of it.
	Attachments     attachment.Reader
	AttachmentStore runtimestorage.AttachmentStore
	// Budget reserves tenant monthly capacity before execution and settles
	// provider-reported token/cost usage after the event stream closes.
	Budget *runtimebudget.Controller
}

// Dispatcher resolves a fixed plan, prepares the Gateway execution context,
// and adapts runtime events into a bounded, redacted protocol stream.
type Dispatcher struct {
	resolver        *PlanResolver
	executor        *execution.Coordinator
	telemetry       observability.Provider
	metrics         metrics.Catalog
	runtimeStore    dispatchStore
	materializer    *outbox.Materializer
	auditWriter     audit.Writer
	handoffStore    audit.HandoffStore
	attachments     attachment.Reader
	attachmentStore runtimestorage.AttachmentStore
	budget          *runtimebudget.Controller
}

type dispatchStore interface {
	sessionstorage.SessionStateStore
	runtimestorage.MessageStore
}

type dispatchStoreView struct {
	sessions sessionstorage.SessionStateStore
	messages runtimestorage.MessageStore
}

func (store dispatchStoreView) GetSession(ctx context.Context, tenantID, sessionID string) (sessionstorage.Session, error) {
	if store.sessions == nil {
		return sessionstorage.Session{}, runtimestorage.ErrInvalid
	}
	return store.sessions.GetSession(ctx, tenantID, sessionID)
}

func (store dispatchStoreView) CreateSession(ctx context.Context, tenantID, sessionID string, state map[string]any) (sessionstorage.Session, error) {
	if store.sessions == nil {
		return sessionstorage.Session{}, runtimestorage.ErrInvalid
	}
	return store.sessions.CreateSession(ctx, tenantID, sessionID, state)
}

func (store dispatchStoreView) UpdateSessionState(ctx context.Context, tenantID, sessionID string, expectedVersion int64, state map[string]any) (sessionstorage.Session, error) {
	if store.sessions == nil {
		return sessionstorage.Session{}, runtimestorage.ErrInvalid
	}
	return store.sessions.UpdateSessionState(ctx, tenantID, sessionID, expectedVersion, state)
}

func (store dispatchStoreView) DeleteSession(ctx context.Context, tenantID, sessionID string) error {
	if store.sessions == nil {
		return runtimestorage.ErrInvalid
	}
	return store.sessions.DeleteSession(ctx, tenantID, sessionID)
}

func (store dispatchStoreView) RecordMessage(ctx context.Context, input runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	if store.messages == nil {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrInvalid
	}
	return store.messages.RecordMessage(ctx, input)
}

func (store dispatchStoreView) GetMessage(ctx context.Context, tenantID, eventID string) (runtimestorage.MessageEvent, error) {
	if store.messages == nil {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
	}
	return store.messages.GetMessage(ctx, tenantID, eventID)
}

func (store dispatchStoreView) TransitionMessage(ctx context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	if store.messages == nil {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
	}
	return store.messages.TransitionMessage(ctx, transition)
}

type durableExecution struct {
	store        runtimestorage.MessageStore
	tenantID     string
	eventID      string
	owner        string
	fencingToken int64
	replyTarget  runtimestorage.ReplyTarget
}

// dispatchMetadata contains the trusted identity and correlation data shared
// by Gateway execution phases.
type dispatchMetadata struct {
	principal      Principal
	message        InboundMessage
	identity       tenant.RunnerIdentity
	requestID      string
	traceID        string
	modelProfileID string
	modelProvider  string
	modelName      string
}

// dispatchExecution owns Gateway-side state for one asynchronous execution.
// Gateway consumes the runtime event stream; the runtime Coordinator owns its
// closure.
type dispatchExecution struct {
	metadata          dispatchMetadata
	durable           *durableExecution
	mediaReplies      *servicetool.ReplyCollector
	span              observability.Span
	started           time.Time
	executionEvents   <-chan execution.Event
	output            chan<- DispatchEvent
	auditFinalized    bool
	budgetReservation runtimebudget.Reservation
	usageAccumulator  *usageAccumulator
	pricing           runtimebudget.Pricing
	auditUsage        *audit.Usage
}

type dispatchCapabilities struct {
	sessions     sessionstorage.SessionStateStore
	messages     runtimestorage.MessageStore
	replyBatches runtimestorage.ReplyBatchEnqueuer
}

func resolveDispatchCapabilities(config DispatchConfig) dispatchCapabilities {
	capabilities := dispatchCapabilities{
		sessions:     config.SessionStore,
		messages:     config.MessageStore,
		replyBatches: config.ReplyBatchStore,
	}
	return capabilities
}

func resolveDispatchAttachments(config DispatchConfig) (attachment.Reader, runtimestorage.AttachmentStore) {
	reader := config.Attachments
	return reader, config.AttachmentStore
}

func newDispatchStore(capabilities dispatchCapabilities) dispatchStore {
	if capabilities.sessions != nil && capabilities.messages != nil {
		return dispatchStoreView{sessions: capabilities.sessions, messages: capabilities.messages}
	}
	return nil
}

func newDispatchMaterializer(config DispatchConfig, batchStore runtimestorage.ReplyBatchEnqueuer) (*outbox.Materializer, error) {
	if config.Materializer != nil {
		return config.Materializer, nil
	}
	if batchStore == nil {
		return nil, nil
	}
	return outbox.NewMaterializer(outbox.MaterializerConfig{
		BatchStore: batchStore, Observability: config.Observability,
	})
}

func validateDispatchCapabilities(config DispatchConfig, capabilities dispatchCapabilities) error {
	if config.Materializer == nil && capabilities.sessions != nil && capabilities.messages != nil && capabilities.replyBatches == nil {
		return fmt.Errorf("%w: durable dispatch requires reply materialization capability", ErrInvalid)
	}
	return nil
}

// NewDispatcher validates the protocol-neutral execution dependencies.
func NewDispatcher(config DispatchConfig) (*Dispatcher, error) {
	if config.Resolver == nil || config.Registry == nil {
		return nil, fmt.Errorf("%w: dispatcher dependencies are required", ErrInvalid)
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = defaultDispatchDrainTimeout
	}
	if config.DrainTimeout < 0 {
		return nil, fmt.Errorf("%w: dispatch drain timeout cannot be negative", ErrInvalid)
	}
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	capabilities := resolveDispatchCapabilities(config)
	if err := validateDispatchCapabilities(config, capabilities); err != nil {
		return nil, err
	}
	executor, err := execution.NewCoordinator(execution.Config{
		Registry: config.Registry, DrainTimeout: config.DrainTimeout, Observability: config.Observability,
	})
	if err != nil {
		return nil, err
	}
	config.Attachments, config.AttachmentStore = resolveDispatchAttachments(config)
	config.AuditWriter = metrics.WrapAuditWriter(config.AuditWriter, config.Observability)
	materializer, err := newDispatchMaterializer(config, capabilities.replyBatches)
	if err != nil {
		return nil, err
	}
	return &Dispatcher{resolver: config.Resolver, executor: executor, telemetry: config.Observability, metrics: metrics.New(config.Observability), runtimeStore: newDispatchStore(capabilities), materializer: materializer, auditWriter: config.AuditWriter, handoffStore: config.HandoffStore, attachments: config.Attachments, attachmentStore: config.AttachmentStore, budget: config.Budget}, nil
}

// Ready reports whether both plan resolution and Runner acquisition are ready.
func (dispatcher *Dispatcher) Ready() bool {
	return dispatcher != nil && dispatcher.resolver != nil && dispatcher.resolver.Ready() && dispatcher.executor != nil && dispatcher.executor.Ready()
}

// Dispatch starts one execution and returns a redacted event stream. The
// returned stream owns the Runner lease until it reaches terminal state or the
// caller Context is canceled.
//
//nolint:gocyclo // Dispatch coordinates validation, durable state, audit, lease, and stream lifecycle.
func (dispatcher *Dispatcher) Dispatch(ctx context.Context, request DispatchRequest) (<-chan DispatchEvent, error) {
	if dispatcher == nil || dispatcher.resolver == nil || dispatcher.executor == nil {
		return nil, ErrNotReady
	}
	message, requestID, traceID, err := normalizeDispatchRequest(ctx, request)
	if err != nil {
		return nil, err
	}
	metadata := dispatchMetadata{principal: request.Principal, message: message, requestID: requestID, traceID: traceID}
	ctx, span := dispatcher.telemetry.Tracer("trpcservice.gateway").Start(observability.WithCorrelation(ctx, requestID, traceID), observability.OperationGatewayDispatch,
		observability.Attribute{Key: "component", Value: "gateway"}, observability.Attribute{Key: "operation", Value: observability.OperationGatewayDispatch})
	started := time.Now()
	_ = dispatcher.metrics.Request(ctx, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch, "status": "started"})
	finishWithError := func(cause error) {
		span.SetAttributes(observability.Attribute{Key: "error_class", Value: observability.ErrorClass(cause)})
		span.SetStatus(observability.StatusError, observability.ErrorClass(cause))
		span.RecordError(cause)
		span.End()
		_ = dispatcher.metrics.Operation(ctx, started, map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch}, cause)
		logDispatchFailure(metadata.principal, metadata.requestID, metadata.traceID, cause)
	}

	plan, err := dispatcher.resolver.Resolve(ctx, request.Principal)
	if err != nil {
		finishWithError(err)
		return nil, err
	}
	identity, err := dispatchRunnerIdentity(request.Principal, message)
	if err != nil {
		finishWithError(err)
		return nil, err
	}
	metadata.identity = identity
	planSnapshot := plan.AgentSnapshot()
	planApp := planSnapshot.App()
	modelProfile := plan.ModelSnapshot().Profile()
	metadata.modelProfileID = modelProfile.ProfileID
	metadata.modelProvider = modelProfile.Configuration.Provider
	metadata.modelName = modelProfile.Configuration.Model
	durable, err := dispatcher.claimInboundWithLease(ctx, metadata, durableInboundLeaseForRuntime(planSnapshot.Revision().Runtime))
	if err != nil {
		finishWithError(err)
		return nil, err
	}
	attachmentEventID := ""
	if durable != nil {
		attachmentEventID = durable.eventID
	}
	if len(message.Attachments) > 0 {
		binder, ok := dispatcher.attachments.(attachment.Binder)
		if !ok || attachmentEventID == "" {
			dispatcher.failDurable(durable, ErrExecution)
			finishWithError(ErrExecution)
			return nil, ErrExecution
		}
		if err := binder.BindAttachments(ctx, request.Principal.TenantID(), attachmentEventID, message.Attachments); err != nil {
			dispatcher.failDurable(durable, err)
			finishWithError(err)
			return nil, ErrExecution
		}
	}
	userMessage, err := buildUserMessage(ctx, dispatcher.attachments, request.Principal.TenantID(), attachmentEventID, message)
	if err != nil {
		dispatcher.failDurable(durable, err)
		finishWithError(err)
		if IsContextCancellation(err) {
			return nil, err
		}
		return nil, ErrExecution
	}
	if planApp.CanaryRevision != nil && planSnapshot.Revision().Revision == *planApp.CanaryRevision {
		selectedRevision := planSnapshot.Revision().Revision
		if err := dispatcher.writeExecutionAuditRevision(ctx, metadata, audit.EventCanarySelected, "", &selectedRevision); err != nil {
			dispatcher.failDurable(durable, err)
			finishWithError(err)
			return nil, auditWriteFailure()
		}
	}
	budgetReservation, usageAccumulator, pricing, err := dispatcher.reserveBudget(ctx, plan, requestID)
	if err != nil {
		dispatcher.failDurable(durable, err)
		if auditErr := budgetAdmissionAudit(context.Background(), dispatcher.auditWriter, metadata.principal.TenantID(), metadata.requestID, metadata.traceID); auditErr != nil {
			finishWithError(auditWriteFailure())
			return nil, auditWriteFailure()
		}
		finishWithError(err)
		return nil, err
	}
	releaseBudget := func() {
		_ = dispatcher.releaseBudget(context.Background(), budgetReservation)
	}
	if err := dispatcher.writeExecutionAudit(ctx, metadata, audit.EventExecutionStarted, ""); err != nil {
		releaseBudget()
		dispatcher.failDurable(durable, err)
		finishWithError(err)
		return nil, auditWriteFailure()
	}
	if err := dispatcher.reserveHandoff(ctx, metadata); err != nil {
		releaseBudget()
		finishWithError(err)
		return nil, auditWriteFailure()
	}
	if request.Accepted != nil {
		select {
		case request.Accepted <- struct{}{}:
		default:
		}
	}
	runnerCtx := ctx
	mediaReplies := servicetool.NewReplyCollector()
	if durable != nil {
		runnerCtx = servicetool.WithExecutionContext(runnerCtx, servicetool.ExecutionContext{
			TenantID: request.Principal.TenantID(), EventID: durable.eventID, RequestID: requestID, TraceID: traceID,
			Attachments: dispatcher.attachmentStore, Replies: mediaReplies,
			Audit: audit.NewRecorder(dispatcher.auditWriter, request.Principal.TenantID()),
		})
	}
	if usageAccumulator != nil {
		runnerCtx = serviceagent.WithUsageObserver(runnerCtx, usageAccumulator.Observe)
	}
	runnerEvents, err := dispatcher.executor.Execute(runnerCtx, execution.Request{
		Plan: plan, Identity: identity, Message: userMessage, RequestID: requestID, TraceID: traceID,
	})
	if err != nil {
		releaseBudget()
		executionErr := err
		if errors.Is(executionErr, execution.ErrExecution) {
			executionErr = ErrExecution
		}
		dispatcher.failDurable(durable, executionErr)
		eventType, errorType := audit.EventExecutionFailed, string(audit.ErrorUnavailable)
		if IsContextCancellation(err) {
			eventType, errorType = audit.EventExecutionCanceled, string(audit.ErrorCanceled)
		}
		if auditErr := dispatcher.writeExecutionAudit(context.Background(), metadata, eventType, errorType); auditErr != nil {
			dispatcher.failDurable(durable, auditErr)
			finishWithError(auditErr)
			return nil, auditWriteFailure()
		}
		if IsContextCancellation(err) {
			finishWithError(err)
			return nil, err
		}
		finishWithError(executionErr)
		return nil, executionErr
	}

	output := make(chan DispatchEvent, 32)
	run := &dispatchExecution{
		metadata: metadata, durable: durable, mediaReplies: mediaReplies, span: span, started: started,
		executionEvents: runnerEvents, output: output, budgetReservation: budgetReservation,
		usageAccumulator: usageAccumulator, pricing: pricing,
	}
	go dispatcher.forwardExecution(runnerCtx, run)
	return output, nil
}

func normalizeCorrelationID(value string, generate bool) (string, error) {
	if value == "" && generate {
		return uuid.NewString(), nil
	}
	if value == "" {
		return "", nil
	}
	if strings.TrimSpace(value) == "" || hasControl(value) || len([]rune(value)) > maxPrincipalIDRunes {
		return "", fmt.Errorf("%w: correlation ID is invalid", ErrInvalid)
	}
	return value, nil
}

func dispatchRunnerIdentity(principal Principal, message InboundMessage) (tenant.RunnerIdentity, error) {
	switch principal.Kind() {
	case PrincipalChannel:
		target, ok := principal.RoutingTarget()
		if !ok {
			return tenant.RunnerIdentity{}, ErrUnauthenticated
		}
		return target.RunnerIdentity(channels.IdentityInput{
			ExternalUserID: message.ExternalUserID, Kind: message.ConversationKind,
			ExternalPeerID: message.ExternalPeerID, ExternalChatID: message.ExternalChatID,
			ExternalThreadID: message.ExternalThreadID,
		})
	case PrincipalAPI:
		conversation := message.ExternalPeerID
		if message.ConversationKind == channels.ConversationGroup {
			conversation = message.ExternalChatID
		}
		sessionID := encodeDispatchIdentity("api", principal.AppID(), string(message.ConversationKind), conversation, message.ExternalThreadID)
		userID := encodeDispatchIdentity("api", principal.AppID(), principal.SubjectID())
		return tenant.NewRunnerIdentity(principal.TenantID(), userID, sessionID)
	default:
		return tenant.RunnerIdentity{}, ErrUnauthenticated
	}
}

func encodeDispatchIdentity(parts ...string) string {
	var builder strings.Builder
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len([]byte(part))))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}
