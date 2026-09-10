package gateway

import (
	"context"
	"errors"
	"reflect"
	"slices"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type authenticatedRequestContextKey struct{}
type admittedRequestContextKey struct{}

type admittedRequest struct {
	identity AdmissionIdentity
	message  Message
	result   AdmissionResult
}

// AuthenticatedRequest is the request identity established by an ingress
// authentication boundary. Tenant must be revalidated by Admission before the
// request is accepted.
type AuthenticatedRequest struct {
	RequestID      string
	IdempotencyKey string
	Tenant         TenantResolver
}

// Validate checks the identity fields required before a protocol adapter can
// submit a request to the Gateway.
func (r AuthenticatedRequest) Validate() error {
	if r.RequestID == "" {
		return errors.New("request_id is required")
	}
	if r.IdempotencyKey == "" {
		return errors.New("idempotency_key is required")
	}
	if r.Tenant == nil {
		return errors.New("tenant resolver is required")
	}
	return nil
}

// ContextWithAuthenticatedRequest attaches identity established by a trusted
// protocol authentication boundary. It does not make the identity trusted by
// itself; Admission revalidates it against the authoritative backend.
func ContextWithAuthenticatedRequest(
	ctx context.Context,
	request AuthenticatedRequest,
) (context.Context, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, authenticatedRequestContextKey{}, request), nil
}

// AuthenticatedRequestFromContext returns the request identity attached by an
// ingress authentication boundary.
func AuthenticatedRequestFromContext(ctx context.Context) (AuthenticatedRequest, bool) {
	if ctx == nil {
		return AuthenticatedRequest{}, false
	}
	request, ok := ctx.Value(authenticatedRequestContextKey{}).(AuthenticatedRequest)
	return request, ok
}

// ExecutionEvent is one durable execution event and its monotonically
// increasing sequence number within an execution.
type ExecutionEvent struct {
	Sequence int64
	Event    *event.Event
}

// Validate checks the event values required for protocol streaming and resume.
func (e ExecutionEvent) Validate() error {
	if e.Sequence <= 0 {
		return errors.New("execution event sequence must be positive")
	}
	if e.Event == nil {
		return errors.New("execution event is required")
	}
	return nil
}

// ExecutionEventSource provides a tenant-scoped, durable event stream. Calls
// resume after the supplied sequence; zero streams an execution from its first
// persisted event.
type ExecutionEventSource interface {
	SubscribeExecutionEvents(
		ctx context.Context,
		scope tenant.Scope,
		requestID string,
		afterSequence int64,
	) (<-chan ExecutionEvent, error)
}

// QueuedRunner adapts protocol Runner calls into Gateway admission and reads
// results only from a durable execution event source. It never owns a real
// Agent runner.
type QueuedRunner struct {
	gateway *Gateway
	events  ExecutionEventSource
}

// NewQueuedRunner creates a Runner adapter that submits authenticated requests
// through Gateway and streams only persisted execution events.
func NewQueuedRunner(gateway *Gateway, events ExecutionEventSource) (*QueuedRunner, error) {
	if gateway == nil {
		return nil, errors.New("gateway is required")
	}
	if events == nil {
		return nil, errors.New("execution event source is required")
	}
	return &QueuedRunner{gateway: gateway, events: events}, nil
}

// Run submits one authenticated user message to Gateway and maps the durable
// event stream back to the framework Runner channel. userID, sessionID, and
// RunOption values cannot alter the authenticated request identity.
func (r *QueuedRunner) Run(
	ctx context.Context,
	userID string,
	sessionID string,
	message model.Message,
	runOpts ...agent.RunOption,
) (<-chan *event.Event, error) {
	return r.run(ctx, userID, sessionID, message, false, runOpts...)
}

// RunWithTerminalErrors retains a terminal execution error for a protocol
// adapter that can represent it safely. It is not a public event projection.
func (r *QueuedRunner) RunWithTerminalErrors(
	ctx context.Context,
	userID string,
	sessionID string,
	message model.Message,
	runOpts ...agent.RunOption,
) (<-chan *event.Event, error) {
	return r.run(ctx, userID, sessionID, message, true, runOpts...)
}

func (r *QueuedRunner) run(
	ctx context.Context,
	_ string,
	_ string,
	message model.Message,
	includeTerminalErrors bool,
	runOpts ...agent.RunOption,
) (<-chan *event.Event, error) {
	if r == nil || r.gateway == nil || r.events == nil {
		return nil, errors.New("queued runner is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, ok := AuthenticatedRequestFromContext(ctx)
	if !ok {
		return nil, errors.New("authenticated request is required")
	}
	if err := validateQueuedRunOptions(runOpts); err != nil {
		return nil, err
	}
	command, err := queuedGatewayMessage(message)
	if err != nil {
		return nil, err
	}
	admitted, ok := ctx.Value(admittedRequestContextKey{}).(admittedRequest)
	if !ok || admitted.message.Text != command.Text ||
		!slices.Equal(admitted.message.ArtifactRefs, command.ArtifactRefs) {
		ctx, err = r.Admit(ctx, command)
		if err != nil {
			return nil, err
		}
		admitted, _ = ctx.Value(admittedRequestContextKey{}).(admittedRequest)
	}
	persisted, err := r.events.SubscribeExecutionEvents(
		ctx,
		admitted.identity.Tenant.Scope(),
		admitted.result.RequestID,
		0,
	)
	if err != nil {
		return nil, err
	}
	return forwardExecutionEvents(ctx, persisted, includeTerminalErrors), nil
}

// Admit performs authenticated atomic admission and stores its result in the
// returned context so a following Run does not submit the request twice.
func (r *QueuedRunner) Admit(ctx context.Context, message Message) (context.Context, error) {
	if r == nil || r.gateway == nil {
		return nil, errors.New("queued runner is not initialized")
	}
	request, ok := AuthenticatedRequestFromContext(ctx)
	if !ok {
		return nil, errors.New("authenticated request is required")
	}
	identityResolver, ok := request.Tenant.(AdmissionIdentityResolver)
	if !ok {
		return nil, ErrAdmissionIdentityRequired
	}
	identity, err := identityResolver.ResolveAdmissionIdentity(ctx)
	if err != nil {
		return nil, err
	}
	result, err := r.gateway.Handle(ctx, Request{
		RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey,
		Tenant: fixedTenantResolver{identity: identity}, Message: message,
	})
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, admittedRequestContextKey{}, admittedRequest{
		identity: identity, message: message, result: result,
	}), nil
}

// Close releases no resources. QueuedRunner never owns the Gateway or event
// source supplied by its caller.
func (*QueuedRunner) Close() error {
	return nil
}

func validateQueuedRunOptions(options []agent.RunOption) error {
	if len(options) == 0 {
		return nil
	}
	for _, option := range options {
		if option == nil {
			return errors.New("runner option is required")
		}
	}
	if !reflect.DeepEqual(agent.NewRunOptions(options...), agent.RunOptions{}) {
		return errors.New("runner options are not supported by queued runner")
	}
	return nil
}

func queuedGatewayMessage(message model.Message) (Message, error) {
	if message.Role != model.RoleUser {
		return Message{}, errors.New("queued runner message must have user role")
	}
	if message.Content == "" {
		return Message{}, errors.New("queued runner message content is required")
	}
	if len(message.ContentParts) != 0 || message.ToolID != "" ||
		message.ToolName != "" || len(message.ToolCalls) != 0 ||
		message.ReasoningContent != "" || message.ReasoningSignature != "" {
		return Message{}, errors.New("queued runner message contains unsupported fields")
	}
	return Message{Text: message.Content}, nil
}

func forwardExecutionEvents(
	ctx context.Context,
	persisted <-chan ExecutionEvent,
	includeTerminalErrors bool,
) <-chan *event.Event {
	forwarded := make(chan *event.Event)
	go func() {
		defer close(forwarded)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-persisted:
				if !ok {
					return
				}
				if item.Validate() != nil {
					return
				}
				if !clientVisibleExecutionEvent(item.Event) &&
					!(includeTerminalErrors && item.Event.IsTerminalError()) {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case forwarded <- item.Event:
				}
			}
		}
	}()
	return forwarded
}

func clientVisibleExecutionEvent(evt *event.Event) bool {
	if evt == nil || evt.IsTerminalError() || evt.IsRunnerCompletion() {
		return false
	}
	if evt.Response == nil || evt.Response.IsToolCallResponse() || evt.Response.IsToolResultResponse() {
		return false
	}
	return evt.Response.Object == model.ObjectTypeChatCompletion ||
		evt.Response.Object == model.ObjectTypeChatCompletionChunk
}

type fixedTenantResolver struct {
	identity AdmissionIdentity
}

func (r fixedTenantResolver) ResolveTenant(context.Context) (tenant.RuntimeContext, TenantSource, error) {
	return r.identity.Tenant, r.identity.Source, nil
}

func (r fixedTenantResolver) ResolveAdmissionIdentity(context.Context) (AdmissionIdentity, error) {
	return r.identity, nil
}
