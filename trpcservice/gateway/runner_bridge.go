package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

type invocationContextKey struct{}

// ServerInvocationContext is injected only after protocol authentication and
// route authorization. Bridge.Run fails closed when it is absent.
type ServerInvocationContext struct {
	Tenant           tenant.Context
	PrincipalID      string
	UserID           string
	SessionID        string
	Protocol         string
	IdempotencyKey   string
	TraceParent      string
	CanRead          bool
	CanCancel        bool
	CanRun           bool
	RedactionProgram *redaction.Program
}

func WithServerInvocationContext(ctx context.Context, value ServerInvocationContext) context.Context {
	return context.WithValue(ctx, invocationContextKey{}, value)
}

func ServerInvocationFromContext(ctx context.Context) (ServerInvocationContext, bool) {
	value, ok := ctx.Value(invocationContextKey{}).(ServerInvocationContext)
	return value, ok
}

// GatewayRunnerBridge adapts upstream protocol façades to the shared durable
// execution path. It deliberately has no RuntimeBundle or local Runner field.
type GatewayRunnerBridge struct {
	Submitter    RunSubmitter
	Events       SharedEventStore
	PollInterval time.Duration
	ReplayLimit  int

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeCh   chan struct{}
}

func NewGatewayRunnerBridge(submitter RunSubmitter, events SharedEventStore) *GatewayRunnerBridge {
	return &GatewayRunnerBridge{Submitter: submitter, Events: events, closeCh: make(chan struct{})}
}

func (b *GatewayRunnerBridge) Run(ctx context.Context, userID, sessionID string, message model.Message,
	runOpts ...agent.RunOption) (<-chan *event.Event, error) {
	if b == nil || b.Events == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	b.mu.Lock()
	if b.closeCh == nil {
		b.closeCh = make(chan struct{})
	}
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return nil, runtime.ErrCapabilityUnsupported
	}
	trusted, ok := ServerInvocationFromContext(ctx)
	if !ok || trusted.Tenant.Validate() != nil || trusted.PrincipalID == "" || trusted.Tenant.SubjectID != trusted.PrincipalID || trusted.UserID == "" ||
		trusted.SessionID == "" || trusted.Protocol == "" || trusted.IdempotencyKey == "" || !trusted.CanRun {
		return nil, ErrUnauthenticated
	}
	if userID != trusted.UserID || sessionID != trusted.SessionID {
		return nil, ErrForbidden
	}
	if message.Role != model.RoleUser || strings.TrimSpace(message.Content) == "" ||
		len(message.ContentParts) != 0 || len(message.ToolCalls) != 0 || message.ToolID != "" {
		return nil, runtime.ErrInvalidEnvelope
	}
	opts := agent.NewRunOptions(runOpts...)
	if opts.AppName != "" && opts.AppName != trusted.Tenant.AgentAppID {
		return nil, ErrForbidden
	}
	handle, err := b.Submitter.Submit(ctx, RunSubmission{Tenant: trusted.Tenant, UserID: trusted.UserID,
		SessionID: trusted.SessionID, IdempotencyKey: trusted.IdempotencyKey, Protocol: trusted.Protocol,
		Text: message.Content, TraceParent: trusted.TraceParent})
	if err != nil {
		return nil, err
	}
	out := make(chan *event.Event, 4)
	go b.replay(ctx, trusted.Tenant.TenantID, handle.RequestID, out)
	return out, nil
}

func (b *GatewayRunnerBridge) replay(ctx context.Context, tenantID, requestID string, out chan<- *event.Event) {
	defer close(out)
	interval := b.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	limit := normalizeReplayLimit(b.ReplayLimit)
	key := ExecutionKey{TenantID: tenantID, RequestID: requestID}
	var after uint64
	for {
		page, err := b.Events.Replay(ctx, key, after, limit)
		if err != nil {
			b.emit(ctx, out, bridgeError(requestID, "shared_event_replay", err.Error()))
			return
		}
		if page.LastSequence < after {
			b.emit(ctx, out, bridgeError(requestID, "invalid_shared_event", runtime.ErrInvariantViolation.Error()))
			return
		}
		for _, shared := range page.Events {
			if shared.TenantID != tenantID || shared.RequestID != requestID || shared.Sequence != after+1 {
				b.emit(ctx, out, bridgeError(requestID, "invalid_shared_event", runtime.ErrInvariantViolation.Error()))
				return
			}
			converted, err := bridgeEvent(shared)
			if err != nil {
				b.emit(ctx, out, bridgeError(requestID, "invalid_shared_event", err.Error()))
				return
			}
			if !b.emit(ctx, out, converted) {
				return
			}
			after = shared.Sequence
		}
		if page.Terminal {
			if page.LastSequence < after {
				b.emit(ctx, out, bridgeError(requestID, "invalid_shared_event", runtime.ErrInvariantViolation.Error()))
			}
			return
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-b.closeCh:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (b *GatewayRunnerBridge) emit(ctx context.Context, out chan<- *event.Event, value *event.Event) bool {
	select {
	case out <- value:
		return true
	case <-ctx.Done():
		return false
	case <-b.closeCh:
		return false
	}
}

func bridgeEvent(shared SharedEvent) (*event.Event, error) {
	base := event.New(shared.RequestID, "gateway")
	base.ID = fmt.Sprintf("%s-%d", shared.RequestID, shared.Sequence)
	base.RequestID = shared.RequestID
	base.Timestamp = shared.CreatedAt
	switch shared.Type {
	case "message.completed":
		var value struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(shared.Data, &value); err != nil || value.Content == "" {
			return nil, runtime.ErrInvariantViolation
		}
		finish := "stop"
		message := model.Message{Role: model.RoleAssistant, Content: value.Content}
		base.Response = &model.Response{Object: model.ObjectTypeChatCompletion, Done: true,
			Created: shared.CreatedAt.Unix(), Choices: []model.Choice{{Message: message, Delta: message, FinishReason: &finish}}}
		return base, nil
	case "run.completed":
		base.Response = &model.Response{Object: model.ObjectTypeRunnerCompletion, Done: true, Created: shared.CreatedAt.Unix()}
		return base, nil
	case "run.failed", "run.denied":
		return bridgeError(shared.RequestID, shared.Type, string(shared.Data)), nil
	default:
		return nil, runtime.ErrInvariantViolation
	}
}

func bridgeError(requestID, errorType, message string) *event.Event {
	value := event.NewErrorEvent(requestID, "gateway", errorType, message)
	value.RequestID = requestID
	return value
}

func (b *GatewayRunnerBridge) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		if b.closeCh != nil {
			close(b.closeCh)
		}
	})
	return nil
}

var _ runner.Runner = (*GatewayRunnerBridge)(nil)
