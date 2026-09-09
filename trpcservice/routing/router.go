// Package routing resolves verified channel bindings into immutable execution
// tasks without constructing Agent Runners or storage backends.
package routing

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrUnknownBinding           = errors.New("unknown binding")
	ErrConfigurationUnavailable = errors.New("routing configuration unavailable")
)

type Router struct {
	repository     tenant.Repository
	identitySecret []byte
	digestV2       bool
}

func (r *Router) InboxID(ctx context.Context, channel, bindingID, platformMessageID string) (string, error) {
	if strings.TrimSpace(platformMessageID) == "" {
		return "", fmt.Errorf("platform message id is required")
	}
	binding, err := r.repository.ResolveBinding(ctx, channel, bindingID)
	if err != nil {
		if errors.Is(err, tenant.ErrBindingNotFound) {
			return "", ErrUnknownBinding
		}
		return "", fmt.Errorf("%w: binding lookup failed", ErrConfigurationUnavailable)
	}
	return (message.ExecutionTask{TenantID: binding.TenantID, ChannelBindingID: binding.ID, PlatformMessageID: platformMessageID}).InboxID(), nil
}

func New(repository tenant.Repository, identitySecret []byte) (*Router, error) {
	return NewWithDigestV2(repository, identitySecret, false)
}

func NewWithDigestV2(repository tenant.Repository, identitySecret []byte, enabled bool) (*Router, error) {
	if repository == nil {
		return nil, errors.New("tenant repository is required")
	}
	if len(identitySecret) == 0 {
		return nil, errors.New("identity secret is required")
	}
	return &Router{repository: repository, identitySecret: append([]byte(nil), identitySecret...), digestV2: enabled}, nil
}

func (r *Router) TraceDigestV2Enabled() bool {
	return r != nil && r.digestV2
}

func (r *Router) Resolve(ctx context.Context, inbound message.InboundMessage) (message.ExecutionTask, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := telemetry.Start(ctx, "gateway.resolve")
	defer span.End()
	channel := inbound.Channel
	if channel == "" {
		channel = "demo"
	}
	receivedAt := inbound.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	requestID := inbound.RequestID
	if requestID == "" {
		var err error
		requestID, err = identity.RequestID()
		if err != nil {
			return message.ExecutionTask{}, fmt.Errorf("create request id: %w", err)
		}
	}
	traceID := inbound.TraceID
	if traceID == "" {
		var err error
		traceID, err = identity.TraceID()
		if err != nil {
			return message.ExecutionTask{}, fmt.Errorf("create trace id: %w", err)
		}
	}
	binding, err := r.repository.ResolveBinding(ctx, channel, inbound.BindingID)
	if err != nil {
		if errors.Is(err, tenant.ErrBindingNotFound) {
			return message.ExecutionTask{}, ErrUnknownBinding
		}
		return message.ExecutionTask{}, fmt.Errorf("%w: binding lookup failed", ErrConfigurationUnavailable)
	}
	if _, err := r.repository.GetTenant(ctx, binding.TenantID); err != nil {
		return message.ExecutionTask{}, fmt.Errorf("%w: tenant lookup failed", ErrConfigurationUnavailable)
	}
	app, err := r.repository.GetAgentApp(ctx, binding.TenantID, binding.AgentAppID)
	if err != nil {
		return message.ExecutionTask{}, fmt.Errorf("%w: agent app lookup failed", ErrConfigurationUnavailable)
	}
	if _, err := r.repository.GetConfigVersion(ctx, binding.TenantID, binding.AgentAppID, app.ActiveConfigVersion); err != nil {
		return message.ExecutionTask{}, fmt.Errorf("%w: active config lookup failed", ErrConfigurationUnavailable)
	}
	platformMessageID := inbound.PlatformMessageID
	if platformMessageID == "" {
		platformMessageID = inbound.MessageID
	}
	actorUserID := inbound.ActorUserID
	if actorUserID == "" {
		actorUserID = inbound.ExternalUserID
	}
	conversationType := inbound.ConversationType
	if conversationType == "" {
		conversationType = message.ConversationDirect
	}
	if !conversationType.Valid() || platformMessageID == "" || actorUserID == "" || inbound.ConversationID == "" {
		return message.ExecutionTask{}, fmt.Errorf("%w: inbound conversation metadata is invalid", ErrConfigurationUnavailable)
	}
	runnerUserID := identity.RunnerUserID(r.identitySecret, binding.ID, actorUserID)
	sessionID := identity.SessionID(r.identitySecret, binding.ID, inbound.ConversationID)
	if conversationType == message.ConversationGroup {
		runnerUserID = identity.GroupRunnerUserID(r.identitySecret, binding.ID, inbound.ConversationID)
		sessionID = identity.GroupSessionID(r.identitySecret, binding.ID, inbound.ConversationID)
	}
	taskID, err := identity.TaskID()
	if err != nil {
		return message.ExecutionTask{}, fmt.Errorf("create task id: %w", err)
	}
	task := message.ExecutionTask{
		SchemaVersion:     message.TaskSchemaVersion,
		TaskID:            taskID,
		Channel:           channel,
		ChannelBindingID:  binding.ID,
		ExternalAccountID: binding.ExternalAccountID,
		TenantID:          binding.TenantID,
		AgentAppID:        binding.AgentAppID,
		ConfigVersion:     app.ActiveConfigVersion,
		RunnerUserID:      runnerUserID,
		SessionID:         sessionID,
		PlatformMessageID: platformMessageID,
		ActorUserID:       actorUserID,
		ConversationID:    inbound.ConversationID,
		ConversationType:  conversationType,
		ReplyToMessageID:  inbound.ReplyToMessageID,
		PlatformRequestID: inbound.PlatformRequestID,
		Text:              inbound.Text,
		RequestID:         requestID,
		TraceID:           traceID,
		ReceivedAt:        receivedAt,
		Attempt:           1,
	}
	if r.digestV2 {
		task.TraceParent = inbound.TraceParent
		task.DigestVersion = 2
		if task.TraceParent == "" {
			spanID, spanErr := identity.RequestID()
			if spanErr != nil {
				return message.ExecutionTask{}, fmt.Errorf("create trace parent: %w", spanErr)
			}
			task.TraceParent = "00-" + traceID + "-" + spanID + "-01"
		}
	}
	task.PayloadDigest = task.CanonicalDigest()
	if err := task.Validate(); err != nil {
		return message.ExecutionTask{}, fmt.Errorf("create execution task: %w", err)
	}
	return task, nil
}
