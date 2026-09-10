package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const InboundMessageType = "agent.inbound.v1"

// InboundPayload preserves the tenant configuration version selected at the
// webhook boundary. Workers must use this immutable version rather than
// resolving the active configuration again.
type InboundPayload struct {
	BindingID         string                  `json:"binding_id"`
	AppCode           string                  `json:"app_code"`
	ConfigVersion     uint64                  `json:"config_version"`
	ReleaseVariant    tenant.ReleaseVariant   `json:"release_variant"`
	RolloutGeneration uint64                  `json:"rollout_generation,omitempty"`
	Inbound           channels.InboundMessage `json:"inbound"`
}

// NewInboundEnvelope creates the versioned Kafka contract for one verified,
// tenant-routed channel message. Call NewInboundEnvelopeWithContext from an
// ingress handler so its W3C parent reaches the Kafka worker.
//
// 注意：本便捷函数使用 context.Background()，不会携带上游 trace parent，
// 生成的信封 TraceParent 为空。生产入站路径请使用
// NewInboundEnvelopeWithContext 传入带 trace 的 ctx；本函数仅用于
// 无 ctx 可用的场景（如离线重放、单元测试）。
func NewInboundEnvelope(selection tenant.ReleaseSelection, bindingID, sessionKey string, inbound channels.InboundMessage, manifests *ExecutionManifestCodec) (Envelope, error) {
	return NewInboundEnvelopeWithContext(context.Background(), selection, bindingID, sessionKey, inbound, manifests)
}

// NewInboundEnvelopeWithContext copies the current trace context into the
// immutable broker contract. Only the W3C traceparent is carried; headers,
// body, principal, credentials, and request metadata never leave the ingress
// process through this field.
func NewInboundEnvelopeWithContext(ctx context.Context, selection tenant.ReleaseSelection, bindingID, sessionKey string, inbound channels.InboundMessage, manifests *ExecutionManifestCodec) (Envelope, error) {
	snapshot := selection.Snapshot
	if manifests == nil {
		return Envelope{}, fmt.Errorf("execution manifest codec is required")
	}
	if strings.TrimSpace(bindingID) == "" {
		return Envelope{}, fmt.Errorf("external binding ID is required")
	}
	if err := inbound.Validate(); err != nil {
		return Envelope{}, fmt.Errorf("validate inbound message: %w", err)
	}
	if strings.TrimSpace(snapshot.Config.TenantID) == "" || strings.TrimSpace(snapshot.Config.AppCode) == "" || snapshot.Config.ConfigVersion == 0 {
		return Envelope{}, fmt.Errorf("tenant configuration snapshot is invalid")
	}
	if strings.TrimSpace(sessionKey) == "" || !strings.HasPrefix(sessionKey, snapshot.Config.TenantID+"/"+snapshot.Config.AppCode+"/session/") {
		return Envelope{}, fmt.Errorf("inbound session key is not scoped to tenant application")
	}
	payload, err := json.Marshal(InboundPayload{
		BindingID: bindingID, AppCode: snapshot.Config.AppCode, ConfigVersion: snapshot.Config.ConfigVersion,
		ReleaseVariant: selection.Variant, RolloutGeneration: selection.RolloutGeneration, Inbound: inbound,
	})
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal inbound payload: %w", err)
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	envelope := Envelope{
		Version:     CurrentEnvelopeVersion,
		EventID:     inbound.MessageID,
		TenantID:    snapshot.Config.TenantID,
		SessionKey:  sessionKey,
		Type:        InboundMessageType,
		Payload:     payload,
		TraceParent: carrier.Get("traceparent"),
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	traceID := inbound.MessageID
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		traceID = spanContext.TraceID().String()
	}
	if err := manifests.SignEnvelope(&envelope, snapshot.Config.AppCode, snapshot.Config.ConfigVersion, traceID); err != nil {
		return Envelope{}, fmt.Errorf("sign execution manifest: %w", err)
	}
	return envelope, nil
}

// DecodeInboundPayload validates and decodes one Kafka inbound-message envelope.
func DecodeInboundPayload(envelope Envelope) (InboundPayload, error) {
	if err := envelope.Validate(); err != nil {
		return InboundPayload{}, err
	}
	if envelope.Type != InboundMessageType {
		return InboundPayload{}, fmt.Errorf("%w: unexpected envelope type %q", ErrInvalidEnvelope, envelope.Type)
	}
	var payload InboundPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return InboundPayload{}, fmt.Errorf("%w: decode inbound payload", ErrInvalidEnvelope)
	}
	if strings.TrimSpace(payload.BindingID) == "" || strings.TrimSpace(payload.AppCode) == "" || payload.ConfigVersion == 0 {
		return InboundPayload{}, fmt.Errorf("%w: inbound routing metadata is incomplete", ErrInvalidEnvelope)
	}
	if payload.ReleaseVariant != tenant.ReleaseStable && payload.ReleaseVariant != tenant.ReleaseCandidate {
		return InboundPayload{}, fmt.Errorf("%w: inbound release variant is invalid", ErrInvalidEnvelope)
	}
	if err := payload.Inbound.Validate(); err != nil {
		return InboundPayload{}, fmt.Errorf("%w: invalid normalized inbound message: %v", ErrInvalidEnvelope, err)
	}
	if !strings.HasPrefix(envelope.SessionKey, envelope.TenantID+"/"+payload.AppCode+"/session/") {
		return InboundPayload{}, fmt.Errorf("%w: inbound session key is not scoped to application", ErrInvalidEnvelope)
	}
	return payload, nil
}

// ChannelIdentityResolver is the narrow ingress seam needed to canonicalize an
// external sender before Session, Memory, Artifact, or governance state is used.
type ChannelIdentityResolver interface {
	ResolveChannelIdentity(context.Context, string, channels.Channel, string, string, string) (identity.ChannelIdentity, bool, error)
	ResolveSessionUser(context.Context, string) (identity.SessionUser, error)
	RoleFor(context.Context, string, string) (identity.Role, error)
	TenantStatus(context.Context, string) (string, error)
}

type sessionRouteCapture struct{ route storage.SessionRoute }

func (c *sessionRouteCapture) ResolveSession(_ context.Context, route storage.SessionRoute, preferred string) (string, error) {
	c.route = route
	return preferred, nil
}

func (*sessionRouteCapture) ArchiveSession(context.Context, string, string) error { return nil }

// PrepareInboundSessionRoute applies the exact same identity, access-policy and
// subject canonicalization as normal ingress without mutating Session state.
// Platform commands such as /new use the resulting route as the input to one
// dedicated transactional Session transition.
func PrepareInboundSessionRoute(ctx context.Context, identities ChannelIdentityResolver, snapshot tenant.Snapshot, bindingID string, inbound channels.InboundMessage) (storage.SessionRoute, channels.InboundMessage, error) {
	capture := &sessionRouteCapture{}
	_, normalized, err := ResolveInboundSession(ctx, capture, identities, snapshot, bindingID, inbound)
	if err != nil {
		return storage.SessionRoute{}, channels.InboundMessage{}, err
	}
	if strings.TrimSpace(capture.route.TenantID) == "" {
		return storage.SessionRoute{}, channels.InboundMessage{}, fmt.Errorf("prepared inbound session route is empty")
	}
	return capture.route, normalized, nil
}

// ResolveInboundSession canonicalizes the framework subject and ownership
// metadata before the message enters Kafka, then resolves the channel-neutral
// Session. The returned InboundMessage is the only form that should be sent to
// a Worker.
func ResolveInboundSession(ctx context.Context, state storage.SessionManager, identities ChannelIdentityResolver, snapshot tenant.Snapshot, bindingID string, inbound channels.InboundMessage) (string, channels.InboundMessage, error) {
	if state == nil {
		return "", channels.InboundMessage{}, fmt.Errorf("inbound session state store is required")
	}
	if err := inbound.Validate(); err != nil {
		return "", channels.InboundMessage{}, fmt.Errorf("validate inbound message: %w", err)
	}
	if identities == nil {
		return "", channels.InboundMessage{}, fmt.Errorf("identity resolver is required")
	}
	tenantStatus, err := identities.TenantStatus(ctx, snapshot.Config.TenantID)
	if err != nil {
		return "", channels.InboundMessage{}, fmt.Errorf("resolve tenant status: %w", err)
	}
	if tenantStatus != identity.TenantActive {
		return "", channels.InboundMessage{}, fmt.Errorf("tenant is suspended")
	}
	if snapshot.Config.Status != config.AgentActive {
		return "", channels.InboundMessage{}, fmt.Errorf("agent is not active")
	}
	bindingID = strings.TrimSpace(bindingID)
	if bindingID == "" {
		return "", channels.InboundMessage{}, fmt.Errorf("inbound binding ID is required")
	}
	scope := inbound.ConversationScope
	if scope == "" {
		scope = channels.ConversationDirect
	}
	inbound.ConversationScope = scope
	if inbound.TriggerType == "" {
		if scope == channels.ConversationGroup {
			inbound.TriggerType = channels.TriggerMention
		} else {
			inbound.TriggerType = channels.TriggerDirect
		}
	}

	if inbound.Channel == channels.Web {
		platformUserID := strings.TrimSpace(inbound.WebOwnerID)
		if platformUserID == "" {
			platformUserID = strings.TrimSpace(inbound.SenderID)
		}
		if platformUserID == "" {
			return "", channels.InboundMessage{}, fmt.Errorf("web platform user is required")
		}
		inbound.SubjectID = platformUserID
		inbound.OwnerPlatformUserID = platformUserID
		inbound.ActorPlatformUserID = platformUserID
		inbound.WebOwnerID = platformUserID
	} else {
		binding, ok := externalBinding(snapshot, inbound.Channel, bindingID)
		if !ok {
			return "", channels.InboundMessage{}, fmt.Errorf("channel binding is not owned by this agent")
		}
		trustedEnterpriseID := strings.TrimSpace(binding.TrustedEnterpriseID)
		actorPlatformUserID := ""
		resolved, linked, err := identities.ResolveChannelIdentity(
			ctx, snapshot.Config.TenantID, inbound.Channel, bindingID, inbound.SenderID, trustedEnterpriseID,
		)
		if err != nil {
			return "", channels.InboundMessage{}, fmt.Errorf("resolve channel identity: %w", err)
		}
		if linked {
			actorPlatformUserID = resolved.PlatformUserID
			if _, err := identities.ResolveSessionUser(ctx, actorPlatformUserID); err != nil {
				return "", channels.InboundMessage{}, fmt.Errorf("linked platform user is not active")
			}
		}
		member := false
		if actorPlatformUserID != "" {
			_, memberErr := identities.RoleFor(ctx, snapshot.Config.TenantID, actorPlatformUserID)
			member = memberErr == nil
		}
		switch binding.EffectiveAccessPolicy() {
		case config.ChannelAccessMemberOnly:
			if !member {
				return "", channels.InboundMessage{}, fmt.Errorf("channel access requires tenant membership")
			}
		case config.ChannelAccessAllowlist:
			if !member && !containsExternalIdentity(binding.Allowlist, inbound.SenderID) {
				return "", channels.InboundMessage{}, fmt.Errorf("channel identity is not allowlisted")
			}
		case config.ChannelAccessPublic:
		default:
			return "", channels.InboundMessage{}, fmt.Errorf("channel access policy is invalid")
		}
		inbound.ActorPlatformUserID = actorPlatformUserID
		if scope == channels.ConversationGroup {
			subjectID, err := channels.GroupSubjectID(inbound.Channel, bindingID, inbound.ConversationID)
			if err != nil {
				return "", channels.InboundMessage{}, fmt.Errorf("resolve group subject: %w", err)
			}
			inbound.SubjectID = subjectID
			inbound.OwnerPlatformUserID = ""
		} else if actorPlatformUserID != "" {
			inbound.SubjectID = actorPlatformUserID
			inbound.OwnerPlatformUserID = actorPlatformUserID
		} else {
			subjectID, err := channels.ExternalSubjectID(inbound.Channel, bindingID, inbound.SenderID)
			if err != nil {
				return "", channels.InboundMessage{}, fmt.Errorf("resolve anonymous external subject: %w", err)
			}
			inbound.SubjectID = subjectID
			inbound.OwnerPlatformUserID = ""
		}
	}

	preferred, err := channels.BuildSessionKey(snapshot.Config.TenantID, snapshot.Config.AppCode, uuid.NewString())
	if err != nil {
		return "", channels.InboundMessage{}, fmt.Errorf("build preferred session key: %w", err)
	}
	sessionKey, err := state.ResolveSession(ctx, storage.SessionRoute{
		TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode,
		Channel: string(inbound.Channel), BindingID: bindingID,
		ConversationID: inbound.ConversationID, ExternalUserID: inbound.SenderID,
		SubjectID: inbound.SubjectID, OwnerPlatformUserID: inbound.OwnerPlatformUserID,
		Scope: string(scope),
	}, preferred)
	if err != nil {
		return "", channels.InboundMessage{}, err
	}
	return sessionKey, inbound, nil
}

func externalBinding(snapshot tenant.Snapshot, channel channels.Channel, bindingID string) (config.ChannelBinding, bool) {
	for _, binding := range snapshot.Config.Channels {
		if binding.Type == string(channel) && binding.BindingID == bindingID {
			return binding, true
		}
	}
	return config.ChannelBinding{}, false
}

func containsExternalIdentity(allowlist []string, externalUserID string) bool {
	externalUserID = strings.TrimSpace(externalUserID)
	for _, allowed := range allowlist {
		if strings.TrimSpace(allowed) == externalUserID {
			return true
		}
	}
	return false
}
