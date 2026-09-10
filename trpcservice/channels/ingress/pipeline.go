package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type IdentityMapper interface {
	Map(context.Context, channel.VerifiedBinding, channel.ProviderEvent) (identity.Result, error)
}

type Verifier interface {
	VerifyAndDecode(context.Context, channel.CallbackRequest) (VerifiedIngress, error)
}

// ConfigSelector chooses the immutable snapshot at the callback acceptance
// boundary. The resulting version is persisted with the preprocess job, so a
// later rollout update cannot alter an accepted provider message.
type ConfigSelector interface {
	SelectEffective(context.Context, string, int64) (config.Snapshot, error)
}

type AcceptedEvent struct {
	RequestID, PayloadRef, PreprocessJobID string
}

// Pipeline persists every verified event before returning. Payload persistence
// happens before the atomic Inbox+PreprocessJob claim; an interrupted attempt
// is safely completed by the provider's duplicate callback.
type Pipeline struct {
	Verification Verifier
	Identity     IdentityMapper
	Intake       preprocess.Store
	Payloads     messaging.PayloadStore
	KeyVersion   int64
	Telemetry    telemetry.Provider
	Configs      ConfigSelector
}

func (p Pipeline) Accept(ctx context.Context, request channel.CallbackRequest) ([]AcceptedEvent, error) {
	if p.Verification == nil || p.Identity == nil || p.Intake == nil || p.Payloads == nil || p.KeyVersion < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	verified, err := p.Verification.VerifyAndDecode(ctx, request)
	if err != nil {
		return nil, err
	}
	keyVersion := p.KeyVersion
	accepted := make([]AcceptedEvent, 0, len(verified.Events))
	for _, event := range verified.Events {
		traceParent := event.TraceParent
		if traceParent == "" {
			traceParent = verified.TraceParent
		}
		if traceParent == "" {
			traceParent = headerValue(request.Headers, "traceparent")
		}
		eventCtx, finish := telemetry.StartOperation(ctx, p.Telemetry, traceParent, telemetry.OperationChannelPreprocess,
			telemetry.ComponentAttribute(telemetry.ComponentPreprocess))
		acceptedEvent, eventErr := p.acceptEvent(eventCtx, verified, event, keyVersion, traceParent)
		finish(eventErr)
		if eventErr != nil {
			return nil, eventErr
		}
		accepted = append(accepted, acceptedEvent)
	}
	return accepted, nil
}

func (p Pipeline) acceptEvent(ctx context.Context, verified VerifiedIngress, event channel.ProviderEvent, keyVersion int64, traceParent string) (AcceptedEvent, error) {
	if event.Channel != verified.Binding.Channel || event.ExternalAccountID != verified.Binding.ExternalAccountID ||
		!validProviderContent(event) {
		return AcceptedEvent{}, runtime.ErrInvalidEnvelope
	}
	ids, err := p.Identity.Map(ctx, verified.Binding, event)
	if err != nil {
		return AcceptedEvent{}, err
	}
	configVersion, err := p.selectConfig(ctx, verified.Binding)
	if err != nil {
		return AcceptedEvent{}, err
	}
	messageType := event.MessageType
	if messageType == "text" {
		messageType = ""
	}
	// The verified binding is part of the durable authorization context for
	// every input, not only media.  In particular, a later confirmation must
	// be bound to the same channel session that submitted a text-only request.
	bindingID, accountID := verified.Binding.ChannelBindingID, verified.Binding.ExternalAccountID
	normalized, err := json.Marshal(preprocess.NormalizedInput{ExternalMessageID: event.ExternalMessageID,
		ExternalUserID: event.ExternalUserID, ExternalChatID: event.ExternalChatID,
		ChannelBindingID: bindingID, ExternalAccountID: accountID,
		ConfigVersion: configVersion,
		MessageType:   messageType, Text: event.Text, MediaRefs: event.MediaRefs})
	if err != nil {
		return AcceptedEvent{}, err
	}
	digest := sha256.Sum256(normalized)
	key := messaging.InboxKey{TenantID: verified.Binding.TenantID, Channel: event.Channel,
		ExternalAccountID: event.ExternalAccountID, ExternalMessageID: event.ExternalMessageID}
	requestID, payloadRef := messaging.StableInboxIdentity(key)
	contentDigest := hex.EncodeToString(digest[:])
	if err := p.Payloads.PutPayload(ctx, messaging.PayloadRecord{TenantID: key.TenantID, RequestID: requestID,
		PayloadRef: payloadRef, ContentDigest: contentDigest, Content: normalized, KeyVersion: keyVersion}); err != nil {
		return AcceptedEvent{}, err
	}
	inbox, job, err := p.Intake.ClaimInboxAndSchedule(ctx, preprocess.ClaimRequest{
		Inbox: messaging.ClaimInboxRequest{InboxKey: key, AgentAppID: verified.Binding.AgentAppID, SessionID: ids.SessionID,
			ExternalChatID: event.ExternalChatID, ExternalUserID: event.ExternalUserID, PayloadDigest: contentDigest,
			KeyVersion: keyVersion, InitialState: messaging.InboxPreprocessPending},
		TenantVersion: verified.Binding.TenantVersion, ConfigVersion: configVersion,
		ChannelBindingID: verified.Binding.ChannelBindingID, UserID: ids.UserID, TraceParent: telemetry.EffectiveTraceParent(ctx, traceParent),
	})
	if err != nil {
		return AcceptedEvent{}, err
	}
	if inbox.RequestID != requestID || inbox.PayloadRef != payloadRef || job.RequestID != requestID || job.PayloadRef != payloadRef {
		return AcceptedEvent{}, runtime.ErrIdempotencyCollision
	}
	return AcceptedEvent{RequestID: requestID, PayloadRef: payloadRef, PreprocessJobID: job.JobID}, nil
}

func (p Pipeline) selectConfig(ctx context.Context, binding channel.VerifiedBinding) (int64, error) {
	if p.Configs == nil {
		return binding.BindingVersion, nil
	}
	snapshot, err := p.Configs.SelectEffective(ctx, binding.TenantID, binding.BindingVersion)
	if err != nil {
		return 0, err
	}
	for _, candidate := range snapshot.Payload.ChannelBindings {
		if candidate.BindingID == binding.ChannelBindingID && candidate.Channel == binding.Channel &&
			candidate.ExternalAccountID == binding.ExternalAccountID && candidate.AgentAppID == binding.AgentAppID {
			return snapshot.ConfigVersion, nil
		}
	}
	// Verification always uses the baseline secret. A candidate that changes
	// the authenticated channel identity must be introduced by the dedicated
	// route rotation flow, not by a percentage release.
	return 0, config.ErrTenantScope
}

func validProviderContent(event channel.ProviderEvent) bool {
	if event.ExternalMessageID == "" || event.ExternalUserID == "" || len(event.MediaRefs) > 1 {
		return false
	}
	switch event.MessageType {
	case "text":
		return strings.TrimSpace(event.Text) != "" && len(event.MediaRefs) == 0
	case "image", "file":
		return strings.TrimSpace(event.Text) == "" && len(event.MediaRefs) == 1 && event.MediaRefs[0].ID != "" &&
			event.MediaRefs[0].Kind == event.MessageType && event.MediaRefs[0].Size >= 0
	default:
		return false
	}
}
