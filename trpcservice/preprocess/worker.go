package preprocess

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type NormalizedInput struct {
	ExternalMessageID string             `json:"external_message_id"`
	ExternalUserID    string             `json:"external_user_id"`
	ExternalChatID    string             `json:"external_chat_id"`
	ChannelBindingID  string             `json:"channel_binding_id,omitempty"`
	ExternalAccountID string             `json:"external_account_id,omitempty"`
	ConfigVersion     int64              `json:"config_version,omitempty"`
	MessageType       string             `json:"message_type,omitempty"`
	Text              string             `json:"text,omitempty"`
	MediaRefs         []channel.MediaRef `json:"media_refs,omitempty"`
}

// NormalizedText preserves the source-compatible name used by the initial
// text-only fixtures while sharing the version-one normalized input schema.
type NormalizedText = NormalizedInput

type PreparedMedia struct {
	ArtifactID    string `json:"artifact_id"`
	ArtifactRef   string `json:"artifact_ref"`
	Kind          string `json:"kind"`
	MediaType     string `json:"media_type"`
	ContentDigest string `json:"content_digest"`
	Size          int64  `json:"size"`
}

type PreparedInput struct {
	ExternalMessageID string          `json:"external_message_id"`
	ExternalUserID    string          `json:"external_user_id"`
	ExternalChatID    string          `json:"external_chat_id"`
	MessageType       string          `json:"message_type,omitempty"`
	Text              string          `json:"text,omitempty"`
	Media             []PreparedMedia `json:"media,omitempty"`
}

type Worker struct {
	Store       Store
	Payloads    messaging.PayloadStore
	Dispatcher  gateway.Dispatcher
	Owner       string
	LeaseTTL    time.Duration
	RetryDelay  time.Duration
	MaxAttempts int
	Now         func() time.Time
	Media       *MediaStager
	// ArtifactRetention is independent from audit/log retention. Media
	// preprocessing fails closed unless this policy is explicitly configured.
	ArtifactRetention time.Duration
	Telemetry         telemetry.Provider
}

func (w Worker) RunOnce(ctx context.Context, limit int) (int, error) {
	if w.Store == nil || w.Payloads == nil || w.Dispatcher == nil || w.Owner == "" || limit < 1 {
		return 0, runtime.ErrInvariantViolation
	}
	now := w.now()
	ttl := w.LeaseTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	jobs, err := w.Store.ClaimJobs(ctx, ClaimOptions{Owner: w.Owner, Now: now, TTL: ttl, Limit: limit})
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, job := range jobs {
		processed++
		if err := w.preprocess(ctx, job); err != nil {
			return processed, err
		}
	}
	ready, err := w.Store.ClaimReadyForDispatch(ctx, ClaimOptions{Owner: w.Owner, Now: w.now(), TTL: ttl, Limit: limit})
	if err != nil {
		return processed, err
	}
	for _, job := range ready {
		if err := w.dispatch(ctx, job); err != nil {
			return processed, err
		}
	}
	return processed, nil
}

func (w Worker) preprocess(ctx context.Context, job Job) (resultErr error) {
	ctx, finish := telemetry.StartOperation(ctx, w.Telemetry, job.TraceParent, telemetry.OperationChannelPreprocess,
		telemetry.ComponentAttribute(telemetry.ComponentPreprocess))
	defer func() { finish(resultErr) }()
	payload, err := w.Payloads.GetPayload(ctx, job.TenantID, job.RequestID)
	if err != nil {
		return w.retry(ctx, job, "payload_unavailable")
	}
	sum := sha256.Sum256(payload.Content)
	if payload.PayloadRef != job.PayloadRef || payload.ContentDigest != hex.EncodeToString(sum[:]) {
		_, finishErr := w.Store.FinishRejected(ctx, job, "payload_integrity")
		return finishErr
	}
	var normalized NormalizedInput
	decoder := json.NewDecoder(strings.NewReader(string(payload.Content)))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&normalized)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || !errors.Is(trailingErr, io.EOF) || !validNormalizedInput(normalized) {
		_, finishErr := w.Store.FinishRejected(ctx, job, "invalid_text_payload")
		return finishErr
	}
	if len(normalized.MediaRefs) > 0 {
		return w.prepareMedia(ctx, job, payload.PayloadRef, payload.KeyVersion, normalized)
	}
	_, err = w.Store.FinishReady(ctx, job)
	return err
}

func (w Worker) prepareMedia(ctx context.Context, job Job, sourcePayloadRef string, sourceKeyVersion int64, normalized NormalizedInput) error {
	preparedStore, ok := w.Payloads.(messaging.PreparedPayloadStore)
	if !ok || w.Media == nil || w.ArtifactRetention < time.Second || w.ArtifactRetention%time.Second != 0 {
		return w.retry(ctx, job, "media_prepared_payload_unavailable")
	}
	staged := make([]StagedMedia, 0, len(normalized.MediaRefs))
	for ordinal, media := range normalized.MediaRefs {
		value, err := w.Media.Stage(ctx, MediaStageRequest{TenantID: job.TenantID, RequestID: job.RequestID, Channel: job.Channel,
			ChannelBindingID: normalized.ChannelBindingID, ExternalAccountID: normalized.ExternalAccountID,
			ConfigVersion: normalized.ConfigVersion, Ordinal: ordinal, Media: media})
		if err != nil {
			switch {
			case errors.Is(err, ErrMediaRejected), errors.Is(err, runtime.ErrInvalidEnvelope), errors.Is(err, runtime.ErrCapabilityUnsupported):
				_, finishErr := w.Store.FinishRejected(ctx, job, "media_rejected")
				return finishErr
			case errors.Is(err, ErrMediaScanUnavailable), errors.Is(err, runtime.ErrBackendUnavailable):
				return w.retry(ctx, job, "media_stage_unavailable")
			default:
				return w.retry(ctx, job, "media_stage_unavailable")
			}
		}
		staged = append(staged, value)
	}
	preparedRef, err := StablePreparedPayloadRef(job.TenantID, job.RequestID, sourcePayloadRef, staged)
	if err != nil {
		_, finishErr := w.Store.FinishRejected(ctx, job, "prepared_payload_invalid")
		return finishErr
	}
	prepared := PreparedInput{ExternalMessageID: normalized.ExternalMessageID, ExternalUserID: normalized.ExternalUserID,
		ExternalChatID: normalized.ExternalChatID, MessageType: normalized.MessageType, Text: normalized.Text,
		Media: make([]PreparedMedia, 0, len(staged))}
	artifactReferences := make([]messaging.PreparedArtifactReference, 0, len(staged))
	for _, value := range staged {
		prepared.Media = append(prepared.Media, PreparedMedia{ArtifactID: value.ArtifactID, ArtifactRef: value.ArtifactRef,
			Kind: value.Kind, MediaType: value.MediaType, ContentDigest: value.ContentDigest, Size: value.Size})
		artifactReferences = append(artifactReferences, messaging.PreparedArtifactReference{ArtifactID: value.ArtifactID})
	}
	content, err := json.Marshal(prepared)
	if err != nil {
		return w.retry(ctx, job, "prepared_payload_unavailable")
	}
	digest := sha256.Sum256(content)
	if err := preparedStore.PutPreparedPayload(ctx, messaging.PreparedPayloadRecord{TenantID: job.TenantID, RequestID: job.RequestID,
		PayloadRef: preparedRef, SourcePayloadRef: sourcePayloadRef, ContentDigest: hex.EncodeToString(digest[:]), Content: content,
		KeyVersion: sourceKeyVersion, ArtifactRetention: w.ArtifactRetention, ArtifactReferences: artifactReferences}); err != nil {
		if errors.Is(err, runtime.ErrIdempotencyCollision) {
			_, finishErr := w.Store.FinishRejected(ctx, job, "prepared_payload_collision")
			return finishErr
		}
		return w.retry(ctx, job, "prepared_payload_unavailable")
	}
	job.PreparedPayloadRef = preparedRef
	_, err = w.Store.FinishReady(ctx, job)
	return err
}

func StablePreparedPayloadRef(tenantID, requestID, sourcePayloadRef string, staged []StagedMedia) (string, error) {
	if tenantID == "" || requestID == "" || sourcePayloadRef == "" || len(staged) == 0 {
		return "", runtime.ErrInvalidEnvelope
	}
	for _, value := range staged {
		if value.ArtifactID == "" || value.ArtifactRef == "" || value.MediaType == "" || value.ContentDigest == "" || value.Kind == "" || value.Size <= 0 {
			return "", runtime.ErrInvalidEnvelope
		}
	}
	value, err := json.Marshal(struct {
		TenantID, RequestID, SourcePayloadRef string
		Media                                 []StagedMedia
	}{tenantID, requestID, sourcePayloadRef, staged})
	if err != nil {
		return "", runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(value)
	return "prepared://" + tenantID + "/" + requestID + "/" + hex.EncodeToString(sum[:16]), nil
}

func validNormalizedInput(value NormalizedInput) bool {
	if value.ExternalMessageID == "" || value.ExternalUserID == "" || len(value.MediaRefs) > 1 {
		return false
	}
	messageType := value.MessageType
	if messageType == "" {
		messageType = "text"
	}
	switch messageType {
	case "text":
		return strings.TrimSpace(value.Text) != "" && len(value.MediaRefs) == 0
	case "image", "file":
		return strings.TrimSpace(value.Text) == "" && value.ChannelBindingID != "" && value.ExternalAccountID != "" &&
			value.ConfigVersion >= 1 && len(value.MediaRefs) == 1 && value.MediaRefs[0].ID != "" &&
			value.MediaRefs[0].Kind == messageType && value.MediaRefs[0].Size >= 0
	default:
		return false
	}
}

func (w Worker) retry(ctx context.Context, job Job, reason string) error {
	max := w.MaxAttempts
	if max <= 0 {
		max = 8
	}
	if job.Attempt >= max {
		_, err := w.Store.FinishRejected(ctx, job, "retry_exhausted:"+reason)
		return err
	}
	delay := w.RetryDelay
	if delay <= 0 {
		delay = time.Second
	}
	_, err := w.Store.FinishRetry(ctx, job, w.now().Add(delay), reason)
	return err
}

func (w Worker) dispatch(ctx context.Context, job Job) error {
	payloadRef := job.PayloadRef
	if job.PreparedPayloadRef != "" {
		payloadRef = job.PreparedPayloadRef
	}
	_, err := w.Dispatcher.Dispatch(ctx, gateway.DispatchRequest{
		Tenant: tenant.Context{TenantID: job.TenantID, TenantVersion: job.TenantVersion, AgentAppID: job.AgentAppID,
			SubjectID: job.UserID, Channel: job.Channel, TrustedSource: "channel_binding:" + job.ChannelBindingID},
		RequestID: job.RequestID, SessionID: job.SessionID, UserID: job.UserID, PayloadRef: payloadRef,
		TraceParent:   telemetry.EffectiveTraceParent(ctx, job.TraceParent),
		ConfigVersion: job.ConfigVersion,
	})
	if err != nil {
		return err
	}
	_, err = w.Store.MarkDispatched(ctx, job, w.now())
	return err
}

func (w Worker) now() time.Time {
	if w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}
