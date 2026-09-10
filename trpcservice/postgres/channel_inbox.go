package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

const (
	channelInboxStatusAdmitted = "ADMITTED"
	channelInboxStatusRejected = "REJECTED"
)

type channelInboxRecord struct {
	RequestID    string
	PayloadHash  []byte
	Status       string
	MessageType  string
	RejectReason string
}

func findChannelInbox(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, externalMessageID string,
) (channelInboxRecord, bool, error) {
	var record channelInboxRecord
	err := tx.QueryRow(ctx, `
SELECT request_id, payload_hash, status, message_type, COALESCE(reject_reason, '')
FROM platform.channel_inbox
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND external_message_id = $4
FOR UPDATE`, tenantID, appID, bindingID, externalMessageID).Scan(
		&record.RequestID,
		&record.PayloadHash,
		&record.Status,
		&record.MessageType,
		&record.RejectReason,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return channelInboxRecord{}, false, nil
	}
	if err != nil {
		return channelInboxRecord{}, false, fmt.Errorf("find channel inbox: %w", err)
	}
	return record, true, nil
}

func insertChannelInbox(
	ctx context.Context,
	tx pgx.Tx,
	request gateway.AdmissionRequest,
	payloadHash []byte,
	status, rejectReason string,
	targetEnvelope *channels.TargetEnvelope,
	targetExpiresAt *time.Time,
) error {
	input := request.ChannelInput
	if input == nil {
		return errors.New("channel input is required")
	}
	identity := request.Identity.Tenant
	var providerTimestamp *time.Time
	if !input.ProviderTimestamp.IsZero() {
		value := input.ProviderTimestamp.UTC()
		providerTimestamp = &value
	}
	var storedRejectReason any
	if rejectReason != "" {
		storedRejectReason = rejectReason
	}
	var storedTarget any
	if targetEnvelope != nil {
		if err := targetEnvelope.Validate(); err != nil {
			return fmt.Errorf("channel inbox reply target envelope: %w", err)
		}
		if targetExpiresAt == nil || targetExpiresAt.IsZero() {
			return errors.New("channel inbox reply target expiry is required")
		}
		encoded, err := json.Marshal(targetEnvelope)
		if err != nil {
			return fmt.Errorf("marshal channel inbox reply target: %w", err)
		}
		storedTarget = encoded
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.channel_inbox (
    tenant_id, app_id, binding_id, external_message_id, payload_hash,
    request_id, status, message_type, reject_reason, provider_reply_target_envelope,
    reply_target_expires_at, provider_timestamp
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (tenant_id, app_id, binding_id, external_message_id) DO NOTHING`,
		identity.TenantID,
		identity.AppID,
		identity.BindingID,
		input.ExternalMessageID,
		payloadHash,
		request.RequestID,
		status,
		input.MessageType,
		storedRejectReason,
		storedTarget,
		targetExpiresAt,
		providerTimestamp,
	); err != nil {
		return fmt.Errorf("insert channel inbox: %w", err)
	}
	return nil
}

func channelInputRejectReason(input channels.ChannelInput) (string, error) {
	if input.RejectReason != "" {
		switch input.RejectReason {
		case "ATTACHMENT_REJECTED", "UNSUPPORTED_MESSAGE_TYPE":
			return input.RejectReason, nil
		default:
			return "", errors.New("channel input reject reason is unsupported")
		}
	}
	switch input.MessageType {
	case channels.MessageTypeText:
		return "", nil
	case channels.MessageTypeImage, channels.MessageTypeFile, channels.MessageTypeMixed:
		if len(input.ArtifactRefs) == 0 {
			return "ATTACHMENT_REJECTED", nil
		}
		return "", nil
	default:
		return "UNSUPPORTED_MESSAGE_TYPE", nil
	}
}
