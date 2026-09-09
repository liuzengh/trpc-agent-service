package pipeline

import (
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

// stablePipelineError keeps extension-point failures out of process logs.
// Durable queue errors are redacted by reliable.errorText; logs must apply the
// same boundary because worker and provider errors may contain credentials.
func stablePipelineError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, reliable.ErrNoWork):
		return "no_work"
	case errors.Is(err, reliable.ErrStaleLease):
		return "stale_lease"
	case errors.Is(err, reliable.ErrTenantInactive):
		return "tenant_inactive"
	case errors.Is(err, reliable.ErrInvalidInboxMessage):
		return "invalid_inbox"
	case errors.Is(err, reliable.ErrIdempotencyConflict):
		return "idempotency_conflict"
	case errors.Is(err, reliable.ErrOutboxConflict):
		return "outbox_conflict"
	case errors.Is(err, channel.ErrChannelCredentialInvalid):
		return "channel_credential_invalid"
	case errors.Is(err, channel.ErrChannelTransport):
		return "channel_transport"
	case errors.Is(err, channel.ErrDeliveryOutcomeUnknown):
		return "delivery_outcome_unknown"
	case errors.Is(err, channel.ErrInvalidOutboundMessage):
		return "invalid_outbound"
	case errors.Is(err, channel.ErrChannelRequestBuild):
		return "channel_request_build"
	case errors.Is(err, channel.ErrStreamingUnsupported):
		return "streaming_unsupported"
	default:
		return "internal_error"
	}
}
