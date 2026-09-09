package gateway

import (
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

// stableErrorCode is the only error detail written to Gateway process logs.
// Adapter, datastore and provider errors are supplied by extension points and
// may contain credentials, DSNs or untrusted request bytes. Detailed causes
// belong in a separately controlled, redacting telemetry sink.
func stableErrorCode(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ErrBodyTooLarge):
		return "body_too_large"
	case errors.Is(err, ErrEmptyBody):
		return "empty_body"
	case errors.Is(err, ErrJSONTooDeep):
		return "json_too_deep"
	case errors.Is(err, ErrInvalidUTF8):
		return "invalid_utf8"
	case errors.Is(err, channel.ErrIgnoredInbound):
		return "ignored_inbound"
	case errors.Is(err, channel.ErrInvalidInboundMessage):
		return "invalid_inbound"
	case errors.Is(err, reliable.ErrIdempotencyConflict):
		return "idempotency_conflict"
	case errors.Is(err, reliable.ErrTenantInactive):
		return "tenant_inactive"
	case errors.Is(err, reliable.ErrInvalidInboxMessage):
		return "invalid_inbox"
	case errors.Is(err, reliable.ErrStaleLease):
		return "stale_lease"
	default:
		return "internal_error"
	}
}
