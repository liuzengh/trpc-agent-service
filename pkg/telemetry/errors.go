package telemetry

import (
	"context"
	"errors"
)

// StableErrorCode returns a bounded, credential-free value suitable for
// process logs. Extension-point and driver errors may contain DSNs, tokens,
// prompts, or response bodies and must not be formatted into ordinary logs.
func StableErrorCode(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "internal_error"
	}
}
