// limit.go holds IM platform limits the gateway enforces on the reply path
// (text-length truncation) and the inbound rate-limiter contract.
package channels

import (
	"context"
	"strings"
	"unicode/utf8"
)

// Platform text-length ceilings, in runes, per channel. These are the
// documented upper bounds of each platform's plain-text message; longer
// replies are truncated before send so a platform error can never fail the
// outbound stream.
const (
	wecomTextMax   = 2000
	feishuTextMax  = 4000
	defaultTextMax = 4000
)

// maxTextLen returns the text ceiling for a channel.
func maxTextLen(channel string) int {
	switch channel {
	case "wecom":
		return wecomTextMax
	case "feishu":
		return feishuTextMax
	default:
		return defaultTextMax
	}
}

// truncateText cuts s to at most max runes, appending an ellipsis when
// truncated (max <= 1 is simply cut). It is rune-safe: multi-byte UTF-8 is
// never split mid-sequence.
func truncateText(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	runes := []rune(s)
	if max <= 1 {
		return string(runes[:max])
	}
	return strings.TrimRight(string(runes[:max-1]), " \t\n") + "…"
}

// RateLimiter decides whether an inbound message may enter the pipeline. A
// nil limiter on the Gateway means no limiting (default). Implementations must
// be safe for concurrent use; gateways may span nodes, so a shared Redis
// backing is expected in production.
type RateLimiter interface {
	// Allow reports whether the key may proceed. An error means the limiter
	// is unavailable; callers treat it as allow (fail-open, availability over
	// strictness) and log it.
	Allow(ctx context.Context, key string) (bool, error)
}

// inboundLimitKey derives the per-sender limiter key. It is scoped by tenant
// so one tenant's abuse cannot throttle another, and by channel so identical
// user ids on different platforms never share a bucket.
func inboundLimitKey(tenantID, channel, userID string) string {
	return tenantID + "|" + channel + "|" + userID
}
