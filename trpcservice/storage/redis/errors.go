package redis

import "errors"

var (
	ErrRedisUnavailable       = errors.New("redis unavailable")
	ErrAlreadyClaimed         = errors.New("claim already held")
	ErrLeaseLost              = errors.New("lease lost")
	ErrFenceRejected          = errors.New("stale fencing token")
	ErrRateLimitExceeded      = errors.New("rate limit exceeded")
	ErrRateLimiterUnavailable = errors.New("rate limiter unavailable")
	ErrInvalidKey             = errors.New("invalid redis key")
)
