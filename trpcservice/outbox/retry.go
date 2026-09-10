package outbox

import (
	"hash/fnv"
	"math"
	"time"
)

type retryPolicy struct {
	maxAttempts int
	base        time.Duration
	maximum     time.Duration
	jitter      float64
}

func newRetryPolicy(config Config) (retryPolicy, error) {
	policy := retryPolicy{maxAttempts: config.MaxAttempts, base: config.BackoffBase, maximum: config.BackoffMax, jitter: config.Jitter}
	if policy.maxAttempts <= 0 {
		policy.maxAttempts = 3
	}
	if policy.base < 0 || policy.maximum < 0 || policy.jitter < 0 || policy.jitter > 1 || math.IsNaN(policy.jitter) {
		return retryPolicy{}, ErrInvalid
	}
	if policy.base == 0 {
		policy.base = 100 * time.Millisecond
	}
	if policy.maximum == 0 {
		policy.maximum = 30 * time.Second
	}
	if policy.maximum < policy.base {
		return retryPolicy{}, ErrInvalid
	}
	return policy, nil
}

func (policy retryPolicy) delay(replyID string, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := float64(policy.base) * math.Pow(2, float64(attempt-1))
	if delay > float64(policy.maximum) {
		delay = float64(policy.maximum)
	}
	if policy.jitter > 0 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(replyID))
		factor := 1 + ((float64(h.Sum32()%1000)/999)-0.5)*2*policy.jitter
		delay *= factor
	}
	return time.Duration(delay)
}
