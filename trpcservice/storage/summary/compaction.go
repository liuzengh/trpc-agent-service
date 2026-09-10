package summary

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// Compactor only removes a body after a higher-watermark replacement was
// explicitly recorded. A failed delete retains the claimed record for retry.
type Compactor struct {
	Store        Store
	Owner        string
	Now          func() time.Time
	ClaimTTL     time.Duration
	RetryBackoff time.Duration
	BatchSize    int
}

func (c Compactor) RunOnce(ctx context.Context) (int, error) {
	if c.Store == nil || c.Owner == "" {
		return 0, runtime.ErrInvariantViolation
	}
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	ttl := c.ClaimTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	limit := c.BatchSize
	if limit <= 0 {
		limit = 100
	}
	values, err := c.Store.ClaimSuperseded(ctx, now, c.Owner, ttl, limit)
	if err != nil {
		return 0, err
	}
	var joined error
	completed := 0
	for _, value := range values {
		if err := c.Store.FinishDelete(ctx, value); err != nil {
			backoff := c.RetryBackoff
			if backoff <= 0 {
				backoff = time.Minute
			}
			joined = errors.Join(joined, err, c.Store.DeferDelete(ctx, value, now.Add(backoff), "delete_failed"))
			continue
		}
		completed++
	}
	return completed, joined
}
