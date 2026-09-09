package pipeline

import (
	"context"
	"fmt"
	"log"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

const (
	defaultExpiryReapInterval  = time.Minute
	defaultExpiryReapBatchSize = 100
)

// normalizeExpiryReapConfig applies one bounded maintenance policy to both
// Consumer and Delivery. A zero value keeps construction source-compatible;
// negative values are configuration errors rather than an accidental disable.
func normalizeExpiryReapConfig(interval time.Duration, batchSize int) (time.Duration, int, error) {
	if interval == 0 {
		interval = defaultExpiryReapInterval
	}
	if interval < 0 {
		return 0, 0, fmt.Errorf("expiry reaper interval must be positive")
	}
	if batchSize == 0 {
		batchSize = defaultExpiryReapBatchSize
	}
	if err := reliable.ValidateExpiredWorkReapBatchSize(batchSize); err != nil {
		return 0, 0, err
	}
	return interval, batchSize, nil
}

// runExpiryReaper runs once at startup so pre-existing terminal-expiry rows do
// not wait a full interval, then keeps maintenance independent from Claim
// polling. The Store owns row locking and fencing; this loop deliberately
// contains no queue state or leader-election logic.
func runExpiryReaper(ctx context.Context, runner string, reaper reliable.ExpiredWorkReaper, interval time.Duration, batchSize int) {
	ctx = nonNilContext(ctx)
	if isNilPipelineDependency(reaper) {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		result, err := reaper.ReapExpired(ctx, batchSize)
		if err != nil {
			pipelineExpiryReaperFailures.WithLabelValues(runner).Inc()
			log.Printf("%s expiry reaper failed: error=%s", runner, stablePipelineError(err))
		} else {
			observeExpiredReap(runner, result)
		}
		observeQueue(runner, reaper, ctx)
		if !wait(ctx, interval) {
			return
		}
	}
}

func observeQueue(runner string, reaper reliable.ExpiredWorkReaper, ctx context.Context) {
	ctx = nonNilContext(ctx)
	inspector, ok := reaper.(reliable.QueueInspector)
	if !ok || isNilPipelineDependency(inspector) {
		return
	}
	stats, err := inspector.InspectQueue(ctx)
	if err != nil {
		if ctx.Err() == nil {
			pipelineQueueInspectionFailures.WithLabelValues(runner).Inc()
			log.Printf("%s queue inspection failed: error=%s", runner, stablePipelineError(err))
		}
		return
	}
	now := stats.ObservedAt
	if now.IsZero() {
		// Compatibility path for external inspectors predating ObservedAt.
		now = time.Now()
	}
	observeQueueStats(runner, stats, now)
}
