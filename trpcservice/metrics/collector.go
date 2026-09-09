package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// Queue gauges: stream length, pending count and the oldest pending
// message's idle time, the inputs of the backlog alerts.
var (
	// StreamLength is the number of entries in a stream (XLEN).
	StreamLength otelmetric.Int64Gauge
	// StreamPending is the number of pending (delivered, un-acked) entries.
	StreamPending otelmetric.Int64Gauge
	// StreamOldestPendingSeconds is how long the oldest pending entry waits.
	StreamOldestPendingSeconds otelmetric.Float64Gauge
)

func init() {
	meter := otel.Meter("trpc-agent-service")
	var err error
	if StreamLength, err = meter.Int64Gauge("stream_length"); err != nil {
		panic(err)
	}
	if StreamPending, err = meter.Int64Gauge("stream_pending"); err != nil {
		panic(err)
	}
	if StreamOldestPendingSeconds, err = meter.Float64Gauge("stream_oldest_pending_seconds"); err != nil {
		panic(err)
	}
}

// StreamStats is the read side of a stream queue the collector needs: stream
// length and pending-entry queries.
type StreamStats interface {
	Len(ctx context.Context, stream string) (int64, error)
	Pending(ctx context.Context, stream, group string) (count int64, oldestIdle time.Duration, err error)
}

// StartStreamCollector polls the queues into the gauges every interval until
// ctx is canceled. Scrapers read the gauges; the poll cadence decouples Redis
// load from the scrape interval.
func StartStreamCollector(ctx context.Context, stats StreamStats, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	targets := []struct{ stream, group string }{
		{"stream:inbound", "workers"},
		{"stream:outbound", "senders"},
		// The wecomws leader's consumer group: without this target a wedged
		// or leaderless senders-ws backlog would be invisible to the alerts.
		{"stream:outbound", "senders-ws"},
		{"stream:deadletter", ""},
	}
	collect := func() {
		// stream:outbound appears once per consumer group; one XLEN per
		// stream per cycle is enough.
		type lenResult struct {
			n   int64
			err error
		}
		lens := make(map[string]lenResult, len(targets))
		for _, t := range targets {
			r, ok := lens[t.stream]
			if !ok {
				r.n, r.err = stats.Len(ctx, t.stream)
				lens[t.stream] = r
			}
			if r.err != nil {
				plog.Warnf("stream collector len %s: %v", t.stream, r.err)
				continue
			}
			streamAttr := otelmetric.WithAttributes(attribute.String("stream", t.stream))
			StreamLength.Record(ctx, r.n, streamAttr)
			if t.group == "" {
				continue
			}
			attr := otelmetric.WithAttributes(
				attribute.String("stream", t.stream), attribute.String("group", t.group))
			count, oldest, err := stats.Pending(ctx, t.stream, t.group)
			if err != nil {
				plog.Warnf("stream collector pending %s/%s: %v", t.stream, t.group, err)
				continue
			}
			StreamPending.Record(ctx, count, attr)
			StreamOldestPendingSeconds.Record(ctx, oldest.Seconds(), attr)
		}
	}
	go func() {
		collect()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collect()
			}
		}
	}()
}
