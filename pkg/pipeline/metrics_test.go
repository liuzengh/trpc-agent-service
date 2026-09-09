package pipeline

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

type queueProbe struct {
	stats reliable.QueueStats
}

func (*queueProbe) ReapExpired(context.Context, int) (reliable.ExpiredWorkReapResult, error) {
	return reliable.ExpiredWorkReapResult{}, nil
}

func (p *queueProbe) InspectQueue(context.Context) (reliable.QueueStats, error) {
	return p.stats, nil
}

func TestQueueAgeSecondsClampsEmptyAndFutureSnapshots(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	if got := queueAgeSeconds(time.Time{}, now); got != 0 {
		t.Fatalf("empty oldest age=%v, want 0", got)
	}
	if got := queueAgeSeconds(now.Add(time.Second), now); got != 0 {
		t.Fatalf("future oldest age=%v, want 0", got)
	}
	if got := queueAgeSeconds(now.Add(-1500*time.Millisecond), now); got != 1.5 {
		t.Fatalf("oldest age=%v, want 1.5", got)
	}
}

func TestObserveQueueStatsPublishesBoundedGlobalSnapshot(t *testing.T) {
	runner := "metrics-test"
	now := time.Now()
	probe := &queueProbe{stats: reliable.QueueStats{
		InboxDepth:   4,
		InboxOldest:  now.Add(-2 * time.Minute),
		OutboxDepth:  1,
		OutboxOldest: now.Add(-500 * time.Millisecond),
	}}
	observeQueue(runner, probe, context.Background())
	depth := &dto.Metric{}
	if err := pipelineQueueDepth.WithLabelValues(runner, "inbox").Write(depth); err != nil {
		t.Fatal(err)
	}
	if got := depth.GetGauge().GetValue(); got != 4 {
		t.Fatalf("inbox depth=%v, want 4", got)
	}
	age := &dto.Metric{}
	if err := pipelineQueueOldestAge.WithLabelValues(runner, "outbox").Write(age); err != nil {
		t.Fatal(err)
	}
	if got := age.GetGauge().GetValue(); got < 0.4 || got > 1 {
		t.Fatalf("outbox oldest age=%v, want roughly 0.5", got)
	}
}

func TestObserveQueueIgnoresTypedNilInspector(t *testing.T) {
	var probe *queueProbe
	observeQueue("typed-nil-test", probe, context.Background())
}

func TestObserveQueueUsesStoreObservationClock(t *testing.T) {
	// Simulate a PostgreSQL clock that is ahead of this process. The exported
	// age must use the store's observation instant, not time.Now in the worker.
	storeNow := time.Now().Add(10 * time.Minute)
	runner := "store-clock-test"
	probe := &queueProbe{stats: reliable.QueueStats{
		InboxDepth:  1,
		InboxOldest: storeNow.Add(-90 * time.Second),
		ObservedAt:  storeNow,
	}}
	observeQueue(runner, probe, context.Background())
	metric := &dto.Metric{}
	if err := pipelineQueueOldestAge.WithLabelValues(runner, "inbox").Write(metric); err != nil {
		t.Fatal(err)
	}
	if got := metric.GetGauge().GetValue(); got < 89 || got > 91 {
		t.Fatalf("oldest age=%v, want roughly 90 seconds", got)
	}
}
