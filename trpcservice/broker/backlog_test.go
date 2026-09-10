package broker

import (
	"context"
	"testing"
	"time"
)

type sequenceSource struct{ value Backlog }

func (s sequenceSource) BrokerBacklog(context.Context, time.Time) (Backlog, error) {
	return s.value, nil
}

type backlogObserver struct{ values chan Backlog }

func (o backlogObserver) ObserveBrokerBacklog(value Backlog) { o.values <- value }

func TestBacklogAggregatesAndMonitorPublishes(t *testing.T) {
	clock := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	value := Backlog{ObservedAt: clock, Shards: []ShardBacklog{{Shard: 1, Pending: 2, Undelivered: 3, OldestAge: 4 * time.Second}, {Shard: 0, Undelivered: 5, OldestAge: time.Second}}}
	if err := ValidateBacklog(value); err != nil || value.Total() != 10 || value.MaxOldestAge() != 4*time.Second {
		t.Fatalf("value=%+v err=%v", value, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	observer := backlogObserver{values: make(chan Backlog, 1)}
	done := make(chan error, 1)
	go func() {
		done <- (BacklogMonitor{Source: sequenceSource{value}, Observer: observer, PollInterval: time.Hour}).Run(ctx)
	}()
	select {
	case got := <-observer.values:
		if got.Total() != 10 {
			t.Fatalf("got=%+v", got)
		}
		cancel()
	case <-time.After(time.Second):
		t.Fatal("monitor did not publish")
	}
	<-done
}

func TestBacklogRejectsDuplicateShard(t *testing.T) {
	value := Backlog{ObservedAt: time.Now(), Shards: []ShardBacklog{{Shard: 1}, {Shard: 1}}}
	if ValidateBacklog(value) == nil {
		t.Fatal("duplicate shard accepted")
	}
}
