package broker

import (
	"context"
	"sort"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type ShardBacklog struct {
	Shard                Shard
	Pending, Undelivered int64
	OldestAge            time.Duration
}

type Backlog struct {
	Shards     []ShardBacklog
	ObservedAt time.Time
}

func (b Backlog) Total() int64 {
	var total int64
	for _, shard := range b.Shards {
		total += shard.Pending + shard.Undelivered
	}
	return total
}

func (b Backlog) MaxOldestAge() time.Duration {
	var maximum time.Duration
	for _, shard := range b.Shards {
		if shard.OldestAge > maximum {
			maximum = shard.OldestAge
		}
	}
	return maximum
}

type BacklogSource interface {
	BrokerBacklog(context.Context, time.Time) (Backlog, error)
}
type BacklogObserver interface{ ObserveBrokerBacklog(Backlog) }

type BacklogMonitor struct {
	Source       BacklogSource
	Observer     BacklogObserver
	PollInterval time.Duration
}

func (m BacklogMonitor) Run(ctx context.Context) error {
	if m.Source == nil || m.Observer == nil || m.PollInterval <= 0 {
		return runtime.ErrInvariantViolation
	}
	ticker := time.NewTicker(m.PollInterval)
	defer ticker.Stop()
	for {
		value, err := m.Source.BrokerBacklog(ctx, time.Now().UTC())
		if err == nil {
			m.Observer.ObserveBrokerBacklog(value)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func ValidateBacklog(value Backlog) error {
	if value.ObservedAt.IsZero() {
		return runtime.ErrInvariantViolation
	}
	seen := make(map[Shard]struct{}, len(value.Shards))
	for _, shard := range value.Shards {
		if shard.Pending < 0 || shard.Undelivered < 0 || shard.OldestAge < 0 {
			return runtime.ErrInvariantViolation
		}
		if _, exists := seen[shard.Shard]; exists {
			return runtime.ErrInvariantViolation
		}
		seen[shard.Shard] = struct{}{}
	}
	return nil
}

func SortedBacklog(value Backlog) Backlog {
	value.Shards = append([]ShardBacklog(nil), value.Shards...)
	sort.Slice(value.Shards, func(i, j int) bool { return value.Shards[i].Shard < value.Shards[j].Shard })
	return value
}
