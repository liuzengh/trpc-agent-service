package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
)

type BrokerRegistry struct {
	mu          sync.RWMutex
	backlog     broker.Backlog
	known       bool
	SnapshotTTL time.Duration
}

func (r *BrokerRegistry) ObserveBrokerBacklog(value broker.Backlog) {
	if r == nil || broker.ValidateBacklog(value) != nil {
		return
	}
	r.mu.Lock()
	r.backlog, r.known = broker.SortedBacklog(value), true
	r.mu.Unlock()
}

func (r *BrokerRegistry) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	value, known := r.backlog, r.known
	r.mu.RUnlock()
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	fmt.Fprint(writer, "# HELP trpc_broker_backlog_snapshot_ready Whether this process has a valid broker backlog snapshot.\n# TYPE trpc_broker_backlog_snapshot_ready gauge\n")
	if !known {
		fmt.Fprint(writer, "trpc_broker_backlog_snapshot_ready 0\n")
		return
	}
	fmt.Fprint(writer, "trpc_broker_backlog_snapshot_ready 1\n")
	fmt.Fprint(writer, "# HELP trpc_broker_backlog_snapshot_timestamp_seconds Unix timestamp of the last valid broker backlog snapshot.\n# TYPE trpc_broker_backlog_snapshot_timestamp_seconds gauge\n")
	fmt.Fprintf(writer, "trpc_broker_backlog_snapshot_timestamp_seconds %d\n", value.ObservedAt.Unix())
	ttl := r.SnapshotTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	now := time.Now()
	fresh := !value.ObservedAt.After(now) && now.Sub(value.ObservedAt) <= ttl
	fmt.Fprint(writer, "# HELP trpc_broker_autoscaling_snapshot_ready Whether broker autoscaling gauges are fresh.\n# TYPE trpc_broker_autoscaling_snapshot_ready gauge\n")
	if fresh {
		fmt.Fprint(writer, "trpc_broker_autoscaling_snapshot_ready 1\n")
	} else {
		fmt.Fprint(writer, "trpc_broker_autoscaling_snapshot_ready 0\n")
	}
	fmt.Fprint(writer, "# HELP trpc_broker_backlog Dispatch entries pending or not yet delivered by shard and state.\n# TYPE trpc_broker_backlog gauge\n")
	fmt.Fprint(writer, "# HELP trpc_broker_delivery_lag_seconds Age of the oldest active dispatch entry by shard.\n# TYPE trpc_broker_delivery_lag_seconds gauge\n")
	for _, shard := range value.Shards {
		fmt.Fprintf(writer, "trpc_broker_backlog{shard=\"%d\",state=\"pending\"} %d\n", shard.Shard, shard.Pending)
		fmt.Fprintf(writer, "trpc_broker_backlog{shard=\"%d\",state=\"undelivered\"} %d\n", shard.Shard, shard.Undelivered)
		fmt.Fprintf(writer, "trpc_broker_delivery_lag_seconds{shard=\"%d\"} %g\n", shard.Shard, shard.OldestAge.Seconds())
	}
	if fresh {
		fmt.Fprint(writer, "# HELP trpc_broker_backlog_total Total active dispatch entries for autoscaling.\n# TYPE trpc_broker_backlog_total gauge\n")
		fmt.Fprintf(writer, "trpc_broker_backlog_total %d\n", value.Total())
		fmt.Fprint(writer, "# HELP trpc_broker_delivery_lag_max_seconds Maximum dispatch age for autoscaling.\n# TYPE trpc_broker_delivery_lag_max_seconds gauge\n")
		fmt.Fprintf(writer, "trpc_broker_delivery_lag_max_seconds %g\n", value.MaxOldestAge().Seconds())
	}
}

var _ broker.BacklogObserver = (*BrokerRegistry)(nil)
var _ http.Handler = (*BrokerRegistry)(nil)
