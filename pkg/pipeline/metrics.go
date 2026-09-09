package pipeline

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
)

var (
	pipelineMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_pipeline_messages_total", Help: "Durable pipeline outcomes by stage and tenant.",
	}, []string{"stage", "result", "tenant_id"})
	pipelineDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "agent_pipeline_duration_seconds", Help: "Processing or delivery duration.",
		// Agent/model calls regularly exceed Prometheus' default 10-second
		// upper bucket. These boundaries make the documented 10s/30s SLOs
		// measurable instead of collapsing every slow request into +Inf.
		Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10, 20, 30, 60, 120, 300},
	}, []string{"stage", "result"})
	pipelineLag = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "agent_pipeline_queue_lag_seconds", Help: "Age of a message when claimed.",
		Buckets: []float64{.1, .5, 1, 2, 5, 10, 30, 60, 300, 900},
	}, []string{"stage"})
	pipelineRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_pipeline_retries_total", Help: "Retry transitions by stage and tenant.",
	}, []string{"stage", "tenant_id"})
	pipelineDeadLetters = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_pipeline_dead_letters_total", Help: "Deterministic failures moved directly to a pipeline dead-letter state.",
	}, []string{"stage", "tenant_id"})
	pipelineFenceRejects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_pipeline_fence_rejections_total", Help: "Writes rejected because lease ownership was stale.",
	}, []string{"stage"})
	pipelineExpiryReaped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_pipeline_expiry_reaped_total", Help: "Expired durable work terminalized by the bounded maintenance loop.",
	}, []string{"runner", "queue", "reason"})
	pipelineExpiryReaperFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_pipeline_expiry_reaper_failures_total", Help: "Failed bounded expiry-reaper maintenance passes.",
	}, []string{"runner"})
	pipelineQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_pipeline_queue_depth", Help: "Current automatic durable queue depth observed by a pipeline process.",
	}, []string{"runner", "queue"})
	pipelineQueueOldestAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_pipeline_queue_oldest_age_seconds", Help: "Age of the oldest automatic durable queue message observed by a pipeline process.",
	}, []string{"runner", "queue"})
	pipelineQueueInspectionFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_pipeline_queue_inspection_failures_total", Help: "Failed durable queue inspection snapshots.",
	}, []string{"runner"})
)

func observePipeline(stage, result, tenantID string, started, created time.Time) {
	pipelineMessages.WithLabelValues(stage, result, telemetry.MetricTenantLabel(tenantID)).Inc()
	pipelineDuration.WithLabelValues(stage, result).Observe(time.Since(started).Seconds())
	if !created.IsZero() {
		// Queue lag ends when processing starts; it must not include model or
		// provider execution time.
		lag := started.Sub(created).Seconds()
		if lag < 0 {
			lag = 0
		}
		pipelineLag.WithLabelValues(stage).Observe(lag)
	}
}

func observeExpiredReap(runner string, result reliable.ExpiredWorkReapResult) {
	if result.InboxFinalAttemptExpired > 0 {
		pipelineExpiryReaped.WithLabelValues(runner, "inbox", "final_attempt").Add(float64(result.InboxFinalAttemptExpired))
	}
	if result.InboxApprovalExpired > 0 {
		pipelineExpiryReaped.WithLabelValues(runner, "inbox", "approval").Add(float64(result.InboxApprovalExpired))
	}
	if result.OutboxFinalAttemptExpired > 0 {
		pipelineExpiryReaped.WithLabelValues(runner, "outbox", "final_attempt").Add(float64(result.OutboxFinalAttemptExpired))
	}
	if result.OutboxDispatchUnknown > 0 {
		pipelineExpiryReaped.WithLabelValues(runner, "outbox", "dispatch_unknown").Add(float64(result.OutboxDispatchUnknown))
	}
}

// observeQueueStats records a bounded, process-independent snapshot. The
// QueueInspector contract deliberately exposes only global queue state; tenant
// identifiers never become metric labels here, so queue dashboards cannot
// create unbounded Prometheus cardinality.
func observeQueueStats(runner string, stats reliable.QueueStats, now time.Time) {
	pipelineQueueDepth.WithLabelValues(runner, "inbox").Set(float64(stats.InboxDepth))
	pipelineQueueDepth.WithLabelValues(runner, "outbox").Set(float64(stats.OutboxDepth))
	pipelineQueueOldestAge.WithLabelValues(runner, "inbox").Set(queueAgeSeconds(stats.InboxOldest, now))
	pipelineQueueOldestAge.WithLabelValues(runner, "outbox").Set(queueAgeSeconds(stats.OutboxOldest, now))
}

func queueAgeSeconds(oldest, now time.Time) float64 {
	if oldest.IsZero() {
		return 0
	}
	age := now.Sub(oldest).Seconds()
	if age < 0 {
		return 0
	}
	return age
}
