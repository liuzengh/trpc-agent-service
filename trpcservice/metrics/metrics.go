// Package metrics exposes tenant-aware, fail-open telemetry sinks.
package metrics

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type AuditSink interface {
	Emit(context.Context, control.AuditRecord)
}
type MetricSink interface {
	Record(context.Context, control.MetricEvent)
}

type AsyncAuditSink struct {
	repository control.Repository
	queue      chan control.AuditRecord
	dropped    atomic.Uint64
	closed     atomic.Bool
	closeOnce  sync.Once
	done       chan struct{}
}

func NewAuditSink(repository control.Repository, capacity int) *AsyncAuditSink {
	if capacity < 1 {
		capacity = 256
	}
	s := &AsyncAuditSink{repository: repository, queue: make(chan control.AuditRecord, capacity), done: make(chan struct{})}
	go s.loop()
	return s
}

func (s *AsyncAuditSink) Emit(_ context.Context, record control.AuditRecord) {
	if s == nil || s.repository == nil || s.closed.Load() {
		return
	}
	defer func() { _ = recover() }()
	record.Channel = redaction.Default.Redact(record.Channel)
	record.ToolName = redaction.Default.Redact(record.ToolName)
	record.ErrorType = redaction.Default.Redact(record.ErrorType)
	select {
	case s.queue <- record:
	default:
		s.dropped.Add(1)
		telemetry.RecordTelemetryDrop(context.Background(), "audit_sink", "queue_full")
	}
}

func (s *AsyncAuditSink) loop() {
	defer close(s.done)
	for record := range s.queue {
		_ = s.repository.AppendAudit(context.Background(), record)
	}
}

func (s *AsyncAuditSink) Dropped() uint64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}
func (s *AsyncAuditSink) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() { s.closed.Store(true); close(s.queue); <-s.done })
	return nil
}

type AsyncMetricSink struct {
	repository control.Repository
	queue      chan control.MetricEvent
	dropped    atomic.Uint64
	closed     atomic.Bool
	closeOnce  sync.Once
	done       chan struct{}
}

func NewMetricSink(repository control.Repository, capacity int) *AsyncMetricSink {
	if capacity < 1 {
		capacity = 256
	}
	s := &AsyncMetricSink{repository: repository, queue: make(chan control.MetricEvent, capacity), done: make(chan struct{})}
	go s.loop()
	return s
}
func (s *AsyncMetricSink) Record(_ context.Context, record control.MetricEvent) {
	if s == nil || s.repository == nil || s.closed.Load() {
		return
	}
	defer func() { _ = recover() }()
	if len(record.Labels) > 0 {
		labels := make(map[string]string, len(record.Labels))
		for key, value := range record.Labels {
			labels[key] = redaction.Default.Redact(value)
		}
		record.Labels = labels
	}
	select {
	case s.queue <- record:
	default:
		s.dropped.Add(1)
		telemetry.RecordTelemetryDrop(context.Background(), "metric_sink", "queue_full")
	}
}
func (s *AsyncMetricSink) loop() {
	defer close(s.done)
	for record := range s.queue {
		_ = s.repository.AppendMetric(context.Background(), record)
	}
}
func (s *AsyncMetricSink) Dropped() uint64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}
func (s *AsyncMetricSink) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() { s.closed.Store(true); close(s.queue); <-s.done })
	return nil
}

var _ AuditSink = (*AsyncAuditSink)(nil)
var _ MetricSink = (*AsyncMetricSink)(nil)
