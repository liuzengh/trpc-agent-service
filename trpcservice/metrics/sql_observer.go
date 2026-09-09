package metrics

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SQLObserver exports durable queue depth and platform PostgreSQL health
// without placing request, user, session, or message identifiers in labels.
type SQLObserver struct {
	db           *sql.DB
	registration metric.Registration
	queueDepth   metric.Int64ObservableGauge
	workers      metric.Int64ObservableGauge
	storageOK    metric.Int64ObservableGauge
	storageMS    metric.Float64ObservableGauge
}

// RegisterSQLObserver installs asynchronous production gauges.
func RegisterSQLObserver(db *sql.DB) (*SQLObserver, error) {
	if db == nil {
		return nil, errors.New("metrics: SQL observer database is required")
	}
	meter := otel.Meter("trpc-agent-service")
	queueDepth, err := meter.Int64ObservableGauge("agent.queue.depth")
	if err != nil {
		return nil, err
	}
	workers, err := meter.Int64ObservableGauge("agent.worker.live")
	if err != nil {
		return nil, err
	}
	storageOK, err := meter.Int64ObservableGauge("agent.storage.health")
	if err != nil {
		return nil, err
	}
	storageMS, err := meter.Float64ObservableGauge("agent.storage.healthcheck.duration", metric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}
	observer := &SQLObserver{db: db, queueDepth: queueDepth, workers: workers, storageOK: storageOK, storageMS: storageMS}
	registration, err := meter.RegisterCallback(observer.observe, queueDepth, workers, storageOK, storageMS)
	if err != nil {
		return nil, err
	}
	observer.registration = registration
	return observer, nil
}

func (observer *SQLObserver) observe(parent context.Context, output metric.Observer) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	started := time.Now()
	err := observer.db.PingContext(ctx)
	if err == nil {
		err = observer.observeQueue(ctx, output, "inbox")
	}
	if err == nil {
		err = observer.observeQueue(ctx, output, "outbox")
	}
	var live int64
	if err == nil {
		err = observer.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_nodes WHERE NOT draining AND last_heartbeat>=NOW()-INTERVAL '20 seconds'`).Scan(&live)
	}
	status, healthy := "success", int64(1)
	if err != nil {
		status, healthy = "failed", 0
	} else {
		output.ObserveInt64(observer.workers, live)
	}
	options := metric.WithAttributes(storageAttributes(status)...)
	output.ObserveInt64(observer.storageOK, healthy, options)
	output.ObserveFloat64(observer.storageMS, float64(time.Since(started).Microseconds())/1000, options)
	return nil
}

func (observer *SQLObserver) observeQueue(ctx context.Context, output metric.Observer, queue string) error {
	query, err := queueBacklogQuery(queue)
	if err != nil {
		return err
	}
	rows, err := observer.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tenantID, status string
		var count int64
		if err := rows.Scan(&tenantID, &status, &count); err != nil {
			return err
		}
		output.ObserveInt64(observer.queueDepth, count, metric.WithAttributes(
			attribute.String("tenant.id", tenantID),
			attribute.String("queue", queue),
			attribute.String("status", status),
		))
	}
	return rows.Err()
}

func queueBacklogQuery(queue string) (string, error) {
	switch queue {
	case "inbox":
		// DLQ is deliberately retained: it is terminal for processing but still
		// represents work that requires operator attention.
		return `SELECT tenant_id,status,COUNT(*) FROM inbox_messages WHERE status NOT IN ('completed','canceled','rejected') GROUP BY tenant_id,status`, nil
	case "outbox":
		// DLQ and uncertain are likewise operational backlog; sent is the only
		// successfully completed Outbox state.
		return `SELECT tenant_id,status,COUNT(*) FROM outbox_messages WHERE status<>'sent' GROUP BY tenant_id,status`, nil
	default:
		return "", errors.New("metrics: unsupported durable queue")
	}
}

func storageAttributes(status string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("domain", "platform"),
		attribute.String("backend", "postgres"),
		attribute.String("operation", "ping"),
		attribute.String("status", status),
	}
}

// Close unregisters callbacks before the database is closed.
func (observer *SQLObserver) Close() error {
	if observer == nil || observer.registration == nil {
		return nil
	}
	return observer.registration.Unregister()
}
