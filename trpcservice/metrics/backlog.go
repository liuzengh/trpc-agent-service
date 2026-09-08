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

type BacklogSample struct {
	TenantID, Stage, State string
	Items                  int64
	OldestAgeSeconds       float64
}
type BacklogSource func(context.Context) ([]BacklogSample, error)

func SQLBacklogSource(db *sql.DB) BacklogSource {
	return func(ctx context.Context) ([]BacklogSample, error) {
		if db == nil {
			return nil, errors.New("backlog snapshot unavailable")
		}
		rows, err := db.QueryContext(ctx, `SELECT tenant_id,stage,state,items,oldest_age_seconds FROM platform_backlog LIMIT 10001`)
		if err != nil {
			return nil, errors.New("backlog snapshot unavailable")
		}
		defer func(closer interface{ Close() error }) { _ = closer.Close() }(rows)
		items := []BacklogSample{}
		for rows.Next() {
			var item BacklogSample
			if rows.Scan(&item.TenantID, &item.Stage, &item.State, &item.Items, &item.OldestAgeSeconds) != nil {
				return nil, errors.New("invalid backlog snapshot")
			}
			items = append(items, item)
			if len(items) > 10000 {
				return nil, errors.New("backlog snapshot cardinality limit exceeded")
			}
		}
		if rows.Err() != nil {
			return nil, errors.New("backlog snapshot incomplete")
		}
		return items, nil
	}
}

// RegisterBacklog uses an aggregate SQL view only, with a bounded query on each
// OTel collection. Failures export up=0, never a fake zero backlog.
func RegisterBacklog(repository any) (func(), error) {
	provider, ok := repository.(interface{ SQLDB() *sql.DB })
	if !ok {
		return func() {}, nil
	}
	return registerBacklog(otel.Meter("trpc-agent-service/backlog"), SQLBacklogSource(provider.SQLDB()))
}
func registerBacklog(meter metric.Meter, source BacklogSource) (func(), error) {
	items, err := meter.Int64ObservableGauge("agent.backlog.items")
	if err != nil {
		return nil, err
	}
	age, err := meter.Float64ObservableGauge("agent.backlog.oldest_age", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	up, err := meter.Int64ObservableGauge("agent.backlog.snapshot_up")
	if err != nil {
		return nil, err
	}
	stamp, err := meter.Float64ObservableGauge("agent.backlog.snapshot_timestamp", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	registration, err := meter.RegisterCallback(func(parent context.Context, observer metric.Observer) error {
		ctx, cancel := context.WithTimeout(parent, 3*time.Second)
		defer cancel()
		snapshot, err := source(ctx)
		if err != nil {
			observer.ObserveInt64(up, 0)
			return nil
		}
		observer.ObserveInt64(up, 1)
		observer.ObserveFloat64(stamp, float64(time.Now().Unix()))
		for _, item := range snapshot {
			attrs := metric.WithAttributes(attribute.String("tenant.id", item.TenantID), attribute.String("backlog.stage", item.Stage), attribute.String("backlog.state", item.State))
			observer.ObserveInt64(items, item.Items, attrs)
			observer.ObserveFloat64(age, item.OldestAgeSeconds, attrs)
		}
		return nil
	}, items, age, up, stamp)
	if err != nil {
		return nil, err
	}
	return func() { _ = registration.Unregister() }, nil
}
