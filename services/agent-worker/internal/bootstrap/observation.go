package bootstrap

import (
	"context"
	"errors"
	"time"

	execution "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/telemetry"
	manifest "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/application"
	manifestdomain "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
)

type TelemetryConfig struct {
	MetricsEndpoint string   `json:"metrics_endpoint"`
	ExportInterval  Duration `json:"export_interval"`
	ExportTimeout   Duration `json:"export_timeout"`
}

func (c *TelemetryConfig) Export() telemetry.Export {
	if c == nil {
		return telemetry.Export{}
	}
	return telemetry.Export{Endpoint: c.MetricsEndpoint, Interval: c.ExportInterval.Value(), Timeout: c.ExportTimeout.Value()}
}

type observedProjection struct {
	manifest.Projection
	observer execution.Observer
}

func (p observedProjection) Apply(ctx context.Context, m manifestdomain.Publication, capacity int) (err error) {
	start := time.Now()
	defer func() {
		result := execution.ObservationResult(err)
		if errors.Is(err, manifestdomain.ErrConflict) {
			result = "conflict"
		}
		if errors.Is(err, manifestdomain.ErrCapacity) {
			result = "capacity"
		}
		p.observer.Observe(ctx, execution.Observation{Operation: "manifest_apply", Result: result, TenantID: m.TenantID, Duration: time.Since(start)})
	}()
	return p.Projection.Apply(ctx, m, capacity)
}

type observedRejector struct {
	delegate interface {
		Reject(context.Context, string, string, string) error
	}
	observer execution.Observer
}

func (r observedRejector) Reject(ctx context.Context, source, digest, reason string) (err error) {
	start := time.Now()
	defer func() {
		r.observer.Observe(ctx, execution.Observation{Operation: "wire_reject", Result: execution.ObservationResult(err), Duration: time.Since(start)})
	}()
	return r.delegate.Reject(ctx, source, digest, reason)
}
func (a *App) observeStorage(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		op, cancel := context.WithTimeout(ctx, a.config.Timing.OperationTimeout.Value())
		s, err := a.ledger.ObserveStorage(op)
		if err == nil {
			s.ActiveLocal = a.active.Load()
			a.observation.Sample(op, s)
		}
		a.observation.SampleStatus(op, err == nil)
		cancel()
		if !wait(ctx, a.config.Timing.HealthInterval.Value()) {
			return
		}
	}
}
