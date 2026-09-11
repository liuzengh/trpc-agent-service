package datamigration

import (
	"context"
	"errors"
	"strings"
	"time"
)

type TenantFreezeControl interface {
	TenantFreeze(ctx context.Context, tenantID, appName string) (migrationID string, frozenAt time.Time, frozen bool, err error)
}

// DistributedMaintenanceGate proves that every compatible worker sees the
// same persistent freeze and that pre-freeze turns had time to drain.
type DistributedMaintenanceGate struct {
	control TenantFreezeControl
	drain   time.Duration
	now     func() time.Time
}

type RoutedMaintenanceGate struct {
	freeze SessionSafetyGate
	route  func() string
}

// NewRoutedMaintenanceGate reads the full non-secret route identity stored in
// Job.Source or Job.Target (including the secret reference and Redis prefix).
// The historical "redis"/"sql" reader remains compatible only with jobs that
// use those same backend-only identities; it cannot acknowledge a bound job.
func NewRoutedMaintenanceGate(freeze SessionSafetyGate, route func() string) (*RoutedMaintenanceGate, error) {
	if freeze == nil || route == nil {
		return nil, errors.New("session migration freeze and routing state reader are required")
	}
	return &RoutedMaintenanceGate{freeze: freeze, route: route}, nil
}

func (g *RoutedMaintenanceGate) Check(ctx context.Context, job Job, phase Phase) error {
	if err := g.freeze.Check(ctx, job, phase); err != nil {
		return err
	}
	return checkSessionRoute(g.route(), job, phase)
}

func checkSessionRoute(route string, job Job, phase Phase) error {
	switch phase {
	case PhasePrepare, PhaseSnapshot, PhaseCatchUp, PhaseShadowRead, PhaseCanary:
		if sqlRoute(route) {
			return ErrTargetActive
		}
		if !redisRoute(route) || route != job.Source {
			return ErrRoutingNotReady
		}
	case PhaseCutover, PhaseDrain, PhaseFinalize:
		if !sqlRoute(route) || route != job.Target {
			return ErrRoutingNotReady
		}
	case PhaseRollback:
		if !redisRoute(route) || route != job.Source {
			return ErrRoutingNotReady
		}
	default:
		return ErrRoutingNotReady
	}
	return nil
}

func redisRoute(route string) bool {
	return route == "redis" || (strings.HasPrefix(route, "redis-env:") && len(route) > len("redis-env:"))
}

func sqlRoute(route string) bool {
	return route == "sql" || (strings.HasPrefix(route, "postgres-env:") && len(route) > len("postgres-env:"))
}

func NewDistributedMaintenanceGate(control TenantFreezeControl, drain time.Duration) (*DistributedMaintenanceGate, error) {
	if control == nil || drain <= 0 {
		return nil, errors.New("session migration distributed freeze and positive drain window are required")
	}
	return &DistributedMaintenanceGate{control: control, drain: drain, now: time.Now}, nil
}

func (g *DistributedMaintenanceGate) Check(ctx context.Context, job Job, _ Phase) error {
	migrationID, frozenAt, frozen, err := g.control.TenantFreeze(ctx, job.TenantID, job.AppName)
	if err != nil {
		return err
	}
	if !frozen || migrationID != job.MigrationID {
		return ErrSourceNotFrozen
	}
	if g.now().Before(frozenAt.Add(g.drain)) {
		return ErrSourceNotFrozen
	}
	return nil
}

// ConfigRoutingSwitcher is a fail-closed acknowledgement that deployment
// routing has already been changed while workers remain frozen. It never
// edits configuration itself.
type ConfigRoutingSwitcher struct {
	route func() string
}

// NewConfigRoutingSwitcher requires the same full route identity reader as
// NewRoutedMaintenanceGate. No credentials are needed or included in errors.
func NewConfigRoutingSwitcher(route func() string) (*ConfigRoutingSwitcher, error) {
	if route == nil {
		return nil, errors.New("session migration routing state reader is required")
	}
	return &ConfigRoutingSwitcher{route: route}, nil
}

func (s *ConfigRoutingSwitcher) Cutover(_ context.Context, job Job) error {
	return checkSessionRoute(s.route(), job, PhaseCutover)
}

func (s *ConfigRoutingSwitcher) Rollback(_ context.Context, job Job) error {
	return checkSessionRoute(s.route(), job, PhaseRollback)
}
