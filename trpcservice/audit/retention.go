package audit

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// TenantPolicy is the published retention policy for one enabled tenant.
type TenantPolicy struct {
	TenantID string
	Policy   tenant.AuditPolicy
}

type PolicySource interface {
	ListAuditPolicies(context.Context) ([]TenantPolicy, error)
}

type cycleLocker interface {
	WithRetentionLock(context.Context, func(context.Context) error) error
}

// SQLPolicySource resolves policy only from current immutable publications.
type SQLPolicySource struct {
	DB       *sql.DB
	TenantID string
}

func (source *SQLPolicySource) ListAuditPolicies(ctx context.Context) ([]TenantPolicy, error) {
	if source == nil || source.DB == nil {
		return nil, errors.New("audit: policy database is required")
	}
	query := `SELECT t.tenant_id,cv.config_yaml FROM tenants t JOIN config_versions cv ON cv.tenant_id=t.tenant_id AND cv.version=t.current_config_version WHERE t.enabled`
	var args []any
	if source.TenantID != "" {
		query += ` AND t.tenant_id=$1`
		args = append(args, source.TenantID)
	}
	rows, err := source.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []TenantPolicy
	for rows.Next() {
		var tenantID string
		var payload []byte
		if err := rows.Scan(&tenantID, &payload); err != nil {
			return nil, err
		}
		file, err := config.Load(bytes.NewReader(payload))
		if err != nil {
			return nil, errors.New("audit: published retention policy is invalid")
		}
		found := false
		for _, configured := range file.Tenants {
			if configured.ID == tenantID && configured.Enabled {
				result = append(result, TenantPolicy{TenantID: tenantID, Policy: configured.Audit.Clone()})
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("audit: published retention policy tenant mismatch")
		}
	}
	return result, rows.Err()
}

func (source *SQLPolicySource) WithRetentionLock(ctx context.Context, run func(context.Context) error) error {
	if source == nil || source.DB == nil || run == nil {
		return errors.New("audit: retention lock dependencies are required")
	}
	connection, err := source.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	var acquired bool
	if err := connection.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(7420142026)`).Scan(&acquired); err != nil || !acquired {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = connection.ExecContext(unlockCtx, `SELECT pg_advisory_unlock(7420142026)`)
	}()
	return run(ctx)
}

// RetentionWorker prunes expired tenant audit rows under a cluster-wide lock.
type RetentionWorker struct {
	Store     RetentionStore
	Policies  PolicySource
	Interval  time.Duration
	Timeout   time.Duration
	Now       func() time.Time
	OnResult  func(deleted int64, err error)
	cancel    context.CancelFunc
	done      chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func (worker *RetentionWorker) Start(parent context.Context) error {
	if worker == nil || parent == nil || worker.Store == nil || worker.Policies == nil {
		return errors.New("audit: retention worker dependencies are required")
	}
	worker.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		worker.cancel = cancel
		worker.done = make(chan struct{})
		go worker.loop(ctx)
	})
	return nil
}

func (worker *RetentionWorker) loop(ctx context.Context) {
	defer close(worker.done)
	interval := worker.Interval
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	worker.run(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.run(ctx)
		}
	}
}

func (worker *RetentionWorker) run(parent context.Context) {
	timeout := worker.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	var deleted int64
	run := func(ctx context.Context) error {
		policies, err := worker.Policies.ListAuditPolicies(ctx)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if worker.Now != nil {
			now = worker.Now().UTC()
		}
		for _, current := range policies {
			count, err := PruneTenant(ctx, worker.Store, current.TenantID, current.Policy, now)
			if err != nil {
				return err
			}
			deleted += count
		}
		return nil
	}
	var err error
	if locker, ok := worker.Policies.(cycleLocker); ok {
		err = locker.WithRetentionLock(ctx, run)
	} else {
		err = run(ctx)
	}
	if worker.OnResult != nil {
		worker.OnResult(deleted, err)
	}
}

func (worker *RetentionWorker) Close(ctx context.Context) error {
	if worker == nil {
		return nil
	}
	worker.closeOnce.Do(func() {
		if worker.cancel != nil {
			worker.cancel()
		}
	})
	if worker.done == nil {
		return nil
	}
	select {
	case <-worker.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
