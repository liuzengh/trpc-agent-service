package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"
	"time"
)

// RuntimeDatabaseOptions bounds the total connections across one process's
// pools. Lock waiters never consume the pool needed by the lock holder's
// metadata or backend operations. Every pool connects to the same authority.
type RuntimeDatabaseOptions struct {
	MaxConnections   int
	ExecutionFencing bool
}

type RuntimeDatabases struct {
	Control       *sql.DB
	Execution     *sql.DB
	MigrationGate *sql.DB
	closeOnce     sync.Once
	closeErr      error
}

func ParseRuntimeDatabaseBudget(value string, fallback int) (int, error) {
	budget := fallback
	var err error
	if value != "" {
		budget, err = strconv.Atoi(value)
	}
	if err != nil || budget < 9 || budget > 300 {
		return 0, errors.New("CONTROL_DB_MAX_CONNECTIONS must be between 9 and 300")
	}
	return budget, nil
}

// OpenRuntimeDatabases partitions, rather than multiplies, the connection
// budget. Worker defaults to 9 metadata + 8 execution + 8 migration connections;
// processes without execution fencing reserve only the migration share.
func OpenRuntimeDatabases(ctx context.Context, dsn string, options RuntimeDatabaseOptions) (*RuntimeDatabases, error) {
	return openRuntimeDatabases(ctx, dsn, options, sql.Open)
}

func openRuntimeDatabases(ctx context.Context, dsn string, options RuntimeDatabaseOptions, open func(string, string) (*sql.DB, error)) (*RuntimeDatabases, error) {
	if options.MaxConnections < 9 || options.MaxConnections > 300 || open == nil {
		return nil, errors.New("control database connection budget must be between 9 and 300")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	gateLimit := options.MaxConnections / 3
	executionLimit := 0
	if options.ExecutionFencing {
		executionLimit = gateLimit
	}
	pools := &RuntimeDatabases{}
	for _, spec := range []struct {
		destination **sql.DB
		limit       int
	}{
		{&pools.Control, options.MaxConnections - gateLimit - executionLimit},
		{&pools.MigrationGate, gateLimit},
		{&pools.Execution, executionLimit},
	} {
		if spec.limit == 0 {
			continue
		}
		db, err := open("postgres", dsn)
		if err != nil {
			_ = pools.Close()
			return nil, errors.New("cannot open control database pool")
		}
		*spec.destination = db
		db.SetMaxOpenConns(spec.limit)
		db.SetMaxIdleConns(min(spec.limit, 5))
		db.SetConnMaxLifetime(30 * time.Minute)
		db.SetConnMaxIdleTime(5 * time.Minute)
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err != nil {
			_ = pools.Close()
			return nil, errors.New("control database pool is unavailable")
		}
	}
	return pools, nil
}

// Close is called after Workers, cached services and migration handles drain.
// It is also safe during partially completed startup and repeated shutdown.
func (p *RuntimeDatabases) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		for _, db := range []*sql.DB{p.Execution, p.MigrationGate, p.Control} {
			if db != nil {
				p.closeErr = errors.Join(p.closeErr, db.Close())
			}
		}
	})
	return p.closeErr
}
