package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	frameworkpostgres "trpc.group/trpc-go/trpc-agent-go/storage/postgres"
)

// trpc-agent-go/knowledge/vectorstore/pgvector v1.11.0 always executes its
// bootstrap DDL from New. Production schema ownership belongs to migrations,
// while runtime vector connections deliberately SET ROLE trpc_tenant so RLS
// cannot be bypassed. This marker tells the framework client adapter to skip
// only that bootstrap DDL for the migration-owned table.
type frameworkSchemaManaged struct{ table string }

type frameworkPostgresScope struct {
	component          string
	tenantID           string
	appName            string
	enforceTenantScope bool
}

var installFrameworkPostgresClientOnce sync.Once

var frameworkPostgresObserver struct {
	sync.RWMutex
	observer metrics.StoreObserver
}

// ObserveFrameworkPostgres connects framework-owned PostgreSQL clients to the
// platform Store observer without exposing SQL text or query arguments.
func ObserveFrameworkPostgres(observer metrics.StoreObserver) error {
	if observer == nil {
		return errors.New("framework PostgreSQL observer is required")
	}
	frameworkPostgresObserver.Lock()
	frameworkPostgresObserver.observer = observer
	frameworkPostgresObserver.Unlock()
	installFrameworkPostgresClientAdapter()
	return nil
}

// FrameworkPostgresScope marks a framework client with its stable component
// and tenant dimensions. The returned value is passed through framework
// ExtraOptions and is not interpreted by the framework itself.
func FrameworkPostgresScope(component, tenantID string) any {
	return frameworkPostgresScope{component: strings.TrimSpace(component), tenantID: strings.TrimSpace(tenantID)}
}

// FrameworkTenantPostgresScope marks a framework client that uses the shared
// platform PostgreSQL database. In addition to observability dimensions, every
// operation is executed under the trpc_tenant role and one exact app_name RLS
// scope. External tenant-owned PostgreSQL backends must use FrameworkPostgresScope.
func FrameworkTenantPostgresScope(component, tenantID, appName string) any {
	return frameworkPostgresScope{
		component: strings.TrimSpace(component), tenantID: strings.TrimSpace(tenantID),
		appName: strings.TrimSpace(appName), enforceTenantScope: true,
	}
}

func installFrameworkPostgresClientAdapter() {
	installFrameworkPostgresClientOnce.Do(func() {
		fallback := frameworkpostgres.GetClientBuilder()
		frameworkpostgres.SetClientBuilder(func(ctx context.Context, opts ...frameworkpostgres.ClientBuilderOpt) (frameworkpostgres.Client, error) {
			configured := &frameworkpostgres.ClientBuilderOpts{}
			for _, opt := range opts {
				opt(configured)
			}
			var client frameworkpostgres.Client
			var err error
			managed, ok := frameworkManagedSchema(configured.ExtraOptions)
			if ok {
				client, err = newMigrationManagedFrameworkClient(ctx, configured.ConnString, managed.table)
			} else {
				client, err = fallback(ctx, opts...)
			}
			if err != nil {
				return nil, err
			}
			scope, scoped := frameworkClientScope(configured.ExtraOptions)
			if scoped && scope.enforceTenantScope {
				if err := validateFrameworkTenantScope(scope); err != nil {
					_ = client.Close()
					return nil, err
				}
				client = &tenantScopedFrameworkPostgresClient{delegate: client, scope: scope}
			}
			frameworkPostgresObserver.RLock()
			observer := frameworkPostgresObserver.observer
			frameworkPostgresObserver.RUnlock()
			if observer == nil || !scoped {
				return client, nil
			}
			return &observedFrameworkPostgresClient{delegate: client, observer: observer, scope: scope}, nil
		})
	})
}

func validateFrameworkTenantScope(scope frameworkPostgresScope) error {
	if scope.component == "" || scope.tenantID == "" || scope.appName == "" {
		return errors.New("framework PostgreSQL tenant scope requires component, tenant, and application")
	}
	if !strings.HasPrefix(scope.appName, scope.tenantID+"/") {
		return fmt.Errorf("framework PostgreSQL application %q is outside tenant %q", scope.appName, scope.tenantID)
	}
	return nil
}

type tenantScopedFrameworkPostgresClient struct {
	delegate frameworkpostgres.Client
	scope    frameworkPostgresScope
}

func (c *tenantScopedFrameworkPostgresClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := c.withTenantTransaction(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = tx.ExecContext(ctx, query, args...)
		return err
	})
	return result, err
}

func (c *tenantScopedFrameworkPostgresClient) Query(ctx context.Context, handler frameworkpostgres.HandlerFunc, query string, args ...any) error {
	return c.withTenantTransaction(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return handler(rows)
	})
}

func (c *tenantScopedFrameworkPostgresClient) Transaction(ctx context.Context, fn frameworkpostgres.TxFunc) error {
	return c.withTenantTransaction(ctx, fn)
}

func (c *tenantScopedFrameworkPostgresClient) withTenantTransaction(ctx context.Context, fn frameworkpostgres.TxFunc) error {
	return c.delegate.Transaction(ctx, func(tx *sql.Tx) error {
		if err := dbscope.ScopeTenantTransaction(ctx, tx, c.scope.tenantID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "SELECT set_config('app.app_name', $1, true)", c.scope.appName); err != nil {
			return fmt.Errorf("set framework PostgreSQL application context: %w", err)
		}
		return fn(tx)
	})
}

func (c *tenantScopedFrameworkPostgresClient) Close() error { return c.delegate.Close() }

func frameworkManagedSchema(options []any) (frameworkSchemaManaged, bool) {
	for _, option := range options {
		if managed, ok := option.(frameworkSchemaManaged); ok {
			return managed, strings.TrimSpace(managed.table) != ""
		}
	}
	return frameworkSchemaManaged{}, false
}

func frameworkClientScope(options []any) (frameworkPostgresScope, bool) {
	for _, option := range options {
		if scope, ok := option.(frameworkPostgresScope); ok {
			return scope, scope.component != ""
		}
	}
	return frameworkPostgresScope{}, false
}

type observedFrameworkPostgresClient struct {
	delegate frameworkpostgres.Client
	observer metrics.StoreObserver
	scope    frameworkPostgresScope
}

func (c *observedFrameworkPostgresClient) start(ctx context.Context, operation string) (context.Context, func(error)) {
	return c.observer.StartStore(ctx, metrics.StoreAttributes{
		TenantID: c.scope.tenantID, Backend: "postgres", Operation: "framework." + c.scope.component + "." + operation,
	})
}

func (c *observedFrameworkPostgresClient) ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error) {
	ctx, finish := c.start(ctx, "exec")
	defer func() { finish(err) }()
	return c.delegate.ExecContext(ctx, query, args...)
}

func (c *observedFrameworkPostgresClient) Query(ctx context.Context, handler frameworkpostgres.HandlerFunc, query string, args ...any) (err error) {
	ctx, finish := c.start(ctx, "query")
	defer func() { finish(err) }()
	return c.delegate.Query(ctx, handler, query, args...)
}

func (c *observedFrameworkPostgresClient) Transaction(ctx context.Context, fn frameworkpostgres.TxFunc) (err error) {
	ctx, finish := c.start(ctx, "transaction")
	defer func() { finish(err) }()
	return c.delegate.Transaction(ctx, fn)
}

func (c *observedFrameworkPostgresClient) Close() error { return c.delegate.Close() }

type migrationManagedFrameworkClient struct {
	database *sql.DB
	table    string
}

func newMigrationManagedFrameworkClient(ctx context.Context, dsn, table string) (*migrationManagedFrameworkClient, error) {
	if strings.TrimSpace(dsn) == "" || strings.TrimSpace(table) == "" {
		return nil, errors.New("framework PostgreSQL DSN and migration-managed table are required")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open framework PostgreSQL connection: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("ping framework PostgreSQL connection: %w", err)
	}
	var relation sql.NullString
	if err := database.QueryRowContext(ctx, "SELECT to_regclass($1)::text", "public."+table).Scan(&relation); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("verify framework PostgreSQL schema: %w", err)
	}
	if !relation.Valid || relation.String == "" {
		_ = database.Close()
		return nil, fmt.Errorf("framework PostgreSQL table %q is missing; apply the current database baseline", table)
	}
	return &migrationManagedFrameworkClient{database: database, table: table}, nil
}

func (c *migrationManagedFrameworkClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if isFrameworkBootstrapDDL(query, c.table) {
		return driver.RowsAffected(0), nil
	}
	return c.database.ExecContext(ctx, query, args...)
}

func (c *migrationManagedFrameworkClient) Query(ctx context.Context, handler frameworkpostgres.HandlerFunc, query string, args ...any) error {
	rows, err := c.database.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return handler(rows)
}

func (c *migrationManagedFrameworkClient) Transaction(ctx context.Context, fn frameworkpostgres.TxFunc) error {
	tx, err := c.database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (c *migrationManagedFrameworkClient) Close() error { return c.database.Close() }

func isFrameworkBootstrapDDL(query, table string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
	table = strings.ToLower(strings.TrimSpace(table))
	if normalized == "create extension if not exists vector" {
		return true
	}
	if strings.HasPrefix(normalized, "create table if not exists "+table+" (") {
		return true
	}
	return strings.HasPrefix(normalized, "create index if not exists "+table+"_embedding_idx on "+table+" ") ||
		strings.HasPrefix(normalized, "create index if not exists "+table+"_content_fts_idx on "+table+" ")
}

var _ frameworkpostgres.Client = (*migrationManagedFrameworkClient)(nil)
var _ frameworkpostgres.Client = (*observedFrameworkPostgresClient)(nil)
var _ frameworkpostgres.Client = (*tenantScopedFrameworkPostgresClient)(nil)
