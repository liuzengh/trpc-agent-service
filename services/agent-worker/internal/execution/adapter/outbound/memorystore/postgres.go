package memorystore

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Target is the immutable managed PostgreSQL target in the RuntimeManifest.
// Schema is fixed by the V1 storage adapter, never supplied by input/DSN.
type Target struct {
	// MaxConcurrency is caller-resolved adapter policy; zero retains pgx defaults.
	MaxConcurrency              int32
	Host                        string
	Port                        uint16
	Database, Username, SSLMode string
}

type Postgres struct {
	pool          *pgxpool.Pool
	capacityBytes int
}

// Open checks the published target and runtime role before reading any content.
// It never creates a schema, a table, a role, or runs migrations.
func Open(ctx context.Context, dsn string, target Target, capacityBytes int) (*Postgres, error) {
	if capacityBytes <= 0 {
		return nil, fmt.Errorf("positive memory capacity is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, openFailure("parse_config", err)
	}
	if target.MaxConcurrency < 0 {
		return nil, ErrIdentity
	}
	if target.MaxConcurrency > 0 {
		cfg.MaxConns = target.MaxConcurrency
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.User == nil || u.Hostname() == "" || u.Opaque != "" || strings.Contains(dsn, "#") {
		return nil, openFailure("parse_config", err)
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query) != 1 || len(query["sslmode"]) != 1 {
		return nil, ErrIdentity
	}
	sslmode := query.Get("sslmode")
	if sslmode != "disable" && sslmode != "require" && sslmode != "verify-full" {
		return nil, ErrIdentity
	}
	if password, ok := u.User.Password(); !ok || password == "" {
		return nil, ErrIdentity
	}
	if !strings.EqualFold(cfg.ConnConfig.Host, target.Host) || cfg.ConnConfig.Port != target.Port || cfg.ConnConfig.Database != target.Database || cfg.ConnConfig.User != target.Username || sslmode != target.SSLMode {
		return nil, ErrIdentity
	}
	// Forbid credential-controlled routing and identity overrides. A schema search
	// path is allowed only when it already points at the fixed V1 namespace.
	for key, value := range cfg.ConnConfig.RuntimeParams {
		switch key {
		case "search_path":
			if strings.ReplaceAll(value, " ", "") != "runtime_memory" && strings.ReplaceAll(value, " ", "") != "runtime_memory,pg_temp" {
				return nil, ErrIdentity
			}
		case "application_name":
		default:
			return nil, ErrIdentity
		}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = "runtime_memory,pg_temp"
	// A multi-host fallback could evade the target comparison.
	for _, f := range cfg.ConnConfig.Fallbacks {
		if f.Host != target.Host || f.Port != target.Port {
			return nil, ErrIdentity
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, openFailure("connect", err)
	}
	success := false
	defer func() {
		if !success {
			pool.Close()
		}
	}()
	var valid bool
	err = pool.QueryRow(ctx, `SELECT current_database()=$1
  AND current_user=$2 AND session_user=current_user AND current_user='memory_runtime'
  AND NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls
  AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid)
  AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datname=current_database() AND datdba=r.oid)
  FROM pg_roles r WHERE r.rolname=current_user`, target.Database, target.Username).Scan(&valid)
	if err != nil {
		return nil, openFailure("target_query", err)
	}
	if !valid {
		return nil, ErrIdentity
	}
	var schema *string
	if err = pool.QueryRow(ctx, `SELECT to_regnamespace('runtime_memory')::text`).Scan(&schema); err != nil {
		return nil, openFailure("namespace_query", err)
	}
	if schema == nil {
		return nil, ErrPreparation
	}
	if err = pool.QueryRow(ctx, `SELECT current_schema()='runtime_memory' AND has_schema_privilege(current_user,'runtime_memory','USAGE') AND NOT has_schema_privilege(current_user,'runtime_memory','CREATE')`).Scan(&valid); err != nil || !valid {
		return nil, ErrIdentity
	}
	var table *string
	if err = pool.QueryRow(ctx, `SELECT to_regclass('runtime_memory.memory_heads')::text`).Scan(&table); err != nil {
		return nil, openFailure("table_query", err)
	}
	if table == nil {
		return nil, ErrPreparation
	}
	for _, table := range []string{"memory_heads", "memory_receipts"} {
		var tableName *string
		if err = pool.QueryRow(ctx, `SELECT to_regclass($1)::text`, "runtime_memory."+table).Scan(&tableName); err != nil || tableName == nil {
			return nil, ErrPreparation
		}
		if err = pool.QueryRow(ctx, `SELECT has_table_privilege(current_user,$1,'SELECT') AND has_table_privilege(current_user,$1,'INSERT') AND NOT has_table_privilege(current_user,$1,'DELETE,TRUNCATE,TRIGGER,REFERENCES')`, "runtime_memory."+table).Scan(&valid); err != nil || !valid {
			return nil, ErrIdentity
		}
	}
	if err = pool.QueryRow(ctx, `SELECT has_table_privilege(current_user,'runtime_memory.memory_heads','UPDATE') AND NOT has_table_privilege(current_user,'runtime_memory.memory_receipts','UPDATE')`).Scan(&valid); err != nil || !valid {
		return nil, ErrIdentity
	}
	success = true
	return &Postgres{pool: pool, capacityBytes: capacityBytes}, nil
}

func (s *Postgres) Close()                    { s.pool.Close() }
func openFailure(stage string, _ error) error { return fmt.Errorf("memory store open: %s", stage) }
