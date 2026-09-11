package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Target is the immutable postgres_state target in the RuntimeManifest.
// Schema is fixed by the V1 storage adapter, never supplied by input/DSN.
type Target struct {
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
		return nil, fmt.Errorf("positive session capacity is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, openFailure("parse_config", err)
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
			if strings.ReplaceAll(value, " ", "") != "runtime_session" && strings.ReplaceAll(value, " ", "") != "runtime_session,pg_temp" {
				return nil, ErrIdentity
			}
		case "application_name":
		default:
			return nil, ErrIdentity
		}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = "runtime_session,pg_temp"
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
  AND current_user=$2 AND session_user=current_user AND current_user='session_runtime'
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
	if err = pool.QueryRow(ctx, `SELECT to_regnamespace('runtime_session')::text`).Scan(&schema); err != nil {
		return nil, openFailure("namespace_query", err)
	}
	if schema == nil {
		return nil, ErrPreparation
	}
	if err = pool.QueryRow(ctx, `SELECT current_schema()='runtime_session' AND has_schema_privilege(current_user,'runtime_session','USAGE') AND NOT has_schema_privilege(current_user,'runtime_session','CREATE')`).Scan(&valid); err != nil || !valid {
		return nil, ErrIdentity
	}
	var table *string
	if err = pool.QueryRow(ctx, `SELECT to_regclass('runtime_session.session_candidates')::text`).Scan(&table); err != nil {
		return nil, openFailure("table_query", err)
	}
	if table == nil {
		return nil, ErrPreparation
	}
	if err = pool.QueryRow(ctx, `SELECT has_table_privilege(current_user,'runtime_session.session_candidates','SELECT') AND has_table_privilege(current_user,'runtime_session.session_candidates','INSERT')
  AND NOT has_table_privilege(current_user,'runtime_session.session_candidates','UPDATE,DELETE,TRUNCATE,TRIGGER,REFERENCES,MAINTAIN')`).Scan(&valid); err != nil || !valid {
		return nil, openFailure("candidate_acl", err)
	}
	success = true
	return &Postgres{pool: pool, capacityBytes: capacityBytes}, nil
}

func (s *Postgres) Close() { s.pool.Close() }

func (s *Postgres) Put(ctx context.Context, c Candidate) (Head, error) {
	body, head, err := c.Encode(s.capacityBytes)
	if err != nil {
		return Head{}, err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO runtime_session.session_candidates
  (candidate_ref,tenant_id,session_id,run_id,attempt_id,parent_ref,parent_digest,content_version,content_digest,content)
  VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING`, head.Ref, c.Identity.TenantID, c.Identity.SessionID, c.Identity.RunID, c.Identity.AttemptID, c.Parent.Ref, c.Parent.Digest, c.ContentVersion, head.Digest, body)
	if err != nil {
		return Head{}, fmt.Errorf("session candidate write: %w", err)
	}
	// A concurrent insertion, retry after uncertain response, and ordinary replay
	// all follow the same read-and-compare path. No UPDATE privilege is needed.
	var actual []byte
	var digest string
	err = s.pool.QueryRow(ctx, `SELECT content,content_digest FROM runtime_session.session_candidates WHERE tenant_id=$1 AND session_id=$2 AND run_id=$3 AND attempt_id=$4`, c.Identity.TenantID, c.Identity.SessionID, c.Identity.RunID, c.Identity.AttemptID).Scan(&actual, &digest)
	if err != nil {
		return Head{}, fmt.Errorf("session candidate durability check: %w", err)
	}
	if digest != head.Digest || string(actual) != string(body) {
		return Head{}, ErrConflict
	}
	return head, nil
}

func (s *Postgres) Load(ctx context.Context, tenantID, sessionID string, head Head) (Candidate, error) {
	if err := head.Validate(); err != nil {
		return Candidate{}, err
	}
	if head.Ref == "" {
		return Candidate{}, ErrNotFound
	}
	var body []byte
	var digest string
	err := s.pool.QueryRow(ctx, `SELECT content,content_digest FROM runtime_session.session_candidates WHERE tenant_id=$1 AND session_id=$2 AND candidate_ref=$3`, tenantID, sessionID, head.Ref).Scan(&body, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return Candidate{}, ErrNotFound
	}
	if err != nil {
		return Candidate{}, fmt.Errorf("session candidate read: %w", err)
	}
	if digest != head.Digest {
		return Candidate{}, ErrCorrupt
	}
	return Decode(body, tenantID, sessionID, head, s.capacityBytes)
}
