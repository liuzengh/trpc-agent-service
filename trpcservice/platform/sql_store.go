package platform

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

type SQLStore struct {
	db      *sql.DB
	backend string
}

func NewSQLiteStore(path string) (*SQLStore, error) {
	if path == "" {
		path = ":memory:"
	}
	store, err := newSQLStore("sqlite", path, "sqlite")
	if err == nil {
		_, _ = store.db.Exec("PRAGMA busy_timeout = 5000")
	}
	return store, err
}

func NewPostgresStore(dsn string) (*SQLStore, error) { return newSQLStore("pgx", dsn, "postgres") }

func newSQLStore(driver, dsn, backend string) (*SQLStore, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	s := &SQLStore{db: db, backend: backend}
	if err := s.init(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLStore) q(query string) string {
	if s.backend != "postgres" {
		return query
	}
	n := 1
	var b strings.Builder
	for _, r := range query {
		if r == '?' {
			fmt.Fprintf(&b, "$%d", n)
			n++
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
func (s *SQLStore) init(ctx context.Context) error {
	var schemaTx *sql.Tx
	execer := interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}(s.db)
	if s.backend == "postgres" {
		var err error
		schemaTx, err = s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer schemaTx.Rollback()
		// CREATE TABLE IF NOT EXISTS is not safe when multiple PostgreSQL
		// sessions create the same relation concurrently. Serialize the small
		// compatibility bootstrap used by independently starting Gateways.
		if _, err := schemaTx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(0x5452504353544f52)); err != nil {
			return err
		}
		execer = schemaTx
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS audit_events (tenant_id TEXT NOT NULL, audit_id TEXT NOT NULL, occurred_at TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(tenant_id,audit_id))`,
		`CREATE TABLE IF NOT EXISTS session_write_fences (tenant_id TEXT NOT NULL, session_id TEXT NOT NULL, fencing_token BIGINT NOT NULL, PRIMARY KEY(tenant_id,session_id))`,
		`CREATE TABLE IF NOT EXISTS session_events (tenant_id TEXT NOT NULL, session_id TEXT NOT NULL, sequence BIGINT NOT NULL, event_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, event_type TEXT NOT NULL, payload BYTEA NOT NULL, occurred_at TEXT NOT NULL, fencing_token BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (tenant_id, session_id, sequence), UNIQUE (tenant_id, session_id, idempotency_key))`,
		`CREATE TABLE IF NOT EXISTS session_memory (tenant_id TEXT NOT NULL, session_id TEXT NOT NULL, memory_key TEXT NOT NULL, memory_id TEXT NOT NULL, value TEXT NOT NULL, updated_at TEXT NOT NULL, fencing_token BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (tenant_id, session_id, memory_key))`,
		`CREATE TABLE IF NOT EXISTS artifacts (tenant_id TEXT NOT NULL, artifact_id TEXT NOT NULL, session_id TEXT NOT NULL, name TEXT NOT NULL, content_reference TEXT NOT NULL, request_id TEXT NOT NULL DEFAULT '', trace_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'published', created_at TEXT NOT NULL, fencing_token BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (tenant_id, artifact_id))`,
		`CREATE TABLE IF NOT EXISTS knowledge_records (tenant_id TEXT NOT NULL, knowledge_id TEXT NOT NULL, agent_app_id TEXT NOT NULL, source TEXT NOT NULL, content TEXT NOT NULL, index_status TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (tenant_id, knowledge_id))`,
	}
	if s.backend == "sqlite" {
		statements[2] = strings.Replace(statements[2], "BYTEA", "BLOB", 1)
	}
	for _, stmt := range statements {
		if _, err := execer.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if s.backend == "postgres" {
		if _, err := execer.ExecContext(ctx, `ALTER TABLE session_events ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0`); err != nil {
			return err
		}
		for _, statement := range []string{
			`ALTER TABLE session_memory ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS trace_id TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'published'`,
			`ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0`,
		} {
			if _, err := execer.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return schemaTx.Commit()
	} else {
		if err := s.ensureSQLiteColumn(ctx, "session_memory", "fencing_token", "BIGINT NOT NULL DEFAULT 0"); err != nil {
			return err
		}
		for name, definition := range map[string]string{"request_id": "TEXT NOT NULL DEFAULT ''", "trace_id": "TEXT NOT NULL DEFAULT ''", "status": "TEXT NOT NULL DEFAULT 'published'", "fencing_token": "BIGINT NOT NULL DEFAULT 0"} {
			if err := s.ensureSQLiteColumn(ctx, "artifacts", name, definition); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SQLStore) ensureSQLiteColumn(ctx context.Context, table, name, definition string) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid int
		var column, typ string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &column, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		found = found || column == name
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = s.db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+name+" "+definition)
	return err
}

func (s *SQLStore) GetSession(ctx context.Context, tenant, session string) (Session, error) {
	state, err := s.GetSessionState(ctx, tenant, session)
	return state.Session, err
}
func (s *SQLStore) GetSessionState(ctx context.Context, tenant, session string) (SessionState, error) {
	events, err := s.ListSessionEvents(ctx, tenant, session, 0)
	if err != nil {
		return SessionState{}, err
	}
	return materializeSession(tenant, session, events)
}
func (s *SQLStore) AppendSessionEvent(ctx context.Context, event SessionEvent) error {
	if event.Payload == nil {
		event.Payload = []byte{}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.TenantID == "" || event.SessionID == "" || event.IdempotencyKey == "" {
		return errors.New("platform: invalid session event")
	}
	for attempts := 0; attempts < 5; attempts++ {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		if err = s.validateFencingToken(ctx, tx, event.TenantID, event.SessionID, event.FencingToken); err != nil {
			tx.Rollback()
			if isPostgresRetryable(err) {
				continue
			}
			return err
		}
		var typ string
		var payload []byte
		err = tx.QueryRowContext(ctx, s.q(`SELECT event_type,payload FROM session_events WHERE tenant_id=? AND session_id=? AND idempotency_key=?`), event.TenantID, event.SessionID, event.IdempotencyKey).Scan(&typ, &payload)
		if err == nil {
			tx.Rollback()
			if typ == event.Type && string(payload) == string(event.Payload) {
				return nil
			}
			return ErrDuplicateEvent
		}
		if !errors.Is(err, sql.ErrNoRows) {
			tx.Rollback()
			return err
		}
		var sequence uint64
		if err = tx.QueryRowContext(ctx, s.q(`SELECT COALESCE(MAX(sequence),0)+1 FROM session_events WHERE tenant_id=? AND session_id=?`), event.TenantID, event.SessionID).Scan(&sequence); err != nil {
			tx.Rollback()
			return err
		}
		if event.ID == "" {
			event.ID = event.TenantID + ":" + event.SessionID + ":" + itoa(sequence)
		}
		if event.OccurredAt.IsZero() {
			event.OccurredAt = time.Now().UTC()
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO session_events(tenant_id,session_id,sequence,event_id,idempotency_key,event_type,payload,occurred_at,fencing_token) VALUES(?,?,?,?,?,?,?,?,?)`), event.TenantID, event.SessionID, sequence, event.ID, event.IdempotencyKey, event.Type, event.Payload, event.OccurredAt.Format(time.RFC3339Nano), event.FencingToken)
		if err != nil {
			tx.Rollback()
			continue
		}
		if err = tx.Commit(); err == nil {
			return nil
		}
	}
	return errors.New("platform: concurrent append retry exhausted")
}
func (s *SQLStore) ListSessionEvents(ctx context.Context, tenant, session string, after uint64) ([]SessionEvent, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT event_id,sequence,idempotency_key,event_type,payload,occurred_at,fencing_token FROM session_events WHERE tenant_id=? AND session_id=? AND sequence>? ORDER BY sequence`), tenant, session, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []SessionEvent{}
	for rows.Next() {
		var e SessionEvent
		var occurred string
		e.TenantID = tenant
		e.SessionID = session
		if err := rows.Scan(&e.ID, &e.Sequence, &e.IdempotencyKey, &e.Type, &e.Payload, &occurred, &e.FencingToken); err != nil {
			return nil, err
		}
		e.OccurredAt, _ = time.Parse(time.RFC3339Nano, occurred)
		items = append(items, e)
	}
	return items, rows.Err()
}
func (s *SQLStore) ListMemory(ctx context.Context, tenant, session string) ([]MemoryRecord, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT memory_id,memory_key,value,updated_at,fencing_token FROM session_memory WHERE tenant_id=? AND session_id=? ORDER BY memory_key`), tenant, session)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []MemoryRecord{}
	for rows.Next() {
		var m MemoryRecord
		var updated string
		m.TenantID = tenant
		m.SessionID = session
		if err := rows.Scan(&m.ID, &m.Key, &m.Value, &updated, &m.FencingToken); err != nil {
			return nil, err
		}
		m.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
		items = append(items, m)
	}
	return items, rows.Err()
}
func (s *SQLStore) PutMemory(ctx context.Context, m MemoryRecord) error {
	if m.ID == "" {
		m.ID = m.TenantID + ":" + m.SessionID + ":" + m.Key
	}
	if m.UpdatedAt.IsZero() {
		m.UpdatedAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.validateFencingToken(ctx, tx, m.TenantID, m.SessionID, m.FencingToken); err != nil {
		return err
	}
	query := `INSERT INTO session_memory(tenant_id,session_id,memory_key,memory_id,value,updated_at,fencing_token) VALUES(?,?,?,?,?,?,?) ON CONFLICT(tenant_id,session_id,memory_key) DO UPDATE SET memory_id=excluded.memory_id,value=excluded.value,updated_at=excluded.updated_at,fencing_token=excluded.fencing_token`
	if _, err := tx.ExecContext(ctx, s.q(query), m.TenantID, m.SessionID, m.Key, m.ID, m.Value, m.UpdatedAt.Format(time.RFC3339Nano), m.FencingToken); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLStore) validateFencingToken(ctx context.Context, tx *sql.Tx, tenantID, sessionID string, token uint64) error {
	if token == 0 {
		return nil
	}
	// PostgreSQL deployments may share the authoritative execution lease table.
	// Independently routed databases still fence using their local generation.
	if importingMigration(ctx) {
		_, err := tx.ExecContext(ctx, s.q(`INSERT INTO session_write_fences(tenant_id,session_id,fencing_token) VALUES(?,?,?) ON CONFLICT(tenant_id,session_id) DO UPDATE SET fencing_token=CASE WHEN session_write_fences.fencing_token<excluded.fencing_token THEN excluded.fencing_token ELSE session_write_fences.fencing_token END`), tenantID, sessionID, token)
		return err
	}
	if s.backend == "postgres" {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT to_regclass('session_execution_leases') IS NOT NULL`).Scan(&exists); err != nil {
			return err
		}
		if exists {
			var current uint64
			err := tx.QueryRowContext(ctx, `SELECT fencing_token FROM session_execution_leases WHERE tenant_id=$1 AND session_id=$2 FOR SHARE`, tenantID, sessionID).Scan(&current)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil && current != token {
				return ErrStaleFencingToken
			}
		}
	}
	var current uint64
	err := tx.QueryRowContext(ctx, s.q(`INSERT INTO session_write_fences(tenant_id,session_id,fencing_token) VALUES(?,?,?) ON CONFLICT(tenant_id,session_id) DO UPDATE SET fencing_token=excluded.fencing_token WHERE session_write_fences.fencing_token<=excluded.fencing_token RETURNING fencing_token`), tenantID, sessionID, token).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrStaleFencingToken
	}
	return err
}
func (s *SQLStore) Health(ctx context.Context) BackendHealth {
	status := "healthy"
	if err := s.db.PingContext(ctx); err != nil {
		status = "unavailable"
	}
	return BackendHealth{Backend: s.backend, Status: status, Checked: time.Now().UTC()}
}
func (s *SQLStore) Close() error { return s.db.Close() }
