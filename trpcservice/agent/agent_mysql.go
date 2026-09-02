package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	gosql "github.com/go-sql-driver/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/llm"
)

// mysqlStore persists agents and their immutable versions in MySQL, per
// deployments/mysql/init/003_agents.sql. agent_code has no Go-layer
// counterpart yet and is filled with agent_id; the `group` column keeps its
// ” default.
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLManager returns a manager persisting agents in MySQL (tables
// agents / agent_versions). The caller owns the *sql.DB lifecycle.
func NewMySQLManager(db *sql.DB, llmReg *llm.Registry) *Manager {
	return &Manager{store: &mysqlStore{db: db}, llmReg: llmReg}
}

// agentCols lists the agents columns shared by Get and List.
const agentCols = "agent_id, tenant_id, name, description, status, current_version"

// Create inserts a new agent row; agent_code carries the agent_id value.
func (s *mysqlStore) Create(ctx context.Context, a Agent) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO agents (agent_id, tenant_id, agent_code, name, description, status, current_version)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.TenantID, a.ID, a.Name, nullString(a.Description), a.Status, a.CurrentVersion)
	if isDuplicate(err) {
		return fmt.Errorf("agent: %q already exists", a.ID)
	}
	return err
}

func (s *mysqlStore) Get(ctx context.Context, id string) (*Agent, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+agentCols+` FROM agents WHERE agent_id = ? AND is_deleted = 0`, id)
	return scanAgent(row)
}

func (s *mysqlStore) List(ctx context.Context, tenantID string) ([]Agent, error) {
	query := `SELECT ` + agentCols + ` FROM agents WHERE is_deleted = 0`
	args := []any{}
	if tenantID != "" {
		query += ` AND tenant_id = ?`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]Agent, 0)
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// Update changes editable fields only; current_version is managed by
// publish/rollback. An empty status keeps the stored one.
func (s *mysqlStore) Update(ctx context.Context, a Agent) error {
	query := `UPDATE agents SET name = ?, description = ?`
	args := []any{a.Name, nullString(a.Description)}
	if a.Status != "" {
		query += `, status = ?`
		args = append(args, a.Status)
	}
	query += ` WHERE agent_id = ? AND is_deleted = 0`
	args = append(args, a.ID)
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return rowsAffectedNotFound(res, a.ID)
}

// Delete soft-deletes the agents row; version rows are kept for audit.
func (s *mysqlStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE agents SET is_deleted = 1 WHERE agent_id = ? AND is_deleted = 0`, id)
	if err != nil {
		return err
	}
	return rowsAffectedNotFound(res, id)
}

// Publish appends an immutable version and flips the agent to published in
// one transaction. The FOR UPDATE lock serializes concurrent publishes.
func (s *mysqlStore) Publish(ctx context.Context, id string, p RuntimeProfile) (int, error) {
	profile, err := json.Marshal(p)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	// Lock the row (and fail fast on a missing agent); the scanned value only
	// proves the row exists.
	var cur int
	if err := tx.QueryRowContext(ctx,
		`SELECT current_version FROM agents WHERE agent_id = ? AND is_deleted = 0 FOR UPDATE`,
		id).Scan(&cur); err != nil {
		return 0, mapNoRows(err)
	}

	var maxVersion int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM agent_versions WHERE agent_id = ?`,
		id).Scan(&maxVersion); err != nil {
		return 0, err
	}
	next := maxVersion + 1

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO agent_versions (agent_id, version, runtime_profile, status)
		 VALUES (?, ?, ?, 'published')`, id, next, profile); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE agents SET current_version = ?, status = 'published' WHERE agent_id = ? AND is_deleted = 0`,
		next, id); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// Rollback points current_version at an existing version. The target version
// is verified inside the transaction; RowsAffected is not used for existence
// because with clientFoundRows the DSN counts matched, not changed, rows.
func (s *mysqlStore) Rollback(ctx context.Context, id string, version int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	var cur int
	if err := tx.QueryRowContext(ctx,
		`SELECT current_version FROM agents WHERE agent_id = ? AND is_deleted = 0 FOR UPDATE`,
		id).Scan(&cur); err != nil {
		return mapNoRows(err)
	}

	var n int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agent_versions WHERE agent_id = ? AND version = ?`,
		id, version).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE agents SET current_version = ? WHERE agent_id = ? AND is_deleted = 0`,
		version, id); err != nil {
		return err
	}
	return tx.Commit()
}

// Resolve loads the current runtime profile in two steps: the agents row
// gives current_version, agent_versions holds the frozen JSON snapshot.
func (s *mysqlStore) Resolve(ctx context.Context, id string) (RuntimeProfile, error) {
	var cur int
	if err := s.db.QueryRowContext(ctx,
		`SELECT current_version FROM agents WHERE agent_id = ? AND is_deleted = 0`,
		id).Scan(&cur); err != nil {
		return RuntimeProfile{}, mapNoRows(err)
	}
	if cur < 1 {
		return RuntimeProfile{}, ErrNotFound // never published
	}
	var profile []byte
	if err := s.db.QueryRowContext(ctx,
		`SELECT runtime_profile FROM agent_versions WHERE agent_id = ? AND version = ?`,
		id, cur).Scan(&profile); err != nil {
		return RuntimeProfile{}, mapNoRows(err)
	}
	var p RuntimeProfile
	if err := json.Unmarshal(profile, &p); err != nil {
		return RuntimeProfile{}, err
	}
	return p, nil
}

// Versions lists the published versions ordered by version number.
func (s *mysqlStore) Versions(ctx context.Context, id string) ([]VersionInfo, error) {
	var cur int
	if err := s.db.QueryRowContext(ctx,
		`SELECT current_version FROM agents WHERE agent_id = ? AND is_deleted = 0`,
		id).Scan(&cur); err != nil {
		return nil, mapNoRows(err)
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT version FROM agent_versions WHERE agent_id = ? ORDER BY version`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]VersionInfo, 0)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, VersionInfo{Version: v, Status: StatusPublished})
	}
	return out, rows.Err()
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanAgent(sc scanner) (*Agent, error) {
	var (
		a           Agent
		description sql.NullString
	)
	if err := sc.Scan(&a.ID, &a.TenantID, &a.Name, &description, &a.Status, &a.CurrentVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	a.Description = description.String
	return &a, nil
}

// mapNoRows maps sql.ErrNoRows to the ErrNotFound sentinel.
func mapNoRows(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// nullString converts "" to SQL NULL for nullable columns.
func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func isDuplicate(err error) bool {
	var my *gosql.MySQLError
	return errors.As(err, &my) && my.Number == 1062
}

func rowsAffectedNotFound(res sql.Result, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return nil
}
