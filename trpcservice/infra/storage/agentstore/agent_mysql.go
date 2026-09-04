// Package agentstore persists agents and their immutable versions in MySQL.
// It implements domain/agent.Store; shared SQL helpers come from sqlutil.
package agentstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/sqlutil"
)

// mysqlStore persists agents in the agents / agent_versions tables (soft
// delete via is_deleted on agents only).
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLManager returns a manager persisting agents in MySQL (tables
// agents / agent_versions). The caller owns the *sql.DB lifecycle.
func NewMySQLManager(db *sql.DB, llmReg *llm.Registry) *agent.Manager {
	return agent.NewManagerWithStore(&mysqlStore{db: db}, llmReg)
}

// agentCols lists the agents columns shared by Get and List.
const agentCols = "agent_id, tenant_id, name, description, status, current_version"

func (s *mysqlStore) Create(ctx context.Context, a agent.Agent) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO agents (agent_id, tenant_id, agent_code, name, description, status, current_version)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.TenantID, a.ID, a.Name, sqlutil.Null(a.Description), a.Status, a.CurrentVersion)
	if sqlutil.IsDuplicate(err) {
		return fmt.Errorf("agent: %q already exists", a.ID)
	}
	return err
}

func (s *mysqlStore) Get(ctx context.Context, id string) (*agent.Agent, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+agentCols+` FROM agents WHERE agent_id = ? AND is_deleted = 0`, id)
	return scanAgent(row)
}

func (s *mysqlStore) List(ctx context.Context, tenantID string) ([]agent.Agent, error) {
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

	out := make([]agent.Agent, 0)
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func (s *mysqlStore) Update(ctx context.Context, a agent.Agent) error {
	query := `UPDATE agents SET name = ?, description = ?`
	args := []any{a.Name, sqlutil.Null(a.Description)}
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
	return sqlutil.RowsAffected(res, agent.ErrNotFound, a.ID)
}

func (s *mysqlStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE agents SET is_deleted = 1 WHERE agent_id = ? AND is_deleted = 0`, id)
	if err != nil {
		return err
	}
	return sqlutil.RowsAffected(res, agent.ErrNotFound, id)
}

func (s *mysqlStore) Publish(ctx context.Context, id string, p agent.RuntimeProfile) (int, error) {
	profile, err := json.Marshal(p)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	var cur int
	if err := tx.QueryRowContext(ctx,
		`SELECT current_version FROM agents WHERE agent_id = ? AND is_deleted = 0 FOR UPDATE`,
		id).Scan(&cur); err != nil {
		return 0, sqlutil.NoRows(err, agent.ErrNotFound)
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
		return sqlutil.NoRows(err, agent.ErrNotFound)
	}

	var n int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agent_versions WHERE agent_id = ? AND version = ?`,
		id, version).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return agent.ErrNotFound
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE agents SET current_version = ? WHERE agent_id = ? AND is_deleted = 0`,
		version, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *mysqlStore) Resolve(ctx context.Context, id string) (agent.RuntimeProfile, error) {
	var cur int
	if err := s.db.QueryRowContext(ctx,
		`SELECT current_version FROM agents WHERE agent_id = ? AND is_deleted = 0`,
		id).Scan(&cur); err != nil {
		return agent.RuntimeProfile{}, sqlutil.NoRows(err, agent.ErrNotFound)
	}
	if cur < 1 {
		return agent.RuntimeProfile{}, agent.ErrNotFound // never published
	}
	var profile []byte
	if err := s.db.QueryRowContext(ctx,
		`SELECT runtime_profile FROM agent_versions WHERE agent_id = ? AND version = ?`,
		id, cur).Scan(&profile); err != nil {
		return agent.RuntimeProfile{}, sqlutil.NoRows(err, agent.ErrNotFound)
	}
	var p agent.RuntimeProfile
	if err := json.Unmarshal(profile, &p); err != nil {
		return agent.RuntimeProfile{}, err
	}
	return p, nil
}

func (s *mysqlStore) Versions(ctx context.Context, id string) ([]agent.VersionInfo, error) {
	var cur int
	if err := s.db.QueryRowContext(ctx,
		`SELECT current_version FROM agents WHERE agent_id = ? AND is_deleted = 0`,
		id).Scan(&cur); err != nil {
		return nil, sqlutil.NoRows(err, agent.ErrNotFound)
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT version FROM agent_versions WHERE agent_id = ? ORDER BY version`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]agent.VersionInfo, 0)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, agent.VersionInfo{Version: v, Status: agent.StatusPublished})
	}
	return out, rows.Err()
}

func scanAgent(sc sqlutil.RowScanner) (*agent.Agent, error) {
	var (
		a           agent.Agent
		description sql.NullString
	)
	if err := sc.Scan(&a.ID, &a.TenantID, &a.Name, &description, &a.Status, &a.CurrentVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, agent.ErrNotFound
		}
		return nil, err
	}
	a.Description = description.String
	return &a, nil
}
