// Package toolstore persists tool definitions and grants in MySQL. It
// implements domain/tool.Store; shared SQL helpers come from sqlutil.
package toolstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/sqlutil"
)

// toolDefJSON is the payload stored in the tools.definition JSON column.
type toolDefJSON struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// mysqlStore persists tools and grants in MySQL, per
// deployments/mysql/init/004_tools.sql.
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLRegistry returns a tool registry backed by MySQL. The caller owns
// the *sql.DB lifecycle.
func NewMySQLRegistry(db *sql.DB) *tool.Registry {
	return tool.NewRegistryWithStore(&mysqlStore{db: db})
}

// toolColumns lists the columns shared by Get and List.
const toolColumns = "tool_id, scope, tenant_id, name, definition, risk_level"

func (s *mysqlStore) Register(ctx context.Context, d tool.Definition) error {
	def, err := json.Marshal(toolDefJSON{Name: d.Name, Description: d.Description})
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO tools (tool_id, scope, tenant_id, name, definition, risk_level)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			scope = VALUES(scope),
			tenant_id = VALUES(tenant_id),
			name = VALUES(name),
			definition = VALUES(definition),
			risk_level = VALUES(risk_level),
			is_deleted = 0`,
		d.ID, normalizeScope(d.Scope), nullString(d.TenantID), d.Name, def, d.RiskLevel)
	return err
}

func (s *mysqlStore) Get(ctx context.Context, id string) (tool.Definition, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT "+toolColumns+" FROM tools WHERE tool_id = ? AND is_deleted = 0", id)
	return scanTool(row)
}

func (s *mysqlStore) List(ctx context.Context, tenantID string) ([]tool.Definition, error) {
	query := "SELECT " + toolColumns + " FROM tools WHERE is_deleted = 0"
	args := []any{}
	if tenantID != "" {
		query += " AND (scope = 'global' OR tenant_id = ?)"
		args = append(args, tenantID)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]tool.Definition, 0)
	for rows.Next() {
		d, err := scanTool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *mysqlStore) Grant(ctx context.Context, agentID, toolID string) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT IGNORE INTO agent_tool_grants (agent_id, tool_id) VALUES (?, ?)",
		agentID, toolID)
	return err
}

func (s *mysqlStore) Revoke(ctx context.Context, agentID, toolID string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM agent_tool_grants WHERE agent_id = ? AND tool_id = ?",
		agentID, toolID)
	return err
}

func (s *mysqlStore) IsAllowed(ctx context.Context, agentID, toolID string) (bool, error) {
	var n int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM agent_tool_grants WHERE agent_id = ? AND tool_id = ?",
		agentID, toolID).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *mysqlStore) Allowed(ctx context.Context, agentID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT tool_id FROM agent_tool_grants WHERE agent_id = ?", agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *mysqlStore) GrantedAgents(ctx context.Context, toolID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT agent_id FROM agent_tool_grants WHERE tool_id = ?", toolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func scanTool(rs sqlutil.RowScanner) (tool.Definition, error) {
	var (
		d        tool.Definition
		tenantID sql.NullString
		def      []byte
	)
	if err := rs.Scan(&d.ID, &d.Scope, &tenantID, &d.Name, &def, &d.RiskLevel); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return tool.Definition{}, tool.ErrNotFound
		}
		return tool.Definition{}, err
	}
	d.TenantID = tenantID.String
	var payload toolDefJSON
	if err := json.Unmarshal(def, &payload); err != nil {
		return tool.Definition{}, err
	}
	d.Name = payload.Name
	d.Description = payload.Description
	return d, nil
}

// normalizeScope maps an empty scope to "global" for storage.
func normalizeScope(scope string) string {
	if scope == "" {
		return tool.ScopeGlobal
	}
	return scope
}

// nullString converts "" to SQL NULL for nullable columns.
func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
