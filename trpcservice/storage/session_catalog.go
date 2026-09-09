package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

// MySQLSessionCatalog enumerates platform session metadata for migration.
type MySQLSessionCatalog struct {
	db *sql.DB
}

// NewMySQLSessionCatalog constructs a migration catalog.
func NewMySQLSessionCatalog(db *sql.DB) (*MySQLSessionCatalog, error) {
	if db == nil {
		return nil, fmt.Errorf("session catalog database is required")
	}
	return &MySQLSessionCatalog{db: db}, nil
}

// ListSessionKeys implements SessionCatalog.
func (c *MySQLSessionCatalog) ListSessionKeys(
	ctx context.Context,
	appID string,
) ([]session.Key, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT a.app_name, s.user_ref, s.session_id
		FROM session s
		JOIN agent_app a ON a.app_id = s.app_id AND a.is_current = TRUE
		WHERE s.app_id = ?
		ORDER BY s.session_id`, appID)
	if err != nil {
		return nil, fmt.Errorf("list session catalog: %w", err)
	}
	defer rows.Close()
	var result []session.Key
	for rows.Next() {
		var key session.Key
		var userRef string
		if err := rows.Scan(&key.AppName, &userRef, &key.SessionID); err != nil {
			return nil, err
		}
		key.UserID = userRef
		if strings.Contains(key.SessionID, ":group:") {
			key.UserID = "group:" + userRef
		}
		result = append(result, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
