package channels

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// mysqlBindingStore persists channel bindings in MySQL. Deletes are physical
// (not the is_deleted soft flag in the DDL): bindings are low-frequency admin
// config, and a physical delete keeps the uk_channel_account unique key usable
// for re-binding the same account.
type mysqlBindingStore struct {
	db *sql.DB
}

// NewMySQLBindingStore returns a binding store over an existing connection.
func NewMySQLBindingStore(db *sql.DB) BindingStore {
	return &mysqlBindingStore{db: db}
}

const bindingCols = `binding_id, tenant_id, agent_id, channel, account_id, credential_ref, created_at`

func (s *mysqlBindingStore) Create(ctx context.Context, b ChannelBinding) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO channel_bindings (binding_id, tenant_id, agent_id, channel, account_id, credential_ref)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		b.BindingID, b.TenantID, b.AgentID, b.Channel, b.AccountID, b.CredentialRef)
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") {
			return ErrBindingDuplicate
		}
		return fmt.Errorf("channels: insert binding: %w", err)
	}
	return nil
}

func (s *mysqlBindingStore) List(ctx context.Context, tenantID, channel string) ([]ChannelBinding, error) {
	query := `SELECT ` + bindingCols + ` FROM channel_bindings WHERE 1=1`
	args := []any{}
	if tenantID != "" {
		query += ` AND tenant_id = ?`
		args = append(args, tenantID)
	}
	if channel != "" {
		query += ` AND channel = ?`
		args = append(args, channel)
	}
	query += ` ORDER BY created_at`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("channels: list bindings: %w", err)
	}
	defer rows.Close()
	out := make([]ChannelBinding, 0)
	for rows.Next() {
		b, err := scanBinding(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

func (s *mysqlBindingStore) Get(ctx context.Context, bindingID string) (*ChannelBinding, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+bindingCols+` FROM channel_bindings WHERE binding_id = ?`, bindingID)
	b, err := scanBinding(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBindingNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("channels: get binding: %w", err)
	}
	return b, nil
}

func (s *mysqlBindingStore) Delete(ctx context.Context, bindingID string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM channel_bindings WHERE binding_id = ?`, bindingID)
	if err != nil {
		return fmt.Errorf("channels: delete binding: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrBindingNotFound
	}
	return nil
}

// rowScanner mirrors sql.Row / sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanBinding(row rowScanner) (*ChannelBinding, error) {
	var b ChannelBinding
	err := row.Scan(&b.BindingID, &b.TenantID, &b.AgentID, &b.Channel, &b.AccountID,
		&b.CredentialRef, &b.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &b, nil
}
