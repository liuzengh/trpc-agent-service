// Package bindingstore persists IM channel bindings in MySQL. It implements
// infra/channels.BindingStore; shared SQL helpers come from sqlutil.
package bindingstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/sqlutil"
)

// mysqlStore persists channel bindings in MySQL. Deletes are physical
// (not the is_deleted soft flag in the DDL): bindings are low-frequency admin
// config, and a physical delete keeps the uk_channel_account unique key usable
// for re-binding the same account.
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLBindingStore returns a binding store over an existing connection.
func NewMySQLBindingStore(db *sql.DB) channels.BindingStore {
	return &mysqlStore{db: db}
}

const bindingCols = `binding_id, tenant_id, agent_id, channel, account_id, credential_ref, verification_token_ref, created_at`

func (s *mysqlStore) Create(ctx context.Context, b channels.ChannelBinding) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO channel_bindings (binding_id, tenant_id, agent_id, channel, account_id, credential_ref, verification_token_ref)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		b.BindingID, b.TenantID, b.AgentID, b.Channel, b.AccountID, b.CredentialRef, b.VerificationTokenRef)
	if err != nil {
		if sqlutil.IsDuplicate(err) {
			return channels.ErrBindingDuplicate
		}
		return fmt.Errorf("channels: insert binding: %w", err)
	}
	return nil
}

func (s *mysqlStore) List(ctx context.Context, tenantID, channel string) ([]channels.ChannelBinding, error) {
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
	out := make([]channels.ChannelBinding, 0)
	for rows.Next() {
		b, err := scanBinding(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

func (s *mysqlStore) Get(ctx context.Context, bindingID string) (*channels.ChannelBinding, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+bindingCols+` FROM channel_bindings WHERE binding_id = ?`, bindingID)
	b, err := scanBinding(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, channels.ErrBindingNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("channels: get binding: %w", err)
	}
	return b, nil
}

func (s *mysqlStore) Delete(ctx context.Context, bindingID string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM channel_bindings WHERE binding_id = ?`, bindingID)
	if err != nil {
		return fmt.Errorf("channels: delete binding: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return channels.ErrBindingNotFound
	}
	return nil
}

func scanBinding(row sqlutil.RowScanner) (*channels.ChannelBinding, error) {
	var b channels.ChannelBinding
	err := row.Scan(&b.BindingID, &b.TenantID, &b.AgentID, &b.Channel, &b.AccountID,
		&b.CredentialRef, &b.VerificationTokenRef, &b.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &b, nil
}
