package member

import (
	"context"
	"database/sql"
	"fmt"
)

type mysqlStore struct {
	db *sql.DB
}

func NewMySQLStore(db *sql.DB) Store {
	return &mysqlStore{db: db}
}

func (s *mysqlStore) Create(ctx context.Context, m *Member) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tenant_members (tenant_id, user_id, role, password_hash)
		VALUES (?, ?, ?, ?)`,
		m.TenantID, m.UserID, m.Role, m.Password)
	if err != nil {
		return fmt.Errorf("create member: %w", err)
	}
	return nil
}

func (s *mysqlStore) Get(ctx context.Context, tenantID, userID string) (*Member, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT tenant_id, user_id, role, password_hash, created_at FROM tenant_members WHERE tenant_id = ? AND user_id = ?`,
		tenantID, userID)
	var m Member
	if err := row.Scan(&m.TenantID, &m.UserID, &m.Role, &m.Password, &m.CreatedAt); err != nil {
		return nil, fmt.Errorf("get member: %w", err)
	}
	return &m, nil
}

func (s *mysqlStore) GetByUserID(ctx context.Context, userID string) (*Member, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT tenant_id, user_id, role, password_hash, created_at FROM tenant_members WHERE user_id = ?`,
		userID)
	var m Member
	if err := row.Scan(&m.TenantID, &m.UserID, &m.Role, &m.Password, &m.CreatedAt); err != nil {
		return nil, fmt.Errorf("get member by user id: %w", err)
	}
	return &m, nil
}

func (s *mysqlStore) List(ctx context.Context, tenantID string) ([]*Member, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant_id, user_id, role, password_hash, created_at FROM tenant_members WHERE tenant_id = ? ORDER BY user_id`,
		tenantID)
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	defer rows.Close()
	var result []*Member
	for rows.Next() {
		var m Member
		if err := rows.Scan(&m.TenantID, &m.UserID, &m.Role, &m.Password, &m.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, &m)
	}
	return result, rows.Err()
}

func (s *mysqlStore) UpdatePassword(ctx context.Context, tenantID, userID, passwordHash string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE tenant_members SET password_hash = ? WHERE tenant_id = ? AND user_id = ?`,
		passwordHash, tenantID, userID)
	if err != nil {
		return fmt.Errorf("update member password: %w", err)
	}
	return nil
}

func (s *mysqlStore) UpdateRole(ctx context.Context, tenantID, userID, role string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE tenant_members SET role = ? WHERE tenant_id = ? AND user_id = ?`,
		role, tenantID, userID)
	if err != nil {
		return fmt.Errorf("update role: %w", err)
	}
	return nil
}

func (s *mysqlStore) Delete(ctx context.Context, tenantID, userID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM tenant_members WHERE tenant_id = ? AND user_id = ?`,
		tenantID, userID)
	if err != nil {
		return fmt.Errorf("delete member: %w", err)
	}
	return nil
}
