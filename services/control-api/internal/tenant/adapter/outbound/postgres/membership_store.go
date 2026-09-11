package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

func (s *Store) GetMembership(
	ctx context.Context,
	tenantID, userID string,
) (domain.Membership, error) {
	const query = `
		SELECT id, tenant_id, user_id, role, created_by, created_at
		FROM tenant_memberships
		WHERE tenant_id = $1 AND user_id = $2
	`
	var membership domain.Membership
	var role string
	err := s.db.QueryRow(ctx, query, tenantID, userID).Scan(
		&membership.ID,
		&membership.TenantID,
		&membership.UserID,
		&role,
		&membership.CreatedBy,
		&membership.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Membership{}, application.ErrMembershipNotFound
	}
	if err != nil {
		return domain.Membership{}, fmt.Errorf("query membership: %w", err)
	}
	membership.Role = domain.MembershipRole(role)
	return membership, nil
}

func (s *Store) CreateMembership(ctx context.Context, membership domain.Membership) error {
	const statement = `
		INSERT INTO tenant_memberships (id, tenant_id, user_id, role, created_by, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := s.db.Exec(
		ctx,
		statement,
		membership.ID,
		membership.TenantID,
		membership.UserID,
		string(membership.Role),
		membership.CreatedBy,
		membership.CreatedAt,
	)
	if err != nil {
		if hasConstraint(err, "tenant_memberships_tenant_user_unique") {
			return application.ErrMembershipExists
		}
		return fmt.Errorf("insert membership: %w", err)
	}
	return nil
}

func (s *Store) DeleteMembership(ctx context.Context, tenantID, userID string) error {
	const statement = `DELETE FROM tenant_memberships WHERE tenant_id = $1 AND user_id = $2`
	tag, err := s.db.Exec(ctx, statement, tenantID, userID)
	if err != nil {
		return fmt.Errorf("delete membership: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return application.ErrMembershipNotFound
	}
	return nil
}

type membershipRecord struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	UserID    string    `json:"user_id"`
	Role      string    `json:"role"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) ListMembers(ctx context.Context, tenantID string) ([]domain.Membership, error) {
	const query = `
		SELECT COALESCE(jsonb_agg(
			jsonb_build_object(
				'id', id,
				'tenant_id', tenant_id,
				'user_id', user_id,
				'role', role,
				'created_by', created_by,
				'created_at', created_at
			) ORDER BY created_at, id
		), '[]'::jsonb)
		FROM tenant_memberships
		WHERE tenant_id = $1
	`
	var encoded []byte
	if err := s.db.QueryRow(ctx, query, tenantID).Scan(&encoded); err != nil {
		return nil, fmt.Errorf("query tenant members: %w", err)
	}
	var records []membershipRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return nil, fmt.Errorf("decode tenant members: %w", err)
	}
	members := make([]domain.Membership, 0, len(records))
	for _, record := range records {
		members = append(members, domain.Membership{
			ID: record.ID, TenantID: record.TenantID, UserID: record.UserID,
			Role: domain.MembershipRole(record.Role), CreatedBy: record.CreatedBy,
			CreatedAt: record.CreatedAt,
		})
	}
	return members, nil
}

func hasConstraint(err error, constraintName string) bool {
	var pgError *pgconn.PgError
	return errors.As(err, &pgError) && pgError.Code == "23505" &&
		pgError.ConstraintName == constraintName
}
