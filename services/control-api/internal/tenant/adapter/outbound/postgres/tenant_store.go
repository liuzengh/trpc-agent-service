package postgresadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

// DB is the PostgreSQL surface required by Tenant persistence.
type DB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// Store persists tenants and memberships.
type Store struct {
	db DB
}

func NewStore(db DB) *Store {
	return &Store{db: db}
}

func (s *Store) ProvisionTenant(
	ctx context.Context,
	tenant domain.Tenant,
	owner domain.Membership,
) error {
	const statement = `
		WITH inserted_tenant AS (
			INSERT INTO tenants (id, slug, name, status, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id
		)
		INSERT INTO tenant_memberships (
			id, tenant_id, user_id, role, created_by, created_at
		)
		SELECT $7, id, $8, $9, $10, $11 FROM inserted_tenant
	`
	_, err := s.db.Exec(
		ctx,
		statement,
		tenant.ID,
		tenant.Slug,
		tenant.Name,
		string(tenant.Status),
		tenant.CreatedAt,
		tenant.UpdatedAt,
		owner.ID,
		owner.UserID,
		string(owner.Role),
		owner.CreatedBy,
		owner.CreatedAt,
	)
	if err != nil {
		if hasConstraint(err, "tenants_slug_unique") {
			return application.ErrTenantSlugTaken
		}
		return fmt.Errorf("insert tenant and owner: %w", err)
	}
	return nil
}

func (s *Store) GetTenantMembership(
	ctx context.Context,
	tenantID, userID string,
) (domain.TenantMembership, error) {
	const query = `
		SELECT
			t.id, t.slug, t.name, t.status, t.created_at, t.updated_at,
			m.id, m.user_id, m.role, m.created_by, m.created_at
		FROM tenants AS t
		JOIN tenant_memberships AS m ON m.tenant_id = t.id
		WHERE t.id = $1 AND m.user_id = $2
	`
	var state domain.TenantMembership
	var tenantStatus, membershipRole string
	err := s.db.QueryRow(ctx, query, tenantID, userID).Scan(
		&state.Tenant.ID,
		&state.Tenant.Slug,
		&state.Tenant.Name,
		&tenantStatus,
		&state.Tenant.CreatedAt,
		&state.Tenant.UpdatedAt,
		&state.Membership.ID,
		&state.Membership.UserID,
		&membershipRole,
		&state.Membership.CreatedBy,
		&state.Membership.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.TenantMembership{}, application.ErrMembershipNotFound
	}
	if err != nil {
		return domain.TenantMembership{}, fmt.Errorf("query tenant membership: %w", err)
	}
	state.Tenant.Status = domain.TenantStatus(tenantStatus)
	state.Membership.TenantID = state.Tenant.ID
	state.Membership.Role = domain.MembershipRole(membershipRole)
	return state, nil
}
