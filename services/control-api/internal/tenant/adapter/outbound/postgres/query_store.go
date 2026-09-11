package postgresadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

type tenantMembershipRecord struct {
	TenantID        string    `json:"tenant_id"`
	Slug            string    `json:"slug"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	TenantCreatedAt time.Time `json:"tenant_created_at"`
	TenantUpdatedAt time.Time `json:"tenant_updated_at"`
	MembershipID    string    `json:"membership_id"`
	UserID          string    `json:"user_id"`
	Role            string    `json:"role"`
	CreatedBy       string    `json:"created_by"`
	MembershipAt    time.Time `json:"membership_created_at"`
}

func (s *Store) ListMyTenants(
	ctx context.Context,
	userID string,
) ([]domain.TenantMembership, error) {
	const query = `
		SELECT COALESCE(jsonb_agg(
			jsonb_build_object(
				'tenant_id', t.id,
				'slug', t.slug,
				'name', t.name,
				'status', t.status,
				'tenant_created_at', t.created_at,
				'tenant_updated_at', t.updated_at,
				'membership_id', m.id,
				'user_id', m.user_id,
				'role', m.role,
				'created_by', m.created_by,
				'membership_created_at', m.created_at
			) ORDER BY t.created_at DESC, t.id
		), '[]'::jsonb)
		FROM tenant_memberships AS m
		JOIN tenants AS t ON t.id = m.tenant_id
		WHERE m.user_id = $1
	`
	var encoded []byte
	if err := s.db.QueryRow(ctx, query, userID).Scan(&encoded); err != nil {
		return nil, fmt.Errorf("query user tenants: %w", err)
	}
	var records []tenantMembershipRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return nil, fmt.Errorf("decode user tenants: %w", err)
	}
	result := make([]domain.TenantMembership, 0, len(records))
	for _, record := range records {
		result = append(result, domain.TenantMembership{
			Tenant: domain.Tenant{
				ID: record.TenantID, Slug: record.Slug, Name: record.Name,
				Status: domain.TenantStatus(record.Status), CreatedAt: record.TenantCreatedAt,
				UpdatedAt: record.TenantUpdatedAt,
			},
			Membership: domain.Membership{
				ID: record.MembershipID, TenantID: record.TenantID, UserID: record.UserID,
				Role: domain.MembershipRole(record.Role), CreatedBy: record.CreatedBy,
				CreatedAt: record.MembershipAt,
			},
		})
	}
	return result, nil
}

type tenantRecord struct {
	ID        string    `json:"id"`
	Slug      string    `json:"slug"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (s *Store) ListTenants(
	ctx context.Context,
	page application.Page,
) (application.TenantPage, error) {
	const query = `
		SELECT
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'id', p.id,
						'slug', p.slug,
						'name', p.name,
						'status', p.status,
						'created_at', p.created_at,
						'updated_at', p.updated_at
					) ORDER BY p.created_at DESC, p.id
				)
				FROM (
					SELECT id, slug, name, status, created_at, updated_at
					FROM tenants
					ORDER BY created_at DESC, id
					LIMIT $1 OFFSET $2
				) AS p
			), '[]'::jsonb),
			(SELECT count(*)::int FROM tenants)
	`
	var encoded []byte
	var total int
	if err := s.db.QueryRow(ctx, query, page.Limit, page.Offset).Scan(&encoded, &total); err != nil {
		return application.TenantPage{}, fmt.Errorf("query tenants: %w", err)
	}
	var records []tenantRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.TenantPage{}, fmt.Errorf("decode tenant page: %w", err)
	}
	tenants := make([]domain.Tenant, 0, len(records))
	for _, record := range records {
		tenants = append(tenants, domain.Tenant{
			ID: record.ID, Slug: record.Slug, Name: record.Name,
			Status: domain.TenantStatus(record.Status), CreatedAt: record.CreatedAt,
			UpdatedAt: record.UpdatedAt,
		})
	}
	return application.TenantPage{Tenants: tenants, Total: total}, nil
}

var _ application.Store = (*Store)(nil)
