package application

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

var tenantSlugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,31}$`)

type ProvisionTenantCommand struct {
	Slug        string
	Name        string
	OwnerUserID string
	ActorUserID string
}

type ProvisionTenantResult struct {
	Tenant domain.Tenant
	Owner  domain.Membership
}

func (s *Service) ProvisionTenant(
	ctx context.Context,
	command ProvisionTenantCommand,
) (ProvisionTenantResult, error) {
	if err := s.validate(); err != nil {
		return ProvisionTenantResult{}, err
	}
	slug := strings.ToLower(strings.TrimSpace(command.Slug))
	name := strings.TrimSpace(command.Name)
	if !tenantSlugPattern.MatchString(slug) || name == "" || len(name) > 100 ||
		command.OwnerUserID == "" || command.ActorUserID == "" {
		return ProvisionTenantResult{}, ErrInvalidTenant
	}
	active, err := s.deps.Accounts.IsActiveAccount(ctx, command.OwnerUserID)
	if err != nil {
		return ProvisionTenantResult{}, fmt.Errorf("resolve owner account: %w", err)
	}
	if !active {
		return ProvisionTenantResult{}, ErrAccountUnavailable
	}
	tenantID, err := s.deps.NewTenantID()
	if err != nil {
		return ProvisionTenantResult{}, fmt.Errorf("generate tenant id: %w", err)
	}
	membershipID, err := s.deps.NewMembershipID()
	if err != nil {
		return ProvisionTenantResult{}, fmt.Errorf("generate owner membership id: %w", err)
	}
	now := s.deps.Now().UTC()
	tenant := domain.Tenant{
		ID: tenantID, Slug: slug, Name: name, Status: domain.TenantStatusActive,
		CreatedAt: now, UpdatedAt: now,
	}
	owner := domain.Membership{
		ID: membershipID, TenantID: tenantID, UserID: command.OwnerUserID,
		Role: domain.MembershipRoleOwner, CreatedBy: command.ActorUserID, CreatedAt: now,
	}
	if err := s.deps.Store.ProvisionTenant(ctx, tenant, owner); err != nil {
		if errors.Is(err, ErrTenantSlugTaken) {
			return ProvisionTenantResult{}, ErrTenantSlugTaken
		}
		return ProvisionTenantResult{}, fmt.Errorf("provision tenant: %w", err)
	}
	return ProvisionTenantResult{Tenant: tenant, Owner: owner}, nil
}
