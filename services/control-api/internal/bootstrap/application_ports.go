package bootstrap

import (
	"context"
	"errors"

	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
	tenantdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

type activeAccountLookup struct {
	accounts *identityapp.AccountManagement
}

func (lookup activeAccountLookup) IsActiveAccount(ctx context.Context, userID string) (bool, error) {
	account, err := lookup.accounts.GetAccount(ctx, userID)
	if errors.Is(err, identityapp.ErrAccountNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return account.CanLogin(), nil
}

type activeTenantMemberLookup struct {
	tenants *tenantapp.Service
}

func (lookup activeTenantMemberLookup) IsActiveMember(
	ctx context.Context, tenantID, userID string,
) (bool, error) {
	_, err := lookup.tenants.GetTenant(ctx, tenantID, userID)
	if errors.Is(err, tenantapp.ErrTenantForbidden) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (lookup activeTenantMemberLookup) IsActiveOwner(ctx context.Context, tenantID, userID string) (bool, error) {
	membership, err := lookup.tenants.GetTenant(ctx, tenantID, userID)
	if errors.Is(err, tenantapp.ErrTenantForbidden) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return membership.Membership.Role == tenantdomain.MembershipRoleOwner, nil
}
