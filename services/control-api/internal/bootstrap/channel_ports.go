package bootstrap

import (
	"context"

	"github.com/jackc/pgx/v5"
	identitypostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/outbound/postgres"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/outbound/postgres"
)

// Composition combines two owner authorization adapters, without moving either
// owner's SQL into Channel or making Application depend on a database handle.
type channelTransactionAuthorizer struct{}

func (channelTransactionAuthorizer) AuthorizeOwner(ctx context.Context, tx pgx.Tx, tenant, user string) (bool, error) {
	ok, err := (identitypostgres.TransactionAuthorizer{}).AuthorizeSession(ctx, tx, user)
	if err != nil || !ok {
		return ok, err
	}
	return (tenantpostgres.TransactionAuthorizer{}).AuthorizeOwner(ctx, tx, tenant, user)
}
func (channelTransactionAuthorizer) AuthorizeActiveTenant(ctx context.Context, tx pgx.Tx, tenant string) (bool, error) {
	return (tenantpostgres.TransactionAuthorizer{}).AuthorizeActiveTenant(ctx, tx, tenant)
}

type channelTenantAccess struct{ members activeTenantMemberLookup }

func (a channelTenantAccess) IsActiveMember(ctx context.Context, tenant, user string) (bool, error) {
	identity, ok := identityapp.IdentityFromContext(ctx)
	if !ok || identity.UserID != user || identity.Restricted {
		return false, nil
	}
	return a.members.IsActiveMember(ctx, tenant, user)
}
func (a channelTenantAccess) IsActiveOwner(ctx context.Context, tenant, user string) (bool, error) {
	ok, err := a.IsActiveMember(ctx, tenant, user)
	if err != nil || !ok {
		return ok, err
	}
	return a.members.IsActiveOwner(ctx, tenant, user)
}

// AuthorizeRequester intentionally checks the durable user/tenant identity,
// not the browser Session that originally created a background diagnostic.
func (channelTransactionAuthorizer) AuthorizeRequester(ctx context.Context, tx pgx.Tx, tenant, user string) (bool, error) {
	ok, err := (identitypostgres.TransactionAuthorizer{}).AuthorizeActiveUser(ctx, tx, user)
	if err != nil || !ok {
		return ok, err
	}
	return (tenantpostgres.TransactionAuthorizer{}).AuthorizeOwner(ctx, tx, tenant, user)
}

// LockRequesterUsers locks every involved Identity before any Tenant lock;
// the Preflight adapter supplies a sorted, unique list. Status decisions remain
// in AuthorizeOwner/AuthorizeRequester after all identity locks are acquired.
func (channelTransactionAuthorizer) LockRequesterUsers(ctx context.Context, tx pgx.Tx, users []string) error {
	for _, user := range users {
		if _, err := (identitypostgres.TransactionAuthorizer{}).AuthorizeActiveUser(ctx, tx, user); err != nil {
			return err
		}
	}
	return nil
}
