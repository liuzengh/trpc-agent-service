package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	identitydomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
)

func TestServiceGrantsOperatorOnlyToActiveAccount(t *testing.T) {
	operators := &operatorStoreStub{}
	accounts := &accountServiceStub{account: identitydomain.UserAccount{
		ID: "user-2", Username: "alice", Status: identitydomain.AccountStatusActive,
	}}
	now := time.Date(2026, time.August, 31, 16, 0, 0, 0, time.UTC)
	service := application.NewService(application.Dependencies{
		Operators: operators, Accounts: accounts, Tenants: tenantServiceStub{},
		Now: func() time.Time { return now },
	})

	grant, err := service.GrantOperator(context.Background(), "operator-1", "user-2")
	if err != nil {
		t.Fatalf("GrantOperator() error = %v", err)
	}
	if grant.GrantedByUserID != "operator-1" || operators.granted.UserID != "user-2" {
		t.Fatalf("grant = %#v, stored = %#v", grant, operators.granted)
	}
}

func TestServicePreservesLastOperatorError(t *testing.T) {
	operators := &operatorStoreStub{revokeError: application.ErrLastOperator}
	service := application.NewService(application.Dependencies{
		Operators: operators, Accounts: &accountServiceStub{}, Tenants: tenantServiceStub{},
	})

	err := service.RevokeOperator(context.Background(), "operator-1", "operator-1")
	if !errors.Is(err, application.ErrLastOperator) {
		t.Fatalf("RevokeOperator() error = %v", err)
	}
}

func TestServiceBootstrapsOnlyAnEmptyPlatform(t *testing.T) {
	operators := &operatorStoreStub{}
	accounts := &accountServiceStub{account: identitydomain.UserAccount{
		ID: "user-1", Username: "root", Status: identitydomain.AccountStatusActive,
	}}
	service := application.NewService(application.Dependencies{
		Operators: operators, Accounts: accounts, Tenants: tenantServiceStub{},
	})
	result, err := service.EnsureInitialOperator(context.Background(), application.EnsureInitialOperatorCommand{
		Username: "root", TemporaryPassword: "temporary-123",
	})
	if err != nil {
		t.Fatalf("EnsureInitialOperator() error = %v", err)
	}
	if !result.Created || operators.granted.GrantedByActorType != domain.GrantedBySystemBootstrap {
		t.Fatalf("result/grant = %#v/%#v", result, operators.granted)
	}
}

func TestServiceBootstrapIsIdempotentWhenOperatorExists(t *testing.T) {
	operators := &operatorStoreStub{grants: []domain.OperatorGrant{{UserID: "user-1"}}}
	accounts := &accountServiceStub{total: 1}
	service := application.NewService(application.Dependencies{
		Operators: operators, Accounts: accounts, Tenants: tenantServiceStub{},
	})
	result, err := service.EnsureInitialOperator(context.Background(), application.EnsureInitialOperatorCommand{})
	if err != nil {
		t.Fatalf("EnsureInitialOperator() error = %v", err)
	}
	if result.Created || result.UserID != "user-1" || operators.granted.UserID != "" {
		t.Fatalf("result/grant = %#v/%#v", result, operators.granted)
	}
}

func TestServiceBootstrapRequiresRecoveryForExistingAccounts(t *testing.T) {
	service := application.NewService(application.Dependencies{
		Operators: &operatorStoreStub{}, Accounts: &accountServiceStub{total: 1},
		Tenants: tenantServiceStub{},
	})
	_, err := service.EnsureInitialOperator(context.Background(), application.EnsureInitialOperatorCommand{})
	if !errors.Is(err, application.ErrBootstrapRecovery) {
		t.Fatalf("EnsureInitialOperator() error = %v", err)
	}
}

type operatorStoreStub struct {
	granted     domain.OperatorGrant
	grants      []domain.OperatorGrant
	revokeError error
}

func (s *operatorStoreStub) IsActiveOperator(context.Context, string) (bool, error) {
	return true, nil
}
func (s *operatorStoreStub) ListOperatorGrants(context.Context) ([]domain.OperatorGrant, error) {
	return s.grants, nil
}
func (s *operatorStoreStub) GrantOperator(_ context.Context, grant domain.OperatorGrant) error {
	s.granted = grant
	return nil
}
func (s *operatorStoreStub) RevokeOperator(context.Context, string, string, time.Time) error {
	return s.revokeError
}

type accountServiceStub struct {
	account identitydomain.UserAccount
	total   int
}

func (s *accountServiceStub) CreateManagedAccount(context.Context, identityapp.CreateManagedAccountCommand) (identitydomain.UserAccount, error) {
	return s.account, nil
}
func (s *accountServiceStub) GetAccount(context.Context, string) (identitydomain.UserAccount, error) {
	return s.account, nil
}
func (s *accountServiceStub) ListAccounts(context.Context, identityapp.Page) (identityapp.AccountPage, error) {
	return identityapp.AccountPage{Total: s.total}, nil
}

type tenantServiceStub struct{}

func (tenantServiceStub) ProvisionTenant(context.Context, tenantapp.ProvisionTenantCommand) (tenantapp.ProvisionTenantResult, error) {
	return tenantapp.ProvisionTenantResult{}, nil
}
func (tenantServiceStub) ListTenants(context.Context, tenantapp.Page) (tenantapp.TenantPage, error) {
	return tenantapp.TenantPage{}, nil
}
