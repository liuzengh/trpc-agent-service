// Package application implements platform-level administration use cases.
package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	identitydomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
)

var (
	ErrAdminForbidden       = errors.New("platform administration forbidden")
	ErrOperatorNotFound     = errors.New("platform operator not found")
	ErrLastOperator         = errors.New("last platform operator cannot be revoked")
	ErrBootstrapRecovery    = errors.New("bootstrap recovery is required")
	ErrBootstrapUnsupported = errors.New("bootstrap mode is unsupported")
)

type OperatorStore interface {
	IsActiveOperator(context.Context, string) (bool, error)
	ListOperatorGrants(context.Context) ([]domain.OperatorGrant, error)
	GrantOperator(context.Context, domain.OperatorGrant) error
	RevokeOperator(context.Context, string, string, time.Time) error
}

type AccountService interface {
	CreateManagedAccount(context.Context, identityapp.CreateManagedAccountCommand) (identitydomain.UserAccount, error)
	GetAccount(context.Context, string) (identitydomain.UserAccount, error)
	ListAccounts(context.Context, identityapp.Page) (identityapp.AccountPage, error)
}

type TenantService interface {
	ProvisionTenant(context.Context, tenantapp.ProvisionTenantCommand) (tenantapp.ProvisionTenantResult, error)
	ListTenants(context.Context, tenantapp.Page) (tenantapp.TenantPage, error)
}

type Dependencies struct {
	Operators OperatorStore
	Accounts  AccountService
	Tenants   TenantService
	Now       func() time.Time
}

type Service struct {
	deps Dependencies
}

func NewService(deps Dependencies) *Service {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Service{deps: deps}
}

func (s *Service) IsOperator(ctx context.Context, userID string) (bool, error) {
	return s.deps.Operators.IsActiveOperator(ctx, userID)
}

type EnsureInitialOperatorCommand struct {
	Username          string
	DisplayName       string
	TemporaryPassword string
}

type EnsureInitialOperatorResult struct {
	Created bool
	UserID  string
}

// EnsureInitialOperator is the deliberately small V1 startup path. Process
// bootstrap serializes it with a PostgreSQL advisory lock.
func (s *Service) EnsureInitialOperator(
	ctx context.Context,
	command EnsureInitialOperatorCommand,
) (EnsureInitialOperatorResult, error) {
	grants, err := s.deps.Operators.ListOperatorGrants(ctx)
	if err != nil {
		return EnsureInitialOperatorResult{}, fmt.Errorf("inspect operator bootstrap state: %w", err)
	}
	if len(grants) > 0 {
		return EnsureInitialOperatorResult{UserID: grants[0].UserID}, nil
	}
	accounts, err := s.deps.Accounts.ListAccounts(ctx, identityapp.Page{Limit: 1})
	if err != nil {
		return EnsureInitialOperatorResult{}, fmt.Errorf("inspect account bootstrap state: %w", err)
	}
	if accounts.Total > 0 {
		return EnsureInitialOperatorResult{}, ErrBootstrapRecovery
	}
	account, err := s.deps.Accounts.CreateManagedAccount(ctx, identityapp.CreateManagedAccountCommand{
		Username: command.Username, DisplayName: command.DisplayName,
		TemporaryPassword: command.TemporaryPassword,
	})
	if err != nil {
		return EnsureInitialOperatorResult{}, fmt.Errorf("create bootstrap account: %w", err)
	}
	grant := domain.OperatorGrant{
		UserID: account.ID, GrantedByActorType: domain.GrantedBySystemBootstrap,
		GrantedAt: s.deps.Now().UTC(),
	}
	if err := s.deps.Operators.GrantOperator(ctx, grant); err != nil {
		return EnsureInitialOperatorResult{}, fmt.Errorf("grant bootstrap operator: %w", err)
	}
	return EnsureInitialOperatorResult{Created: true, UserID: account.ID}, nil
}

type OperatorView struct {
	Grant       domain.OperatorGrant
	Username    string
	DisplayName string
}

func (s *Service) ListOperators(ctx context.Context) ([]OperatorView, error) {
	grants, err := s.deps.Operators.ListOperatorGrants(ctx)
	if err != nil {
		return nil, fmt.Errorf("list operator grants: %w", err)
	}
	operators := make([]OperatorView, 0, len(grants))
	for _, grant := range grants {
		account, err := s.deps.Accounts.GetAccount(ctx, grant.UserID)
		if err != nil {
			return nil, fmt.Errorf("get operator account: %w", err)
		}
		operators = append(operators, OperatorView{
			Grant: grant, Username: account.Username, DisplayName: account.DisplayName,
		})
	}
	return operators, nil
}

func (s *Service) GrantOperator(
	ctx context.Context,
	actorUserID, targetUserID string,
) (domain.OperatorGrant, error) {
	account, err := s.deps.Accounts.GetAccount(ctx, targetUserID)
	if err != nil {
		return domain.OperatorGrant{}, err
	}
	if !account.CanLogin() {
		return domain.OperatorGrant{}, identityapp.ErrAccountNotFound
	}
	grant := domain.OperatorGrant{
		UserID: targetUserID, GrantedByActorType: domain.GrantedByUser,
		GrantedByUserID: actorUserID, GrantedAt: s.deps.Now().UTC(),
	}
	if err := s.deps.Operators.GrantOperator(ctx, grant); err != nil {
		return domain.OperatorGrant{}, fmt.Errorf("grant platform operator: %w", err)
	}
	return grant, nil
}

func (s *Service) RevokeOperator(ctx context.Context, actorUserID, targetUserID string) error {
	if err := s.deps.Operators.RevokeOperator(
		ctx, targetUserID, actorUserID, s.deps.Now().UTC(),
	); err != nil {
		return err
	}
	return nil
}

func (s *Service) CreateUser(
	ctx context.Context,
	command identityapp.CreateManagedAccountCommand,
) (identitydomain.UserAccount, error) {
	return s.deps.Accounts.CreateManagedAccount(ctx, command)
}

func (s *Service) ListUsers(
	ctx context.Context,
	page identityapp.Page,
) (identityapp.AccountPage, error) {
	return s.deps.Accounts.ListAccounts(ctx, page)
}

func (s *Service) ProvisionTenant(
	ctx context.Context,
	command tenantapp.ProvisionTenantCommand,
) (tenantapp.ProvisionTenantResult, error) {
	return s.deps.Tenants.ProvisionTenant(ctx, command)
}

func (s *Service) ListTenants(
	ctx context.Context,
	page tenantapp.Page,
) (tenantapp.TenantPage, error) {
	return s.deps.Tenants.ListTenants(ctx, page)
}
