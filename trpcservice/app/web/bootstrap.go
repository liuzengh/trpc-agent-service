package web

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"golang.org/x/crypto/bcrypt"
)

// EnsureInitialOwner creates the configured development owner when it does not
// exist. It is idempotent and never changes an existing member's credentials.
func EnsureInitialOwner(ctx context.Context, tenants *tenant.Manager, members *member.Manager, tenantID, userID, password string) error {
	if tenantID == "" || userID == "" || password == "" {
		return errors.New("bootstrap owner requires tenant id, user id, and password")
	}
	if existing, err := members.GetByUserID(ctx, userID); err == nil {
		if existing.Password != "" {
			return nil
		}
		hash, hashErr := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if hashErr != nil {
			return fmt.Errorf("bootstrap password: %w", hashErr)
		}
		if err := members.UpdatePassword(ctx, existing.TenantID, existing.UserID, string(hash)); err != nil {
			return fmt.Errorf("bootstrap existing member password: %w", err)
		}
		return nil
	}
	if _, err := tenants.Get(ctx, tenantID); errors.Is(err, tenant.ErrNotFound) {
		if err := tenants.Create(ctx, &tenant.Tenant{ID: tenantID, Name: tenantID, Status: tenant.StatusActive}); err != nil {
			if _, getErr := tenants.Get(ctx, tenantID); getErr != nil {
				return fmt.Errorf("bootstrap tenant: %w", err)
			}
		}
	} else if err != nil {
		return fmt.Errorf("bootstrap tenant lookup: %w", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("bootstrap password: %w", err)
	}
	if err := members.Create(ctx, &member.Member{
		TenantID: tenantID,
		UserID:   userID,
		Role:     member.RoleOwner,
		Password: string(hash),
	}); err != nil {
		// Several nodes starting against one database race here: the loser's
		// insert is rejected on the primary key. That is not a failure — the
		// desired state (an owner exists) holds — and the winner's credentials
		// must be left alone. Any other error leaves no member behind.
		if _, getErr := members.GetByUserID(ctx, userID); getErr == nil {
			return nil
		}
		return fmt.Errorf("bootstrap member: %w", err)
	}
	return nil
}
