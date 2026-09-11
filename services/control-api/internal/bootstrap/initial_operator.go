package bootstrap

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin"
	adminapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity"
)

const initialOperatorLockID int64 = 731947202

func ensureInitialPlatformOperator(
	ctx context.Context,
	pool *pgxpool.Pool,
	config Config,
) error {
	if config.BootstrapMode != "auto" {
		return nil
	}
	password := config.BootstrapPassword
	if password == "" {
		return fmt.Errorf("initial operator password is empty")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin initial operator bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(
		ctx, "SELECT pg_advisory_xact_lock($1)", initialOperatorLockID,
	); err != nil {
		return fmt.Errorf("lock initial operator bootstrap: %w", err)
	}

	accounts := identity.NewAccountManagement(tx)
	service := admin.NewStartupService(tx, accounts)
	_, err = service.EnsureInitialOperator(ctx, adminapp.EnsureInitialOperatorCommand{
		Username: config.BootstrapUsername, DisplayName: config.BootstrapDisplayName,
		TemporaryPassword: password,
	})
	if err != nil {
		return fmt.Errorf("ensure initial platform operator: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit initial platform operator: %w", err)
	}
	return nil
}
