// Command capacity-prepare provisions an isolated local capacity tenant and
// writes its short-lived test credential to a private file.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	postgresDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	apiKeyFileEnv  = "TRPC_CAPACITY_API_KEY_FILE"
)

func main() {
	if err := run(context.Background(), os.Getenv); err != nil {
		log.Print(platformlog.SafeError(err))
		os.Exit(1)
	}
}

func run(parent context.Context, getenv func(string) string) error {
	if parent == nil {
		parent = context.Background()
	}
	if getenv == nil {
		return errors.New("environment reader is required")
	}
	dsn := strings.TrimSpace(getenv(postgresDSNEnv))
	if dsn == "" {
		return fmt.Errorf("%s is required", postgresDSNEnv)
	}
	apiKeyPath := strings.TrimSpace(getenv(apiKeyFileEnv))
	if apiKeyPath == "" {
		return fmt.Errorf("%s is required", apiKeyFileEnv)
	}

	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	pool, store, err := openStore(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	tenantID := "capacity-" + uuid.NewString()
	appID := "capacity"
	config := tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:  tenant.ModelProviderOpenAI,
			Model:     "capacity",
			APIKeyRef: tenant.SecretRef{Name: "capacity-model-key", Version: "v1"},
		},
		BackendConfig: tenant.BackendConfig{
			Name: "capacity-backends",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "capacity-session",
			},
		},
	}
	controlPlane := admin.API{Repository: store}
	if err := controlPlane.CreateTenant(ctx, tenant.Tenant{
		ID:     tenantID,
		Name:   "Capacity",
		Status: tenant.StatusActive,
	}); err != nil {
		return fmt.Errorf("create capacity tenant: %w", err)
	}
	if err := controlPlane.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID:            tenantID,
		AppID:               appID,
		Name:                "Capacity",
		ActiveConfigVersion: config.Version,
		Status:              tenant.StatusActive,
	}, config); err != nil {
		return fmt.Errorf("create capacity app: %w", err)
	}
	issued, err := controlPlane.IssueCredential(ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, time.Time{})
	if err != nil {
		return fmt.Errorf("issue capacity credential: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(apiKeyPath), 0o700); err != nil {
		return fmt.Errorf("create credential directory: %w", err)
	}
	if err := os.WriteFile(apiKeyPath, []byte(issued.APIKey+"\n"), 0o600); err != nil {
		return fmt.Errorf("write capacity credential: %w", err)
	}
	if err := os.Chmod(apiKeyPath, 0o600); err != nil {
		return fmt.Errorf("protect capacity credential: %w", err)
	}
	fmt.Printf("tenant_id=%s app_id=%s credential_file=%s\n", tenantID, appID, apiKeyPath)
	return nil
}

func openStore(ctx context.Context, dsn string) (*pgxpool.Pool, *postgres.Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("connect postgres: %w", err)
	}
	store, err := postgres.New(pool)
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("migrate postgres: %w", err)
	}
	return pool, store, nil
}
