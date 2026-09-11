package controlplane

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

// New creates the configured control-plane repository.
func New(ctx context.Context, cfg config.ControlPlaneConfig) (Repository, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	switch cfg.Backend {
	case config.ControlPlaneBackendInMemory:
		return NewMemoryRepository(DefaultBootstrapData()), nil
	case config.ControlPlaneBackendPostgres:
		db, err := database.OpenPostgres(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if cfg.AutoMigrate {
			if err := database.Migrate(ctx, db); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("migrate control-plane database: %w", err)
			}
		}
		if cfg.BootstrapTutorial {
			if err := SeedBootstrap(ctx, db, DefaultBootstrapData()); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("bootstrap control-plane database: %w", err)
			}
		}
		repository, err := NewPostgresRepository(db)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		return repository, nil
	default:
		return nil, fmt.Errorf("unsupported control-plane backend %q", cfg.Backend)
	}
}
