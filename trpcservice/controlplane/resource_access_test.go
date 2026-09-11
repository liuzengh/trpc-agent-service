package controlplane

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
)

type resourceContractRepository interface {
	Repository
	ResourceSyncRepository
	ResourceAccessRepository
}

func TestResourceAccessMemoryConcurrencyAndFencing(t *testing.T) {
	r := NewMemoryRepository(DefaultBootstrapData())
	defer func() { _ = r.Close() }()
	resourceAccessContract(t, r)
}

func TestResourceAccessPostgresConcurrencyAndFencing(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL not configured")
	}
	r, err := New(context.Background(), config.ControlPlaneConfig{Backend: "postgres", PostgresURL: dsn, AutoMigrate: true, MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if err := SeedBootstrap(context.Background(), r.(*PostgresRepository).SQLDB(), DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	resourceAccessContract(t, r.(resourceContractRepository))
}

func resourceAccessContract(t *testing.T, repo resourceContractRepository) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const tenant, app, kind = "tutorial-tenant", "tutorial-app", "memory"
	// Keep the existing state intact: unique subjects work even when this
	// contract shares an isolated database with the migration tests.
	a := "contract-a-" + time.Now().Format("150405.000000000")
	b := "contract-b-" + a
	entered, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- repo.WithResourceAccess(ctx, tenant, app, kind, ResourceAccess{Subject: a, Write: true, HasFencingToken: true, FencingToken: 4}, func(ctx context.Context) error {
			if _, err := repo.GetTenant(ctx, tenant); err != nil {
				return err
			}
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	defer func() {
		close(release)
		if err := <-finished; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first subject failed to acquire", ctx.Err())
	}
	limited, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if err := repo.WithResourceAccess(limited, tenant, app, kind, ResourceAccess{Subject: b, Write: true}, func(context.Context) error { return nil }); err != nil {
		t.Fatal("different subject blocked behind backend I/O", err)
	}
	for _, exclusive := range []bool{false, true} {
		limited, stop := context.WithTimeout(ctx, 30*time.Millisecond)
		var err error
		if exclusive {
			err = repo.WithResourceSync(limited, tenant, app, kind, func(context.Context, *ResourceSync, func() error) error { return nil })
		} else {
			err = repo.WithResourceAccess(limited, tenant, app, kind, ResourceAccess{Subject: a}, func(context.Context) error { return nil })
		}
		stop()
		if err == nil {
			t.Fatalf("exclusive=%t overtook active subject", exclusive)
		}
	}
	// Inspect via the other subject while A is held: metadata updates must not
	// lose A's registration/fence when B saves its own epoch.
	if err := repo.WithResourceAccess(ctx, tenant, app, kind, ResourceAccess{Subject: b}, func(ctx context.Context) error {
		s, save, ok := ResourceFromContext(ctx, tenant, app, kind)
		if !ok || !s.Subjects[a] || !s.Subjects[b] || s.Fences[a] != 4 {
			return errors.New("lost concurrent resource metadata")
		}
		if save() == nil {
			return errors.New("subject access may overwrite global metadata")
		}
		if repo.WithResourceSync(ctx, tenant, app, kind, func(context.Context, *ResourceSync, func() error) error { return nil }) == nil {
			return errors.New("unsafe lock upgrade allowed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.WithResourceAccess(ctx, tenant, app, kind, ResourceAccess{Subject: b, HasFencingToken: true, FencingToken: 5}, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := repo.WithResourceAccess(ctx, tenant, app, kind, ResourceAccess{Subject: b, HasFencingToken: true, FencingToken: 3}, func(context.Context) error { return errors.New("stale writer reached backend") }); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("stale fence: %v", err)
	}
}
