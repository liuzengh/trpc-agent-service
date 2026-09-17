package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	memorymigrationpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver/postgres"
	migrationpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	providerpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/provider/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// memoryMigrationConfig is a bounded, confirmed operator job.  It progresses
// only one authority phase per invocation and cannot publish a tenant config
// pointer, so running it is safe to retry after a process interruption.
type memoryMigrationConfig struct {
	ControlDSN, TenantID, MigrationID, WorkerID string
	PostgresConnections, RedisConnections       map[string]string
	RepairLimit                                 int
	Lease, RetryDelay, Timeout                  time.Duration
}

func loadMemoryMigrationConfig(getenv func(string) string) (memoryMigrationConfig, error) {
	if getenv == nil {
		return memoryMigrationConfig{}, errors.New("environment reader is required")
	}
	c := memoryMigrationConfig{ControlDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), TenantID: strings.TrimSpace(getenv("TRPC_MEMORY_MIGRATION_TENANT_ID")),
		MigrationID: strings.TrimSpace(getenv("TRPC_MEMORY_MIGRATION_ID")), WorkerID: strings.TrimSpace(getenv("TRPC_MEMORY_MIGRATION_WORKER_ID")),
		RepairLimit: 100, Lease: 30 * time.Second, RetryDelay: time.Second, Timeout: 10 * time.Minute}
	var err error
	if c.PostgresConnections, err = parseSessionPostgresConnections(c.ControlDSN, getenv("TRPC_MEMORY_POSTGRES_CONNECTIONS")); err != nil {
		return c, errors.New("invalid TRPC_MEMORY_POSTGRES_CONNECTIONS")
	}
	redisDB, redisErr := envInt(getenv, "TRPC_REDIS_DB", 0)
	if redisErr != nil {
		return c, errors.New("invalid TRPC_REDIS_DB")
	}
	if c.RedisConnections, err = parseMemoryRedisConnections(strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")), getenv("TRPC_REDIS_PASSWORD"), redisDB, getenv("TRPC_MEMORY_REDIS_CONNECTIONS")); err != nil {
		return c, errors.New("invalid TRPC_MEMORY_REDIS_CONNECTIONS")
	}
	if c.RepairLimit, err = envInt(getenv, "TRPC_MEMORY_MIGRATION_REPAIR_LIMIT", c.RepairLimit); err != nil || c.RepairLimit < 1 || c.RepairLimit > 1000 {
		return c, errors.New("invalid TRPC_MEMORY_MIGRATION_REPAIR_LIMIT")
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_MEMORY_MIGRATION_LEASE", &c.Lease, time.Second}, {"TRPC_MEMORY_MIGRATION_RETRY_DELAY", &c.RetryDelay, 0}, {"TRPC_MEMORY_MIGRATION_TIMEOUT", &c.Timeout, 10 * time.Second},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return c, errors.New("invalid " + item.name)
		}
	}
	if c.Timeout > 2*time.Hour || c.RetryDelay > time.Hour || c.Lease >= c.Timeout || c.ControlDSN == "" || c.TenantID == "" || c.MigrationID == "" || c.WorkerID == "" || strings.ContainsAny(c.TenantID+c.MigrationID+c.WorkerID, "\x00\r\n") {
		return c, errors.New("required memory migration configuration is missing or invalid")
	}
	confirmed, confirmErr := envBool(getenv, "TRPC_MEMORY_MIGRATION_CONFIRM", false)
	if confirmErr != nil || !confirmed {
		return c, errors.New("TRPC_MEMORY_MIGRATION_CONFIRM=true is required")
	}
	return c, nil
}

func runMemoryMigrate(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid memory migration dependencies")
	}
	c, err := loadMemoryMigrationConfig(getenv)
	if err != nil {
		return fmt.Errorf("memory migration configuration rejected: %w", err)
	}
	ctx, cancel := context.WithTimeout(parent, c.Timeout)
	defer cancel()
	controlDB, err := sql.Open("pgx", c.ControlDSN)
	if err != nil {
		return errors.New("memory migration control postgres client initialization failed")
	}
	defer controlDB.Close()
	if err := controlDB.PingContext(ctx); err != nil {
		return errors.New("memory migration control postgres unavailable")
	}
	if err := migrations.NewRunner(controlDB).Ready(ctx); err != nil {
		return fmt.Errorf("memory migration control schema is not ready: %w", err)
	}
	authorityBase := migrationpostgres.New(controlDB)
	authority := migration.NewJournaledRepository(authorityBase, authorityBase)
	if _, err := authority.RecoverPending(ctx, c.TenantID, c.MigrationID); err != nil {
		return fmt.Errorf("recover pending memory migration phase intent: %w", err)
	}
	current, err := authority.Get(ctx, c.TenantID, c.MigrationID)
	if err != nil {
		return fmt.Errorf("load memory migration authority: %w", err)
	}
	if current.Domain != memorydriver.Domain {
		return errors.New("migration domain is not memory")
	}
	catalog, err := provider.NewCatalog(provider.PostgresBackendSchema(), provider.PostgresBackendSchemaV2(), provider.RedisMemoryBackendSchema())
	if err != nil {
		return errors.New("memory migration provider catalog initialization failed")
	}
	profiles := providerpostgres.New(controlDB, catalog)
	sourceProfile, err := profiles.GetBackend(ctx, c.TenantID, current.Source.BackendProfileID, current.Source.BackendVersion)
	if err != nil {
		return fmt.Errorf("resolve source memory backend: %w", err)
	}
	targetProfile, err := profiles.GetBackend(ctx, c.TenantID, current.Target.BackendProfileID, current.Target.BackendVersion)
	if err != nil {
		return fmt.Errorf("resolve target memory backend: %w", err)
	}
	if sourceProfile.Provider != "redis-memory" || sourceProfile.SchemaVersion != 1 || targetProfile.Provider != "postgres" || (targetProfile.SchemaVersion != 1 && targetProfile.SchemaVersion != 2) ||
		sourceProfile.CredentialRef.Ref != "" || targetProfile.CredentialRef.Ref != "" || !sourceProfile.Capabilities["strong_ryw"] || !targetProfile.Capabilities["strong_ryw"] {
		return runtime.ErrCapabilityUnsupported
	}
	redisURL := c.RedisConnections[sourceProfile.Configuration["connection_id"]]
	if redisURL == "" {
		return runtime.ErrBackendUnavailable
	}
	redisOptions, err := redisclient.ParseURL(redisURL)
	if err != nil {
		return errors.New("memory migration redis connection rejected")
	}
	sourceRedis := redisclient.NewClient(redisOptions)
	defer sourceRedis.Close()
	if err := sourceRedis.Ping(ctx).Err(); err != nil {
		return errors.New("memory migration redis unavailable")
	}
	targetConnection := "default"
	if targetProfile.SchemaVersion == 2 {
		targetConnection = targetProfile.Configuration["connection_id"]
	}
	targetDSN := c.PostgresConnections[targetConnection]
	if targetDSN == "" {
		return runtime.ErrBackendUnavailable
	}
	targetDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		return errors.New("memory migration target postgres client initialization failed")
	}
	defer targetDB.Close()
	if err := targetDB.PingContext(ctx); err != nil {
		return errors.New("memory migration target postgres unavailable")
	}
	if err := migrations.NewRunner(targetDB).Ready(ctx); err != nil {
		return fmt.Errorf("memory migration target schema is not ready: %w", err)
	}
	source := memorydriver.RedisSource{Client: sourceRedis, KeyPrefix: "trpc-memory"}
	target := memorydriver.PostgresTarget{DB: targetDB}
	driver := memorydriver.Driver{Authority: authority, Source: source, Target: target, Ledger: memorymigrationpostgres.New(controlDB), SourceUser: source, TargetUser: target}
	return runMemoryMigrationStep(ctx, logger, authority, driver, target, c, current)
}

func runMemoryMigrationStep(ctx context.Context, logger *roleLogger, authority migration.Repository, driver memorydriver.Driver, target memorydriver.Exporter, c memoryMigrationConfig, current migration.Migration) error {
	now := time.Now().UTC()
	transition := func(to migration.State, update func(*migration.TransitionRequest)) (migration.Migration, error) {
		request := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: to, At: now}
		if update != nil {
			update(&request)
		}
		return authority.Transition(ctx, request)
	}
	switch current.State {
	case migration.StatePlanned:
		next, err := transition(migration.StateSnapshot, func(request *migration.TransitionRequest) { request.SnapshotWatermark = "redis-scan-v1" })
		if err != nil {
			return err
		}
		logger.Printf("memory migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
	case migration.StateSnapshot:
		next, err := transition(migration.StateDualWrite, func(request *migration.TransitionRequest) {
			request.DualWriteRef = "memory-mutation-ledger://" + current.TenantID + "/" + current.MigrationID
		})
		if err != nil {
			return err
		}
		logger.Printf("memory migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
	case migration.StateDualWrite:
		next, err := transition(migration.StateBackfill, nil)
		if err != nil {
			return err
		}
		logger.Printf("memory migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
	case migration.StateBackfill:
		repair, err := driver.Repair(ctx, memorydriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, WorkerID: c.WorkerID, Limit: c.RepairLimit, Now: now, Lease: c.Lease, RetryDelay: c.RetryDelay})
		if err != nil {
			return fmt.Errorf("repair memory migration: %w", err)
		}
		if current.BackfillComplete {
			next, enterErr := driver.EnterVerify(ctx, current.TenantID, current.MigrationID, now)
			if enterErr != nil {
				return fmt.Errorf("enter memory migration verification: %w", enterErr)
			}
			logger.Printf("memory migration advanced tenant=%q migration=%q state=%s repair_claimed=%d repair_applied=%d", next.TenantID, next.MigrationID, next.State, repair.Claimed, repair.Applied)
			return nil
		}
		batch, err := driver.BackfillOnce(ctx, current.TenantID, current.MigrationID, now)
		if err != nil {
			return fmt.Errorf("backfill memory migration: %w", err)
		}
		logger.Printf("memory migration backfill tenant=%q migration=%q repair_claimed=%d repair_applied=%d repair_retried=%d batch_records=%d complete=%t", current.TenantID, current.MigrationID, repair.Claimed, repair.Applied, repair.Retried, batch.Batch.RecordCount, batch.Migration.BackfillComplete)
	case migration.StateVerify:
		repair, err := driver.Repair(ctx, memorydriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, WorkerID: c.WorkerID, Limit: c.RepairLimit, Now: now, Lease: c.Lease, RetryDelay: c.RetryDelay})
		if err != nil {
			return fmt.Errorf("repair memory migration: %w", err)
		}
		verification, err := driver.Verify(ctx, current.TenantID, current.MigrationID, target)
		if err != nil {
			return fmt.Errorf("verify memory migration: %w", err)
		}
		recorded, err := authority.RecordVerification(ctx, migration.VerificationRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, Verification: verification, RecordedAt: now})
		if err != nil {
			return fmt.Errorf("record memory migration verification: %w", err)
		}
		logger.Printf("memory migration verified tenant=%q migration=%q version=%d repair_claimed=%d repair_applied=%d source_count=%d target_count=%d source_digest=%s target_digest=%s; explicit cutover remains disabled", recorded.TenantID, recorded.MigrationID, recorded.Version, repair.Claimed, repair.Applied, verification.SourceCount, verification.TargetCount, verification.SourceDigest, verification.TargetDigest)
	default:
		logger.Printf("memory migration has no worker action tenant=%q migration=%q state=%s", current.TenantID, current.MigrationID, current.State)
	}
	return nil
}
