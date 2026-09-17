package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	sessionmigrationpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	providerpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/provider/postgres"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/postgres"
)

// sessionMigrationConfig is deliberately a bounded, one-pass operator job.
// It may repair, snapshot, or advance a safe pre-cutover phase, but cannot
// publish a tenant ConfigSnapshot or switch traffic by itself.
type sessionMigrationConfig struct {
	ControlDSN, TenantID, MigrationID, WorkerID string
	Connections                                 map[string]string
	BatchLimit, RepairLimit                     int
	Lease, RetryDelay, Timeout                  time.Duration
}

func loadSessionMigrationConfig(getenv func(string) string) (sessionMigrationConfig, error) {
	if getenv == nil {
		return sessionMigrationConfig{}, errors.New("environment reader is required")
	}
	config := sessionMigrationConfig{
		ControlDSN:  strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		TenantID:    strings.TrimSpace(getenv("TRPC_SESSION_MIGRATION_TENANT_ID")),
		MigrationID: strings.TrimSpace(getenv("TRPC_SESSION_MIGRATION_ID")),
		WorkerID:    strings.TrimSpace(getenv("TRPC_SESSION_MIGRATION_WORKER_ID")),
		BatchLimit:  100,
		RepairLimit: 100,
		Lease:       30 * time.Second,
		RetryDelay:  time.Second,
		Timeout:     10 * time.Minute,
	}
	var err error
	if config.Connections, err = parseSessionPostgresConnections(config.ControlDSN, getenv("TRPC_SESSION_POSTGRES_CONNECTIONS")); err != nil {
		return sessionMigrationConfig{}, errors.New("invalid TRPC_SESSION_POSTGRES_CONNECTIONS")
	}
	if config.BatchLimit, err = envInt(getenv, "TRPC_SESSION_MIGRATION_BATCH_LIMIT", config.BatchLimit); err != nil || config.BatchLimit < 1 || config.BatchLimit > 1000 {
		return sessionMigrationConfig{}, errors.New("invalid TRPC_SESSION_MIGRATION_BATCH_LIMIT")
	}
	if config.RepairLimit, err = envInt(getenv, "TRPC_SESSION_MIGRATION_REPAIR_LIMIT", config.RepairLimit); err != nil || config.RepairLimit < 1 || config.RepairLimit > 1000 {
		return sessionMigrationConfig{}, errors.New("invalid TRPC_SESSION_MIGRATION_REPAIR_LIMIT")
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
		min    time.Duration
	}{
		{"TRPC_SESSION_MIGRATION_LEASE", &config.Lease, time.Second},
		{"TRPC_SESSION_MIGRATION_RETRY_DELAY", &config.RetryDelay, 0},
		{"TRPC_SESSION_MIGRATION_TIMEOUT", &config.Timeout, 10 * time.Second},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.min {
			return sessionMigrationConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.Timeout > 2*time.Hour || config.RetryDelay > time.Hour || config.Lease >= config.Timeout ||
		config.ControlDSN == "" || config.TenantID == "" || config.MigrationID == "" || config.WorkerID == "" ||
		strings.ContainsAny(config.TenantID+config.MigrationID+config.WorkerID, "\x00\r\n") {
		return sessionMigrationConfig{}, errors.New("required session migration configuration is missing or invalid")
	}
	confirmed, confirmErr := envBool(getenv, "TRPC_SESSION_MIGRATION_CONFIRM", false)
	if confirmErr != nil || !confirmed {
		return sessionMigrationConfig{}, errors.New("TRPC_SESSION_MIGRATION_CONFIRM=true is required")
	}
	return config, nil
}

func runSessionMigrate(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid session migration dependencies")
	}
	config, err := loadSessionMigrationConfig(getenv)
	if err != nil {
		return fmt.Errorf("session migration configuration rejected: %w", err)
	}
	ctx, cancel := context.WithTimeout(parent, config.Timeout)
	defer cancel()
	controlDB, err := sql.Open("pgx", config.ControlDSN)
	if err != nil {
		return errors.New("session migration control postgres client initialization failed")
	}
	defer controlDB.Close()
	if err := controlDB.PingContext(ctx); err != nil {
		return errors.New("session migration control postgres unavailable")
	}
	if err := migrations.NewRunner(controlDB).Ready(ctx); err != nil {
		return fmt.Errorf("session migration control schema is not ready: %w", err)
	}
	catalog, err := provider.NewCatalog(provider.PostgresBackendSchema(), provider.PostgresBackendSchemaV2())
	if err != nil {
		return errors.New("session migration provider catalog initialization failed")
	}
	planes, err := sessionpostgres.NewProfileServiceResolver(providerpostgres.New(controlDB, catalog), config.Connections)
	if err != nil {
		return errors.New("session migration data-plane resolver rejected")
	}
	defer planes.Close()
	baseAuthority := migrationpostgres.New(controlDB)
	// The journal shares the control-plane database with the authority. Domain
	// drivers still see only migration.Repository; crash recovery remains a
	// role concern rather than a dependency of the session data plane.
	authority := migration.NewJournaledRepository(baseAuthority, baseAuthority)
	if _, err := authority.RecoverPending(ctx, config.TenantID, config.MigrationID); err != nil {
		return fmt.Errorf("recover pending session migration phase intent: %w", err)
	}
	current, err := authority.Get(ctx, config.TenantID, config.MigrationID)
	if err != nil {
		return fmt.Errorf("load session migration authority: %w", err)
	}
	if current.Domain != sessiondriver.Domain {
		return errors.New("migration domain is not session")
	}
	_, sourceDSN, err := planes.ResolvePostgresDSN(ctx, current.TenantID, current.Source.BackendProfileID, current.Source.BackendVersion)
	if err != nil {
		return fmt.Errorf("resolve source session data plane: %w", err)
	}
	_, targetDSN, err := planes.ResolvePostgresDSN(ctx, current.TenantID, current.Target.BackendProfileID, current.Target.BackendVersion)
	if err != nil {
		return fmt.Errorf("resolve target session data plane: %w", err)
	}
	if strings.TrimSpace(sourceDSN) == strings.TrimSpace(targetDSN) {
		return errors.New("source and target session data planes must differ")
	}
	sourceDB, closeSource, err := openMigrationDataPlane(sourceDSN, config.ControlDSN, controlDB)
	if err != nil {
		return err
	}
	defer closeSource()
	targetDB, closeTarget, err := openMigrationDataPlane(targetDSN, config.ControlDSN, controlDB)
	if err != nil {
		return err
	}
	defer closeTarget()
	if err := migrations.NewRunner(sourceDB).Ready(ctx); err != nil {
		return fmt.Errorf("source session data-plane schema is not ready: %w", err)
	}
	if err := migrations.NewRunner(targetDB).Ready(ctx); err != nil {
		return fmt.Errorf("target session data-plane schema is not ready: %w", err)
	}
	source := sessionmigrationpostgres.NewSplitSource(controlDB, sourceDB)
	driver := sessiondriver.Driver{
		Authority: authority, Ledger: sessionmigrationpostgres.New(controlDB),
		Source: source, Backfill: source,
		Target:          sessionmigrationpostgres.NewReplica(targetDB),
		ReverseSource:   sessionmigrationpostgres.NewSplitSource(controlDB, targetDB),
		ReverseTarget:   sessionmigrationpostgres.NewSDKReplica(sourceDB),
		SourceInventory: source,
		TargetInventory: sessionmigrationpostgres.NewSource(targetDB),
	}
	return runSessionMigrationStep(ctx, logger, authority, driver, sessionmigrationpostgres.NewPublisher(controlDB), source, config, current)
}

func openMigrationDataPlane(dsn, controlDSN string, controlDB *sql.DB) (*sql.DB, func(), error) {
	if strings.TrimSpace(dsn) == strings.TrimSpace(controlDSN) {
		return controlDB, func() {}, nil
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, errors.New("session migration data-plane postgres client initialization failed")
	}
	return db, func() { _ = db.Close() }, nil
}

func runSessionMigrationStep(ctx context.Context, logger *roleLogger, authority migration.Repository, driver sessiondriver.Driver,
	publisher sessiondriver.CutoverPublisher, source *sessionmigrationpostgres.Source, config sessionMigrationConfig, current migration.Migration,
) error {
	now := time.Now().UTC()
	transition := func(to migration.State, update func(*migration.TransitionRequest)) (migration.Migration, error) {
		request := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			ExpectedVersion: current.Version, To: to, At: now}
		if update != nil {
			update(&request)
		}
		return authority.Transition(ctx, request)
	}
	switch current.State {
	case migration.StatePlanned:
		watermark, err := source.CaptureWatermark(ctx, current.TenantID)
		if err != nil {
			return fmt.Errorf("capture session migration watermark: %w", err)
		}
		next, err := transition(migration.StateSnapshot, func(request *migration.TransitionRequest) { request.SnapshotWatermark = watermark })
		if err != nil {
			return err
		}
		logger.Printf("session migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
		return nil
	case migration.StateSnapshot:
		next, err := transition(migration.StateDualWrite, func(request *migration.TransitionRequest) {
			request.DualWriteRef = "session-mutation-ledger://" + current.TenantID + "/" + current.MigrationID
		})
		if err != nil {
			return err
		}
		logger.Printf("session migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
		return nil
	case migration.StateDualWrite:
		next, err := transition(migration.StateBackfill, nil)
		if err != nil {
			return err
		}
		logger.Printf("session migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
		return nil
	case migration.StateBackfill:
		repair, err := driver.Repair(ctx, sessiondriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			WorkerID: config.WorkerID, Limit: config.RepairLimit, Now: now, Lease: config.Lease, RetryDelay: config.RetryDelay})
		if err != nil {
			return fmt.Errorf("repair session migration: %w", err)
		}
		if current.BackfillComplete {
			next, err := driver.EnterVerify(ctx, current.TenantID, current.MigrationID, now)
			if err != nil {
				return fmt.Errorf("enter session migration verification: %w", err)
			}
			logger.Printf("session migration advanced tenant=%q migration=%q state=%s repair_claimed=%d repair_applied=%d",
				next.TenantID, next.MigrationID, next.State, repair.Claimed, repair.Applied)
			return nil
		}
		batch, err := driver.BackfillOnce(ctx, sessiondriver.BackfillRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			Limit: config.BatchLimit, At: now})
		if err != nil {
			return fmt.Errorf("backfill session migration: %w", err)
		}
		logger.Printf("session migration backfill tenant=%q migration=%q repair_claimed=%d repair_applied=%d repair_retried=%d batch_records=%d complete=%t",
			current.TenantID, current.MigrationID, repair.Claimed, repair.Applied, repair.Retried, batch.Batch.RecordCount, batch.Migration.BackfillComplete)
		return nil
	case migration.StateVerify:
		repair, err := driver.Repair(ctx, sessiondriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			WorkerID: config.WorkerID, Limit: config.RepairLimit, Now: now, Lease: config.Lease, RetryDelay: config.RetryDelay})
		if err != nil {
			return fmt.Errorf("repair session migration: %w", err)
		}
		verification, err := driver.ShadowVerify(ctx, current.TenantID, current.MigrationID)
		if err != nil {
			return fmt.Errorf("verify session migration: %w", err)
		}
		recorded, err := authority.RecordVerification(ctx, migration.VerificationRequest{TenantID: current.TenantID,
			MigrationID: current.MigrationID, ExpectedVersion: current.Version, Verification: verification, RecordedAt: now})
		if err != nil {
			return fmt.Errorf("record session migration verification: %w", err)
		}
		logger.Printf("session migration verified tenant=%q migration=%q version=%d repair_claimed=%d repair_applied=%d source_count=%d target_count=%d source_digest=%s target_digest=%s; explicit cutover remains required",
			recorded.TenantID, recorded.MigrationID, recorded.Version, repair.Claimed, repair.Applied,
			verification.SourceCount, verification.TargetCount, verification.SourceDigest, verification.TargetDigest)
		return nil
	case migration.StateCutover, migration.StateObserve:
		repair, err := driver.Repair(ctx, sessiondriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			WorkerID: config.WorkerID, Limit: config.RepairLimit, Now: now, Lease: config.Lease, RetryDelay: config.RetryDelay})
		if err != nil {
			return fmt.Errorf("repair session migration: %w", err)
		}
		status, err := publisher.DrainStatus(ctx, current.TenantID, current.MigrationID)
		if err != nil {
			return fmt.Errorf("read session migration drain status: %w", err)
		}
		logger.Printf("session migration observe repair tenant=%q migration=%q claimed=%d applied=%d forward_outstanding=%d reverse_outstanding=%d",
			current.TenantID, current.MigrationID, repair.Claimed, repair.Applied, status.ForwardOutstanding, status.ReverseOutstanding)
		return nil
	default:
		logger.Printf("session migration has no worker action tenant=%q migration=%q state=%s", current.TenantID, current.MigrationID, current.State)
		return nil
	}
}
