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
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	knowledgedriverpg "github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver/postgres"
	migrationpg "github.com/liuzengh/trpc-agent-service/trpcservice/migration/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	providerpg "github.com/liuzengh/trpc-agent-service/trpcservice/provider/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge/qdrant"
	tenantpg "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
)

type knowledgeMigrationConfig struct {
	ControlDSN, SecretRoot, TenantID, MigrationID, WorkerID string
	BatchLimit, RepairLimit                                 int
	Lease, RetryDelay, Timeout                              time.Duration
}

func loadKnowledgeMigrationConfig(getenv func(string) string) (knowledgeMigrationConfig, error) {
	if getenv == nil {
		return knowledgeMigrationConfig{}, errors.New("environment reader is required")
	}
	c := knowledgeMigrationConfig{
		ControlDSN:  strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		SecretRoot:  strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		TenantID:    strings.TrimSpace(getenv("TRPC_KNOWLEDGE_MIGRATION_TENANT_ID")),
		MigrationID: strings.TrimSpace(getenv("TRPC_KNOWLEDGE_MIGRATION_ID")),
		WorkerID:    strings.TrimSpace(getenv("TRPC_KNOWLEDGE_MIGRATION_WORKER_ID")),
		BatchLimit:  100,
		RepairLimit: 100,
		Lease:       30 * time.Second,
		RetryDelay:  time.Second,
		Timeout:     10 * time.Minute,
	}
	var err error
	if c.BatchLimit, err = envInt(getenv, "TRPC_KNOWLEDGE_MIGRATION_BATCH_LIMIT", c.BatchLimit); err != nil || c.BatchLimit < 1 || c.BatchLimit > 1000 {
		return c, errors.New("invalid TRPC_KNOWLEDGE_MIGRATION_BATCH_LIMIT")
	}
	if c.RepairLimit, err = envInt(getenv, "TRPC_KNOWLEDGE_MIGRATION_REPAIR_LIMIT", c.RepairLimit); err != nil || c.RepairLimit < 1 || c.RepairLimit > 1000 {
		return c, errors.New("invalid TRPC_KNOWLEDGE_MIGRATION_REPAIR_LIMIT")
	}
	for _, item := range []struct {
		name string
		p    *time.Duration
		min  time.Duration
	}{
		{"TRPC_KNOWLEDGE_MIGRATION_LEASE", &c.Lease, time.Second},
		{"TRPC_KNOWLEDGE_MIGRATION_RETRY_DELAY", &c.RetryDelay, 0},
		{"TRPC_KNOWLEDGE_MIGRATION_TIMEOUT", &c.Timeout, 10 * time.Second},
	} {
		if *item.p, err = envDuration(getenv, item.name, *item.p); err != nil || *item.p < item.min {
			return c, errors.New("invalid " + item.name)
		}
	}
	if c.Timeout > 2*time.Hour || c.RetryDelay > time.Hour || c.Lease >= c.Timeout ||
		c.ControlDSN == "" || c.SecretRoot == "" || c.TenantID == "" || c.MigrationID == "" || c.WorkerID == "" ||
		strings.ContainsAny(c.TenantID+c.MigrationID+c.WorkerID, "\x00\r\n") {
		return c, errors.New("required knowledge migration configuration is missing or invalid")
	}
	confirmed, confirmErr := envBool(getenv, "TRPC_KNOWLEDGE_MIGRATION_CONFIRM", false)
	if confirmErr != nil || !confirmed {
		return c, errors.New("TRPC_KNOWLEDGE_MIGRATION_CONFIRM=true is required")
	}
	return c, nil
}

func runKnowledgeMigrate(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid knowledge migration dependencies")
	}
	c, err := loadKnowledgeMigrationConfig(getenv)
	if err != nil {
		return fmt.Errorf("knowledge migration configuration rejected: %w", err)
	}
	ctx, cancel := context.WithTimeout(parent, c.Timeout)
	defer cancel()
	db, err := sql.Open("pgx", c.ControlDSN)
	if err != nil {
		return errors.New("knowledge migration control postgres client initialization failed")
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return errors.New("knowledge migration control postgres unavailable")
	}
	if err := migrations.NewRunner(db).Ready(ctx); err != nil {
		return fmt.Errorf("knowledge migration control schema is not ready: %w", err)
	}
	base := migrationpg.New(db)
	authority := migration.NewJournaledRepository(base, base)
	if _, err := authority.RecoverPending(ctx, c.TenantID, c.MigrationID); err != nil {
		return fmt.Errorf("recover pending knowledge migration phase intent: %w", err)
	}
	current, err := authority.Get(ctx, c.TenantID, c.MigrationID)
	if err != nil {
		return fmt.Errorf("load knowledge migration authority: %w", err)
	}
	if current.Domain != knowledgedriver.Domain {
		return errors.New("migration domain is not knowledge")
	}
	secrets, err := filesystem.New(c.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("knowledge migration secret provider rejected")
	}
	catalog, err := provider.NewCatalog(provider.QdrantVectorSchema(), provider.LocalQdrantVectorSchema())
	if err != nil {
		return errors.New("knowledge migration provider catalog rejected")
	}
	resolver := knowledge.BackendAdapterResolver{
		Configs:  configpostgres.New(db, tenantpg.New(db)),
		Backends: providerpg.New(db, catalog),
		Secrets:  secrets,
		Subject:  "knowledge-migration",
	}
	source, err := resolver.ResolveKnowledgeBackend(ctx, c.TenantID, current.Source.ConfigVersion)
	if err != nil {
		return fmt.Errorf("resolve source knowledge backend: %w", err)
	}
	target, err := resolver.ResolveKnowledgeBackend(ctx, c.TenantID, current.Target.ConfigVersion)
	if err != nil {
		return fmt.Errorf("resolve target knowledge backend: %w", err)
	}
	driver, err := knowledgeqdrant.NewMigrationDrill(authority, knowledgedriverpg.New(db), knowledgedriverpg.NewProbeSource(db), source, target)
	if err != nil {
		return fmt.Errorf("knowledge migration drill rejected: %w", err)
	}
	return runKnowledgeMigrationStep(ctx, logger, authority, driver, knowledgedriverpg.NewPublisher(db), source.SnapshotWatermark(), c, current)
}

func runKnowledgeMigrationStep(ctx context.Context, logger *roleLogger, authority migration.Repository, driver knowledgedriver.Driver,
	publisher knowledgedriver.CutoverPublisher, watermark string, c knowledgeMigrationConfig, current migration.Migration,
) error {
	now := time.Now().UTC()
	transition := func(to migration.State, mutate func(*migration.TransitionRequest)) (migration.Migration, error) {
		in := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: to, At: now}
		if mutate != nil {
			mutate(&in)
		}
		return authority.Transition(ctx, in)
	}
	switch current.State {
	case migration.StatePlanned:
		next, err := transition(migration.StateSnapshot, func(in *migration.TransitionRequest) {
			in.SnapshotWatermark = watermark
		})
		if err != nil {
			return err
		}
		logger.Printf("knowledge migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
	case migration.StateSnapshot:
		next, err := transition(migration.StateDualWrite, func(in *migration.TransitionRequest) {
			in.DualWriteRef = "knowledge-mutation-ledger://" + current.TenantID + "/" + current.MigrationID
		})
		if err != nil {
			return err
		}
		logger.Printf("knowledge migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
	case migration.StateDualWrite:
		next, err := transition(migration.StateBackfill, nil)
		if err != nil {
			return err
		}
		logger.Printf("knowledge migration advanced tenant=%q migration=%q state=%s", next.TenantID, next.MigrationID, next.State)
	case migration.StateBackfill:
		repair, err := driver.Repair(ctx, knowledgedriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			WorkerID: c.WorkerID, Limit: c.RepairLimit, Now: now, Lease: c.Lease, RetryDelay: c.RetryDelay})
		if err != nil {
			return fmt.Errorf("repair knowledge migration: %w", err)
		}
		if current.BackfillComplete {
			next, err := driver.EnterVerify(ctx, current.TenantID, current.MigrationID, now)
			if err != nil {
				return fmt.Errorf("enter knowledge migration verification: %w", err)
			}
			logger.Printf("knowledge migration advanced tenant=%q migration=%q state=%s repair_claimed=%d repair_applied=%d",
				next.TenantID, next.MigrationID, next.State, repair.Claimed, repair.Applied)
			return nil
		}
		batch, err := driver.BackfillOnce(ctx, knowledgedriver.BackfillRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			Limit: c.BatchLimit, At: now})
		if err != nil {
			return fmt.Errorf("backfill knowledge migration: %w", err)
		}
		logger.Printf("knowledge migration backfill tenant=%q migration=%q repair_claimed=%d repair_applied=%d repair_retried=%d batch_records=%d complete=%t",
			current.TenantID, current.MigrationID, repair.Claimed, repair.Applied, repair.Retried, batch.Batch.RecordCount, batch.Migration.BackfillComplete)
	case migration.StateVerify:
		repair, err := driver.Repair(ctx, knowledgedriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			WorkerID: c.WorkerID, Limit: c.RepairLimit, Now: now, Lease: c.Lease, RetryDelay: c.RetryDelay})
		if err != nil {
			return fmt.Errorf("repair knowledge migration: %w", err)
		}
		verification, err := driver.ShadowVerify(ctx, current.TenantID, current.MigrationID)
		if err != nil {
			return fmt.Errorf("verify knowledge migration: %w", err)
		}
		recorded, err := authority.RecordVerification(ctx, migration.VerificationRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			ExpectedVersion: current.Version, Verification: verification, RecordedAt: now})
		if err != nil {
			return fmt.Errorf("record knowledge migration verification: %w", err)
		}
		logger.Printf("knowledge migration verified tenant=%q migration=%q version=%d repair_claimed=%d repair_applied=%d source_count=%d target_count=%d source_digest=%s target_digest=%s; explicit cutover remains required",
			recorded.TenantID, recorded.MigrationID, recorded.Version, repair.Claimed, repair.Applied,
			verification.SourceCount, verification.TargetCount, verification.SourceDigest, verification.TargetDigest)
	case migration.StateCutover, migration.StateObserve:
		repair, err := driver.Repair(ctx, knowledgedriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			WorkerID: c.WorkerID, Limit: c.RepairLimit, Now: now, Lease: c.Lease, RetryDelay: c.RetryDelay})
		if err != nil {
			return fmt.Errorf("repair knowledge migration: %w", err)
		}
		status, err := publisher.DrainStatus(ctx, current.TenantID, current.MigrationID)
		if err != nil {
			return fmt.Errorf("read knowledge migration drain status: %w", err)
		}
		logger.Printf("knowledge migration observe repair tenant=%q migration=%q claimed=%d applied=%d forward_outstanding=%d reverse_outstanding=%d source_in_flight=%d target_in_flight=%d",
			current.TenantID, current.MigrationID, repair.Claimed, repair.Applied, status.ForwardOutstanding, status.ReverseOutstanding,
			status.SourceInFlight, status.TargetInFlight)
	default:
		logger.Printf("knowledge migration has no worker action tenant=%q migration=%q state=%s", current.TenantID, current.MigrationID, current.State)
	}
	return nil
}
