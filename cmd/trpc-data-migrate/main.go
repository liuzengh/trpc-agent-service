// Command trpc-data-migrate performs a resumable, maintenance-window backend
// data migration. It intentionally does not implement best-effort dual writes:
// compatible workers observe a persistent Redis freeze before any copy begins.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/datamigration"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/sessionturn"
)

type options struct {
	action         string
	configPath     string
	tenantID       string
	migrationID    string
	controlDSNEnv  string
	resourceKind   string
	sourceRedisEnv string
	targetDSNEnv   string
	sourcePrefix   string
	sourceJSONL    string
	targetTable    string
	batchSize      int
	maxEvents      int
	drain          time.Duration
	timeout        time.Duration
}

func main() {
	var opts options
	flag.StringVar(&opts.action, "action", "status", "create, advance, rollback, status, or unfreeze")
	flag.StringVar(&opts.configPath, "config", "config/example.yaml", "current deployed service config")
	flag.StringVar(&opts.tenantID, "tenant", "", "tenant ID")
	flag.StringVar(&opts.migrationID, "migration-id", "", "stable migration ID")
	flag.StringVar(&opts.controlDSNEnv, "control-dsn-env", "TRPC_AGENT_DATA_MIGRATION_DSN", "environment variable containing the migration-ledger PostgreSQL DSN")
	flag.StringVar(&opts.resourceKind, "resource-kind", "session", "session or knowledge (knowledge migrates versioned vector JSONL)")
	flag.StringVar(&opts.sourceRedisEnv, "source-redis-url-env", "", "environment variable containing the source Redis URL")
	flag.StringVar(&opts.targetDSNEnv, "target-dsn-env", "", "environment variable containing the target PostgreSQL DSN")
	flag.StringVar(&opts.sourcePrefix, "source-key-prefix", "", "source Redis session key prefix")
	flag.StringVar(&opts.sourceJSONL, "source-jsonl", "", "versioned JSONL vector source for resource-kind=knowledge")
	flag.StringVar(&opts.targetTable, "target-table", "", "pre-migrated pgvector table for resource-kind=knowledge")
	flag.IntVar(&opts.batchSize, "batch-size", 100, "objects per resumable batch")
	flag.IntVar(&opts.maxEvents, "max-events", 100000, "fail-closed per-session event safety limit")
	flag.DurationVar(&opts.drain, "drain", 3*time.Minute, "minimum freeze time before copying")
	flag.DurationVar(&opts.timeout, "timeout", 2*time.Minute, "command timeout")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()
	if err := run(ctx, opts); err != nil {
		log.Printf("trpc-data-migrate failed: category=%s", failureCategory(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, opts options) error {
	if opts.tenantID == "" || opts.migrationID == "" {
		return errors.New("tenant and migration ID are required")
	}
	if opts.resourceKind != "session" && opts.resourceKind != "knowledge" {
		return errors.New("resource-kind must be session or knowledge")
	}
	if opts.action == "advance" && opts.resourceKind == "session" && opts.drain < 3*time.Minute {
		return errors.New("migration drain window must be at least three minutes")
	}
	cfg, err := config.Load(opts.configPath)
	if err != nil {
		return errors.New("load current deployment config")
	}
	tenant, err := findTenant(cfg, opts.tenantID)
	if err != nil {
		return err
	}
	appNamespace := domain.AppNamespace(tenant.TenantID, tenant.App.Name)
	controlDSN, err := config.Secret(opts.controlDSNEnv)
	if err != nil {
		return errors.New("migration ledger secret is unavailable")
	}
	controlPool, err := pgxpool.New(ctx, controlDSN)
	if err != nil {
		return errors.New("configure migration ledger")
	}
	defer controlPool.Close()
	if err := migrations.VerifyDataMigrations(ctx, controlPool); err != nil {
		return errors.New("migration ledger schema is unavailable")
	}
	store, err := datamigration.NewPostgresStore(controlPool)
	if err != nil {
		return err
	}
	if opts.action == "status" {
		job, err := tenantJobKind(ctx, store, opts.migrationID, tenant, opts.resourceKind)
		if err != nil {
			return err
		}
		fmt.Printf("migration=%s phase=%s status=%s copied=%d verified=%d failed=%d mismatches=%d generation=%d\n",
			job.MigrationID, job.Phase, job.Status, job.Copied, job.Verified, job.Failed, job.Mismatches, job.Generation)
		return nil
	}
	if opts.action == "rollback" {
		if _, err := tenantJobKind(ctx, store, opts.migrationID, tenant, opts.resourceKind); err != nil {
			return err
		}
		engine, err := datamigration.New(store, inactiveOperator{})
		if err != nil {
			return err
		}
		job, err := engine.RequestRollback(ctx, opts.migrationID)
		if err != nil {
			return err
		}
		fmt.Printf("migration=%s phase=%s status=%s; deploy Redis routing, then advance\n", job.MigrationID, job.Phase, job.Status)
		return nil
	}
	if opts.resourceKind == "knowledge" {
		return runVectorAction(ctx, cfg, tenant, opts, store)
	}
	if cfg.Coordination.Backend != "redis" {
		return errors.New("distributed Redis coordination is required for a safe migration freeze")
	}
	coordURL, err := config.Secret(cfg.Coordination.RedisURLEnv)
	if err != nil {
		return errors.New("coordination Redis secret is unavailable")
	}
	control, err := coordination.NewRedis(coordURL, cfg.Coordination.KeyPrefix)
	if err != nil {
		return errors.New("connect migration freeze control")
	}
	defer control.Close()

	switch opts.action {
	case "create":
		newJob, err := newSessionJob(tenant, opts)
		if err != nil {
			return err
		}
		existing, getErr := store.Get(ctx, opts.migrationID)
		switch {
		case errors.Is(getErr, datamigration.ErrNotFound):
			if err := store.Create(ctx, newJob); err != nil {
				return err
			}
		case getErr != nil:
			return getErr
		case existing.TenantID != newJob.TenantID || existing.AppName != newJob.AppName ||
			existing.ResourceKind != newJob.ResourceKind || existing.Source != newJob.Source || existing.Target != newJob.Target:
			return errors.New("migration ID already belongs to a different durable job")
		case terminalJob(existing):
			return datamigration.ErrTerminal
		}
		if _, err := control.FreezeTenant(ctx, tenant.TenantID, appNamespace, opts.migrationID); err != nil {
			return err
		}
		fmt.Printf("created session migration %s; tenant is frozen\n", opts.migrationID)
		return nil
	case "advance":
		job, err := tenantJob(ctx, store, opts.migrationID, tenant)
		if err != nil {
			return err
		}
		route := deployedSessionRoute(opts.configPath, tenant)
		// A prior finalize may have committed before the freeze release failed.
		// Recover without reconnecting migration data stores, but recheck routing.
		if terminalJob(job) {
			if err := unfreezeTerminal(ctx, control, job, tenant, route); err != nil {
				return err
			}
			printJob(job)
			return nil
		}
		engine, cleanup, err := buildEngine(ctx, tenant, opts, control, store)
		if err != nil {
			return err
		}
		defer cleanup()
		job, err = engine.Advance(ctx, opts.migrationID)
		if err != nil {
			return err
		}
		if job.Status == datamigration.StatusComplete || job.Status == datamigration.StatusRolledBack {
			if err := unfreezeTerminal(ctx, control, job, tenant, route); err != nil {
				return err
			}
		}
		printJob(job)
		return nil
	case "unfreeze":
		job, err := tenantJob(ctx, store, opts.migrationID, tenant)
		if err != nil {
			return err
		}
		return unfreezeTerminal(ctx, control, job, tenant, deployedSessionRoute(opts.configPath, tenant))
	default:
		return errors.New("unknown migration action")
	}
}

type inactiveOperator struct{}

func (inactiveOperator) ExecutePhase(context.Context, datamigration.Phase, datamigration.Job) (datamigration.Result, error) {
	return datamigration.Result{}, errors.New("inactive migration operator")
}

func runVectorAction(ctx context.Context, cfg *config.Config, tenant config.TenantConfig, opts options, store *datamigration.PostgresStore) error {
	switch opts.action {
	case "create":
		job, err := newVectorJob(tenant, opts)
		if err != nil {
			return err
		}
		existing, getErr := store.Get(ctx, opts.migrationID)
		switch {
		case errors.Is(getErr, datamigration.ErrNotFound):
			if err := store.Create(ctx, job); err != nil {
				return err
			}
		case getErr != nil:
			return getErr
		case existing.TenantID != job.TenantID || existing.AppName != job.AppName ||
			existing.ResourceKind != job.ResourceKind || existing.Source != job.Source || existing.Target != job.Target:
			return errors.New("migration ID already belongs to a different durable vector job")
		case terminalJob(existing):
			return datamigration.ErrTerminal
		}
		fmt.Printf("created knowledge vector migration %s\n", opts.migrationID)
		return nil
	case "advance":
		job, err := tenantJobKind(ctx, store, opts.migrationID, tenant, "knowledge")
		if err != nil {
			return err
		}
		if terminalJob(job) {
			printJob(job)
			return nil
		}
		engine, cleanup, err := buildVectorEngine(ctx, tenant, opts, store)
		if err != nil {
			return err
		}
		defer cleanup()
		job, err = engine.Advance(ctx, opts.migrationID)
		if err != nil {
			return err
		}
		printJob(job)
		return nil
	case "unfreeze":
		return errors.New("knowledge vector migrations do not use a tenant freeze")
	default:
		return errors.New("unknown migration action")
	}
}

func newVectorJob(tenant config.TenantConfig, opts options) (datamigration.Job, error) {
	if tenant.Data.Knowledge.Type != "vector" {
		return datamigration.Job{}, errors.New("knowledge vector migration requires a configured vector backend")
	}
	if opts.sourceJSONL == "" {
		return datamigration.Job{}, errors.New("source-jsonl is required for knowledge migration")
	}
	targetEnv := opts.targetDSNEnv
	if targetEnv == "" {
		targetEnv = tenant.Data.Knowledge.MigrationDSNEnv
	}
	if targetEnv == "" {
		return datamigration.Job{}, errors.New("target-dsn-env or knowledge migration_dsn_env is required for knowledge migration")
	}
	table := opts.targetTable
	if table == "" {
		table = tenant.Data.Knowledge.Namespace
	}
	if !secretReference.MatchString(targetEnv) {
		return datamigration.Job{}, errors.New("target-dsn-env must be an environment variable reference")
	}
	source, err := vectorSourceDescriptor(opts.sourceJSONL)
	if err != nil {
		return datamigration.Job{}, err
	}
	return datamigration.Job{
		MigrationID: opts.migrationID, TenantID: tenant.TenantID,
		AppName:      domain.AppNamespace(tenant.TenantID, tenant.App.Name),
		ResourceKind: "knowledge", Source: source,
		Target: "postgres-env:" + targetEnv + ";table:" + table,
	}, nil
}

func buildVectorEngine(ctx context.Context, tenant config.TenantConfig, opts options, store *datamigration.PostgresStore) (*datamigration.Engine, func(), error) {
	job, err := tenantJobKind(ctx, store, opts.migrationID, tenant, "knowledge")
	if err != nil {
		return nil, func() {}, err
	}
	path, err := vectorSourcePathForJob(job, opts.sourceJSONL)
	if err != nil {
		return nil, func() {}, err
	}
	targetEnv := opts.targetDSNEnv
	if targetEnv == "" {
		targetEnv = tenant.Data.Knowledge.MigrationDSNEnv
	}
	if !secretReference.MatchString(targetEnv) {
		return nil, func() {}, errors.New("target-dsn-env must be an environment variable reference")
	}
	if targetEnv == "" {
		return nil, func() {}, errors.New("target-dsn-env is required")
	}
	targetDSN, err := config.Secret(targetEnv)
	if err != nil {
		return nil, func() {}, errors.New("vector target database secret is unavailable")
	}
	pool, err := pgxpool.New(ctx, targetDSN)
	if err != nil {
		return nil, func() {}, errors.New("configure vector target database")
	}
	cleanup := func() { pool.Close() }
	if err := pool.Ping(ctx); err != nil {
		cleanup()
		return nil, func() {}, errors.New("connect vector target database")
	}
	table := opts.targetTable
	if table == "" {
		table = tenant.Data.Knowledge.Namespace
	}
	target, err := datamigration.NewPostgresVectorTarget(pool, table, tenant.TenantID, tenant.App.Name, tenant.Data.Knowledge.EmbeddingDimension)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	if err := target.VerifySchema(ctx); err != nil {
		cleanup()
		return nil, func() {}, errors.New("vector target schema is unavailable")
	}
	source, err := datamigration.NewJSONLVectorSource(path)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	operator, err := datamigration.NewVectorOperator(source, target, store, opts.batchSize)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	engine, err := datamigration.New(store, operator)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return engine, cleanup, nil
}

func vectorSourceDescriptor(path string) (string, error) {
	cleaned, err := filepath.Abs(path)
	if err != nil || cleaned == "" {
		return "", errors.New("source-jsonl path is invalid")
	}
	return "jsonl:" + base64.RawURLEncoding.EncodeToString([]byte(cleaned)), nil
}

func vectorSourcePathForJob(job datamigration.Job, explicit string) (string, error) {
	if explicit != "" {
		descriptor, err := vectorSourceDescriptor(explicit)
		if err != nil || descriptor != job.Source {
			return "", errors.New("source-jsonl does not match the durable vector migration")
		}
		return filepath.Abs(explicit)
	}
	if !strings.HasPrefix(job.Source, "jsonl:") {
		return "", errors.New("durable vector source descriptor is invalid")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(job.Source, "jsonl:"))
	if err != nil || len(decoded) == 0 {
		return "", errors.New("durable vector source descriptor is invalid")
	}
	return string(decoded), nil
}

func buildEngine(
	ctx context.Context,
	tenant config.TenantConfig,
	opts options,
	control *coordination.Redis,
	store *datamigration.PostgresStore,
) (*datamigration.Engine, func(), error) {
	if opts.sourceRedisEnv == "" || opts.targetDSNEnv == "" {
		return nil, func() {}, errors.New("source Redis and target PostgreSQL environment names are required")
	}
	job, err := tenantJob(ctx, store, opts.migrationID, tenant)
	if err != nil {
		return nil, func() {}, err
	}
	appNamespace := domain.AppNamespace(tenant.TenantID, tenant.App.Name)
	sourcePrefix, err := sourcePrefixForJob(job, opts)
	if err != nil {
		return nil, func() {}, err
	}
	sourceURL, err := config.Secret(opts.sourceRedisEnv)
	if err != nil {
		return nil, func() {}, errors.New("source Redis secret is unavailable")
	}
	source, err := datamigration.NewRedisSessionSource(sourceURL, sourcePrefix, appNamespace, opts.maxEvents)
	if err != nil {
		return nil, func() {}, err
	}
	targetDSN, err := config.Secret(opts.targetDSNEnv)
	if err != nil {
		_ = source.Close()
		return nil, func() {}, errors.New("target PostgreSQL secret is unavailable")
	}
	targetPool, err := pgxpool.New(ctx, targetDSN)
	if err != nil {
		_ = source.Close()
		return nil, func() {}, errors.New("configure session migration target")
	}
	cleanup := func() { targetPool.Close(); _ = source.Close() }
	turnStore, err := sessionturn.NewPostgres(targetPool)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	if err := turnStore.VerifySchema(ctx); err != nil {
		cleanup()
		return nil, func() {}, errors.New("session migration target schema is unavailable")
	}
	target, err := datamigration.NewPostgresSessionTarget(turnStore)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	gate, err := datamigration.NewDistributedMaintenanceGate(control, opts.drain)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	route := deployedSessionRoute(opts.configPath, tenant)
	routedGate, err := datamigration.NewRoutedMaintenanceGate(gate, route)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	router, err := datamigration.NewConfigRoutingSwitcher(route)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	operator, err := datamigration.NewSessionOperator(source, target, store, routedGate, router, opts.batchSize)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	engine, err := datamigration.New(store, operator)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return engine, cleanup, nil
}

func configuredSourcePrefix(tenant config.TenantConfig, explicit string) string {
	if explicit != "" {
		return explicit
	}
	if tenant.Data.Session.Namespace != "" {
		return tenant.Data.Session.Namespace
	}
	return domain.AppNamespace(tenant.TenantID, tenant.App.Name)
}

var secretReference = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func newSessionJob(tenant config.TenantConfig, opts options) (datamigration.Job, error) {
	if tenant.Data.Session.Type != "redis" {
		return datamigration.Job{}, errors.New("create requires the current session route to remain on Redis")
	}
	if !secretReference.MatchString(opts.sourceRedisEnv) || !secretReference.MatchString(opts.targetDSNEnv) {
		return datamigration.Job{}, errors.New("source Redis and target PostgreSQL environment names are required")
	}
	source := sourceDescriptor(opts.sourceRedisEnv, configuredSourcePrefix(tenant, opts.sourcePrefix))
	if source != sessionRoute(tenant) {
		return datamigration.Job{}, errors.New("migration source does not match the deployed session route")
	}
	return datamigration.Job{
		MigrationID: opts.migrationID, TenantID: tenant.TenantID,
		AppName: domain.AppNamespace(tenant.TenantID, tenant.App.Name), ResourceKind: "session",
		Source: source, Target: targetDescriptor(opts.targetDSNEnv),
	}, nil
}

func sourcePrefixForJob(job datamigration.Job, opts options) (string, error) {
	// SQL deployments need not retain the old Redis namespace. Resume from the
	// durable source descriptor unless an explicit prefix was supplied.
	prefix := opts.sourcePrefix
	if prefix == "" {
		marker := "redis-env:" + opts.sourceRedisEnv + ";prefix-b64:"
		if strings.HasPrefix(job.Source, marker) {
			decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(job.Source, marker))
			if err == nil {
				prefix = string(decoded)
			}
		}
	}
	if !secretReference.MatchString(opts.sourceRedisEnv) || !secretReference.MatchString(opts.targetDSNEnv) ||
		prefix == "" || job.Source != sourceDescriptor(opts.sourceRedisEnv, prefix) || job.Target != targetDescriptor(opts.targetDSNEnv) {
		return "", errors.New("migration arguments do not match the durable job identity")
	}
	return prefix, nil
}

func sourceDescriptor(envName, prefix string) string {
	return "redis-env:" + envName + ";prefix-b64:" + base64.RawURLEncoding.EncodeToString([]byte(prefix))
}

func targetDescriptor(envName string) string { return "postgres-env:" + envName }

func sessionRoute(tenant config.TenantConfig) string {
	if !secretReference.MatchString(tenant.Data.Session.DSNEnv) {
		return ""
	}
	switch tenant.Data.Session.Type {
	case "redis":
		return sourceDescriptor(tenant.Data.Session.DSNEnv, configuredSourcePrefix(tenant, ""))
	case "sql":
		return targetDescriptor(tenant.Data.Session.DSNEnv)
	default:
		return ""
	}
}

// Re-read the supplied deployment config for every safety check, including
// recovery after a committed terminal phase. This proves the config's route;
// the deployer must ensure workers use that config and keep secret references
// bound to the same databases for the duration of the migration.
func deployedSessionRoute(configPath string, tenant config.TenantConfig) func() string {
	return func() string {
		current, err := config.Load(configPath)
		if err != nil {
			return ""
		}
		currentTenant, err := findTenant(current, tenant.TenantID)
		if err != nil || currentTenant.App.Name != tenant.App.Name {
			return ""
		}
		return sessionRoute(currentTenant)
	}
}

func tenantJob(ctx context.Context, store datamigration.Store, migrationID string, tenant config.TenantConfig) (datamigration.Job, error) {
	return tenantJobKind(ctx, store, migrationID, tenant, "session")
}

func tenantJobKind(ctx context.Context, store datamigration.Store, migrationID string, tenant config.TenantConfig, resourceKind string) (datamigration.Job, error) {
	job, err := store.Get(ctx, migrationID)
	if err != nil {
		return datamigration.Job{}, err
	}
	if err := checkJobTenantKind(job, tenant, resourceKind); err != nil {
		return datamigration.Job{}, err
	}
	return job, nil
}

func checkJobTenant(job datamigration.Job, tenant config.TenantConfig) error {
	return checkJobTenantKind(job, tenant, "session")
}

func checkJobTenantKind(job datamigration.Job, tenant config.TenantConfig, resourceKind string) error {
	if job.TenantID != tenant.TenantID || job.AppName != domain.AppNamespace(tenant.TenantID, tenant.App.Name) || job.ResourceKind != resourceKind {
		return fmt.Errorf("migration does not belong to the configured tenant %s route", resourceKind)
	}
	return nil
}

func terminalJob(job datamigration.Job) bool {
	return job.Status == datamigration.StatusComplete || job.Status == datamigration.StatusRolledBack || job.Phase == datamigration.PhaseComplete
}

type tenantUnfreezer interface {
	UnfreezeTenant(ctx context.Context, tenantID, appName, migrationID string) error
}

func unfreezeTerminal(ctx context.Context, control tenantUnfreezer, job datamigration.Job, tenant config.TenantConfig, route func() string) error {
	if err := checkJobTenant(job, tenant); err != nil {
		return err
	}
	if job.Phase != datamigration.PhaseComplete {
		return errors.New("cannot unfreeze a non-terminal migration")
	}
	router, err := datamigration.NewConfigRoutingSwitcher(route)
	if err != nil {
		return err
	}
	switch job.Status {
	case datamigration.StatusComplete:
		err = router.Cutover(ctx, job)
	case datamigration.StatusRolledBack:
		err = router.Rollback(ctx, job)
	default:
		return errors.New("cannot unfreeze a non-terminal migration")
	}
	if err != nil {
		return err
	}
	return control.UnfreezeTenant(ctx, job.TenantID, job.AppName, job.MigrationID)
}

func printJob(job datamigration.Job) {
	fmt.Printf("migration=%s phase=%s status=%s copied=%d verified=%d mismatches=%d generation=%d\n",
		job.MigrationID, job.Phase, job.Status, job.Copied, job.Verified, job.Mismatches, job.Generation)
}

func findTenant(cfg *config.Config, tenantID string) (config.TenantConfig, error) {
	for _, tenant := range cfg.Tenants {
		if tenant.TenantID == tenantID {
			return tenant, nil
		}
	}
	return config.TenantConfig{}, errors.New("tenant is not present in config")
}

func failureCategory(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, datamigration.ErrSourceNotFrozen):
		return "source_not_frozen_or_drained"
	case errors.Is(err, datamigration.ErrTargetActive):
		return "target_active_before_cutover"
	case errors.Is(err, datamigration.ErrRoutingNotReady):
		return "routing_not_ready"
	case errors.Is(err, datamigration.ErrVerificationMismatch):
		return "verification_mismatch"
	default:
		return "migration_failed"
	}
}
