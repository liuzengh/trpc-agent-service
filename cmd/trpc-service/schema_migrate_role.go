package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/migrations"
)

const schemaMigrationAdvisoryLock int64 = 0x545250434147454e

var migrationVersionPattern = regexp.MustCompile(`^[0-9]{6}$`)

type schemaMigrationConfig struct {
	PostgresDSN     string
	ExpectedCurrent string
	Target          string
	Timeout         time.Duration
}

func loadSchemaMigrationConfig(getenv func(string) string) (schemaMigrationConfig, error) {
	if getenv == nil {
		return schemaMigrationConfig{}, errors.New("environment reader is required")
	}
	config := schemaMigrationConfig{
		PostgresDSN:     strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		ExpectedCurrent: strings.TrimSpace(getenv("TRPC_MIGRATION_EXPECTED_CURRENT")),
		Target:          strings.TrimSpace(getenv("TRPC_MIGRATION_TARGET")),
		Timeout:         10 * time.Minute,
	}
	var err error
	config.Timeout, err = envDuration(getenv, "TRPC_MIGRATION_TIMEOUT", config.Timeout)
	if err != nil || config.Timeout < 10*time.Second || config.Timeout > 2*time.Hour {
		return schemaMigrationConfig{}, errors.New("invalid TRPC_MIGRATION_TIMEOUT")
	}
	if config.PostgresDSN == "" || config.ExpectedCurrent == "" || config.Target == "" {
		return schemaMigrationConfig{}, errors.New("required schema migration configuration is missing")
	}
	if config.ExpectedCurrent != migrations.EmptyVersion && !migrationVersionPattern.MatchString(config.ExpectedCurrent) {
		return schemaMigrationConfig{}, errors.New("invalid TRPC_MIGRATION_EXPECTED_CURRENT")
	}
	if !migrationVersionPattern.MatchString(config.Target) {
		return schemaMigrationConfig{}, errors.New("invalid TRPC_MIGRATION_TARGET")
	}
	if config.ExpectedCurrent == config.Target {
		return schemaMigrationConfig{}, errors.New("migration source and target must differ")
	}
	return config, nil
}

func runSchemaMigrate(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid schema migration dependencies")
	}
	config, err := loadSchemaMigrationConfig(getenv)
	if err != nil {
		return fmt.Errorf("schema migration configuration rejected: %w", err)
	}
	ctx, cancel := context.WithTimeout(parent, config.Timeout)
	defer cancel()
	db, err := sql.Open("pgx", config.PostgresDSN)
	if err != nil {
		return errors.New("postgres migration client initialization failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		return errors.New("postgres migration database unavailable")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return errors.New("postgres migration session unavailable")
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, schemaMigrationAdvisoryLock); err != nil {
		return errors.New("postgres migration lock unavailable")
	}
	defer releaseSchemaMigrationLock(conn)

	runner := migrations.NewRunnerOnConn(conn)
	plan, err := runner.Plan(ctx, config.Target)
	if err != nil {
		return fmt.Errorf("schema migration preflight failed: %w", err)
	}
	alreadyApplied, err := validateSchemaMigrationTransition(plan, config.ExpectedCurrent)
	if err != nil {
		return err
	}
	if alreadyApplied {
		logger.Printf("schema migration already applied current=%s target=%s", plan.Current, plan.Target)
		return nil
	}
	logger.Printf("schema migration starting current=%s target=%s steps=%d", plan.Current, plan.Target, len(plan.Pending))
	if err := runner.UpTo(ctx, config.Target); err != nil {
		return fmt.Errorf("schema migration apply failed: %w", err)
	}
	verified, err := runner.Plan(ctx, config.Target)
	if err != nil {
		return fmt.Errorf("schema migration verification failed: %w", err)
	}
	if verified.Current != config.Target || len(verified.Pending) != 0 {
		return errors.New("schema migration verification did not reach target")
	}
	logger.Printf("schema migration complete current=%s target=%s", verified.Current, verified.Target)
	return nil
}

func validateSchemaMigrationTransition(plan migrations.Plan, expectedCurrent string) (bool, error) {
	if plan.Current == plan.Target && len(plan.Pending) == 0 {
		return true, nil
	}
	if plan.Current != expectedCurrent {
		return false, fmt.Errorf("schema migration source mismatch: current=%s expected=%s", plan.Current, expectedCurrent)
	}
	if len(plan.Pending) == 0 {
		return false, errors.New("schema migration has no forward steps")
	}
	return false, nil
}

func releaseSchemaMigrationLock(conn *sql.Conn) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, schemaMigrationAdvisoryLock)
}
