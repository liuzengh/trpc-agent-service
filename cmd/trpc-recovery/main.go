// Command trpc-recovery is the P2-02 offline recovery operator entrypoint.
//
// It provides four subcommands over the local PostgreSQL logical
// backup/restore and bounded session-event replay boundary:
//
//	backup   consistent-snapshot data-only archive + manifest publication
//	verify   backup directory, manifest, checksum and archive TOC audit
//	restore  migrations-first data-only restore into an isolated target
//	replay   bounded session-event integrity replay (dry-run default)
//
// It is an explicit high-privilege OFFLINE operator tool, strictly separated
// from the P2-01 runtime role model: it never starts business workers,
// dispatchers, vector workers or telemetry exporters, never adds a public
// endpoint, never grants the runtime role extra privileges, never passes
// passwords as process arguments (PGPASSFILE only) and never prints DSNs,
// passwords, tenant identifiers, business identifiers or data content.
// Every failure is reduced to one stable category on stderr plus a stable
// process exit code.
//
// Environment (all optional where a default is given):
//
//	P202_SOURCE_URL         backup source postgres:// DSN (backup)
//	P202_TARGET_URL         restore/replay target postgres:// DSN
//	P202_REPLAY_RUNTIME_URL replay runtime-role postgres:// DSN
//	P202_OUTPUT_DIR         parent directory for the new backup directory
//	P202_BACKUP_DIR         existing backup directory (verify/restore)
//	P202_MIGRATIONS_DIR     migration directory (default: migrations)
//	P202_TIMEOUT            bounded wall clock budget (default 30m, 1s..2h)
//	P202_ALLOW_REPAIR       must be "yes" to enable replay repair mode
//
// Subcommand flags:
//
//	replay --mode dry-run|repair   (default dry-run; repair additionally
//	                                requires P202_ALLOW_REPAIR=yes)
//
// Exit codes:
//
//	0 success
//	2 invalid configuration
//	3 dependency unavailable
//	4 integrity mismatch (archive/manifest/checksum/TOC)
//	5 forbidden state (non-empty target, migration mismatch, role rule,
//	  schema drift, source==target, runtime privilege)
//	6 replay integrity violation
//	7 restore failed (single transaction aborted, target stays empty)
//	8 backup failed (no acceptable artifact published)
//	9 timeout or cancellation
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/recovery"
)

const (
	exitOK              = 0
	exitInvalidConfig   = 2
	exitUnavailable     = 3
	exitIntegrity       = 4
	exitForbiddenState  = 5
	exitReplayViolation = 6
	exitRestoreFailed   = 7
	exitBackupFailed    = 8
	exitTimeoutOrCancel = 9

	defaultTimeout       = 30 * time.Minute
	minimumTimeout       = time.Second
	maximumTimeout       = 2 * time.Hour
	defaultMigrationsDir = "migrations"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		return fail("invalid_config")
	}
	timeout, err := parseTimeout(os.Getenv("P202_TIMEOUT"))
	if err != nil {
		return fail("invalid_config")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	migrationsDir := strings.TrimSpace(os.Getenv("P202_MIGRATIONS_DIR"))
	if migrationsDir == "" {
		migrationsDir = defaultMigrationsDir
	}
	// The pg_dump/pg_restore executor: the host PATH by default, or the
	// operator-pinned client image (tag+digest) when P202_CLIENT_IMAGE is
	// set. Args stay tool-constructed either way.
	var runner recovery.ClientRunner = recovery.ExecClientRunner{}
	if image := strings.TrimSpace(os.Getenv("P202_CLIENT_IMAGE")); image != "" {
		runner = recovery.DockerClientRunner{Image: image}
	}

	switch args[0] {
	case "backup":
		return runBackup(ctx, runner, migrationsDir)
	case "verify":
		return runVerify(ctx, runner, migrationsDir)
	case "restore":
		return runRestore(ctx, runner, migrationsDir)
	case "replay":
		return runReplay(ctx, args[1:])
	default:
		return fail("invalid_config")
	}
}

func runBackup(ctx context.Context, runner recovery.ClientRunner, migrationsDir string) int {
	result, err := recovery.RunBackup(ctx, recovery.BackupConfig{
		SourceURL:     os.Getenv("P202_SOURCE_URL"),
		OutputDir:     os.Getenv("P202_OUTPUT_DIR"),
		MigrationsDir: migrationsDir,
	}, runner)
	if err != nil {
		return fail(categoryOf(err))
	}
	emit(map[string]any{
		"command":        "backup",
		"backup_dir":     result.BackupDir,
		"archive_sha256": result.ArchiveSHA256,
		"archive_size":   result.ArchiveSize,
		"postgres_major": result.PostgresMajor,
		"rows_total":     result.TableSum,
	})
	return exitOK
}

func runVerify(ctx context.Context, runner recovery.ClientRunner, migrationsDir string) int {
	result, err := recovery.VerifyBackup(ctx, recovery.VerifyConfig{
		BackupDir:     os.Getenv("P202_BACKUP_DIR"),
		MigrationsDir: migrationsDir,
	}, runner)
	if err != nil {
		return fail(categoryOf(err))
	}
	emit(map[string]any{
		"command":        "verify",
		"archive_sha256": result.Manifest.Archive.SHA256,
		"archive_size":   result.Manifest.Archive.SizeByte,
		"tables":         len(result.Manifest.Tables),
		"toc_tables":     len(result.TOCTables),
	})
	return exitOK
}

func runRestore(ctx context.Context, runner recovery.ClientRunner, migrationsDir string) int {
	result, err := recovery.RunRestore(ctx, recovery.RestoreConfig{
		BackupDir:     os.Getenv("P202_BACKUP_DIR"),
		TargetURL:     os.Getenv("P202_TARGET_URL"),
		MigrationsDir: migrationsDir,
	}, runner)
	if err != nil {
		return fail(categoryOf(err))
	}
	emit(map[string]any{
		"command":        "restore",
		"rows_total":     result.TableSum,
		"postgres_major": result.PostgresMajor,
		"audits":         len(result.Audits),
	})
	return exitOK
}

func runReplay(ctx context.Context, args []string) int {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	mode := flags.String("mode", recovery.ReplayModeDryRun, "dry-run or repair")
	if err := flags.Parse(args); err != nil {
		return fail("invalid_config")
	}
	allowRepair := strings.TrimSpace(os.Getenv("P202_ALLOW_REPAIR")) == "yes"
	result, err := recovery.RunReplay(ctx, recovery.ReplayConfig{
		OwnerURL:      os.Getenv("P202_TARGET_URL"),
		RuntimeURL:    os.Getenv("P202_REPLAY_RUNTIME_URL"),
		Mode:          *mode,
		RepairAllowed: allowRepair,
	})
	if err != nil {
		return fail(categoryOf(err))
	}
	emit(map[string]any{
		"command":    "replay",
		"mode":       result.Mode,
		"tenants":    result.Tenants,
		"sessions":   result.Sessions,
		"events":     result.Events,
		"max_seq":    result.MaxSeq,
		"digest":     result.Digest,
		"repaired":   result.Repaired,
		"violations": result.Violations,
	})
	return exitOK
}

// emit writes the safe JSON summary (counts, digests, paths) to stdout.
func emit(payload map[string]any) {
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(payload)
}

// fail writes one stable category to stderr; raw errors never cross.
func fail(category string) int {
	fmt.Fprintf(os.Stderr, "recovery failed category=%s\n", category)
	return exitCodeOf(category)
}

func exitCodeOf(category string) int {
	switch category {
	case "invalid_config":
		return exitInvalidConfig
	case "dependency_unavailable":
		return exitUnavailable
	case "integrity_mismatch":
		return exitIntegrity
	case "forbidden_state":
		return exitForbiddenState
	case "replay_integrity_violation":
		return exitReplayViolation
	case "restore_failed":
		return exitRestoreFailed
	case "backup_failed":
		return exitBackupFailed
	case "timeout_or_cancelled":
		return exitTimeoutOrCancel
	default:
		return exitInvalidConfig
	}
}

func categoryOf(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errorsIsRecovery(err, recovery.ErrInvalidConfig):
		return "invalid_config"
	case errorsIsRecovery(err, recovery.ErrDependencyUnavailable):
		return "dependency_unavailable"
	case errorsIsRecovery(err, recovery.ErrIntegrityMismatch):
		return "integrity_mismatch"
	case errorsIsRecovery(err, recovery.ErrForbiddenState):
		return "forbidden_state"
	case errorsIsRecovery(err, recovery.ErrReplayViolation):
		return "replay_integrity_violation"
	case errorsIsRecovery(err, recovery.ErrRestoreFailed):
		return "restore_failed"
	case errorsIsRecovery(err, recovery.ErrBackupFailed):
		return "backup_failed"
	case errorsIsRecovery(err, recovery.ErrTimeoutOrCancelled):
		return "timeout_or_cancelled"
	default:
		return "invalid_config"
	}
}

func errorsIsRecovery(err error, target error) bool {
	return errors.Is(err, target)
}

func parseTimeout(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultTimeout, nil
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil {
		seconds, convErr := strconv.Atoi(raw)
		if convErr != nil {
			return 0, fmt.Errorf("invalid timeout")
		}
		timeout = time.Duration(seconds) * time.Second
	}
	if timeout < minimumTimeout || timeout > maximumTimeout {
		return 0, fmt.Errorf("timeout out of bounds")
	}
	return timeout, nil
}
