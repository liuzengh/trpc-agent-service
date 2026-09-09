//go:build integration

package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

// blockingRunner simulates a client tool that never finishes: it blocks
// until the context is cancelled and then reports the classified timeout
// category, like a killed pg_dump/pg_restore child would.
type blockingRunner struct{ started chan struct{} }

func (r blockingRunner) Run(ctx context.Context, _ ClientTool, _ []string, _ []string, _ []Mount, _ io.Writer) error {
	if r.started != nil {
		close(r.started)
	}
	<-ctx.Done()
	return classifyClientError(ctx.Err(), ToolPgDump)
}

// failingRunner always reports the given category error.
type failingRunner struct{ err error }

func (r failingRunner) Run(context.Context, ClientTool, []string, []string, []Mount, io.Writer) error {
	return r.err
}

// TestBackupCancellationLeavesNoArtifact proves that a cancelled or timed
// out backup publishes nothing acceptable: no completion marker, and
// verify refuses the directory.
func TestBackupCancellationLeavesNoArtifact(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := seedTenantFacts(ctx, t, lab.runtimePool, newTenantFactIDs(tenantAlpha, "alpha")); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	runner := blockingRunner{started: started}
	parent := t.TempDir()
	backupCtx, cancelBackup := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := RunBackup(backupCtx, BackupConfig{
			SourceURL:     lab.ownerURL,
			OutputDir:     parent,
			MigrationsDir: lab.migrations,
		}, runner)
		done <- err
	}()
	<-started
	time.Sleep(50 * time.Millisecond)
	cancelBackup()
	err := <-done
	if !errors.Is(err, ErrTimeoutOrCancelled) {
		t.Fatalf("cancelled backup category: %v", categoryForTest(err))
	}
	// No acceptable artifact: verify must refuse every directory the
	// interrupted run left behind.
	entries, readErr := os.ReadDir(parent)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) == 0 {
		t.Fatalf("interrupted backup left no directory to inspect")
	}
	for _, entry := range entries {
		if _, err := VerifyBackup(ctx, VerifyConfig{BackupDir: filepath.Join(parent, entry.Name())}, runner); !errors.Is(err, ErrIntegrityMismatch) {
			t.Fatalf("verify accepted interrupted backup %s: %v", entry.Name(), categoryForTest(err))
		}
	}
}

// TestTornSnapshotConcurrentTransaction proves snapshot consistency: a
// concurrent cross-table transaction that commits after the exported
// snapshot lands entirely AFTER the backup (neither table shows a partial
// effect), so the archive can never contain a torn cross-table state.
func TestTornSnapshotConcurrentTransaction(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	runner := DockerClientRunner{Image: lab.clientImage}
	ids := newTenantFactIDs(tenantAlpha, "alpha")
	if err := seedTenantFacts(ctx, t, lab.runtimePool, ids); err != nil {
		t.Fatal(err)
	}

	committed := make(chan error, 1)
	proceed := make(chan struct{})
	backupParent := t.TempDir()
	backup, err := RunBackup(ctx, BackupConfig{
		SourceURL:     lab.ownerURL,
		OutputDir:     backupParent,
		MigrationsDir: lab.migrations,
		afterSnapshot: func(ctx context.Context) error {
			// Concurrent cross-table transaction: touches job_queue and
			// outbox_message together, commits while the dump runs.
			go func() {
				tx, txErr := lab.ownerPool.Begin(ctx)
				if txErr != nil {
					committed <- txErr
					return
				}
				if _, txErr = tx.Exec(ctx, `INSERT INTO job_queue (tenant_id, job_id, execution_id, schema_version, payload, status, available_at, attempt)
					VALUES ('p202-alpha','job-concurrent','exec-concurrent',1,'{}'::jsonb,'queued',now(),1)`); txErr != nil {
					_ = tx.Rollback(ctx)
					committed <- txErr
					return
				}
				if _, txErr = tx.Exec(ctx, `INSERT INTO outbox_message (tenant_id, outbox_id, kind, aggregate_id, payload, status, attempt)
					VALUES ('p202-alpha','obx-concurrent','reply','agg','{}'::jsonb,'pending',1)`); txErr != nil {
					_ = tx.Rollback(ctx)
					committed <- txErr
					return
				}
				close(proceed)
				committed <- tx.Commit(ctx)
			}()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-proceed:
				return nil
			}
		},
	}, runner)
	if err != nil {
		t.Fatalf("concurrent backup: %v", categoryForTest(err))
	}
	if err := <-committed; err != nil {
		t.Fatalf("concurrent transaction: %v", err)
	}

	// Restore the archive into a fresh target and prove neither concurrent
	// row is present while every snapshot row is.
	targetURL := lab.createTargetDatabase(t)
	restore, err := RunRestore(ctx, RestoreConfig{
		BackupDir:     backup.BackupDir,
		TargetURL:     targetURL,
		MigrationsDir: lab.migrations,
	}, runner)
	if err != nil {
		t.Fatalf("restore: %v", categoryForTest(err))
	}
	target, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal("target pool unavailable")
	}
	defer target.Close()
	for _, probe := range []struct{ table, key string }{
		{"job_queue", "job-concurrent"},
		{"outbox_message", "obx-concurrent"},
	} {
		var count int
		if err := target.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s WHERE tenant_id=$1 AND %s=$2`, probe.table, map[string]string{"job_queue": "job_id", "outbox_message": "outbox_id"}[probe.table]), tenantAlpha, probe.key).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("torn snapshot: concurrent row leaked into archive (%s)", probe.table)
		}
	}
	var snapshotJobs, snapshotOutbox int
	if err := target.QueryRow(ctx, `SELECT count(*) FROM public.job_queue WHERE tenant_id=$1`, tenantAlpha).Scan(&snapshotJobs); err != nil {
		t.Fatal(err)
	}
	if err := target.QueryRow(ctx, `SELECT count(*) FROM public.outbox_message WHERE tenant_id=$1`, tenantAlpha).Scan(&snapshotOutbox); err != nil {
		t.Fatal(err)
	}
	if snapshotJobs != 2 || snapshotOutbox != 3 {
		t.Fatalf("snapshot facts drifted: jobs=%d outbox=%d", snapshotJobs, snapshotOutbox)
	}
	_ = restore
}

// TestBackupIntegrityFailureMatrix covers the tamper matrix: truncated
// archive, single-byte corruption, damaged manifest and removed completion
// marker. Every variant must fail verification with the integrity category.
func TestBackupIntegrityFailureMatrix(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	runner := DockerClientRunner{Image: lab.clientImage}
	if err := seedTenantFacts(ctx, t, lab.runtimePool, newTenantFactIDs(tenantAlpha, "alpha")); err != nil {
		t.Fatal(err)
	}
	backup, err := RunBackup(ctx, BackupConfig{
		SourceURL:     lab.ownerURL,
		OutputDir:     t.TempDir(),
		MigrationsDir: lab.migrations,
	}, runner)
	if err != nil {
		t.Fatalf("backup: %v", categoryForTest(err))
	}
	manifestRaw, err := os.ReadFile(filepath.Join(backup.BackupDir, manifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	archiveRaw, err := os.ReadFile(filepath.Join(backup.BackupDir, archiveFileName))
	if err != nil {
		t.Fatal(err)
	}

	rebuild := func(t *testing.T, manifest []byte, archive []byte, marker bool) string {
		t.Helper()
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, manifestFileName), manifest, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, archiveFileName), archive, 0o600); err != nil {
			t.Fatal(err)
		}
		if marker {
			if err := os.WriteFile(filepath.Join(dir, BackupDirMarker), []byte("p2-02\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}

	t.Run("archive_truncated", func(t *testing.T) {
		dir := rebuild(t, manifestRaw, archiveRaw[:len(archiveRaw)/2], true)
		if _, err := VerifyBackup(ctx, VerifyConfig{BackupDir: dir}, runner); !errors.Is(err, ErrIntegrityMismatch) {
			t.Fatalf("truncated archive category: %v", categoryForTest(err))
		}
	})
	t.Run("archive_single_byte_corruption", func(t *testing.T) {
		corrupted := append([]byte(nil), archiveRaw...)
		corrupted[len(corrupted)/2] ^= 0xFF
		dir := rebuild(t, manifestRaw, corrupted, true)
		if _, err := VerifyBackup(ctx, VerifyConfig{BackupDir: dir}, runner); !errors.Is(err, ErrIntegrityMismatch) {
			t.Fatalf("corrupted archive category: %v", categoryForTest(err))
		}
	})
	t.Run("manifest_damaged", func(t *testing.T) {
		damaged := append([]byte(nil), manifestRaw...)
		damaged[0] = 'x'
		dir := rebuild(t, damaged, archiveRaw, true)
		if _, err := VerifyBackup(ctx, VerifyConfig{BackupDir: dir}, runner); !errors.Is(err, ErrIntegrityMismatch) {
			t.Fatalf("damaged manifest category: %v", categoryForTest(err))
		}
	})
	t.Run("marker_missing", func(t *testing.T) {
		dir := rebuild(t, manifestRaw, archiveRaw, false)
		if _, err := VerifyBackup(ctx, VerifyConfig{BackupDir: dir}, runner); !errors.Is(err, ErrIntegrityMismatch) {
			t.Fatalf("missing marker category: %v", categoryForTest(err))
		}
	})
}

// TestRestoreRefusalMatrix proves the restore safety protocol rejects every
// forbidden precondition with a stable category and zero side effects.
func TestRestoreRefusalMatrix(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	runner := DockerClientRunner{Image: lab.clientImage}
	if err := seedTenantFacts(ctx, t, lab.runtimePool, newTenantFactIDs(tenantAlpha, "alpha")); err != nil {
		t.Fatal(err)
	}
	backup, err := RunBackup(ctx, BackupConfig{
		SourceURL:     lab.ownerURL,
		OutputDir:     t.TempDir(),
		MigrationsDir: lab.migrations,
	}, runner)
	if err != nil {
		t.Fatalf("backup: %v", categoryForTest(err))
	}
	restore := func(targetURL string) error {
		_, err := RunRestore(ctx, RestoreConfig{
			BackupDir:     backup.BackupDir,
			TargetURL:     targetURL,
			MigrationsDir: lab.migrations,
		}, runner)
		return err
	}

	t.Run("same_source_target", func(t *testing.T) {
		if err := restore(lab.ownerURL); !errors.Is(err, ErrForbiddenState) {
			t.Fatalf("same source/target category: %v", categoryForTest(err))
		}
	})
	t.Run("runtime_role_target", func(t *testing.T) {
		if err := restore(lab.runtimeURL); !errors.Is(err, ErrForbiddenState) {
			t.Fatalf("runtime role target category: %v", categoryForTest(err))
		}
	})
	t.Run("non_privileged_role_target", func(t *testing.T) {
		lowURL, err := createLowPrivilegeTarget(ctx, t, lab)
		if err != nil {
			t.Fatal(err)
		}
		if err := restore(lowURL); !errors.Is(err, ErrForbiddenState) {
			t.Fatalf("low privilege role category: %v", categoryForTest(err))
		}
	})
	t.Run("migration_checksum_mismatch", func(t *testing.T) {
		targetURL := lab.createTargetDatabase(t)
		pool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetURL, MaxConns: 2, MinConns: 1})
		if err != nil {
			t.Fatal("target pool")
		}
		defer pool.Close()
		if _, err := pool.Exec(ctx, `UPDATE schema_migration SET checksum='tampered' WHERE version=1`); err != nil {
			t.Fatal(err)
		}
		if err := restore(targetURL); !errors.Is(err, ErrForbiddenState) {
			t.Fatalf("checksum mismatch category: %v", categoryForTest(err))
		}
	})
	t.Run("unknown_future_migration", func(t *testing.T) {
		targetURL := lab.createTargetDatabase(t)
		pool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetURL, MaxConns: 2, MinConns: 1})
		if err != nil {
			t.Fatal("target pool")
		}
		defer pool.Close()
		if _, err := pool.Exec(ctx, `INSERT INTO schema_migration (version, name, checksum) VALUES (99,'future','deadbeef')`); err != nil {
			t.Fatal(err)
		}
		if err := restore(targetURL); !errors.Is(err, ErrForbiddenState) {
			t.Fatalf("unknown migration category: %v", categoryForTest(err))
		}
	})
	t.Run("non_empty_target", func(t *testing.T) {
		targetURL := lab.createTargetDatabase(t)
		// First restore succeeds; the second must refuse the now non-empty
		// business target.
		if err := restore(targetURL); err != nil {
			t.Fatalf("first restore should succeed: %v", categoryForTest(err))
		}
		if err := restore(targetURL); !errors.Is(err, ErrForbiddenState) {
			t.Fatalf("non-empty target category: %v", categoryForTest(err))
		}
	})
	t.Run("non_data_only_archive", func(t *testing.T) {
		dir := t.TempDir()
		full, err := RunBackup(ctx, BackupConfig{
			SourceURL:     lab.ownerURL,
			OutputDir:     dir,
			MigrationsDir: lab.migrations,
		}, runner)
		if err != nil {
			t.Fatal(err)
		}
		// Overwrite the archive with a full schema+data dump of the source
		// and repair the manifest checksum so only the TOC audit can catch it.
		manifestPath := filepath.Join(full.BackupDir, manifestFileName)
		manifestRaw, err := os.ReadFile(manifestPath)
		if err != nil {
			t.Fatal(err)
		}
		var manifest map[string]any
		if err := jsonUnmarshalLoose(manifestRaw, &manifest); err != nil {
			t.Fatal(err)
		}
		archivePath := filepath.Join(full.BackupDir, archiveFileName)
		dumpEnv, dumpCleanup, err := writePGPassFile(mustParse(t, lab.ownerURL))
		if err != nil {
			t.Fatal(err)
		}
		defer dumpCleanup()
		dumpArgs := []string{"--format=custom", "--file=" + archivePath}
		for _, table := range ArchiveTables() {
			dumpArgs = append(dumpArgs, "--table=public."+table)
		}
		if err := runner.Run(ctx, ToolPgDump, dumpArgs, libpqEnv(mustParse(t, lab.ownerURL), dumpEnv), []Mount{{Host: full.BackupDir, Container: full.BackupDir}}, io.Discard); err != nil {
			t.Fatalf("full dump: %v", err)
		}
		digest, size, err := sha256File(archivePath)
		if err != nil {
			t.Fatal(err)
		}
		archiveSection := manifest["archive"].(map[string]any)
		archiveSection["sha256"] = digest
		archiveSection["size_bytes"] = size
		updated, err := jsonMarshalLoose(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, updated, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyBackup(ctx, VerifyConfig{BackupDir: full.BackupDir}, runner); !errors.Is(err, ErrIntegrityMismatch) {
			t.Fatalf("non-data-only archive category: %v", categoryForTest(err))
		}
	})
}

// TestRestoreInterruptionRollsBack proves the single-transaction restore
// contract under a real child kill: pg_restore is cancelled while blocked on
// a table lock mid-COPY, the server aborts the whole transaction, the target
// stays migrations-only and empty, and a retry succeeds without duplicate
// rows.
func TestRestoreInterruptionRollsBack(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	runner := DockerClientRunner{Image: lab.clientImage}
	ids := newTenantFactIDs(tenantAlpha, "alpha")
	if err := seedTenantFacts(ctx, t, lab.runtimePool, ids); err != nil {
		t.Fatal(err)
	}
	backup, err := RunBackup(ctx, BackupConfig{
		SourceURL:     lab.ownerURL,
		OutputDir:     t.TempDir(),
		MigrationsDir: lab.migrations,
	}, runner)
	if err != nil {
		t.Fatalf("backup: %v", categoryForTest(err))
	}
	targetURL := lab.createTargetDatabase(t)
	target, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetURL, MaxConns: 6, MinConns: 1})
	if err != nil {
		t.Fatal("target pool")
	}
	defer target.Close()

	// The test seam acquires an ACCESS EXCLUSIVE lock on job_queue right
	// before pg_restore starts, so the COPY blocks deterministically instead
	// of racing a wall clock. The lock connection is owned by the test and
	// released only after the cancellation.
	lockReady := make(chan struct{})
	var lockConn *pgxpool.Conn
	var lockTx pgx.Tx
	var lockErr error
	restoreCtx, cancelRestore := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := RunRestore(restoreCtx, RestoreConfig{
			BackupDir:     backup.BackupDir,
			TargetURL:     targetURL,
			MigrationsDir: lab.migrations,
			beforeRestore: func(ctx context.Context) error {
				conn, acquireErr := target.Acquire(context.Background())
				if acquireErr != nil {
					return acquireErr
				}
				lockConn = conn
				lockTx, lockErr = conn.Begin(context.Background())
				if lockErr != nil {
					conn.Release()
					return lockErr
				}
				if _, lockErr = lockTx.Exec(context.Background(), `LOCK TABLE public.job_queue IN ACCESS EXCLUSIVE MODE`); lockErr != nil {
					_ = lockTx.Rollback(context.Background())
					conn.Release()
					return lockErr
				}
				close(lockReady)
				return nil
			},
		}, runner)
		done <- err
	}()
	select {
	case <-lockReady:
	case err := <-done:
		t.Fatalf("restore failed before reaching the lock seam: %v", categoryForTest(err))
	case <-time.After(3 * time.Minute):
		t.Fatalf("lock seam never ran")
	}
	// Bounded polling: wait until the restore child is really blocked on the
	// lock inside COPY job_queue (event-observed, never a fixed sleep).
	blocked := false
	for i := 0; i < 300 && !blocked; i++ {
		var blockedCount int
		if err := lab.ownerPool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE 'COPY public.job_queue%'`).Scan(&blockedCount); err == nil && blockedCount > 0 {
			blocked = true
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("restore did not reach the locked COPY in time")
		case err := <-done:
			t.Fatalf("restore finished before reaching the locked COPY: %v", categoryForTest(err))
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !blocked {
		t.Fatalf("restore never blocked on the locked table")
	}
	cancelRestore()
	// The container may outlive its killed CLI client, so the restore
	// backend is terminated server-side: the single transaction aborts and
	// rolls back deterministically.
	_, _ = lab.ownerPool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE query LIKE 'COPY public.job_queue%'`)
	if err := <-done; err == nil || (!errors.Is(err, ErrTimeoutOrCancelled) && !errors.Is(err, ErrRestoreFailed)) {
		t.Fatalf("cancelled restore category: %v", categoryForTest(err))
	}
	_ = lockTx.Rollback(context.Background())
	lockConn.Release()
	// Bounded wait until the restore backend is fully gone.
	for i := 0; i < 300; i++ {
		var remaining int
		if err := lab.ownerPool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE query LIKE '%COPY public.job_queue%'`).Scan(&remaining); err == nil && remaining == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("restore backend never exited")
		case <-time.After(100 * time.Millisecond):
		}
	}
	// The aborted single transaction left the target empty.
	for _, table := range ArchiveTables() {
		var count int64
		if err := target.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s`, table)).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("restore was not rolled back: %s has %d rows", table, count)
		}
	}
	// Retry after cleanup: identical result, no duplicate rows.
	retry, err := RunRestore(ctx, RestoreConfig{
		BackupDir:     backup.BackupDir,
		TargetURL:     targetURL,
		MigrationsDir: lab.migrations,
	}, runner)
	if err != nil {
		t.Fatalf("retry restore: %v", categoryForTest(err))
	}
	if retry.TableSum != backup.TableSum {
		t.Fatalf("retry row total mismatch: got %d want %d", retry.TableSum, backup.TableSum)
	}
}

// TestClientToolUnavailable proves the dependency category when the pg16
// client tooling is unusable.
func TestClientToolUnavailable(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := seedTenantFacts(ctx, t, lab.runtimePool, newTenantFactIDs(tenantAlpha, "alpha")); err != nil {
		t.Fatal(err)
	}
	_, err := RunBackup(ctx, BackupConfig{
		SourceURL:     lab.ownerURL,
		OutputDir:     t.TempDir(),
		MigrationsDir: lab.migrations,
	}, failingRunner{err: fmt.Errorf("%w: pg_dump", ErrDependencyUnavailable)})
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("client unavailable category: %v", categoryForTest(err))
	}
}

// createLowPrivilegeTarget creates an isolated target database plus a
// NOBYPASSRLS non-superuser role, returning that role's DSN. Restores must
// refuse it: FORCE RLS would reject multi-tenant COPY.
func createLowPrivilegeTarget(ctx context.Context, t *testing.T, lab *recoveryLab) (string, error) {
	t.Helper()
	targetURL := lab.createTargetDatabase(t)
	cfg, err := pgx.ParseConfig(lab.ownerURL)
	if err != nil {
		return "", err
	}
	cfg.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx)
	password := "p202-lowpriv-password"
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE ROLE p202_lowpriv LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '%s'`, password)); err != nil {
		return "", err
	}
	dbName := dbNameOf(t, targetURL)
	if _, err := conn.Exec(ctx, fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO p202_lowpriv`, pgx.Identifier{dbName}.Sanitize())); err != nil {
		return "", err
	}
	return withCredentials(t, targetURL, "p202_lowpriv", password), nil
}

// TestClientVersionMismatchRefused documents the real host-client guard:
// the host pg_dump (major 15) against the PostgreSQL 16 laboratory must fail
// the backup instead of producing an incompatible archive. This runs through
// the direct executor with the host PATH.
func TestClientVersionMismatchRefused(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := seedTenantFacts(ctx, t, lab.runtimePool, newTenantFactIDs(tenantAlpha, "alpha")); err != nil {
		t.Fatal(err)
	}
	_, err := RunBackup(ctx, BackupConfig{
		SourceURL:     lab.ownerURL,
		OutputDir:     t.TempDir(),
		MigrationsDir: lab.migrations,
	}, ExecClientRunner{})
	if err != nil {
		if errors.Is(err, ErrBackupFailed) || errors.Is(err, ErrDependencyUnavailable) {
			return // refused with a stable category: either is correct
		}
		t.Fatalf("version mismatch category: %v", categoryForTest(err))
	}
	t.Fatalf("host pg_dump 15 against server 16 must not succeed")
}
