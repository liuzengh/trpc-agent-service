//go:build integration

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	vectorTask "github.com/liuzengh/trpc-agent-service/trpcservice/vector/task"
)

const (
	tenantAlpha = "p202-alpha"
	tenantBeta  = "p202-beta"
)

// TestBackupRestoreDrill is the end-to-end P2-02 evidence: live backup of a
// seeded two-tenant source, full verification, restore into a fresh isolated
// target database, fact equality, RLS/role audits, reclaim-by-new-owner,
// deterministic replay and a fresh (never restored) Redis.
func TestBackupRestoreDrill(t *testing.T) {
	drillStart := time.Now()
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	runner := DockerClientRunner{Image: lab.clientImage}

	// Seed two tenants with representative facts on every tenant table.
	if err := seedTenantFacts(ctx, t, lab.runtimePool, newTenantFactIDs(tenantAlpha, "alpha")); err != nil {
		t.Fatalf("seed alpha: %v", err)
	}
	if err := seedTenantFacts(ctx, t, lab.runtimePool, newTenantFactIDs(tenantBeta, "beta")); err != nil {
		t.Fatalf("seed beta: %v", err)
	}

	backupParent := t.TempDir()
	backupStart := time.Now()
	backup, err := RunBackup(ctx, BackupConfig{
		SourceURL:     lab.ownerURL,
		OutputDir:     backupParent,
		MigrationsDir: lab.migrations,
	}, runner)
	if err != nil {
		t.Fatalf("backup: category=%v detail=%q", categoryForTest(err), err.Error())
	}
	backupElapsed := time.Since(backupStart)
	if backup.TableSum == 0 || backup.ArchiveSize == 0 || len(backup.ArchiveSHA256) != 64 {
		t.Fatalf("backup artifact incomplete: rows=%d size=%d sha=%d", backup.TableSum, backup.ArchiveSize, len(backup.ArchiveSHA256))
	}
	t.Logf("backup evidence rows_total=%d archive_size=%d backup_elapsed=%s drill_elapsed=%s", backup.TableSum, backup.ArchiveSize, backupElapsed.Round(time.Millisecond), time.Since(drillStart).Round(time.Millisecond))

	// Verify: manifest, checksum, TOC audit, migration digest.
	verified, err := VerifyBackup(ctx, VerifyConfig{BackupDir: backup.BackupDir, MigrationsDir: lab.migrations}, runner)
	if err != nil {
		t.Fatalf("verify: category=%v detail=%q", categoryForTest(err), err.Error())
	}
	if len(verified.TOCTables) != 26 {
		t.Fatalf("TOC audit table count: got %d want 26", len(verified.TOCTables))
	}

	// Backup hygiene: fresh 0700 directory, 0600 files, and a manifest that
	// carries no DSN, no credential, no tenant identifier.
	assertBackupHygiene(t, backup.BackupDir, []string{"postgres://", "postgresql://", "trpc_test", "trpc_runtime", tenantAlpha, tenantBeta, "p006_"})

	// Restore into a fresh isolated target database (migrations-first).
	targetURL := lab.createTargetDatabase(t)
	restore, err := RunRestore(ctx, RestoreConfig{
		BackupDir:     backup.BackupDir,
		TargetURL:     targetURL,
		MigrationsDir: lab.migrations,
	}, runner)
	if err != nil {
		t.Fatalf("restore: category=%v detail=%q", categoryForTest(err), err.Error())
	}
	if restore.TableSum != backup.TableSum {
		t.Fatalf("restored row total mismatch: got %d want %d", restore.TableSum, backup.TableSum)
	}

	target, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetURL, MaxConns: 6, MinConns: 1})
	if err != nil {
		t.Fatal("target pool unavailable")
	}
	defer target.Close()

	// Fact equality: per-table row counts and representative values across
	// both tenants (JSONB, NULLs, timestamps, tombstone, config version,
	// owner/epoch/fence, attempt, lease, Outbox/DLQ, vector cursor).
	assertFactEquality(ctx, t, lab.ownerPool, target, newTenantFactIDs(tenantAlpha, "alpha"))
	assertFactEquality(ctx, t, lab.ownerPool, target, newTenantFactIDs(tenantBeta, "beta"))

	// Runtime role isolation on the restored target (dual tenant).
	targetRuntimeURL := withCredentials(t, targetURL, "trpc_runtime", "p202-runtime-password")
	runtimePool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetRuntimeURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal("target runtime pool unavailable")
	}
	defer runtimePool.Close()
	if err := tenantctx.EnsureRuntimeRoleLimits(ctx, runtimePool); err != nil {
		t.Fatalf("runtime role limits on target: %v", err)
	}
	assertDualTenantIsolation(ctx, t, runtimePool, newTenantFactIDs(tenantAlpha, "alpha").Session, newTenantFactIDs(tenantBeta, "alpha").Session)

	// Reclaim on restored facts: expired queue lease, expired outbox lock,
	// expired session lease and expired vector task lease are recovered by a
	// new owner while the old owner/fence stays rejected.
	assertReclaimOnRestoredFacts(ctx, t, runtimePool, newTenantFactIDs(tenantAlpha, "alpha"))

	// Deterministic replay on the restored target (dry-run, twice).
	replayOne, err := RunReplay(ctx, ReplayConfig{
		OwnerURL:   targetURL,
		RuntimeURL: targetRuntimeURL,
		Mode:       ReplayModeDryRun,
	})
	if err != nil {
		t.Fatalf("replay dry-run one: category=%v", categoryForTest(err))
	}
	replayTwo, err := RunReplay(ctx, ReplayConfig{
		OwnerURL:   targetURL,
		RuntimeURL: targetRuntimeURL,
		Mode:       ReplayModeDryRun,
	})
	if err != nil {
		t.Fatalf("replay dry-run two: category=%v", categoryForTest(err))
	}
	if replayOne.Digest != replayTwo.Digest || replayOne.Events != replayTwo.Events || replayOne.Sessions != replayTwo.Sessions {
		t.Fatalf("replay not deterministic: digest %s vs %s", replayOne.Digest, replayTwo.Digest)
	}
	if replayOne.Tenants != 2 || replayOne.Sessions != 2 || replayOne.Events != 6 {
		t.Fatalf("replay counts unexpected: %+v", replayOne)
	}
	if len(replayOne.Violations) != 0 {
		t.Fatalf("replay violations on valid fixture: %v", replayOne.Violations)
	}

	// Redis stayed empty: it is never part of the PostgreSQL backup and must
	// not have become a fact source.
	assertRedisEmpty(ctx, t, lab)
}

func categoryForTest(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrInvalidConfig):
		return "invalid_config"
	case errors.Is(err, ErrDependencyUnavailable):
		return "dependency_unavailable"
	case errors.Is(err, ErrIntegrityMismatch):
		return "integrity_mismatch"
	case errors.Is(err, ErrForbiddenState):
		return "forbidden_state"
	case errors.Is(err, ErrReplayViolation):
		return "replay_integrity_violation"
	case errors.Is(err, ErrRestoreFailed):
		return "restore_failed"
	case errors.Is(err, ErrBackupFailed):
		return "backup_failed"
	case errors.Is(err, ErrTimeoutOrCancelled):
		return "timeout_or_cancelled"
	default:
		return "uncategorized:" + err.Error()
	}
}

// assertBackupHygiene checks the published backup contract: fresh 0700
// directory, 0600 regular files, marker present and no sensitive content in
// the manifest.
func assertBackupHygiene(t *testing.T, backupDir string, forbidden []string) {
	t.Helper()
	dirInfo, err := os.Stat(backupDir)
	if err != nil || !dirInfo.IsDir() {
		t.Fatalf("backup dir missing")
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("backup dir mode is not 0700: %v", dirInfo.Mode().Perm())
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	markerSeen := false
	for _, entry := range entries {
		if entry.Type() != 0 {
			t.Fatalf("backup dir contains a non regular entry: %s", entry.Name())
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("backup file mode is not 0600: %s %v", entry.Name(), info.Mode().Perm())
		}
		if entry.Name() == BackupDirMarker {
			markerSeen = true
		}
	}
	if !markerSeen {
		t.Fatalf("completion marker missing")
	}
	raw, err := os.ReadFile(filepath.Join(backupDir, manifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	manifestText := string(raw)
	for _, needle := range forbidden {
		if len(needle) > 0 && containsString(manifestText, needle) {
			t.Fatalf("manifest contains forbidden content: category=manifest_leak category_seed=%d", len(needle))
		}
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("manifest json invalid")
	}
	for _, key := range []string{"format_version", "postgres_major", "schema_fingerprint", "migration_digest", "tables", "archive", "completed"} {
		if _, ok := parsed[key]; !ok {
			t.Fatalf("manifest missing key %s", key)
		}
	}
}

func containsString(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

// assertFactEquality compares representative restored values against the
// source for one tenant: whole-row JSONB projections across the fact
// families (JSONB payloads, NULLs, timestamps, tombstone, config version,
// owner/epoch/fence, attempt, lease, Outbox/DLQ state, vector cursor).
func assertFactEquality(ctx context.Context, t *testing.T, source, target *pgxpool.Pool, ids tenantFactIDs) {
	t.Helper()
	type check struct {
		name  string
		query string
		args  []any
	}
	checks := []check{
		{"session", `SELECT to_jsonb(x) FROM public.session x WHERE tenant_id=$1 AND session_id=$2`, []any{ids.Tenant, ids.Session}},
		{"tenant", `SELECT to_jsonb(x) FROM public.tenant x WHERE tenant_id=$1`, []any{ids.Tenant}},
		{"agent_app", `SELECT to_jsonb(x) FROM public.agent_app x WHERE tenant_id=$1 AND agent_app_id=$2`, []any{ids.Tenant, ids.App}},
		{"channel_binding", `SELECT to_jsonb(x) FROM public.channel_binding x WHERE tenant_id=$1 AND binding_id=$2`, []any{ids.Tenant, ids.Binding}},
		{"user_identity", `SELECT to_jsonb(x) FROM public.user_identity x WHERE tenant_id=$1 AND identity_id=$2`, []any{ids.Tenant, ids.Identity}},
		{"event1", `SELECT to_jsonb(x) FROM public.session_event x WHERE tenant_id=$1 AND event_id=$2`, []any{ids.Tenant, ids.Event1}},
		{"event2", `SELECT to_jsonb(x) FROM public.session_event x WHERE tenant_id=$1 AND event_id=$2`, []any{ids.Tenant, ids.Event2}},
		{"event3", `SELECT to_jsonb(x) FROM public.session_event x WHERE tenant_id=$1 AND event_id=$2`, []any{ids.Tenant, ids.Event3}},
		{"dedup_completed", `SELECT to_jsonb(x) FROM public.message_dedup x WHERE tenant_id=$1 AND external_message_id=$2`, []any{ids.Tenant, ids.DedupDone}},
		{"dedup_live", `SELECT to_jsonb(x) FROM public.message_dedup x WHERE tenant_id=$1 AND external_message_id=$2`, []any{ids.Tenant, ids.DedupLive}},
		{"memory_active", `SELECT to_jsonb(x) FROM public.memory x WHERE tenant_id=$1 AND memory_id=$2`, []any{ids.Tenant, ids.MemoryA}},
		{"memory_tombstone", `SELECT to_jsonb(x) FROM public.memory x WHERE tenant_id=$1 AND memory_id=$2`, []any{ids.Tenant, ids.MemoryB}},
		{"summary", `SELECT to_jsonb(x) FROM public.summary x WHERE tenant_id=$1 AND session_id=$2`, []any{ids.Tenant, ids.Session}},
		{"artifact", `SELECT to_jsonb(x) FROM public.artifact x WHERE tenant_id=$1 AND artifact_id=$2`, []any{ids.Tenant, ids.Artifact}},
		{"audit_log", `SELECT to_jsonb(x) FROM public.audit_log x WHERE tenant_id=$1 AND audit_id=$2`, []any{ids.Tenant, ids.Audit}},
		{"outbox_pending", `SELECT to_jsonb(x) FROM public.outbox_message x WHERE tenant_id=$1 AND outbox_id=$2`, []any{ids.Tenant, ids.OutboxPend}},
		{"outbox_locked", `SELECT to_jsonb(x) FROM public.outbox_message x WHERE tenant_id=$1 AND outbox_id=$2`, []any{ids.Tenant, ids.OutboxLock}},
		{"outbox_dead", `SELECT to_jsonb(x) FROM public.outbox_message x WHERE tenant_id=$1 AND outbox_id=$2`, []any{ids.Tenant, ids.OutboxDead}},
		{"dead_letter", `SELECT to_jsonb(x) FROM public.dead_letter x WHERE tenant_id=$1 AND dead_letter_id=$2`, []any{ids.Tenant, ids.DeadLetter}},
		{"agent_release", `SELECT to_jsonb(x) FROM public.agent_release x WHERE tenant_id=$1 AND release_id=$2`, []any{ids.Tenant, ids.Release}},
		{"config_published", `SELECT to_jsonb(x) FROM public.tenant_config_version x WHERE tenant_id=$1 AND config_version=1`, []any{ids.Tenant}},
		{"config_draft", `SELECT to_jsonb(x) FROM public.tenant_config_version x WHERE tenant_id=$1 AND config_version=2`, []any{ids.Tenant}},
		{"rollout", `SELECT to_jsonb(x) FROM public.tenant_config_rollout x WHERE tenant_id=$1`, []any{ids.Tenant}},
		{"config_operation", `SELECT to_jsonb(x) FROM public.tenant_config_operation x WHERE tenant_id=$1 AND operation_id=$2`, []any{ids.Tenant, ids.Operation}},
		{"coordination_epoch", `SELECT to_jsonb(x) FROM public.coordination_epoch x WHERE tenant_id=$1 AND resource_id=$2`, []any{ids.Tenant, ids.Epoch}},
		{"session_lease", `SELECT to_jsonb(x) FROM public.session_lease x WHERE tenant_id=$1 AND session_id=$2`, []any{ids.Tenant, ids.Session}},
		{"execution_result", `SELECT to_jsonb(x) FROM public.execution_result x WHERE tenant_id=$1 AND execution_id=$2`, []any{ids.Tenant, ids.Execution}},
		{"job_in_flight", `SELECT to_jsonb(x) FROM public.job_queue x WHERE tenant_id=$1 AND job_id=$2`, []any{ids.Tenant, ids.Job}},
		{"job_queued", `SELECT to_jsonb(x) FROM public.job_queue x WHERE tenant_id=$1 AND job_id=$2`, []any{ids.Tenant, ids.JobQueued}},
		{"binding_audit", `SELECT to_jsonb(x) FROM public.channel_binding_audit x WHERE tenant_id=$1 AND audit_id=$2`, []any{ids.Tenant, ids.BindingAud}},
		{"vector_task_retry", `SELECT to_jsonb(x) FROM public.vector_projection_task x WHERE tenant_id=$1 AND task_id=$2`, []any{ids.Tenant, ids.TaskRunning}},
		{"vector_task_done", `SELECT to_jsonb(x) FROM public.vector_projection_task x WHERE tenant_id=$1 AND task_id=$2`, []any{ids.Tenant, ids.TaskDone}},
		{"rebuild_run", `SELECT to_jsonb(x) FROM public.vector_rebuild_run x WHERE tenant_id=$1 AND run_id=$2`, []any{ids.Tenant, ids.Rebuild}},
	}
	for _, c := range checks {
		var sourceJSON, targetJSON []byte
		if err := source.QueryRow(ctx, c.query, c.args...).Scan(&sourceJSON); err != nil {
			t.Fatalf("source read %s: %v", c.name, err)
		}
		if err := target.QueryRow(ctx, c.query, c.args...).Scan(&targetJSON); err != nil {
			t.Fatalf("target read %s: %v", c.name, err)
		}
		if string(sourceJSON) != string(targetJSON) {
			t.Fatalf("fact mismatch %s:\nsource=%s\ntarget=%s", c.name, sourceJSON, targetJSON)
		}
	}
}

// assertDualTenantIsolation proves the restored target enforces the P2-01
// isolation with the runtime role: each tenant context sees exactly its own
// session row and never the other tenant's.
func assertDualTenantIsolation(ctx context.Context, t *testing.T, runtimePool *pgxpool.Pool, sessionA, sessionB string) {
	t.Helper()
	for _, tenantID := range []string{tenantAlpha, tenantBeta} {
		err := tenantctx.WithTenantContext(ctx, runtimePool, tenantID, "p2-02 dual tenant probe", func(ctx context.Context, tx pgx.Tx) error {
			var countAlpha, countBeta int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM session WHERE tenant_id=$1`, tenantAlpha).Scan(&countAlpha); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM session WHERE tenant_id=$1`, tenantBeta).Scan(&countBeta); err != nil {
				return err
			}
			if tenantID == tenantAlpha && (countAlpha != 1 || countBeta != 0) {
				t.Fatalf("tenant alpha visibility wrong: alpha=%d beta=%d", countAlpha, countBeta)
			}
			if tenantID == tenantBeta && (countAlpha != 0 || countBeta != 1) {
				t.Fatalf("tenant beta visibility wrong: alpha=%d beta=%d", countAlpha, countBeta)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("dual tenant probe: %v", err)
		}
	}
	_ = sessionA
	_ = sessionB
}

// assertReclaimOnRestoredFacts exercises the real repository reclaim paths
// against restored expired leases/locks with a NEW owner while the old
// owner/fence stays rejected.
func assertReclaimOnRestoredFacts(ctx context.Context, t *testing.T, runtimePool *pgxpool.Pool, ids tenantFactIDs) {
	t.Helper()
	now := time.Now().UTC()
	// 1. Job queue: expired in_flight lease is recovered and re-claimed by
	// the new owner; the stale delivery id can no longer be acked.
	jobQueue, err := queue.NewPostgresQueue(runtimePool, queue.PostgresQueueConfig{})
	if err != nil {
		t.Fatalf("queue init: %v", err)
	}
	delivery, err := jobQueue.Receive(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("queue reclaim receive: %v", err)
	}
	if delivery.Job.Tenant.TenantID != ids.Tenant || delivery.Job.JobID != ids.Job {
		t.Fatalf("queue reclaim got unexpected job tenant=%s job=%s", delivery.Job.Tenant.TenantID, delivery.Job.JobID)
	}
	staleDelivery := delivery
	staleDelivery.DeliveryID = ids.Delivery
	if err := jobQueue.Ack(ctx, staleDelivery); err == nil {
		t.Fatalf("stale delivery ack must be rejected")
	}
	if err := jobQueue.Ack(ctx, delivery); err != nil {
		t.Fatalf("fresh delivery ack: %v", err)
	}

	// 2. Outbox: expired processing lock is reclaimed by the new owner; the
	// old worker id can no longer complete it.
	outboxRepo, err := pgstore.NewOutboxRepository(runtimePool, pgstore.OutboxRepositoryConfig{LockDuration: 30 * time.Second})
	if err != nil {
		t.Fatalf("outbox init: %v", err)
	}
	tc := tenant.TenantContext{
		TenantID: ids.Tenant, AgentAppID: ids.App, BindingID: ids.Binding,
		Channel: "lark", SessionID: ids.Session, RequestID: "req-" + ids.Suffix,
		MessageID: ids.Message, TraceID: ids.Trace, ConfigVersion: 1,
		BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
	}
	claimed, err := outboxRepo.ClaimBatch(ctx, tc, ids.NewOwner, 10)
	if err != nil {
		t.Fatalf("outbox reclaim: %v", err)
	}
	foundLocked := false
	for _, message := range claimed {
		if message.ID == ids.OutboxLock {
			foundLocked = true
		}
	}
	if !foundLocked {
		t.Fatalf("expired outbox lock was not reclaimed")
	}
	if err := outboxRepo.MarkCompleted(ctx, tc, ids.Owner, ids.OutboxLock); err == nil {
		t.Fatalf("old outbox owner must be rejected")
	}
	if err := outboxRepo.MarkCompleted(ctx, tc, ids.NewOwner, ids.OutboxLock); err != nil {
		t.Fatalf("new owner complete: %v", err)
	}

	// 3. Session lease: expired lease is acquired by the new owner with a
	// strictly higher fencing token; the old lease fails validation.
	coordination, err := pgstore.NewCoordinationStore(runtimePool)
	if err != nil {
		t.Fatalf("coordination init: %v", err)
	}
	newLease, err := coordination.Acquire(ctx, tc, ids.Session, ids.NewOwner, 30*time.Second)
	if err != nil {
		t.Fatalf("lease reclaim: %v", err)
	}
	if newLease.FenceToken != 78 {
		t.Fatalf("fencing token did not advance: %d", newLease.FenceToken)
	}
	staleLease := newLease
	staleLease.OwnerID = ids.Owner
	staleLease.FenceToken = 77
	if err := coordination.Validate(ctx, tc, staleLease); err == nil {
		t.Fatalf("stale fence must be rejected")
	}
	if err := coordination.Validate(ctx, tc, newLease); err != nil {
		t.Fatalf("new lease validation: %v", err)
	}

	// 4. Vector task: expired running lease is reclaimable through the
	// SECURITY DEFINER candidate + claim path; the old fence is rejected.
	taskRepo, err := vectorTask.NewPostgresRepository(runtimePool, vectorTask.RepositoryConfig{})
	if err != nil {
		t.Fatalf("vector task init: %v", err)
	}
	candidate, err := taskRepo.Candidate(ctx, now)
	if err != nil {
		t.Fatalf("vector task candidate: %v", err)
	}
	if candidate.TenantID != ids.Tenant || candidate.TaskID != ids.TaskRunning {
		t.Fatalf("candidate mismatch: %+v", candidate)
	}
	newLeaseRef := vectorTask.LeaseRef{Owner: ids.NewOwner, Epoch: 2, Fence: 200, ExpiresAt: now.Add(30 * time.Second)}
	if _, err := taskRepo.Claim(ctx, candidate, newLeaseRef, now); err != nil {
		t.Fatalf("vector task claim: %v", err)
	}
	oldLeaseRef := vectorTask.LeaseRef{Owner: ids.Owner, Epoch: 1, Fence: 9, ExpiresAt: now.Add(30 * time.Second)}
	if _, err := taskRepo.Complete(ctx, candidate, oldLeaseRef, now); err == nil {
		t.Fatalf("old vector task fence must be rejected")
	}
}

// assertRedEmpty proves the drill's Redis stayed empty: Redis is never
// restored from the PostgreSQL backup and never became a fact source.
func assertRedisEmpty(ctx context.Context, t *testing.T, lab *recoveryLab) {
	t.Helper()
	redisID, _ := lab.lab.ContainerIDs()
	cmd := exec.CommandContext(ctx, "docker", "exec", redisID, "redis-cli", "DBSIZE")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("redis dbsize: %v", err)
	}
	if strings.TrimSpace(string(out)) != "0" {
		t.Fatalf("redis is not empty: dbsize=%s", strings.TrimSpace(string(out)))
	}
}

// TestRestoreTargetWatermarkRepairDrill runs the optional bounded repair on
// a restored target: a low watermark injected after restore is reported by
// dry-run, raised monotonically by the gated repair and verified clean.
func TestRestoreTargetWatermarkRepairDrill(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	runner := DockerClientRunner{Image: lab.clientImage}
	ids := newTenantFactIDs(tenantAlpha, "alpha")
	if err := seedTenantFacts(ctx, t, lab.runtimePool, ids); err != nil {
		t.Fatal(err)
	}
	backup, err := RunBackup(ctx, BackupConfig{SourceURL: lab.ownerURL, OutputDir: t.TempDir(), MigrationsDir: lab.migrations}, runner)
	if err != nil {
		t.Fatalf("backup: %v", categoryForTest(err))
	}
	targetURL := lab.createTargetDatabase(t)
	if _, err := RunRestore(ctx, RestoreConfig{BackupDir: backup.BackupDir, TargetURL: targetURL, MigrationsDir: lab.migrations}, runner); err != nil {
		t.Fatalf("restore: %v", categoryForTest(err))
	}
	targetRuntimeURL := withCredentials(t, targetURL, "trpc_runtime", "p202-runtime-password")
	targetOwner, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatal("target owner pool")
	}
	defer targetOwner.Close()

	// Baseline: the restored target replays clean.
	result, err := RunReplay(ctx, ReplayConfig{OwnerURL: targetURL, RuntimeURL: targetRuntimeURL, Mode: ReplayModeDryRun})
	if err != nil {
		t.Fatalf("baseline replay: %v", categoryForTest(err))
	}
	baselineDigest := result.Digest

	// Inject a low watermark on the isolated target (owner operation; the
	// drill's damaged-facts case).
	if _, err := targetOwner.Exec(ctx, `UPDATE session SET last_event_seq = 1
		WHERE tenant_id = $1 AND session_id = $2`, tenantAlpha, ids.Session); err != nil {
		t.Fatal(err)
	}
	dry, err := RunReplay(ctx, ReplayConfig{OwnerURL: targetURL, RuntimeURL: targetRuntimeURL, Mode: ReplayModeDryRun})
	if !errors.Is(err, ErrReplayViolation) {
		t.Fatalf("dry-run after damage: %v", categoryForTest(err))
	}
	if dry.Violations[ViolationWatermarkLow] != 1 {
		t.Fatalf("watermark_low not reported: %v", dry.Violations)
	}
	// Repair raises it monotonically; state and events stay untouched.
	if _, err := RunReplay(ctx, ReplayConfig{
		OwnerURL: targetURL, RuntimeURL: targetRuntimeURL,
		Mode: ReplayModeRepair, RepairAllowed: true,
	}); err != nil {
		t.Fatalf("repair: %v", categoryForTest(err))
	}
	if got := sessionWatermark(ctx, t, targetOwner, tenantAlpha, ids.Session); got != 3 {
		t.Fatalf("watermark after repair: %d", got)
	}
	clean, err := RunReplay(ctx, ReplayConfig{OwnerURL: targetURL, RuntimeURL: targetRuntimeURL, Mode: ReplayModeDryRun})
	if err != nil {
		t.Fatalf("clean replay: %v", categoryForTest(err))
	}
	if clean.Digest != baselineDigest {
		t.Fatalf("digest changed after watermark repair: %s vs %s", clean.Digest, baselineDigest)
	}
}

// TestRestoredFactsDriveBoundedDispatcher proves the restored Outbox facts
// are dispatchable through the real Dispatcher with a RECORDING sender:
// provider requests stay zero by construction, the pending restored message
// completes, and no new facts appear beyond the outbox transition.
func TestRestoredFactsDriveBoundedDispatcher(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	runner := DockerClientRunner{Image: lab.clientImage}
	ids := newTenantFactIDs(tenantAlpha, "alpha")
	if err := seedTenantFacts(ctx, t, lab.runtimePool, ids); err != nil {
		t.Fatal(err)
	}
	backup, err := RunBackup(ctx, BackupConfig{SourceURL: lab.ownerURL, OutputDir: t.TempDir(), MigrationsDir: lab.migrations}, runner)
	if err != nil {
		t.Fatalf("backup: %v", categoryForTest(err))
	}
	targetURL := lab.createTargetDatabase(t)
	if _, err := RunRestore(ctx, RestoreConfig{BackupDir: backup.BackupDir, TargetURL: targetURL, MigrationsDir: lab.migrations}, runner); err != nil {
		t.Fatalf("restore: %v", categoryForTest(err))
	}
	targetRuntimeURL := withCredentials(t, targetURL, "trpc_runtime", "p202-runtime-password")
	runtimePool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetRuntimeURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal("target runtime pool")
	}
	defer runtimePool.Close()
	targetOwner, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatal("target owner pool")
	}
	defer targetOwner.Close()

	var sent []string
	sender := outbox.SenderFunc(func(ctx context.Context, message storage.OutboxMessage) outbox.SenderOutcome {
		sent = append(sent, message.ID)
		return outbox.SenderOutcome{Class: outbox.OutcomeDelivered, Code: "recorded"}
	})
	repository, err := pgstore.NewOutboxRepository(runtimePool, pgstore.OutboxRepositoryConfig{LockDuration: 30 * time.Second})
	if err != nil {
		t.Fatal("outbox repository")
	}
	dispatcher, err := outbox.NewDispatcher(repository, sender, outbox.Config{
		OwnerID: ids.NewOwner,
		Tenant: tenant.TenantContext{
			TenantID: ids.Tenant, AgentAppID: ids.App, BindingID: ids.Binding,
			Channel: "lark", SessionID: ids.Session, RequestID: "req-" + ids.Suffix,
			MessageID: ids.Message, TraceID: ids.Trace, ConfigVersion: 1,
			BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
		},
		ClaimBatchSize: 10,
		ClaimInterval:  50 * time.Millisecond,
		LockGuard:      time.Second,
	})
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	runCtx, runCancel := context.WithTimeout(ctx, 5*time.Second)
	defer runCancel()
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(runCtx) }()
	// Bounded event-driven wait: poll until the pending message completed.
	deadline := time.Now().Add(8 * time.Second)
	for {
		var status string
		if err := targetOwner.QueryRow(ctx, `SELECT status FROM outbox_message WHERE tenant_id=$1 AND outbox_id=$2`, tenantAlpha, ids.OutboxPend).Scan(&status); err == nil && status == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending outbox message never completed")
		}
		select {
		case <-ctx.Done():
			t.Fatal("drill context expired")
		case <-time.After(50 * time.Millisecond):
		}
	}
	dispatcher.Stop(context.Background())
	_ = <-runDone
	if len(sent) != 1 || sent[0] != ids.OutboxPend {
		t.Fatalf("recording sender saw unexpected sends: %d", len(sent))
	}
	// Dead messages never dispatch; DLQ fact intact.
	var dlq int
	if err := targetOwner.QueryRow(ctx, `SELECT count(*) FROM dead_letter WHERE tenant_id=$1`, tenantAlpha).Scan(&dlq); err != nil {
		t.Fatal(err)
	}
	if dlq != 1 {
		t.Fatalf("DLQ fact drifted: %d", dlq)
	}
}
