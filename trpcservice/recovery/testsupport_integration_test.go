//go:build integration

package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
)

// pinnedClientImage resolves the PostgreSQL 16 client image (tag+digest
// pinned) used for every pg_dump/pg_restore invocation inside the tests. A
// missing image is a hard failure: the drill never silently falls back to an
// unpinned or mismatched client.
func pinnedClientImage(t *testing.T) string {
	t.Helper()
	image := "postgres:16-alpine"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	inspect := func() (string, error) {
		cmd := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image)
		out, err := cmd.Output()
		return strings.TrimSpace(string(out)), err
	}
	digest, err := inspect()
	if err != nil {
		pull := exec.CommandContext(ctx, "docker", "pull", image)
		if output, pullErr := pull.CombinedOutput(); pullErr != nil {
			t.Fatalf("pull pinned client image: category=image_pull_failed bytes=%d", len(output))
		}
		if digest, err = inspect(); err != nil {
			t.Fatalf("resolve client image digest: category=digest_unavailable")
		}
	}
	if !strings.Contains(digest, "@sha256:") {
		t.Fatalf("client image digest malformed: category=digest_malformed")
	}
	return digest
}

// migrationsDirFor resolves the repository migration directory.
func migrationsDirFor(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../migrations"))
}

// newRecoveryLab boots one owner-scoped PostgreSQL 16 laboratory, applies
// the release migrations and provisions the NOBYPASSRLS runtime role. It is
// the source database of every backup drill.
type recoveryLab struct {
	t           *testing.T
	lab         *testinfra.DockerLab
	ownerURL    string
	runtimeURL  string
	ownerPool   *pgxpool.Pool
	runtimePool *pgxpool.Pool
	migrations  string
	clientImage string
}

func newRecoveryLab(t *testing.T) *recoveryLab {
	t.Helper()
	lab := testinfra.NewDockerLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	ownerURL := lab.PostgresURL(ctx)
	l := &recoveryLab{
		t: t, lab: lab, ownerURL: ownerURL,
		migrations:  migrationsDirFor(t),
		clientImage: pinnedClientImage(t),
	}
	l.migrateAndProvision(ctx, ownerURL)
	var err error
	l.ownerPool, err = pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: ownerURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal("owner pool unavailable")
	}
	l.runtimeURL = withCredentials(t, ownerURL, "trpc_runtime", "p202-runtime-password")
	l.runtimePool, err = pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: l.runtimeURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal("runtime pool unavailable")
	}
	t.Cleanup(func() {
		l.ownerPool.Close()
		l.runtimePool.Close()
	})
	return l
}

// migrateAndProvision applies migrations and provisions the runtime role
// with the same deployment protocol (P1-09 migrate step / P2-01 role supply).
func (l *recoveryLab) migrateAndProvision(ctx context.Context, databaseURL string) {
	l.t.Helper()
	migrator, err := pgstore.NewMigrator(ctx, pgstore.PostgresConfig{URL: databaseURL}, os.DirFS(l.migrations))
	if err != nil {
		l.t.Fatalf("migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		migrator.Close()
		l.t.Fatalf("migrations: %v", err)
	}
	migrator.Close()
	admin, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: databaseURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		l.t.Fatal("provision pool unavailable")
	}
	defer admin.Close()
	const runtimeRole = "trpc_runtime"
	const runtimePassword = "p202-runtime-password"
	if err := pgstore.EnsureTenantRuntimeRole(ctx, admin, "public", runtimeRole, runtimePassword); err != nil {
		l.t.Fatalf("ensure runtime role: %v", err)
	}
	if err := pgstore.GrantRuntimeSchemaPrivileges(ctx, admin, "public", runtimeRole); err != nil {
		l.t.Fatalf("grant runtime privileges: %v", err)
	}
}

// createTargetDatabase creates a fresh isolated restore-target database
// inside the same laboratory container, initialized with the same release
// migrations and a freshly provisioned runtime role. PostgreSQL authority is
// proven per-database: nothing is copied except the migration schema.
func (l *recoveryLab) createTargetDatabase(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("p202_target_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg, err := pgx.ParseConfig(l.ownerURL)
	if err != nil {
		t.Fatal("target parse")
	}
	cfg.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal("target bootstrap connection unavailable")
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, pgx.Identifier{name}.Sanitize())); err != nil {
		conn.Close(ctx)
		t.Fatal("target database create failed")
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatal("target bootstrap close")
	}
	targetURL := strings.Replace(l.ownerURL, "/trpc_agent_test", "/"+name, 1)
	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer migrateCancel()
	l.migrateAndProvision(migrateCtx, targetURL)
	return targetURL
}

// tenantFactIDs carries the deterministic per-tenant fixture identifiers.
// Values are test-local; nothing here is production data.
type tenantFactIDs struct {
	Tenant      string
	Suffix      string
	App         string
	Binding     string
	ExtApp      string
	Identity    string
	ExtUser     string
	Session     string
	Message     string
	Execution   string
	Trace       string
	Owner       string
	NewOwner    string
	Job         string
	JobQueued   string
	Delivery    string
	OutboxPend  string
	OutboxLock  string
	OutboxDead  string
	DeadLetter  string
	MemoryA     string
	MemoryB     string
	Artifact    string
	Audit       string
	BindingAud  string
	Release     string
	Operation   string
	Epoch       string
	TaskRunning string
	TaskDone    string
	Document    string
	Rebuild     string
	DedupDone   string
	DedupLive   string
	Event1      string
	Event2      string
	Event3      string
}

func newTenantFactIDs(tenant, suffix string) tenantFactIDs {
	h := func(seed string, n int) string {
		sum := sha256.Sum256([]byte(seed))
		return hex.EncodeToString(sum[:n])
	}
	ids := tenantFactIDs{
		Tenant: tenant, Suffix: suffix,
		App: "app-" + suffix, Binding: "binding-" + suffix, ExtApp: "ext-app-" + suffix,
		Identity: "ident-" + suffix, ExtUser: "ext-user-" + suffix,
		Session: "sess-" + suffix, Message: "msg-" + suffix, Execution: "exec-" + suffix,
		Trace: "trace-" + suffix, Owner: "p202-old-owner", NewOwner: "p202-new-owner",
		Job: "job-" + suffix, JobQueued: "job-q-" + suffix, Delivery: "dlv-" + suffix,
		OutboxPend: "obx-p-" + suffix, OutboxLock: "obx-l-" + suffix, OutboxDead: "obx-d-" + suffix,
		DeadLetter: "dl-" + suffix, MemoryA: "mem-a-" + suffix, MemoryB: "mem-b-" + suffix,
		Artifact: "art-" + suffix, Audit: "aud-" + suffix, BindingAud: "cba-" + suffix,
		Release: "rel-" + suffix, Operation: "op-" + suffix, Epoch: "res-" + suffix,
		TaskRunning: "vt-v1-" + h("task-running-"+suffix, 16), TaskDone: "vt-v1-" + h("task-done-"+suffix, 16),
		Document: "vd-v1-" + h("document-"+suffix, 32), Rebuild: "vrr-v1-" + h("rebuild-"+suffix, 16),
		DedupDone: "dedup-done-" + suffix, DedupLive: "dedup-live-" + suffix,
		Event1: "evt-1-" + suffix, Event2: "evt-2-" + suffix, Event3: "evt-3-" + suffix,
	}
	return ids
}

// seedTenantFacts inserts one representative fact row-set for the tenant
// across every tenant table through the P2-01 tenant transaction protocol
// (runtime role + transaction-local GUC). The fixture deliberately covers:
// JSONB payloads, NULL columns, tombstones, config version transitions,
// owner/epoch/fence columns, attempts, expired leases and locks, Outbox/DLQ
// states and vector task/rebuild cursors.
func seedTenantFacts(ctx context.Context, t *testing.T, runtimePool *pgxpool.Pool, ids tenantFactIDs) error {
	t.Helper()
	now := time.Now().UTC()
	// The reclaimed in_flight job carries a fully valid AgentJob payload so
	// the real queue Receive path can decode and fence it after restore.
	jobPayload, err := json.Marshal(queue.AgentJob{
		SchemaVersion: queue.SchemaVersion,
		JobID:         ids.Job,
		ExecutionID:   ids.Execution,
		Tenant: queue.TenantContextDTO{
			TenantID: ids.Tenant, AgentAppID: ids.App, BindingID: ids.Binding,
			Channel: "lark", SessionID: ids.Session, RequestID: "req-" + ids.Suffix,
			MessageID: ids.Message, TraceID: ids.Trace, ConfigVersion: 1,
			BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
		},
		Agent:     queue.AgentRefDTO{TenantID: ids.Tenant, AgentAppID: ids.App, Version: 1},
		Trace:     queue.TraceContextDTO{TraceID: ids.Trace, RequestID: "req-" + ids.Suffix, MessageID: ids.Message, ExecutionID: ids.Execution},
		Message:   queue.MessageDTO{ID: ids.Message, Role: "user", Content: "p202 fixture message", CreatedAt: now},
		CreatedAt: now,
		Deadline:  now.Add(10 * time.Minute),
		Attempt:   1,
	})
	if err != nil {
		return err
	}
	return tenantctx.WithTenantContext(ctx, runtimePool, ids.Tenant, "p2-02 seed facts", func(ctx context.Context, tx pgx.Tx) error {
		stmts := []string{
			fmt.Sprintf(`INSERT INTO tenant (tenant_id, name, status, config_version, default_agent_app_id, backend_config, policy_config, budget_cents)
				VALUES ('%s','tenant-%s','active',3,'%s','{"backend":"local","nested":{"n":1}}','{"policy":true}',500)`, ids.Tenant, ids.Suffix, ids.App),
			fmt.Sprintf(`INSERT INTO agent_app (tenant_id, agent_app_id, name, status, model_config_ref, system_prompt, system_prompt_ref, tool_policy, guardrail_ref)
				VALUES ('%s','%s','app-%s','active','model-ref',NULL,NULL,'{"tools":[]}',NULL)`, ids.Tenant, ids.App, ids.Suffix),
			fmt.Sprintf(`INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id, secret_ref, enabled, config)
				VALUES ('%s','lark','%s','%s','env:placeholder',true,'{"cfg":1}')`, ids.Tenant, ids.Binding, ids.ExtApp),
			fmt.Sprintf(`INSERT INTO user_identity (tenant_id, identity_id, channel, binding_id, external_user_id, internal_user_id, display_name, attributes)
				VALUES ('%s','%s','lark','%s','%s','internal-%s','display-%s','{"attr":"v"}')`, ids.Tenant, ids.Identity, ids.Binding, ids.ExtUser, ids.Suffix, ids.Suffix),
			fmt.Sprintf(`INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user, state, state_version, summary_version, last_event_seq)
				VALUES ('%s','%s','%s',1,'lark','%s','chat-%s','user-%s','active',2,1,3)`,
				ids.Tenant, ids.Session, ids.App, ids.Binding, ids.Suffix, ids.Suffix),
			fmt.Sprintf(`INSERT INTO session_event (tenant_id, session_id, event_id, sequence, event_type, role, message_id, execution_id, parent_event_id, attempt, trace_id, payload)
				VALUES ('%s','%s','%s',1,'user.received','user','%s',NULL,NULL,1,'%s','{"text":"hello","meta":{"k":1},"items":["a","b"]}')`,
				ids.Tenant, ids.Session, ids.Event1, ids.Message, ids.Trace),
			fmt.Sprintf(`INSERT INTO session_event (tenant_id, session_id, event_id, sequence, event_type, role, message_id, execution_id, parent_event_id, attempt, trace_id, payload)
				VALUES ('%s','%s','%s',2,'agent.started','assistant',NULL,'%s','%s',1,'%s','{}')`,
				ids.Tenant, ids.Session, ids.Event2, ids.Execution, ids.Event1, ids.Trace),
			fmt.Sprintf(`INSERT INTO session_event (tenant_id, session_id, event_id, sequence, event_type, role, message_id, execution_id, parent_event_id, attempt, trace_id, payload)
				VALUES ('%s','%s','%s',3,'assistant.completed','assistant',NULL,'%s','%s',1,'%s','{"done":true,"null_field":null}')`,
				ids.Tenant, ids.Session, ids.Event3, ids.Execution, ids.Event2, ids.Trace),
			fmt.Sprintf(`INSERT INTO message_dedup (tenant_id, channel, binding_id, external_message_id, status, owner_id, attempt, fence_token, response_ref, claimed_at, expires_at, epoch)
				VALUES ('%s','lark','%s','%s','completed','%s',1,5,'resp-%s',now(),now()+interval '1 hour',2)`,
				ids.Tenant, ids.Binding, ids.DedupDone, ids.Owner, ids.Suffix),
			fmt.Sprintf(`INSERT INTO message_dedup (tenant_id, channel, binding_id, external_message_id, status, owner_id, attempt, fence_token, claimed_at, expires_at, epoch)
				VALUES ('%s','lark','%s','%s','acquired','%s',1,4,now()-interval '3 hours',now()-interval '1 hour',1)`,
				ids.Tenant, ids.Binding, ids.DedupLive, ids.Owner),
			fmt.Sprintf(`INSERT INTO memory (tenant_id, memory_id, scope, scope_id, session_id, kind, content, vector_ref, version, source_seq, deleted)
				VALUES ('%s','%s','session','%s','%s','fact','content-%s','vec-%s',2,5,false)`,
				ids.Tenant, ids.MemoryA, ids.Session, ids.Session, ids.Suffix, ids.Suffix),
			fmt.Sprintf(`INSERT INTO memory (tenant_id, memory_id, scope, scope_id, session_id, kind, content, vector_ref, version, source_seq, deleted)
				VALUES ('%s','%s','tenant','%s',NULL,'note','old-content',NULL,1,3,true)`,
				ids.Tenant, ids.MemoryB, ids.Tenant),
			fmt.Sprintf(`INSERT INTO summary (tenant_id, session_id, version, covered_seq, content, token_estimate)
				VALUES ('%s','%s',1,2,'summary-%s',42)`, ids.Tenant, ids.Session, ids.Suffix),
			fmt.Sprintf(`INSERT INTO artifact (tenant_id, artifact_id, session_id, message_id, object_key, mime_type, size_bytes, sha256, status, expires_at)
				VALUES ('%s','%s','%s','%s','tenants/%s/art-%s','text/plain',128,'%s','ready',now()+interval '24 hours')`,
				ids.Tenant, ids.Artifact, ids.Session, ids.Message, ids.Tenant, ids.Suffix, strings.Repeat("ab", 32)),
			fmt.Sprintf(`INSERT INTO audit_log (tenant_id, audit_id, trace_id, request_id, execution_id, channel, external_user, session_id, agent_app_id, tool_name, decision, latency_micros, cost_cents, error_type, metadata)
				VALUES ('%s','%s','%s','req-%s','%s','lark','user-%s','%s','%s','tool.name','allow',1500,3,NULL,'{"m":1}')`,
				ids.Tenant, ids.Audit, ids.Trace, ids.Suffix, ids.Execution, ids.Suffix, ids.Session, ids.App),
			fmt.Sprintf(`INSERT INTO outbox_message (tenant_id, outbox_id, kind, aggregate_id, payload, status, attempt, next_attempt_at, dedup_key, config_version)
				VALUES ('%s','%s','reply','agg-%s','{"reply":"r-%s"}','pending',1,now(),'dk-%s',1)`,
				ids.Tenant, ids.OutboxPend, ids.Suffix, ids.Suffix, ids.Suffix),
			fmt.Sprintf(`INSERT INTO outbox_message (tenant_id, outbox_id, kind, aggregate_id, payload, status, attempt, next_attempt_at, locked_by, locked_until, dedup_key, config_version)
				VALUES ('%s','%s','reply','agg-%s','{}','processing',2,now()-interval '1 hour','%s',now()-interval '30 minutes','dkl-%s',1)`,
				ids.Tenant, ids.OutboxLock, ids.Suffix, ids.Owner, ids.Suffix),
			fmt.Sprintf(`INSERT INTO outbox_message (tenant_id, outbox_id, kind, aggregate_id, payload, status, attempt, next_attempt_at, last_error, config_version)
				VALUES ('%s','%s','reply','agg-%s','{}','dead',5,now(),'category=permanent',1)`,
				ids.Tenant, ids.OutboxDead, ids.Suffix),
			fmt.Sprintf(`INSERT INTO dead_letter (tenant_id, dead_letter_id, outbox_id, kind, payload, attempt, reason, last_error)
				VALUES ('%s','%s','%s','reply','{}',5,'max attempts','category=permanent')`,
				ids.Tenant, ids.DeadLetter, ids.OutboxDead),
			fmt.Sprintf(`INSERT INTO agent_release (tenant_id, agent_app_id, agent_version, release_id, status, model_config_ref, tool_policy, artifact_ref, config_ref, released_at)
				VALUES ('%s','%s',1,'%s','released','model-ref','{"tools":[]}','art-ref','cfg-ref',now())`,
				ids.Tenant, ids.App, ids.Release),
			fmt.Sprintf(`INSERT INTO coordination_epoch (tenant_id, resource_id, epoch)
				VALUES ('%s','%s',3)`, ids.Tenant, ids.Epoch),
			fmt.Sprintf(`INSERT INTO session_lease (tenant_id, session_id, owner_id, epoch, fencing_token, leased_until)
				VALUES ('%s','%s','%s',2,77,now()-interval '1 hour')`, ids.Tenant, ids.Session, ids.Owner),
			fmt.Sprintf(`INSERT INTO execution_result (tenant_id, execution_id, job_id, session_id, owner_id, epoch, fence_token, status, result_version, result_json, config_version)
				VALUES ('%s','%s','%s','%s','%s',2,88,'succeeded',1,'{"result":"ok"}',1)`,
				ids.Tenant, ids.Execution, ids.Job, ids.Session, ids.Owner),
			fmt.Sprintf(`INSERT INTO job_queue (tenant_id, job_id, execution_id, schema_version, payload, status, available_at, delivery_id, last_delivery_id, leased_until, attempt, delivery_count)
				VALUES ('%s','%s','%s',%d,'%s'::jsonb,'in_flight',now(),'%s','%s',now()-interval '1 hour',2,1)`,
				ids.Tenant, ids.Job, ids.Execution, queue.SchemaVersion, string(jobPayload), ids.Delivery, ids.Delivery),
			fmt.Sprintf(`INSERT INTO job_queue (tenant_id, job_id, execution_id, schema_version, payload, status, available_at, attempt, delivery_count)
				VALUES ('%s','%s','%s',%d,'{}'::jsonb,'queued',now()+interval '1 hour',1,0)`,
				ids.Tenant, ids.JobQueued, "exec-q-"+ids.Suffix, queue.SchemaVersion),
			fmt.Sprintf(`INSERT INTO channel_binding_audit (tenant_id, audit_id, channel, binding_id, identity_fingerprint, secret_fingerprint, operation, success, version)
				VALUES ('%s','%s','lark','%s','fp-identity','fp-secret','resolve',true,1)`,
				ids.Tenant, ids.BindingAud, ids.Binding),
			fmt.Sprintf(`INSERT INTO vector_projection_task (tenant_id, task_id, source_type, source_id, projection_scope, document_id, operation, source_version, source_sequence, content_hash, model, model_version, dimension, schema_version, status, attempt, max_attempts, next_attempt_at, last_error_category)
				VALUES ('%s','%s','memory','%s','session','%s','upsert',2,5,'%s','model-%s','mv-1',8,'sv-1','retry_wait',2,5,now()-interval '1 hour','deadline')`,
				ids.Tenant, ids.TaskRunning, ids.MemoryA, ids.Document, strings.Repeat("cd", 32), ids.Suffix),
			fmt.Sprintf(`INSERT INTO vector_projection_task (tenant_id, task_id, source_type, source_id, projection_scope, document_id, operation, source_version, source_sequence, content_hash, model, model_version, dimension, schema_version, status, attempt, max_attempts, next_attempt_at, completed_at)
				VALUES ('%s','%s','memory','%s','session','%s','upsert',2,5,'%s','model-%s','mv-1',8,'sv-1','succeeded',1,5,now()-interval '2 hours',now()-interval '1 hour')`,
				ids.Tenant, ids.TaskDone, ids.MemoryA, ids.Document, strings.Repeat("ef", 32), ids.Suffix),
			fmt.Sprintf(`INSERT INTO vector_rebuild_run (tenant_id, run_id, projection_fingerprint, mode, phase, cursor_memory_id, attempt, max_attempts, scanned, enqueued, tombstoned, lease_owner, lease_epoch, lease_fence, lease_expires_at, deadline_at)
				VALUES ('%s','%s','%s','rebuild','scanning','%s',1,3,10,8,1,'%s',1,12,now()-interval '30 minutes',now()+interval '1 hour')`,
				ids.Tenant, ids.Rebuild, strings.Repeat("f0", 32), ids.MemoryA, ids.Owner),
			fmt.Sprintf(`INSERT INTO tenant_config_operation (tenant_id, operation_id, kind, target_version, expected_active_version, requested_percentage, result_active_version, outcome, actor_category)
				VALUES ('%s','%s','publish',1,0,100,1,'committed','operator')`, ids.Tenant, ids.Operation),
		}
		for _, stmt := range stmts {
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("seed %s: %w", ids.Suffix, err)
			}
		}
		// tenant_config_version: the insert trigger forces draft; walk the
		// legal draft -> validated -> published transitions.
		configSteps := []string{
			fmt.Sprintf(`INSERT INTO tenant_config_version (tenant_id, config_version, status, config, checksum, created_by)
				VALUES ('%s',1,'draft','{"model":"m-%s"}','ck1-%s','creator-%s')`, ids.Tenant, ids.Suffix, ids.Suffix, ids.Suffix),
			fmt.Sprintf(`UPDATE tenant_config_version SET status='validated' WHERE tenant_id='%s' AND config_version=1`, ids.Tenant),
			fmt.Sprintf(`UPDATE tenant_config_version SET status='published', published_at=now() WHERE tenant_id='%s' AND config_version=1`, ids.Tenant),
			fmt.Sprintf(`INSERT INTO tenant_config_version (tenant_id, config_version, status, config, checksum, created_by)
				VALUES ('%s',2,'draft','{"model":"m2-%s"}','ck2-%s','creator-%s')`, ids.Tenant, ids.Suffix, ids.Suffix, ids.Suffix),
			fmt.Sprintf(`INSERT INTO tenant_config_rollout (tenant_id, active_version, baseline_version, percentage)
				VALUES ('%s',1,1,100)`, ids.Tenant),
		}
		for _, stmt := range configSteps {
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("seed config %s: %w", ids.Suffix, err)
			}
		}
		return nil
	})
}

// replayEventFixture describes one seeded session_event row.
type replayEventFixture struct {
	Sequence int64
	Type     string
	Parent   string
	Payload  string
}

// seedReplaySession inserts one session plus its event chain (or an empty
// session when events is nil) with the given watermark.
func seedReplaySession(ctx context.Context, t *testing.T, runtimePool *pgxpool.Pool, tenant, sessionID string, watermark int64, events []replayEventFixture) error {
	t.Helper()
	return tenantctx.WithTenantContext(ctx, runtimePool, tenant, "p2-02 seed replay session", func(ctx context.Context, tx pgx.Tx) error {
		stmt := fmt.Sprintf(`INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user, state, state_version, summary_version, last_event_seq)
			VALUES ('%s','%s','app-replay',1,'lark','binding-replay','chat-%s','user-%s','active',1,0,%d)`,
			tenant, sessionID, sessionID, sessionID, watermark)
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("seed replay session %s: %w", sessionID, err)
		}
		for _, event := range events {
			stmt := fmt.Sprintf(`INSERT INTO session_event (tenant_id, session_id, event_id, sequence, event_type, attempt, payload)
				VALUES ('%s','%s','evt-%s-%d',%d,'%s',1,'%s'::jsonb)`,
				tenant, sessionID, sessionID, event.Sequence, event.Sequence, event.Type, event.Payload)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("seed replay event %s/%d: %w", sessionID, event.Sequence, err)
			}
			if event.Parent != "" {
				stmt := fmt.Sprintf(`UPDATE session_event SET parent_event_id='%s'
					WHERE tenant_id='%s' AND session_id='%s' AND sequence=%d`,
					event.Parent, tenant, sessionID, event.Sequence)
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return fmt.Errorf("seed replay parent %s/%d: %w", sessionID, event.Sequence, err)
				}
			}
		}
		return nil
	})
}

// countTable returns the row count of one table via the owner connection.
func countTable(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	var count int64
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s`, table)).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

// sessionWatermark reads one session watermark (test diagnostic only).
func sessionWatermark(ctx context.Context, t *testing.T, pool *pgxpool.Pool, tenant, session string) int64 {
	t.Helper()
	var watermark int64
	if err := pool.QueryRow(ctx, `SELECT last_event_seq FROM session WHERE tenant_id=$1 AND session_id=$2`, tenant, session).Scan(&watermark); err != nil {
		t.Fatalf("watermark %s/%s: %v", tenant, session, err)
	}
	return watermark
}

// tenantIDList lists tenant ids on the owner connection (test diagnostics).
func tenantIDList(ctx context.Context, t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT tenant_id FROM tenant ORDER BY tenant_id`)
	if err != nil {
		t.Fatalf("tenant list: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

// withCredentials returns the URL with a different user/password pair.
func withCredentials(t *testing.T, raw, user, password string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(user, password)
	return parsed.String()
}

// dbNameOf extracts the database name from a postgres URL.
func dbNameOf(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(parsed.Path, "/")
}

// mustParse parses a DSN for the test runner invocations.
func mustParse(t *testing.T, raw string) connParams {
	t.Helper()
	params, err := parseDSN(raw)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	return params
}

// jsonUnmarshalLoose / jsonMarshalLoose are the test-side manifest editors.
func jsonUnmarshalLoose(raw []byte, target any) error {
	return json.Unmarshal(raw, target)
}

func jsonMarshalLoose(value any) ([]byte, error) {
	return json.Marshal(value)
}

// sessionSnapshot reads the fields a watermark repair must never touch.
type sessionSnapshotState struct {
	state          string
	stateVersion   int64
	summaryVersion int64
	updatedAt      time.Time
}

func readSessionSnapshot(ctx context.Context, t *testing.T, pool *pgxpool.Pool, tenant, session string) sessionSnapshotState {
	t.Helper()
	var snapshot sessionSnapshotState
	if err := pool.QueryRow(ctx, `SELECT state, state_version, summary_version, updated_at
		FROM session WHERE tenant_id=$1 AND session_id=$2`, tenant, session).Scan(
		&snapshot.state, &snapshot.stateVersion, &snapshot.summaryVersion, &snapshot.updatedAt); err != nil {
		t.Fatalf("session snapshot: %v", err)
	}
	return snapshot
}
