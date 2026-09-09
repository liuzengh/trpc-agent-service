package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type p104PostgresFixture struct {
	pool     *pgxpool.Pool
	registry *TenantRegistry
	ctx      context.Context
}

func newP104PostgresFixture(t *testing.T) p104PostgresFixture {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if url == "" {
		t.Skip("P1-04 PostgreSQL integration BLOCKED/UNAVAILABLE: stage=prerequisite category=missing_test_database_url last_stage=preflight deadline=false data_race=false")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	base, err := NewPool(ctx, PostgresConfig{URL: url, MaxConns: 4, MinConns: 1})
	if err != nil {
		cancel()
		t.Fatalf("P1-04 PostgreSQL integration FAIL: stage=connect category=database_unavailable: %v", err)
	}
	schema := "p104_" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		base.Close()
		cancel()
		t.Fatalf("P1-04 PostgreSQL integration FAIL: stage=schema_create category=database_error: %v", err)
	}
	var pool *pgxpool.Pool
	t.Cleanup(func() {
		if pool != nil {
			pool.Close()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := base.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Logf("P1-04 owned schema cleanup category=cleanup_failed: %v", err)
		}
		base.Close()
		cancel()
	})
	cfg := PostgresConfig{URL: url, SearchPath: schema, MaxConns: 4, MinConns: 1, AllowDestructiveDown: true}
	pool, err = NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("P1-04 PostgreSQL integration FAIL: stage=schema_connect category=database_unavailable: %v", err)
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("P1-04 PostgreSQL integration FAIL: stage=fixture category=repository_root_unavailable")
	}
	source := os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations"))
	migrator, err := NewMigratorWithPool(pool, cfg, source)
	if err != nil {
		t.Fatalf("P1-04 PostgreSQL integration FAIL: stage=migrator_init category=migration_error: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("P1-04 PostgreSQL integration FAIL: stage=migration_up category=migration_error: %v", err)
	}
	registry, err := NewTenantRegistry(pool)
	if err != nil {
		t.Fatalf("P1-04 PostgreSQL integration FAIL: stage=registry_init category=repository_error: %v", err)
	}
	return p104PostgresFixture{pool: pool, registry: registry, ctx: ctx}
}

func p104TenantContext(tenantID, channel, bindingID, externalUser string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: tenantID, AgentAppID: "agent-" + tenantID, BindingID: bindingID, Channel: channel, ExternalUser: externalUser, RequestID: "request-" + externalUser, MessageID: "message-" + externalUser, TraceID: "trace-" + externalUser, ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "none"}}
}

func p104InsertTenant(t *testing.T, f p104PostgresFixture, tenantID string) {
	t.Helper()
	_, err := f.pool.Exec(f.ctx, `INSERT INTO tenant (tenant_id, name, status, config_version, default_agent_app_id, backend_config) VALUES ($1, $2, 'active', 1, $3, '{"session":"postgres","memory":"postgres","vector":"none","object":"none"}')`, tenantID, "Tenant "+tenantID, "agent-"+tenantID)
	if err != nil {
		t.Fatalf("P1-04 fixture tenant: %v", err)
	}
	_, err = f.pool.Exec(f.ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name, status, model_config_ref) VALUES ($1, $2, $3, 'active', 'synthetic-model')`, tenantID, "agent-"+tenantID, "Agent "+tenantID)
	if err != nil {
		t.Fatalf("P1-04 fixture agent: %v", err)
	}
}

func p104Binding(tenantID, bindingID, channel, externalAppID string, enabled bool) tenant.ChannelBinding {
	return tenant.ChannelBinding{TenantID: tenantID, ID: bindingID, Channel: channel, ExternalAppID: externalAppID, SecretRef: "env://P104_DB_SECRET", VerifyTokenRef: "env://P104_DB_VERIFY", Enabled: enabled}
}

func TestP104PostgresBindingLifecycleIsolationAndCAS(t *testing.T) {
	f := newP104PostgresFixture(t)
	p104InsertTenant(t, f, "tenant-a")
	p104InsertTenant(t, f, "tenant-b")
	tcA := p104TenantContext("tenant-a", tenant.ChannelLark, "binding-a", "user-a")
	tcB := p104TenantContext("tenant-b", tenant.ChannelLark, "binding-b", "user-b")
	binding := p104Binding(tcA.TenantID, tcA.BindingID, tcA.Channel, "lark-app-a", true)
	if err := f.registry.CreateBinding(f.ctx, tcA, binding); err != nil {
		t.Fatalf("create binding: %v", err)
	}
	stored, err := f.registry.GetBinding(f.ctx, tcA, binding.ID)
	if err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if stored.SecretRef != binding.SecretRef || stored.Version != 1 || stored.Status != tenant.BindingStatusActive {
		t.Fatalf("stored binding metadata changed: %+v", stored)
	}
	var rawSecret string
	if err := f.pool.QueryRow(f.ctx, "SELECT secret_ref FROM channel_binding WHERE tenant_id=$1 AND binding_id=$2", tcA.TenantID, binding.ID).Scan(&rawSecret); err != nil {
		t.Fatal(err)
	}
	if rawSecret != binding.SecretRef || rawSecret == "db-plaintext-secret" {
		t.Fatalf("secret reference persistence is unsafe: %q", rawSecret)
	}
	if _, err := f.registry.ResolveBinding(f.ctx, tenant.ChannelLark, binding.ExternalAppID); err != nil {
		t.Fatalf("resolve provider identity: %v", err)
	}
	t.Setenv("P104_DB_SECRET", "")
	t.Setenv("P104_DB_VERIFY", "")
	secretResolver := tenant.RegistryResolver{Registry: f.registry, SecretResolver: tenant.EnvironmentSecretResolver{}}
	_, secretErr := secretResolver.Resolve(f.ctx, tenant.ResolveRequest{Channel: tenant.ChannelLark, ExternalAppID: binding.ExternalAppID, ExternalUser: "user-a", RequestID: "r-secret", MessageID: "m-secret", TraceID: "t-secret"})
	if !errors.Is(secretErr, tenant.ErrBindingNotFound) || strings.Contains(secretErr.Error(), "P104_DB_SECRET") {
		t.Fatalf("missing database binding secret error=%v", secretErr)
	}
	duplicate := p104Binding(tcB.TenantID, tcB.BindingID, tcB.Channel, binding.ExternalAppID, true)
	if err := f.registry.CreateBinding(f.ctx, tcB, duplicate); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("duplicate provider identity error=%v", err)
	}
	if _, err := f.registry.GetBinding(f.ctx, tcB, binding.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant read error=%v", err)
	}
	if err := f.registry.UpdateBinding(f.ctx, tcB, stored, 1); !errors.Is(err, storage.ErrTenantMismatch) {
		t.Fatalf("cross-tenant update error=%v", err)
	}
	disabled := stored
	disabled.Enabled = false
	casBinding := p104Binding(tcA.TenantID, "binding-cas", tcA.Channel, "lark-app-cas", true)
	if err := f.registry.CreateBinding(f.ctx, tcA, casBinding); err != nil {
		t.Fatalf("create CAS binding: %v", err)
	}
	casStored, err := f.registry.GetBinding(f.ctx, tcA, casBinding.ID)
	if err != nil {
		t.Fatalf("read CAS binding: %v", err)
	}
	casOne := casStored
	casOne.Version = 2
	casOne.ExternalTargetType = tenant.BindingTargetUser
	casOne.ExternalTargetID = "user-a"
	casTwo := casStored
	casTwo.Version = 2
	casTwo.ExternalTargetType = tenant.BindingTargetChat
	casTwo.ExternalTargetID = "chat-a"
	casResults := make(chan error, 2)
	var casGroup sync.WaitGroup
	casGroup.Add(2)
	go func() { defer casGroup.Done(); casResults <- f.registry.UpdateBinding(f.ctx, tcA, casOne, 1) }()
	go func() { defer casGroup.Done(); casResults <- f.registry.UpdateBinding(f.ctx, tcA, casTwo, 1) }()
	casGroup.Wait()
	close(casResults)
	casSuccess, casConflict := 0, 0
	for casErr := range casResults {
		if casErr == nil {
			casSuccess++
		} else if errors.Is(casErr, storage.ErrConflict) {
			casConflict++
		} else {
			t.Fatalf("concurrent CAS error=%v", casErr)
		}
	}
	if casSuccess != 1 || casConflict != 1 {
		t.Fatalf("concurrent CAS success=%d conflict=%d", casSuccess, casConflict)
	}
	disabled.Status = tenant.BindingStatusDisabled
	disabled.Version = 2
	if err := f.registry.UpdateBinding(f.ctx, tcA, disabled, 1); err != nil {
		t.Fatalf("disable binding: %v", err)
	}
	stale := stored
	stale.Version = 2
	if err := f.registry.UpdateBinding(f.ctx, tcA, stale, 1); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("stale CAS error=%v", err)
	}
	resolver := tenant.RegistryResolver{Registry: f.registry}
	if _, err := resolver.Resolve(f.ctx, tenant.ResolveRequest{Channel: tenant.ChannelLark, ExternalAppID: binding.ExternalAppID, ExternalUser: "user-a", RequestID: "r-disabled", MessageID: "m-disabled", TraceID: "t-disabled"}); !errors.Is(err, tenant.ErrBindingNotFound) {
		t.Fatalf("disabled binding error=%v", err)
	}
	active := disabled
	active.Enabled = true
	active.Status = tenant.BindingStatusActive
	active.Version = 3
	if err := f.registry.UpdateBinding(f.ctx, tcA, active, 2); err != nil {
		t.Fatalf("re-enable binding: %v", err)
	}
	expired := active
	expired.Enabled = false
	expired.Status = tenant.BindingStatusExpired
	expired.Version = 4
	expired.ExpiresAt = time.Now().Add(-time.Minute)
	if err := f.registry.UpdateBinding(f.ctx, tcA, expired, 3); err != nil {
		t.Fatalf("expire binding: %v", err)
	}
	if _, err := resolver.Resolve(f.ctx, tenant.ResolveRequest{Channel: tenant.ChannelLark, ExternalAppID: binding.ExternalAppID, ExternalUser: "user-a", RequestID: "r-expired", MessageID: "m-expired", TraceID: "t-expired"}); !errors.Is(err, tenant.ErrBindingNotFound) {
		t.Fatalf("expired binding error=%v", err)
	}
	retired := expired
	retired.Status = tenant.BindingStatusRetired
	retired.Version = 5
	if err := f.registry.UpdateBinding(f.ctx, tcA, retired, 4); err != nil {
		t.Fatalf("retire binding: %v", err)
	}
	final, err := f.registry.GetBinding(f.ctx, tcA, binding.ID)
	if err != nil || final.Status != tenant.BindingStatusRetired || final.Version != 5 {
		t.Fatalf("retired binding=%+v err=%v", final, err)
	}
	var stateConstraint bool
	if err := f.pool.QueryRow(f.ctx, `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_channel_binding_state')`).Scan(&stateConstraint); err != nil {
		t.Fatal(err)
	}
	if !stateConstraint {
		t.Fatal("binding state check constraint is missing")
	}
}

func TestP104PostgresIdentityScopeAuditAndCancellation(t *testing.T) {
	f := newP104PostgresFixture(t)
	p104InsertTenant(t, f, "tenant-c")
	tc := p104TenantContext("tenant-c", tenant.ChannelTelegram, "binding-c", "telegram-user")
	binding := p104Binding(tc.TenantID, tc.BindingID, tc.Channel, "telegram-app-c", true)
	if err := f.registry.CreateBinding(f.ctx, tc, binding); err != nil {
		t.Fatal(err)
	}
	group := tc
	group.ExternalChat = "telegram-chat-1"
	group.ExternalChatType = "group"
	identity, err := f.registry.ResolveIdentity(f.ctx, group)
	if err != nil {
		t.Fatalf("resolve group identity: %v", err)
	}
	if identity.Scope != "group:telegram-chat-1" || identity.InternalUserID == "" {
		t.Fatalf("unexpected group identity: %+v", identity)
	}
	thread := group
	thread.ExternalThreadID = "42"
	if _, err := f.registry.ResolveIdentity(f.ctx, thread); !errors.Is(err, tenant.ErrIdentityConflict) {
		t.Fatalf("group/topic scope conflict error=%v", err)
	}
	topic := tc
	topic.ExternalUser = "telegram-topic-user"
	topic.ExternalChat = "telegram-chat-2"
	topic.ExternalChatType = "supergroup"
	topic.ExternalThreadID = "42"
	topicIdentity, err := f.registry.ResolveIdentity(f.ctx, topic)
	if err != nil || topicIdentity.Scope != "topic:telegram-chat-2:42" {
		t.Fatalf("resolve topic identity=%+v err=%v", topicIdentity, err)
	}
	otherThread := topic
	otherThread.ExternalThreadID = "43"
	if _, err := f.registry.ResolveIdentity(f.ctx, otherThread); !errors.Is(err, tenant.ErrIdentityConflict) {
		t.Fatalf("thread scope conflict error=%v", err)
	}
	event := storage.BindingAuditEvent{TenantID: tc.TenantID, AuditID: "audit-c", BindingID: tc.BindingID, Channel: tc.Channel, Operation: "identity_resolve", Success: true, IdentityFingerprint: tenant.IdentityFingerprint(group.ExternalUser), SecretFingerprint: tenant.SecretRefFingerprint(binding.SecretRef), Version: 1}
	if err := f.registry.AppendBindingEvent(f.ctx, tc, event); err != nil {
		t.Fatalf("append audit event: %v", err)
	}
	var auditText string
	if err := f.pool.QueryRow(f.ctx, `SELECT operation || '|' || coalesce(identity_fingerprint,'') || '|' || coalesce(secret_fingerprint,'') || '|' || coalesce(error_type,'') FROM channel_binding_audit WHERE tenant_id=$1 AND audit_id=$2`, tc.TenantID, event.AuditID).Scan(&auditText); err != nil {
		t.Fatal(err)
	}
	for _, unsafe := range []string{"db-plaintext-secret", "Authorization", "webhook-body", "provider-body"} {
		if strings.Contains(auditText, unsafe) {
			t.Fatalf("unsafe audit metadata persisted: %q", auditText)
		}
	}
	cancelled, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := f.registry.GetBinding(cancelled, tc, binding.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read error=%v", err)
	}
	deadline, deadlineCancel := context.WithDeadline(f.ctx, time.Now().Add(-time.Nanosecond))
	defer deadlineCancel()
	if _, err := f.registry.ResolveIdentity(deadline, group); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline read error=%v", err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(f.ctx, `INSERT INTO channel_binding_audit (tenant_id, audit_id, channel, binding_id, operation, success, version) VALUES ('tenant-c', 'rollback-audit', 'telegram', 'missing-binding', 'rollback', true, 1)`)
	if err == nil {
		_ = tx.Rollback(f.ctx)
		t.Fatal("invalid audit foreign key unexpectedly succeeded")
	}
	if rollbackErr := tx.Rollback(f.ctx); rollbackErr != nil {
		t.Fatalf("rollback failed: %v", rollbackErr)
	}
	var rollbackCount int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM channel_binding_audit WHERE audit_id='rollback-audit'`).Scan(&rollbackCount); err != nil {
		t.Fatal(err)
	}
	if rollbackCount != 0 {
		t.Fatalf("rolled back audit row count=%d", rollbackCount)
	}
}

func TestP104PostgresConcurrentIdentityResolution(t *testing.T) {
	f := newP104PostgresFixture(t)
	p104InsertTenant(t, f, "tenant-concurrent")
	tc := p104TenantContext("tenant-concurrent", tenant.ChannelTelegram, "binding-concurrent", "telegram-user-concurrent")
	tc.ExternalChat = "telegram-chat-concurrent"
	tc.ExternalChatType = "supergroup"
	tc.ExternalThreadID = "77"
	binding := p104Binding(tc.TenantID, tc.BindingID, tc.Channel, "telegram-app-concurrent", true)
	if err := f.registry.CreateBinding(f.ctx, tc, binding); err != nil {
		t.Fatal(err)
	}
	firstContext := tc
	firstContext.RequestID = "request-concurrent-a"
	firstContext.MessageID = "message-concurrent-a"
	firstContext.TraceID = "trace-concurrent-a"
	secondContext := tc
	secondContext.RequestID = "request-concurrent-b"
	secondContext.MessageID = "message-concurrent-b"
	secondContext.TraceID = "trace-concurrent-b"
	start := make(chan struct{})
	type result struct {
		identity tenant.Identity
		err      error
	}
	results := make(chan result, 2)
	var group sync.WaitGroup
	group.Add(2)
	resolve := func(value tenant.TenantContext) {
		defer group.Done()
		<-start
		identity, err := f.registry.ResolveIdentity(f.ctx, value)
		results <- result{identity: identity, err: err}
	}
	go resolve(firstContext)
	go resolve(secondContext)
	close(start)
	group.Wait()
	close(results)
	var resolved []result
	for value := range results {
		resolved = append(resolved, value)
	}
	if len(resolved) != 2 {
		t.Fatalf("concurrent resolver result count=%d", len(resolved))
	}
	for index, value := range resolved {
		if value.err != nil {
			t.Fatalf("concurrent resolver %d error=%v", index, value.err)
		}
	}
	want, wantScope, err := tenant.DeriveIdentity(tc)
	if err != nil {
		t.Fatal(err)
	}
	if resolved[0].identity != resolved[1].identity || resolved[0].identity != want || wantScope != "topic:telegram-chat-concurrent:77" {
		t.Fatalf("concurrent identity mismatch: first=%+v second=%+v want=%+v scope=%q", resolved[0].identity, resolved[1].identity, want, wantScope)
	}
	var rowCount int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM user_identity WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_user_id=$4`, tc.TenantID, tc.Channel, tc.BindingID, tc.ExternalUser).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 {
		t.Fatalf("canonical identity row count=%d", rowCount)
	}
	var identityID, internalUserID, scope, status string
	var externalChat, externalThread *string
	var version int64
	if err := f.pool.QueryRow(f.ctx, `SELECT identity_id, internal_user_id, scope, external_chat, external_thread_id, status, version FROM user_identity WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_user_id=$4`, tc.TenantID, tc.Channel, tc.BindingID, tc.ExternalUser).Scan(&identityID, &internalUserID, &scope, &externalChat, &externalThread, &status, &version); err != nil {
		t.Fatal(err)
	}
	if identityID != want.ID || internalUserID != want.InternalUserID || scope != "topic" || nullableString(externalChat) != tc.ExternalChat || nullableString(externalThread) != tc.ExternalThreadID || status != tenant.IdentityStatusActive || version != 1 {
		t.Fatalf("stored canonical identity changed: id=%q internal=%q scope=%q chat=%q thread=%q status=%q version=%d", identityID, internalUserID, scope, nullableString(externalChat), nullableString(externalThread), status, version)
	}
	var canonicalConstraint bool
	if err := f.pool.QueryRow(f.ctx, `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=(SELECT oid FROM pg_class WHERE relname=$1 AND relnamespace=current_schema()::regnamespace) AND contype=$2 AND pg_get_constraintdef(oid)=$3)`, "user_identity", "u", "UNIQUE (tenant_id, channel, binding_id, external_user_id)").Scan(&canonicalConstraint); err != nil {
		t.Fatal(err)
	}
	if !canonicalConstraint {
		t.Fatal("canonical identity unique constraint is missing")
	}
}

func TestP104PostgresIdentityUnknownUniqueConflictFailsClosed(t *testing.T) {
	f := newP104PostgresFixture(t)
	p104InsertTenant(t, f, "tenant-constraint")
	tc := p104TenantContext("tenant-constraint", tenant.ChannelLark, "binding-constraint", "user-constraint")
	binding := p104Binding(tc.TenantID, tc.BindingID, tc.Channel, "lark-app-constraint", true)
	if err := f.registry.CreateBinding(f.ctx, tc, binding); err != nil {
		t.Fatal(err)
	}
	candidate, _, err := tenant.DeriveIdentity(tc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO user_identity (tenant_id, identity_id, channel, binding_id, external_user_id, internal_user_id, scope, status, version) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, tc.TenantID, candidate.ID, tc.Channel, tc.BindingID, "different-user", "different-internal", "private", tenant.IdentityStatusActive, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.registry.ResolveIdentity(f.ctx, tc); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("unknown identity constraint error=%v", err)
	}
	var canonicalRows int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM user_identity WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_user_id=$4`, tc.TenantID, tc.Channel, tc.BindingID, tc.ExternalUser).Scan(&canonicalRows); err != nil {
		t.Fatal(err)
	}
	if canonicalRows != 0 {
		t.Fatalf("unknown identity conflict created canonical row count=%d", canonicalRows)
	}
}

func TestP104RegistryErrorIsCategoryOnly(t *testing.T) {
	raw := &pgconn.PgError{Code: "23505", ConstraintName: "user_identity_secret_constraint", Message: "duplicate key", Detail: "Key (dsn=postgres://user:secret@example.invalid/db)"}
	err := registryError(raw)
	if !errors.Is(err, storage.ErrConflict) || err.Error() != storage.ErrConflict.Error() {
		t.Fatalf("registry error was not category-only conflict: %v", err)
	}
	for _, unsafe := range []string{"user_identity_secret_constraint", "postgres://", "secret@example.invalid", "duplicate key"} {
		if strings.Contains(err.Error(), unsafe) {
			t.Fatalf("registry error leaked %q: %v", unsafe, err)
		}
	}
}

func TestP104PostgresIdentityImmutableFieldsFailClosed(t *testing.T) {
	f := newP104PostgresFixture(t)
	p104InsertTenant(t, f, "tenant-immutable")
	tc := p104TenantContext("tenant-immutable", tenant.ChannelLark, "binding-immutable", "user-immutable")
	binding := p104Binding(tc.TenantID, tc.BindingID, tc.Channel, "lark-app-immutable", true)
	if err := f.registry.CreateBinding(f.ctx, tc, binding); err != nil {
		t.Fatal(err)
	}
	candidate, _, err := tenant.DeriveIdentity(tc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO user_identity (tenant_id, identity_id, channel, binding_id, external_user_id, internal_user_id, scope, status, version) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, tc.TenantID, "identity-corrupt", tc.Channel, tc.BindingID, candidate.ExternalUserID, "user-corrupt", "private", tenant.IdentityStatusActive, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.registry.ResolveIdentity(f.ctx, tc); !errors.Is(err, tenant.ErrIdentityConflict) {
		t.Fatalf("immutable identity mismatch error=%v", err)
	}
	var rowCount int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM user_identity WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_user_id=$4`, tc.TenantID, tc.Channel, tc.BindingID, tc.ExternalUser).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 {
		t.Fatalf("immutable mismatch changed canonical row count=%d", rowCount)
	}
}
