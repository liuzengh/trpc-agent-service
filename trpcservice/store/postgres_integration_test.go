package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/sessionturn"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const postgresTestDSNEnv = "TEST_POSTGRES_DSN"

func newPostgresIntegrationStore(t *testing.T, ensureSchema bool) *Postgres {
	t.Helper()
	dsn := os.Getenv(postgresTestDSNEnv)
	if dsn == "" {
		t.Skip(postgresTestDSNEnv + " is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open integration admin pool: %v", err)
	}
	schema := "store_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create integration schema: %v", err)
	}

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse integration DSN: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		admin.Close()
		t.Fatalf("open schema pool: %v", err)
	}
	st := &Postgres{pool: pool}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop integration schema: %v", err)
		}
		admin.Close()
	})
	if ensureSchema {
		if err := st.EnsureSchema(ctx); err != nil {
			t.Fatalf("ensure integration schema: %v", err)
		}
	}
	return st
}

func newPostgresInbox(dedupKey, partitionKey string) *InboxRecord {
	return &InboxRecord{
		InboxID:           uuid.NewString(),
		TenantID:          "acme",
		ChannelType:       "telegram",
		BindingID:         "bot-a",
		ExternalMessageID: "ext-" + dedupKey,
		DedupKey:          dedupKey,
		PartitionKey:      partitionKey,
		Payload:           []byte(`{"message":"hello"}`),
		TraceCarrier: map[string]string{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
	}
}

func newPostgresOutboxPlan(prefix, partitionKey string, partCount int) []OutboxRecord {
	result := make([]OutboxRecord, 0, partCount)
	for index := 0; index < partCount; index++ {
		operationKey := fmt.Sprintf("op-%s-%d", prefix, index)
		result = append(result, OutboxRecord{
			OutboxID: uuid.NewString(), OperationKey: operationKey, OperationVersion: 1,
			PartIndex: index, PartCount: partCount,
			TenantID: "acme", ChannelType: "telegram", BindingID: "bot-a",
			DedupKey: operationKey, PartitionKey: partitionKey,
			Payload: []byte(fmt.Sprintf(`{"target":"chat","text":"part-%d"}`, index)),
		})
	}
	return result
}

func postgresInboxFence(rec InboxRecord, owner string) InboxLeaseFence {
	return InboxLeaseFence{
		InboxID: rec.InboxID, Owner: owner, AttemptCount: rec.AttemptCount,
		TenantID: rec.TenantID, ChannelType: rec.ChannelType, BindingID: rec.BindingID,
		DedupKey: rec.DedupKey, PartitionKey: rec.PartitionKey,
		PipelineSchemaVersion: rec.PipelineSchemaVersion,
		AtomicCommitMode:      rec.AtomicCommitMode, DatabaseIdentity: rec.DatabaseIdentity,
	}
}

func markPostgresInboxAtomic(st *Postgres, rec *InboxRecord) {
	rec.PipelineSchemaVersion = 2
	rec.AtomicCommitMode = "postgres_same_database_v1"
	rec.DatabaseIdentity = st.DatabaseIdentity()
}

func postgresIntervalSeconds(t *testing.T, st *Postgres, query string, args ...any) float64 {
	t.Helper()
	var seconds float64
	if err := st.pool.QueryRow(context.Background(), query, args...).Scan(&seconds); err != nil {
		t.Fatalf("query database interval: %v", err)
	}
	return seconds
}

func waitForPostgresCondition(
	t *testing.T,
	ctx context.Context,
	st *Postgres,
	description, query string,
	args ...any,
) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		if err := st.pool.QueryRow(ctx, query, args...).Scan(&ready); err != nil {
			t.Fatalf("wait for %s: %v", description, err)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestPostgresIntegrationConcurrentSchemaAndDedup(t *testing.T) {
	st := newPostgresIntegrationStore(t, false)
	ctx := context.Background()
	const starters = 8
	start := make(chan struct{})
	errs := make(chan error, starters)
	var wg sync.WaitGroup
	for i := 0; i < starters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- st.EnsureSchema(ctx)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent EnsureSchema: %v", err)
		}
	}
	if err := st.EnsureSchema(ctx); err != nil {
		t.Fatalf("idempotent EnsureSchema: %v", err)
	}

	var requiredColumns int
	if err := st.pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema()
			AND table_name IN ('runtime_inbox', 'runtime_outbox')
			AND column_name IN ('partition_key', 'queue_sequence')`).Scan(&requiredColumns); err != nil {
		t.Fatal(err)
	}
	if requiredColumns != 4 {
		t.Fatalf("partition schema columns = %d, want 4", requiredColumns)
	}
	var ledgerTables, ledgerColumns int
	if err := st.pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema()
			AND table_name IN ('runtime_outbox_attempts', 'runtime_outbox_resolutions')`).Scan(&ledgerTables); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'runtime_outbox'
			AND column_name IN ('operation_key','operation_version','part_index','part_count',
				'payload_hash','payload_bytes','delivery_state','state_version',
				'provider_code','provider_message_id','provider_request_id','response_hash')`).Scan(&ledgerColumns); err != nil {
		t.Fatal(err)
	}
	if ledgerTables != 2 || ledgerColumns != 12 {
		t.Fatalf("delivery ledger schema tables=%d columns=%d", ledgerTables, ledgerColumns)
	}

	now := time.Now()
	const contenders = 12
	results := make(chan error, contenders)
	start = make(chan struct{})
	var ids sync.Map
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := newPostgresInbox("same-dedup", "session-a")
			ids.Store(rec.InboxID, struct{}{})
			results <- st.InsertInbox(ctx, rec, now)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	succeeded, duplicated := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrDuplicate):
			duplicated++
		default:
			t.Fatalf("concurrent insert: %v", err)
		}
	}
	if succeeded != 1 || duplicated != contenders-1 {
		t.Fatalf("dedup results: succeeded=%d duplicated=%d", succeeded, duplicated)
	}
	var count int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_inbox`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("deduplicated row count = %d, want 1", count)
	}

	var existingID string
	if err := st.pool.QueryRow(ctx, `SELECT inbox_id FROM runtime_inbox`).Scan(&existingID); err != nil {
		t.Fatal(err)
	}
	primaryCollision := newPostgresInbox("different-dedup", "session-b")
	primaryCollision.InboxID = existingID
	err := st.InsertInbox(ctx, primaryCollision, now)
	if err == nil || errors.Is(err, ErrDuplicate) {
		t.Fatalf("primary-key collision = %v, must not masquerade as dedup", err)
	}
}

func TestPostgresIntegrationSharedVersionedMigration(t *testing.T) {
	st := newPostgresIntegrationStore(t, false)
	turnStore, err := sessionturn.NewPostgres(st.pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const replicas = 12
	start := make(chan struct{})
	errs := make(chan error, replicas)
	var wg sync.WaitGroup
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(useQueueBootstrap bool) {
			defer wg.Done()
			<-start
			if useQueueBootstrap {
				errs <- st.EnsureSchema(ctx)
				return
			}
			errs <- turnStore.EnsureSchema(ctx)
		}(i%2 == 0)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent shared migration: %v", err)
		}
	}
	if err := st.VerifySchema(ctx); err != nil {
		t.Fatalf("queue verify applied migration: %v", err)
	}
	if err := turnStore.VerifySchema(ctx); err != nil {
		t.Fatalf("session verify applied migration: %v", err)
	}

	var checksum, description string
	if err := st.pool.QueryRow(ctx, `
		SELECT checksum, description
		FROM schema_migrations
		WHERE version = $1`, migrations.RuntimePipelineVersion).
		Scan(&checksum, &description); err != nil {
		t.Fatal(err)
	}
	if checksum != migrations.RuntimePipelineChecksum() || description != migrations.RuntimePipelineDescription {
		t.Fatalf("migration ledger checksum=%q description=%q", checksum, description)
	}
	var tables int
	if err := st.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.tables
		WHERE table_schema = current_schema()
			AND table_name IN (
				'runtime_inbox', 'runtime_outbox', 'runtime_outbox_attempts',
				'runtime_outbox_resolutions', 'session_turn_sessions', 'session_turns',
				'session_turn_events', 'session_turn_app_states', 'session_turn_user_states'
			)`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 9 {
		t.Fatalf("shared migration created %d required tables, want 9", tables)
	}
}

func TestPostgresIntegrationVerifyOnlyRejectsUnmigratedSchemaWithoutDDL(t *testing.T) {
	st := newPostgresIntegrationStore(t, false)
	turnStore, err := sessionturn.NewPostgres(st.pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for name, verify := range map[string]func(context.Context) error{
		"queue":   st.VerifySchema,
		"session": turnStore.VerifySchema,
	} {
		t.Run(name, func(t *testing.T) {
			err := verify(ctx)
			if !errors.Is(err, migrations.ErrRuntimePipelineNotApplied) {
				t.Fatalf("VerifySchema error = %v, want not applied", err)
			}
		})
	}

	var ledgerExists bool
	if err := st.pool.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&ledgerExists); err != nil {
		t.Fatal(err)
	}
	if ledgerExists {
		t.Fatal("verify-only startup created the migration ledger")
	}
}

func TestPostgresIntegrationMigrationChecksumMismatchFailsClosed(t *testing.T) {
	st := newPostgresIntegrationStore(t, false)
	ctx := context.Background()
	badChecksum := strings.Repeat("0", 64)
	if _, err := st.pool.Exec(ctx, `
		CREATE TABLE schema_migrations (
			version text PRIMARY KEY,
			description text NOT NULL,
			checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO schema_migrations (version, description, checksum)
		VALUES ($1, 'tampered migration', $2)`,
		migrations.RuntimePipelineVersion, badChecksum); err != nil {
		t.Fatal(err)
	}

	turnStore, err := sessionturn.NewPostgres(st.pool)
	if err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func(context.Context) error{
		"queue apply":   st.EnsureSchema,
		"queue verify":  st.VerifySchema,
		"session apply": turnStore.EnsureSchema,
		"session verify": func(ctx context.Context) error {
			return turnStore.VerifySchema(ctx)
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := check(ctx)
			if !errors.Is(err, migrations.ErrRuntimePipelineChecksumMismatch) {
				t.Fatalf("schema check error = %v, want checksum mismatch", err)
			}
		})
	}

	var runtimeTableExists bool
	if err := st.pool.QueryRow(ctx, `SELECT to_regclass('runtime_inbox') IS NOT NULL`).Scan(&runtimeTableExists); err != nil {
		t.Fatal(err)
	}
	if runtimeTableExists {
		t.Fatal("checksum mismatch mutated the runtime schema")
	}
}

func TestPostgresIntegrationUpgradesLegacyPartitionSafely(t *testing.T) {
	st := newPostgresIntegrationStore(t, false)
	ctx := context.Background()
	// This is the runtime schema immediately before partition_key and
	// queue_sequence were introduced. EnsureSchema must upgrade it in place.
	if _, err := st.pool.Exec(ctx, `
		CREATE TABLE runtime_inbox (
			inbox_id uuid PRIMARY KEY,
			tenant_id text NOT NULL,
			channel_type text NOT NULL,
			binding_id text NOT NULL,
			external_message_id text NOT NULL,
			dedup_key text NOT NULL UNIQUE,
			payload jsonb NOT NULL,
			trace_carrier jsonb NOT NULL DEFAULT '{}'::jsonb,
			status text NOT NULL DEFAULT 'received',
			attempt_count integer NOT NULL DEFAULT 0,
			lease_owner text NOT NULL DEFAULT '',
			lease_expires_at timestamptz,
			next_attempt_at timestamptz NOT NULL DEFAULT now(),
			last_error_type text NOT NULL DEFAULT '',
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now()
		);
		CREATE TABLE runtime_outbox (
			outbox_id uuid PRIMARY KEY,
			tenant_id text NOT NULL,
			channel_type text NOT NULL,
			binding_id text NOT NULL,
			dedup_key text NOT NULL UNIQUE,
			payload jsonb NOT NULL,
			trace_id text NOT NULL DEFAULT '',
			trace_carrier jsonb NOT NULL DEFAULT '{}'::jsonb,
			status text NOT NULL DEFAULT 'pending',
			attempt_count integer NOT NULL DEFAULT 0,
			lease_owner text NOT NULL DEFAULT '',
			lease_expires_at timestamptz,
			next_attempt_at timestamptz NOT NULL DEFAULT now(),
			last_error_type text NOT NULL DEFAULT '',
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	now := time.Now()
	legacyFirstID := uuid.NewString()
	legacySecondID := uuid.NewString()
	legacyPendingOutboxID := uuid.NewString()
	legacySendingOutboxID := uuid.NewString()
	legacyNoDeadlineOutboxID := uuid.NewString()
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO runtime_inbox
			(inbox_id, tenant_id, channel_type, binding_id, external_message_id,
			 dedup_key, payload, next_attempt_at, created_at)
		VALUES
			($1, 'acme', 'telegram', 'bot-a', 'legacy-1', 'legacy-1', '{}'::jsonb,
			 $3::timestamptz, $3::timestamptz - interval '2 minutes'),
			($2, 'acme', 'telegram', 'bot-a', 'legacy-2', 'legacy-2', '{}'::jsonb,
			 $3::timestamptz, $3::timestamptz - interval '1 minute')`,
		legacyFirstID, legacySecondID, now); err != nil {
		t.Fatalf("insert legacy backlog: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO runtime_outbox
			(outbox_id, tenant_id, channel_type, binding_id, dedup_key, payload,
			 status, lease_owner, lease_expires_at, next_attempt_at, created_at)
		VALUES
			($1, 'acme', 'telegram', 'bot-a', 'legacy-out-pending', '{"text":"pending"}'::jsonb,
			 'pending', '', NULL, $4::timestamptz, $4::timestamptz),
			($2, 'acme', 'telegram', 'bot-a', 'legacy-out-sending', '{"text":"maybe sent"}'::jsonb,
			 'sending', 'old-sender', $4::timestamptz - interval '1 minute', $4::timestamptz, $4::timestamptz),
			($3, 'acme', 'telegram', 'bot-a', 'legacy-out-no-deadline', '{"text":"maybe sent too"}'::jsonb,
			 'sending', '', NULL, $4::timestamptz, $4::timestamptz)`,
		legacyPendingOutboxID, legacySendingOutboxID, legacyNoDeadlineOutboxID, now); err != nil {
		t.Fatalf("insert legacy outbox: %v", err)
	}
	if err := st.EnsureSchema(ctx); err != nil {
		t.Fatalf("upgrade legacy schema: %v", err)
	}
	var legacyVersion int
	var legacyMode, legacyDatabase string
	if err := st.pool.QueryRow(ctx, `
		SELECT pipeline_schema_version, atomic_commit_mode, database_identity
		FROM runtime_inbox WHERE inbox_id = $1`, legacyFirstID).
		Scan(&legacyVersion, &legacyMode, &legacyDatabase); err != nil {
		t.Fatal(err)
	}
	if legacyVersion != 0 || legacyMode != "" || legacyDatabase != "" {
		t.Fatalf("legacy inbox metadata = %d, %q, %q; want legacy markers", legacyVersion, legacyMode, legacyDatabase)
	}
	var pendingOperation, pendingState string
	var pendingPayload []byte
	if err := st.pool.QueryRow(ctx, `
		SELECT operation_key, delivery_state, payload_bytes
		FROM runtime_outbox WHERE outbox_id=$1`, legacyPendingOutboxID).
		Scan(&pendingOperation, &pendingState, &pendingPayload); err != nil {
		t.Fatal(err)
	}
	if pendingOperation != "legacy:"+legacyPendingOutboxID || pendingState != DeliveryPending || len(pendingPayload) == 0 {
		t.Fatalf("legacy pending backfill operation=%q state=%q payload=%q", pendingOperation, pendingState, pendingPayload)
	}
	if _, reclaimed, err := st.ReclaimExpired(ctx, time.Now()); err != nil || reclaimed != 2 {
		t.Fatalf("reclaim legacy sending: count=%d err=%v", reclaimed, err)
	}
	for _, outboxID := range []string{legacySendingOutboxID, legacyNoDeadlineOutboxID} {
		var sendingStatus, sendingState string
		if err := st.pool.QueryRow(ctx, `SELECT status, delivery_state FROM runtime_outbox WHERE outbox_id=$1`, outboxID).
			Scan(&sendingStatus, &sendingState); err != nil {
			t.Fatal(err)
		}
		if sendingStatus != OutboxSending || sendingState != DeliveryUnknown {
			t.Fatalf("legacy ambiguous send %s status=%q state=%q", outboxID, sendingStatus, sendingState)
		}
	}
	newExact := newPostgresInbox("new-exact", "session-exact")
	newExact.PipelineSchemaVersion = 1
	newExact.AtomicCommitMode = "postgres_same_database_v1"
	newExact.DatabaseIdentity = st.DatabaseIdentity()
	if err := st.InsertInbox(ctx, newExact, now); err != nil {
		t.Fatalf("insert exact partition after upgrade: %v", err)
	}

	// Empty legacy keys act as a conservative binding-wide lane. New exact
	// sessions cannot overtake the pre-upgrade backlog in the same binding.
	leased, err := st.LeaseInbox(ctx, "upgrade-relay", now, time.Minute, 10)
	if err != nil || len(leased) != 1 || leased[0].InboxID != legacyFirstID || leased[0].PartitionKey != "" {
		t.Fatalf("first upgraded lease: records=%+v err=%v", leased, err)
	}
	if err := st.CompleteInbox(ctx, legacyFirstID, "upgrade-relay", nil, now); err != nil {
		t.Fatal(err)
	}
	leased, err = st.LeaseInbox(ctx, "upgrade-relay", now, time.Minute, 10)
	if err != nil || len(leased) != 1 || leased[0].InboxID != legacySecondID {
		t.Fatalf("second upgraded lease: records=%+v err=%v", leased, err)
	}
	if err := st.CompleteInbox(ctx, legacySecondID, "upgrade-relay", nil, now); err != nil {
		t.Fatal(err)
	}
	leased, err = st.LeaseInbox(ctx, "upgrade-relay", now, time.Minute, 10)
	if err != nil || len(leased) != 1 || leased[0].InboxID != newExact.InboxID {
		t.Fatalf("exact session after legacy drain: records=%+v err=%v", leased, err)
	}
	if leased[0].PipelineSchemaVersion != newExact.PipelineSchemaVersion ||
		leased[0].AtomicCommitMode != newExact.AtomicCommitMode ||
		leased[0].DatabaseIdentity != newExact.DatabaseIdentity {
		t.Fatalf("leased pipeline metadata = %+v, want %+v", leased[0], newExact)
	}
}

func TestPostgresIntegrationPartitionOrderingAndOutboxInheritance(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	now := time.Now()
	first := newPostgresInbox("p1-first", "session-a")
	second := newPostgresInbox("p1-second", "session-a")
	other := newPostgresInbox("p2-first", "session-b")
	for _, rec := range []*InboxRecord{first, second, other} {
		if err := st.InsertInbox(ctx, rec, now); err != nil {
			t.Fatalf("insert %s: %v", rec.DedupKey, err)
		}
	}

	leased, err := st.LeaseInbox(ctx, "relay-a", now, time.Minute, 10)
	if err != nil {
		t.Fatalf("lease partition heads: %v", err)
	}
	got := map[string]InboxRecord{}
	for _, rec := range leased {
		got[rec.InboxID] = rec
	}
	if len(got) != 2 || got[first.InboxID].PartitionKey != "session-a" || got[other.InboxID].PartitionKey != "session-b" {
		t.Fatalf("leased partition heads = %+v", leased)
	}
	if _, exists := got[second.InboxID]; exists {
		t.Fatal("same-session successor was leased with its head")
	}
	if err := st.RetryInbox(ctx, first.InboxID, "relay-a", "temporary", now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteInbox(ctx, other.InboxID, "relay-a", nil, now); err != nil {
		t.Fatal(err)
	}
	blocked, err := st.LeaseInbox(ctx, "relay-b", now, time.Minute, 10)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("successor overtook retry head: records=%+v err=%v", blocked, err)
	}

	// PostgreSQL owns queue time. Advance only the persisted deadline instead
	// of pretending that this caller's wall clock moved forward.
	if _, err := st.pool.Exec(ctx, `
		UPDATE runtime_inbox
		SET next_attempt_at = clock_timestamp() - interval '1 microsecond'
		WHERE inbox_id = $1`, first.InboxID); err != nil {
		t.Fatalf("make first retry due: %v", err)
	}
	leased, err = st.LeaseInbox(ctx, "relay-b", now, time.Minute, 10)
	if err != nil || len(leased) != 1 || leased[0].InboxID != first.InboxID {
		t.Fatalf("re-lease first: records=%+v err=%v", leased, err)
	}
	outbox1 := &OutboxRecord{
		OutboxID: uuid.NewString(), TenantID: "acme", ChannelType: "telegram", BindingID: "bot-a",
		DedupKey: "p1-first:outbox", PartitionKey: "must-not-override-inbox",
		Payload:      []byte(`{"text":"first"}`),
		TraceCarrier: map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}
	if err := st.CompleteInbox(ctx, first.InboxID, "relay-b", outbox1, now); err != nil {
		t.Fatal(err)
	}
	leased, err = st.LeaseInbox(ctx, "relay-c", now, time.Minute, 10)
	if err != nil || len(leased) != 1 || leased[0].InboxID != second.InboxID {
		t.Fatalf("lease successor: records=%+v err=%v", leased, err)
	}
	outbox2 := &OutboxRecord{
		OutboxID: uuid.NewString(), TenantID: "acme", ChannelType: "telegram", BindingID: "bot-a",
		DedupKey: "p1-second:outbox", Payload: []byte(`{"text":"second"}`),
	}
	if err := st.CompleteInbox(ctx, second.InboxID, "relay-c", outbox2, now); err != nil {
		t.Fatal(err)
	}

	out, err := st.LeaseOutbox(ctx, "sender-a", now, time.Minute, 10)
	if err != nil || len(out) != 1 || out[0].OutboxID != outbox1.OutboxID {
		t.Fatalf("lease first reply: records=%+v err=%v", out, err)
	}
	if out[0].PartitionKey != "session-a" || out[0].TraceCarrier["traceparent"] == "" {
		t.Fatalf("outbox inheritance/trace = %+v", out[0])
	}
	if err := st.RetryOutbox(ctx, outbox1.OutboxID, "sender-a", "rate_limit", now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	blockedOut, err := st.LeaseOutbox(ctx, "sender-b", now, time.Minute, 10)
	if err != nil || len(blockedOut) != 0 {
		t.Fatalf("second reply overtook retry head: records=%+v err=%v", blockedOut, err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE runtime_outbox
		SET next_attempt_at = clock_timestamp() - interval '1 microsecond'
		WHERE outbox_id = $1`, outbox1.OutboxID); err != nil {
		t.Fatalf("make first outbox retry due: %v", err)
	}
	out, err = st.LeaseOutbox(ctx, "sender-b", now, time.Minute, 10)
	if err != nil || len(out) != 1 || out[0].OutboxID != outbox1.OutboxID {
		t.Fatalf("re-lease first reply: records=%+v err=%v", out, err)
	}
	if err := st.CompleteOutbox(ctx, outbox1.OutboxID, "sender-b", now); err != nil {
		t.Fatal(err)
	}
	out, err = st.LeaseOutbox(ctx, "sender-c", now, time.Minute, 10)
	if err != nil || len(out) != 1 || out[0].OutboxID != outbox2.OutboxID {
		t.Fatalf("lease second reply: records=%+v err=%v", out, err)
	}
}

func TestPostgresIntegrationConcurrentLeaseAndFencing(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	now := time.Now()
	const partitions = 10
	firstIDs := make(map[string]struct{}, partitions)
	secondIDs := make(map[string]struct{}, partitions)
	for i := 0; i < partitions; i++ {
		partition := fmt.Sprintf("session-%02d", i)
		first := newPostgresInbox(fmt.Sprintf("%02d-first", i), partition)
		second := newPostgresInbox(fmt.Sprintf("%02d-second", i), partition)
		firstIDs[first.InboxID] = struct{}{}
		secondIDs[second.InboxID] = struct{}{}
		if err := st.InsertInbox(ctx, first, now); err != nil {
			t.Fatal(err)
		}
		if err := st.InsertInbox(ctx, second, now); err != nil {
			t.Fatal(err)
		}
	}

	type claim struct {
		owner string
		rec   InboxRecord
		err   error
	}
	claims := make(chan claim, partitions)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < partitions; i++ {
		owner := fmt.Sprintf("owner-%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			records, err := st.LeaseInbox(ctx, owner, now, time.Minute, 1)
			if err != nil || len(records) != 1 {
				claims <- claim{owner: owner, err: fmt.Errorf("records=%d: %w", len(records), err)}
				return
			}
			claims <- claim{owner: owner, rec: records[0]}
		}()
	}
	close(start)
	wg.Wait()
	close(claims)
	claimed := make(map[string]string, partitions)
	for result := range claims {
		if result.err != nil {
			t.Fatalf("concurrent lease: %v", result.err)
		}
		if _, ok := firstIDs[result.rec.InboxID]; !ok {
			t.Fatalf("leased non-head row %s", result.rec.InboxID)
		}
		if _, duplicate := claimed[result.rec.InboxID]; duplicate {
			t.Fatalf("row leased twice: %s", result.rec.InboxID)
		}
		claimed[result.rec.InboxID] = result.owner
	}
	if len(claimed) != partitions {
		t.Fatalf("concurrent heads claimed = %d, want %d", len(claimed), partitions)
	}
	blocked, err := st.LeaseInbox(ctx, "extra-owner", now, time.Minute, partitions)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("successors passed processing heads: records=%+v err=%v", blocked, err)
	}
	for inboxID, owner := range claimed {
		if err := st.CompleteInbox(ctx, inboxID, owner, nil, now); err != nil {
			t.Fatalf("complete head %s: %v", inboxID, err)
		}
	}
	next, err := st.LeaseInbox(ctx, "next-owner", now, time.Second, partitions)
	if err != nil || len(next) != partitions {
		t.Fatalf("lease successors: records=%d err=%v", len(next), err)
	}
	for _, rec := range next {
		if _, ok := secondIDs[rec.InboxID]; !ok {
			t.Fatalf("unexpected successor %s", rec.InboxID)
		}
	}

	// One stale attempt cannot mutate a row after expiry and reclamation. Move
	// the authoritative database deadline rather than passing a future app
	// clock into Store methods.
	fenced := next[0]
	if _, err := st.pool.Exec(ctx, `
		UPDATE runtime_inbox
		SET lease_expires_at = clock_timestamp() - interval '1 microsecond'
		WHERE status = 'processing'`); err != nil {
		t.Fatalf("expire processing leases: %v", err)
	}
	if err := st.CompleteInbox(ctx, fenced.InboxID, "next-owner", nil, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired completion = %v, want ErrLeaseLost", err)
	}
	reclaimed, _, err := st.ReclaimExpired(ctx, now)
	if err != nil || reclaimed != partitions {
		t.Fatalf("reclaim expired heads: count=%d err=%v", reclaimed, err)
	}
	released, err := st.LeaseInbox(ctx, "replacement", now, time.Minute, 1)
	if err != nil || len(released) != 1 {
		t.Fatalf("replacement lease: records=%+v err=%v", released, err)
	}
	if err := st.CompleteInbox(ctx, released[0].InboxID, "next-owner", nil, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner completion = %v, want ErrLeaseLost", err)
	}
}

func TestPostgresIntegrationUsesDatabaseClockDespiteApplicationSkew(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	appNow := time.Now()
	tenYears := 10 * 365 * 24 * time.Hour
	fastAppClock := appNow.Add(tenYears)
	slowAppClock := appNow.Add(-tenYears)
	inbox := newPostgresInbox("clock-skew-inbox", "session-clock-skew")

	// A fast writer must not defer a newly accepted message for ten years.
	if err := st.InsertInbox(ctx, inbox, fastAppClock); err != nil {
		t.Fatalf("insert with fast app clock: %v", err)
	}
	leased, err := st.LeaseInbox(ctx, "clock-owner-1", slowAppClock, time.Minute, 1)
	if err != nil || len(leased) != 1 || leased[0].InboxID != inbox.InboxID {
		t.Fatalf("lease with slow app clock: records=%+v err=%v", leased, err)
	}
	remaining := postgresIntervalSeconds(t, st, `
		SELECT EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp()))::double precision
		FROM runtime_inbox WHERE inbox_id = $1`, inbox.InboxID)
	if remaining < 30 || remaining > 65 {
		t.Fatalf("inbox lease remaining = %.3fs, want approximately 60s of database time", remaining)
	}
	if inboxCount, outboxCount, err := st.ReclaimExpired(ctx, fastAppClock); err != nil || inboxCount != 0 || outboxCount != 0 {
		t.Fatalf("fast app clock reclaimed live inbox: inbox=%d outbox=%d err=%v", inboxCount, outboxCount, err)
	}

	// A fast caller cannot make a live database lease appear expired, and the
	// renewed deadline is still based on PostgreSQL's clock.
	if err := st.RenewInboxLease(ctx, inbox.InboxID, "clock-owner-1", fastAppClock, 2*time.Minute); err != nil {
		t.Fatalf("renew inbox with fast app clock: %v", err)
	}
	remaining = postgresIntervalSeconds(t, st, `
		SELECT EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp()))::double precision
		FROM runtime_inbox WHERE inbox_id = $1`, inbox.InboxID)
	if remaining < 90 || remaining > 125 {
		t.Fatalf("renewed inbox lease remaining = %.3fs, want approximately 120s", remaining)
	}

	// retryAt and now form a relative delay. Their absolute skew must not leak
	// into the persisted schedule.
	if err := st.RetryInbox(ctx, inbox.InboxID, "clock-owner-1", "temporary", fastAppClock, fastAppClock.Add(time.Hour)); err != nil {
		t.Fatalf("retry inbox with fast app clock: %v", err)
	}
	remaining = postgresIntervalSeconds(t, st, `
		SELECT EXTRACT(EPOCH FROM (next_attempt_at - clock_timestamp()))::double precision
		FROM runtime_inbox WHERE inbox_id = $1`, inbox.InboxID)
	if remaining < 3500 || remaining > 3650 {
		t.Fatalf("inbox retry remaining = %.3fs, want approximately one hour", remaining)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE runtime_inbox
		SET next_attempt_at = clock_timestamp() - interval '1 microsecond'
		WHERE inbox_id = $1`, inbox.InboxID); err != nil {
		t.Fatalf("make skew inbox retry due: %v", err)
	}

	leased, err = st.LeaseInbox(ctx, "clock-owner-2", fastAppClock, time.Minute, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("re-lease skew inbox: records=%+v err=%v", leased, err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE runtime_inbox
		SET lease_expires_at = clock_timestamp() - interval '1 microsecond'
		WHERE inbox_id = $1`, inbox.InboxID); err != nil {
		t.Fatalf("expire skew inbox lease: %v", err)
	}
	if err := st.CompleteInbox(ctx, inbox.InboxID, "clock-owner-2", nil, slowAppClock); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("slow app clock completed an expired inbox lease: %v", err)
	}
	reclaimedInbox, reclaimedOutbox, err := st.ReclaimExpired(ctx, slowAppClock)
	if err != nil || reclaimedInbox != 1 || reclaimedOutbox != 0 {
		t.Fatalf("reclaim with slow app clock: inbox=%d outbox=%d err=%v", reclaimedInbox, reclaimedOutbox, err)
	}

	leased, err = st.LeaseInbox(ctx, "clock-owner-3", slowAppClock, time.Minute, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease reclaimed skew inbox: records=%+v err=%v", leased, err)
	}
	outbox := &OutboxRecord{
		OutboxID: uuid.NewString(), TenantID: "acme", ChannelType: "telegram", BindingID: "bot-a",
		DedupKey: "clock-skew-outbox", Payload: []byte(`{"text":"ok"}`),
	}
	if err := st.CompleteInbox(ctx, inbox.InboxID, "clock-owner-3", outbox, fastAppClock); err != nil {
		t.Fatalf("complete live inbox with fast app clock: %v", err)
	}

	out, err := st.LeaseOutbox(ctx, "clock-sender-1", fastAppClock, time.Minute, 1)
	if err != nil || len(out) != 1 || out[0].OutboxID != outbox.OutboxID {
		t.Fatalf("lease outbox with fast app clock: records=%+v err=%v", out, err)
	}
	remaining = postgresIntervalSeconds(t, st, `
		SELECT EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp()))::double precision
		FROM runtime_outbox WHERE outbox_id = $1`, outbox.OutboxID)
	if remaining < 30 || remaining > 65 {
		t.Fatalf("outbox lease remaining = %.3fs, want approximately 60s of database time", remaining)
	}
	if inboxCount, outboxCount, err := st.ReclaimExpired(ctx, fastAppClock); err != nil || inboxCount != 0 || outboxCount != 0 {
		t.Fatalf("fast app clock reclaimed live outbox: inbox=%d outbox=%d err=%v", inboxCount, outboxCount, err)
	}
	if err := st.RenewOutboxLease(ctx, outbox.OutboxID, "clock-sender-1", fastAppClock, 2*time.Minute); err != nil {
		t.Fatalf("renew outbox with fast app clock: %v", err)
	}
	if err := st.RetryOutbox(ctx, outbox.OutboxID, "clock-sender-1", "temporary", slowAppClock, slowAppClock.Add(time.Hour)); err != nil {
		t.Fatalf("retry outbox with slow app clock: %v", err)
	}
	remaining = postgresIntervalSeconds(t, st, `
		SELECT EXTRACT(EPOCH FROM (next_attempt_at - clock_timestamp()))::double precision
		FROM runtime_outbox WHERE outbox_id = $1`, outbox.OutboxID)
	if remaining < 3500 || remaining > 3650 {
		t.Fatalf("outbox retry remaining = %.3fs, want approximately one hour", remaining)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE runtime_outbox
		SET next_attempt_at = clock_timestamp() - interval '1 microsecond'
		WHERE outbox_id = $1`, outbox.OutboxID); err != nil {
		t.Fatalf("make skew outbox retry due: %v", err)
	}
	out, err = st.LeaseOutbox(ctx, "clock-sender-2", slowAppClock, time.Minute, 1)
	if err != nil || len(out) != 1 {
		t.Fatalf("re-lease skew outbox: records=%+v err=%v", out, err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE runtime_outbox
		SET lease_expires_at = clock_timestamp() - interval '1 microsecond'
		WHERE outbox_id = $1`, outbox.OutboxID); err != nil {
		t.Fatalf("expire skew outbox lease: %v", err)
	}
	if err := st.CompleteOutbox(ctx, outbox.OutboxID, "clock-sender-2", slowAppClock); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("slow app clock completed an expired outbox lease: %v", err)
	}
	reclaimedInbox, reclaimedOutbox, err = st.ReclaimExpired(ctx, slowAppClock)
	if err != nil || reclaimedInbox != 0 || reclaimedOutbox != 1 {
		t.Fatalf("reclaim expired outbox with slow app clock: inbox=%d outbox=%d err=%v", reclaimedInbox, reclaimedOutbox, err)
	}
}

func TestPostgresIntegrationCompleteInboxRechecksClockAfterRowLockWait(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Now()
	inbox := newPostgresInbox("lock-wait-expiry", "session-lock-wait")
	if err := st.InsertInbox(ctx, inbox, now); err != nil {
		t.Fatal(err)
	}
	leased, err := st.LeaseInbox(ctx, "lock-wait-owner", now, time.Second, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease lock-wait inbox: records=%+v err=%v", leased, err)
	}

	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker transaction: %v", err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	var blockerPID int32
	if err := blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("read blocker pid: %v", err)
	}
	var lockedID string
	if err := blocker.QueryRow(ctx, `
		SELECT inbox_id FROM runtime_inbox
		WHERE inbox_id = $1
		FOR UPDATE`, inbox.InboxID).Scan(&lockedID); err != nil {
		t.Fatalf("lock inbox row: %v", err)
	}
	var leaseIsLive bool
	if err := blocker.QueryRow(ctx, `
		SELECT lease_expires_at > clock_timestamp()
		FROM runtime_inbox WHERE inbox_id = $1`, inbox.InboxID).Scan(&leaseIsLive); err != nil {
		t.Fatalf("check lease before blocked completion: %v", err)
	}
	if !leaseIsLive {
		t.Fatal("lease expired before the blocked completion started")
	}

	completed := make(chan error, 1)
	go func() {
		completed <- st.CompleteInbox(ctx, inbox.InboxID, "lock-wait-owner", nil, now)
	}()
	waitForPostgresCondition(t, ctx, st, "completion to wait on row lock", `
		SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE $1::integer = ANY(pg_blocking_pids(pid))
		)`, blockerPID)
	waitForPostgresCondition(t, ctx, st, "database lease expiry", `
		SELECT lease_expires_at <= clock_timestamp()
		FROM runtime_inbox WHERE inbox_id = $1`, inbox.InboxID)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release blocker transaction: %v", err)
	}

	select {
	case err := <-completed:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("completion that crossed lease expiry = %v, want ErrLeaseLost", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for blocked completion: %v", ctx.Err())
	}
	var status, owner string
	if err := st.pool.QueryRow(ctx, `
		SELECT status, lease_owner FROM runtime_inbox WHERE inbox_id = $1`, inbox.InboxID).
		Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	if status != InboxProcessing || owner != "lock-wait-owner" {
		t.Fatalf("expired blocked completion mutated row: status=%s owner=%s", status, owner)
	}
}

func TestPostgresIntegrationCompleteInboxRechecksClockAfterOutboxInsertWait(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Now()
	inbox := newPostgresInbox("outbox-wait-expiry", "session-outbox-wait")
	if err := st.InsertInbox(ctx, inbox, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `
		CREATE FUNCTION delay_runtime_outbox_insert() RETURNS trigger
		LANGUAGE plpgsql AS $body$
		BEGIN
			PERFORM pg_sleep(1);
			RETURN NEW;
		END
		$body$;
		CREATE TRIGGER delay_runtime_outbox_insert
		BEFORE INSERT ON runtime_outbox
		FOR EACH ROW EXECUTE FUNCTION delay_runtime_outbox_insert()`); err != nil {
		t.Fatalf("install outbox delay trigger: %v", err)
	}
	leased, err := st.LeaseInbox(ctx, "outbox-wait-owner", now, 500*time.Millisecond, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease outbox-wait inbox: records=%+v err=%v", leased, err)
	}
	outbox := &OutboxRecord{
		OutboxID: uuid.NewString(), TenantID: "acme", ChannelType: "telegram", BindingID: "bot-a",
		DedupKey: "outbox-wait-expiry:reply", Payload: []byte(`{"text":"too late"}`),
	}
	if err := st.CompleteInbox(ctx, inbox.InboxID, "outbox-wait-owner", outbox, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("completion whose outbox insert crossed lease expiry = %v, want ErrLeaseLost", err)
	}
	var status, owner string
	var outboxCount int
	if err := st.pool.QueryRow(ctx, `
		SELECT status, lease_owner FROM runtime_inbox WHERE inbox_id = $1`, inbox.InboxID).
		Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if status != InboxProcessing || owner != "outbox-wait-owner" || outboxCount != 0 {
		t.Fatalf("expired outbox wait leaked state: status=%s owner=%s outbox=%d", status, owner, outboxCount)
	}
}

func TestPostgresIntegrationCompleteInboxIsAtomic(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	now := time.Now()
	inbox := newPostgresInbox("atomic-inbox", "session-atomic")
	if err := st.InsertInbox(ctx, inbox, now); err != nil {
		t.Fatal(err)
	}
	leased, err := st.LeaseInbox(ctx, "relay", now, time.Hour, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: records=%+v err=%v", leased, err)
	}
	if _, err := st.pool.Exec(ctx, `
		CREATE FUNCTION fail_runtime_outbox_insert() RETURNS trigger
		LANGUAGE plpgsql AS $body$
		BEGIN
			RAISE EXCEPTION 'injected outbox failure';
		END
		$body$;
		CREATE TRIGGER fail_runtime_outbox_insert
		BEFORE INSERT ON runtime_outbox
		FOR EACH ROW EXECUTE FUNCTION fail_runtime_outbox_insert()`); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}
	outbox := &OutboxRecord{
		OutboxID: uuid.NewString(), TenantID: "acme", ChannelType: "telegram", BindingID: "bot-a",
		DedupKey: "atomic-outbox", Payload: []byte(`{"text":"safe"}`),
	}
	if err := st.CompleteInbox(ctx, inbox.InboxID, "relay", outbox, now); err == nil {
		t.Fatal("injected outbox failure unexpectedly committed")
	}
	var status, owner string
	var outboxCount int
	if err := st.pool.QueryRow(ctx, `SELECT status, lease_owner FROM runtime_inbox WHERE inbox_id=$1`, inbox.InboxID).Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if status != InboxProcessing || owner != "relay" || outboxCount != 0 {
		t.Fatalf("partial transaction leaked: status=%s owner=%s outbox=%d", status, owner, outboxCount)
	}
	if _, err := st.pool.Exec(ctx, `
		DROP TRIGGER fail_runtime_outbox_insert ON runtime_outbox;
		DROP FUNCTION fail_runtime_outbox_insert()`); err != nil {
		t.Fatalf("remove failure trigger: %v", err)
	}
	if err := st.CompleteInbox(ctx, inbox.InboxID, "relay", outbox, now); err != nil {
		t.Fatalf("complete after removing trigger: %v", err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT status FROM runtime_inbox WHERE inbox_id=$1`, inbox.InboxID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if status != InboxProcessed || outboxCount != 1 {
		t.Fatalf("atomic completion state: status=%s outbox=%d", status, outboxCount)
	}
}

func TestPostgresIntegrationDeliveryOperationLedgerAndManualResolution(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	now := time.Now()
	inbox := newPostgresInbox("ledger-inbox", "session-ledger")
	if err := st.InsertInbox(ctx, inbox, now); err != nil {
		t.Fatal(err)
	}
	leasedInbox, err := st.LeaseInbox(ctx, "ledger-relay", now, time.Hour, 1)
	if err != nil || len(leasedInbox) != 1 {
		t.Fatalf("lease inbox: records=%+v err=%v", leasedInbox, err)
	}
	plan := newPostgresOutboxPlan("ledger", "session-ledger", 3)
	if err := st.CompleteInboxBatch(ctx, inbox.InboxID, "ledger-relay", plan, now); err != nil {
		t.Fatalf("complete batch: %v", err)
	}
	conflictInbox := newPostgresInbox("ledger-conflict", "session-ledger")
	if err := st.InsertInbox(ctx, conflictInbox, now); err != nil {
		t.Fatal(err)
	}
	if records, err := st.LeaseInbox(ctx, "conflict-relay", now, time.Hour, 1); err != nil || len(records) != 1 {
		t.Fatalf("lease conflict inbox: %+v %v", records, err)
	}
	conflict := plan[0]
	conflict.OutboxID = uuid.NewString()
	conflict.DedupKey = "different-dedup-for-same-operation"
	conflict.Payload = []byte(`{"target":"chat","text":"changed"}`)
	conflict.PayloadHash = ""
	if err := st.CompleteInboxBatch(ctx, conflictInbox.InboxID, "conflict-relay", []OutboxRecord{conflict}, now); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("operation conflict = %v, want ErrOperationConflict", err)
	}
	var conflictStatus string
	if err := st.pool.QueryRow(ctx, `SELECT status FROM runtime_inbox WHERE inbox_id=$1`, conflictInbox.InboxID).Scan(&conflictStatus); err != nil {
		t.Fatal(err)
	}
	if conflictStatus != InboxProcessing {
		t.Fatalf("conflicting completion mutated inbox to %s", conflictStatus)
	}

	leasePart := func(owner string) OutboxRecord {
		t.Helper()
		records, leaseErr := st.LeaseOutbox(ctx, owner, time.Now(), time.Hour, 1)
		if leaseErr != nil || len(records) != 1 {
			t.Fatalf("lease %s: records=%+v err=%v", owner, records, leaseErr)
		}
		return records[0]
	}
	first := leasePart("sender-first")
	if first.PartIndex != 0 || first.AttemptCount != 1 || first.AttemptPhase != AttemptLeased {
		t.Fatalf("first part lease = %+v", first)
	}
	if err := st.MarkOutboxDispatched(ctx, first.OutboxID, "sender-first", first.AttemptCount, time.Now()); err != nil {
		t.Fatalf("dispatch first: %v", err)
	}
	if err := st.FinishOutboxAttempt(ctx, first.OutboxID, "sender-first", first.AttemptCount,
		delivery.Result{Outcome: delivery.Confirmed, ProviderMessageID: "provider-first"},
		time.Now(), time.Time{}, false); err != nil {
		t.Fatalf("finish first: %v", err)
	}

	second := leasePart("sender-second")
	if second.PartIndex != 1 || second.OperationKey != plan[1].OperationKey {
		t.Fatalf("second part lease = %+v", second)
	}
	if err := st.MarkOutboxDispatched(ctx, second.OutboxID, "sender-second", second.AttemptCount, time.Now()); err != nil {
		t.Fatalf("dispatch second: %v", err)
	}
	if err := st.FinishOutboxAttempt(ctx, second.OutboxID, "sender-second", second.AttemptCount,
		delivery.Result{Outcome: delivery.Unknown, ErrorType: "response_lost", ProviderRequestID: "request-second"},
		time.Now(), time.Time{}, false); err != nil {
		t.Fatalf("finish second unknown: %v", err)
	}
	if records, err := st.LeaseOutbox(ctx, "blocked-third", time.Now(), time.Hour, 1); err != nil || len(records) != 0 {
		t.Fatalf("third part crossed unknown barrier: records=%+v err=%v", records, err)
	}
	unknown, err := st.ListUncertainOutbox(ctx, 10)
	if err != nil || len(unknown) != 1 {
		t.Fatalf("list unknown: records=%+v err=%v", unknown, err)
	}
	parked := unknown[0]
	resolutionID := uuid.NewString()
	resolution := ResolveRequest{
		ResolutionID: resolutionID, OutboxID: parked.OutboxID,
		ExpectedVersion: parked.StateVersion, ExpectedAttempt: parked.AttemptCount,
		Action: ResolveRetry, Actor: "integration-test", Reason: "explicit duplicate-risk acceptance", Now: time.Now(),
	}
	if err := st.ResolveOutbox(ctx, resolution); err != nil {
		t.Fatalf("resolve retry: %v", err)
	}
	if err := st.ResolveOutbox(ctx, resolution); err != nil {
		t.Fatalf("idempotent resolution replay: %v", err)
	}
	stale := resolution
	stale.ResolutionID = uuid.NewString()
	if err := st.ResolveOutbox(ctx, stale); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale resolution = %v, want ErrInvalidTransition", err)
	}

	secondRetry := leasePart("sender-second-retry")
	if secondRetry.OutboxID != second.OutboxID || secondRetry.OperationKey != second.OperationKey || secondRetry.AttemptCount != 2 {
		t.Fatalf("retry changed operation identity: first=%+v retry=%+v", second, secondRetry)
	}
	if err := st.MarkOutboxDispatched(ctx, secondRetry.OutboxID, "sender-second-retry", secondRetry.AttemptCount, time.Now()); err != nil {
		t.Fatalf("dispatch second retry: %v", err)
	}
	if err := st.FinishOutboxAttempt(ctx, secondRetry.OutboxID, "sender-second-retry", secondRetry.AttemptCount,
		delivery.Result{Outcome: delivery.Confirmed}, time.Now(), time.Time{}, false); err != nil {
		t.Fatalf("finish second retry: %v", err)
	}
	third := leasePart("sender-third")
	if third.PartIndex != 2 {
		t.Fatalf("third part lease = %+v", third)
	}

	var attempts, resolutions int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_outbox_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_outbox_resolutions`).Scan(&resolutions); err != nil {
		t.Fatal(err)
	}
	if attempts != 4 || resolutions != 1 {
		t.Fatalf("ledger counts attempts=%d resolutions=%d", attempts, resolutions)
	}
}

func parkPostgresUnknown(t *testing.T, st *Postgres, prefix string) OutboxRecord {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	inbox := newPostgresInbox(prefix+"-inbox", prefix+"-lane")
	if err := st.InsertInbox(ctx, inbox, now); err != nil {
		t.Fatalf("insert unknown inbox: %v", err)
	}
	relayOwner := prefix + "-relay"
	if records, err := st.LeaseInbox(ctx, relayOwner, now, time.Hour, 1); err != nil || len(records) != 1 {
		t.Fatalf("lease unknown inbox: records=%+v err=%v", records, err)
	}
	plan := newPostgresOutboxPlan(prefix, prefix+"-lane", 1)
	if err := st.CompleteInboxBatch(ctx, inbox.InboxID, relayOwner, plan, now); err != nil {
		t.Fatalf("complete unknown inbox: %v", err)
	}
	senderOwner := prefix + "-sender"
	records, err := st.LeaseOutbox(ctx, senderOwner, now, time.Hour, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("lease unknown outbox: records=%+v err=%v", records, err)
	}
	leased := records[0]
	if err := st.MarkOutboxDispatched(ctx, leased.OutboxID, senderOwner, leased.AttemptCount, now); err != nil {
		t.Fatalf("mark unknown outbox dispatched: %v", err)
	}
	if err := st.FinishOutboxAttempt(ctx, leased.OutboxID, senderOwner, leased.AttemptCount,
		delivery.Result{Outcome: delivery.Unknown, ErrorType: "response_lost"},
		now, time.Time{}, false); err != nil {
		t.Fatalf("finish unknown outbox: %v", err)
	}
	unknown, err := st.ListUncertainOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("list unknown outbox: %v", err)
	}
	for _, rec := range unknown {
		if rec.OutboxID == leased.OutboxID {
			return rec
		}
	}
	t.Fatalf("parked outbox %s not listed", leased.OutboxID)
	return OutboxRecord{}
}

func TestPostgresIntegrationConcurrentResolutionIdempotencyAndCAS(t *testing.T) {
	t.Run("identical resolution id is concurrent-idempotent", func(t *testing.T) {
		st := newPostgresIntegrationStore(t, true)
		ctx := context.Background()
		parked := parkPostgresUnknown(t, st, "resolution-idempotent")
		request := ResolveRequest{
			ResolutionID: uuid.NewString(), OutboxID: parked.OutboxID,
			ExpectedVersion: parked.StateVersion, ExpectedAttempt: parked.AttemptCount,
			Action: ResolveRetry, Actor: "integration-test",
			Reason: "explicit duplicate-risk acceptance", Now: time.Now(),
		}

		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results <- st.ResolveOutbox(ctx, request)
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		for err := range results {
			if err != nil {
				t.Fatalf("concurrent identical resolution: %v", err)
			}
		}
		var count int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM runtime_outbox_resolutions WHERE resolution_id=$1`,
			request.ResolutionID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("resolution ledger rows = %d, want 1", count)
		}
		conflict := request
		conflict.Action = ResolveCancel
		if err := st.ResolveOutbox(ctx, conflict); !errors.Is(err, ErrOperationConflict) {
			t.Fatalf("same resolution id with different decision = %v, want ErrOperationConflict", err)
		}
	})

	t.Run("different decisions share one state CAS", func(t *testing.T) {
		st := newPostgresIntegrationStore(t, true)
		ctx := context.Background()
		parked := parkPostgresUnknown(t, st, "resolution-cas")
		requests := []ResolveRequest{
			{
				ResolutionID: uuid.NewString(), OutboxID: parked.OutboxID,
				ExpectedVersion: parked.StateVersion, ExpectedAttempt: parked.AttemptCount,
				Action: ResolveAssumeDelivered, Actor: "admin-a", Reason: "provider lookup confirmed", Now: time.Now(),
			},
			{
				ResolutionID: uuid.NewString(), OutboxID: parked.OutboxID,
				ExpectedVersion: parked.StateVersion, ExpectedAttempt: parked.AttemptCount,
				Action: ResolveCancel, Actor: "admin-b", Reason: "operator canceled delivery", Now: time.Now(),
			},
		}
		start := make(chan struct{})
		results := make(chan error, len(requests))
		var wg sync.WaitGroup
		for i := range requests {
			request := requests[i]
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results <- st.ResolveOutbox(ctx, request)
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		succeeded, conflicted := 0, 0
		for err := range results {
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrInvalidTransition):
				conflicted++
			default:
				t.Fatalf("concurrent different resolution: %v", err)
			}
		}
		if succeeded != 1 || conflicted != 1 {
			t.Fatalf("resolution CAS results: success=%d conflict=%d", succeeded, conflicted)
		}
		var ledgerRows int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM runtime_outbox_resolutions WHERE outbox_id=$1`,
			parked.OutboxID).Scan(&ledgerRows); err != nil {
			t.Fatal(err)
		}
		if ledgerRows != 1 {
			t.Fatalf("concurrent decision ledger rows = %d, want 1", ledgerRows)
		}
	})
}

func TestPostgresIntegrationDeliveryOutcomeMatrix(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	now := time.Now()
	tests := []struct {
		name          string
		outcome       delivery.Outcome
		exhausted     bool
		wantStatus    string
		wantState     string
		attemptResult delivery.Outcome
	}{
		{name: "confirmed", outcome: delivery.Confirmed, wantStatus: OutboxSent, wantState: DeliveryConfirmed, attemptResult: delivery.Confirmed},
		{name: "retryable", outcome: delivery.RetryableNotSent, wantStatus: OutboxRetry, wantState: DeliveryRetryableNotSent, attemptResult: delivery.RetryableNotSent},
		{name: "retry exhausted", outcome: delivery.RetryableNotSent, exhausted: true, wantStatus: OutboxDead, wantState: DeliveryRetryExhausted, attemptResult: delivery.RetryableNotSent},
		{name: "permanent", outcome: delivery.PermanentRejected, wantStatus: OutboxDead, wantState: DeliveryPermanentRejected, attemptResult: delivery.PermanentRejected},
		{name: "unknown", outcome: delivery.Unknown, wantStatus: OutboxSending, wantState: DeliveryUnknown, attemptResult: delivery.Unknown},
	}

	for index, test := range tests {
		prefix := fmt.Sprintf("outcome-%02d", index)
		inbox := newPostgresInbox(prefix, prefix+"-lane")
		if err := st.InsertInbox(ctx, inbox, now); err != nil {
			t.Fatalf("%s insert inbox: %v", test.name, err)
		}
		relayOwner := prefix + "-relay"
		if records, err := st.LeaseInbox(ctx, relayOwner, now, time.Hour, 1); err != nil || len(records) != 1 {
			t.Fatalf("%s lease inbox: records=%+v err=%v", test.name, records, err)
		}
		plan := newPostgresOutboxPlan(prefix, prefix+"-lane", 1)
		if err := st.CompleteInboxBatch(ctx, inbox.InboxID, relayOwner, plan, now); err != nil {
			t.Fatalf("%s complete inbox: %v", test.name, err)
		}
		senderOwner := prefix + "-sender"
		records, err := st.LeaseOutbox(ctx, senderOwner, now, time.Hour, 1)
		if err != nil || len(records) != 1 {
			t.Fatalf("%s lease outbox: records=%+v err=%v", test.name, records, err)
		}
		leased := records[0]
		if err := st.MarkOutboxDispatched(ctx, leased.OutboxID, senderOwner, leased.AttemptCount, now); err != nil {
			t.Fatalf("%s mark dispatched: %v", test.name, err)
		}
		result := delivery.Result{
			Outcome: test.outcome, ErrorType: "provider_result", ProviderCode: "test_code",
			HTTPStatus: 503, ProviderMessageID: "message-" + prefix,
			ProviderRequestID: "request-" + prefix, ResponseHash: strings.Repeat("a", 64),
		}
		if err := st.FinishOutboxAttempt(ctx, leased.OutboxID, senderOwner, leased.AttemptCount,
			result, now, now.Add(time.Hour), test.exhausted); err != nil {
			t.Fatalf("%s finish attempt: %v", test.name, err)
		}
		var status, state, providerCode, messageID, requestID, responseHash string
		var leaseCleared bool
		if err := st.pool.QueryRow(ctx, `
			SELECT status, delivery_state, provider_code, provider_message_id,
				provider_request_id, response_hash,
				lease_owner = '' AND lease_expires_at IS NULL
			FROM runtime_outbox WHERE outbox_id=$1`, leased.OutboxID).
			Scan(&status, &state, &providerCode, &messageID, &requestID, &responseHash, &leaseCleared); err != nil {
			t.Fatal(err)
		}
		if status != test.wantStatus || state != test.wantState || !leaseCleared ||
			providerCode != result.ProviderCode || messageID != result.ProviderMessageID ||
			requestID != result.ProviderRequestID || responseHash != result.ResponseHash {
			t.Fatalf("%s persisted status=%q state=%q leaseCleared=%v metadata=%q/%q/%q/%q",
				test.name, status, state, leaseCleared, providerCode, messageID, requestID, responseHash)
		}
		var phase, attemptOutcome string
		var httpStatus int
		if err := st.pool.QueryRow(ctx, `
			SELECT phase, outcome, http_status FROM runtime_outbox_attempts
			WHERE outbox_id=$1 AND attempt_no=$2`, leased.OutboxID, leased.AttemptCount).
			Scan(&phase, &attemptOutcome, &httpStatus); err != nil {
			t.Fatal(err)
		}
		if phase != AttemptFinished || attemptOutcome != string(test.attemptResult) || httpStatus != result.HTTPStatus {
			t.Fatalf("%s attempt phase=%q outcome=%q http=%d", test.name, phase, attemptOutcome, httpStatus)
		}
	}

	_, _, pending, dead, uncertain, err := st.Depths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 1 || dead != 2 || uncertain != 1 {
		t.Fatalf("outcome depths pending=%d dead=%d uncertain=%d", pending, dead, uncertain)
	}
}

func TestPostgresIntegrationReclaimDistinguishesLeasedAndDispatched(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	now := time.Now()
	for _, item := range []struct{ key, partition string }{{"reclaim-leased", "lane-a"}, {"reclaim-dispatched", "lane-b"}} {
		inbox := newPostgresInbox(item.key, item.partition)
		if err := st.InsertInbox(ctx, inbox, now); err != nil {
			t.Fatal(err)
		}
		owner := "relay-" + item.key
		if records, err := st.LeaseInbox(ctx, owner, now, time.Hour, 1); err != nil || len(records) != 1 {
			t.Fatalf("lease inbox %s: %+v %v", item.key, records, err)
		}
		plan := newPostgresOutboxPlan(item.key, item.partition, 1)
		if err := st.CompleteInboxBatch(ctx, inbox.InboxID, owner, plan, now); err != nil {
			t.Fatalf("complete %s: %v", item.key, err)
		}
	}
	records, err := st.LeaseOutbox(ctx, "reclaim-owner", now, 120*time.Millisecond, 2)
	if err != nil || len(records) != 2 {
		t.Fatalf("lease reclaim candidates: records=%+v err=%v", records, err)
	}
	var dispatched OutboxRecord
	for _, rec := range records {
		if rec.PartitionKey == "lane-b" {
			dispatched = rec
		}
	}
	if dispatched.OutboxID == "" {
		t.Fatalf("missing dispatched candidate: %+v", records)
	}
	if err := st.MarkOutboxDispatched(ctx, dispatched.OutboxID, "reclaim-owner", dispatched.AttemptCount, time.Now()); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	_, reclaimed, err := st.ReclaimExpired(ctx, time.Now())
	if err != nil || reclaimed != 2 {
		t.Fatalf("reclaim count=%d err=%v", reclaimed, err)
	}
	states := map[string]string{}
	rows, err := st.pool.Query(ctx, `SELECT partition_key, delivery_state FROM runtime_outbox`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var partition, state string
		if err := rows.Scan(&partition, &state); err != nil {
			t.Fatal(err)
		}
		states[partition] = state
	}
	if states["lane-a"] != DeliveryRetryableNotSent || states["lane-b"] != DeliveryUnknown {
		t.Fatalf("reclaimed states = %+v", states)
	}
	if _, reclaimedAgain, err := st.ReclaimExpired(ctx, time.Now()); err != nil || reclaimedAgain != 0 {
		t.Fatalf("unknown/retry reclaimed again: count=%d err=%v", reclaimedAgain, err)
	}
}

func TestPostgresIntegrationBatchFailureRollsBackAllParts(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	now := time.Now()
	inbox := newPostgresInbox("batch-rollback", "lane-batch")
	if err := st.InsertInbox(ctx, inbox, now); err != nil {
		t.Fatal(err)
	}
	if records, err := st.LeaseInbox(ctx, "batch-relay", now, time.Hour, 1); err != nil || len(records) != 1 {
		t.Fatalf("lease inbox: %+v %v", records, err)
	}
	if _, err := st.pool.Exec(ctx, `
		CREATE FUNCTION fail_second_outbox_part() RETURNS trigger
		LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.part_index = 1 THEN
				RAISE EXCEPTION 'injected second part failure';
			END IF;
			RETURN NEW;
		END
		$body$;
		CREATE TRIGGER fail_second_outbox_part
		BEFORE INSERT ON runtime_outbox
		FOR EACH ROW EXECUTE FUNCTION fail_second_outbox_part()`); err != nil {
		t.Fatal(err)
	}
	plan := newPostgresOutboxPlan("batch-rollback", "lane-batch", 3)
	if err := st.CompleteInboxBatch(ctx, inbox.InboxID, "batch-relay", plan, now); err == nil {
		t.Fatal("batch with injected second-part failure committed")
	}
	var outboxes int
	var status string
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_outbox`).Scan(&outboxes); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT status FROM runtime_inbox WHERE inbox_id=$1`, inbox.InboxID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if outboxes != 0 || status != InboxProcessing {
		t.Fatalf("partial batch leaked: outboxes=%d inbox=%s", outboxes, status)
	}
}

func TestPostgresIntegrationAtomicTurnInboxOutboxCommit(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	turns, err := sessionturn.NewPostgres(st.pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := turns.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	inbox := newPostgresInbox("atomic-bundle", "lane-atomic-bundle")
	markPostgresInboxAtomic(st, inbox)
	if err := st.InsertInbox(ctx, inbox, time.Now()); err != nil {
		t.Fatal(err)
	}
	const owner = "atomic-bundle-owner"
	leased, err := st.LeaseInbox(ctx, owner, time.Now(), time.Hour, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease Inbox: %+v %v", leased, err)
	}
	key := session.Key{AppName: "app-atomic", UserID: "user-atomic", SessionID: "session-atomic"}
	turnID, err := sessionturn.DeriveTurnID(key, inbox.DedupKey)
	if err != nil {
		t.Fatal(err)
	}
	begun, err := turns.Begin(ctx, sessionturn.BeginRequest{Key: key, TurnID: turnID})
	if err != nil {
		t.Fatal(err)
	}
	plan := newPostgresOutboxPlan("atomic-bundle", inbox.PartitionKey, 3)
	replay := []byte(`{"reply":"canonical"}`)
	result, err := turns.CommitWithParticipant(ctx, sessionturn.CommitRequest{
		Handle: begun.Handle,
		Events: []event.Event{{ID: "atomic-event", Author: "agent"}},
		State:  session.StateMap{"answer": []byte("42")},
		Replay: replay,
	}, func(participantCtx context.Context, tx pgx.Tx, canonical []byte) error {
		if string(canonical) != string(replay) {
			return fmt.Errorf("participant canonical replay = %q", canonical)
		}
		return st.CompleteInboxBatchTx(participantCtx, tx, postgresInboxFence(leased[0], owner), plan)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Replayed || result.Version != 1 || string(result.Replay) != string(replay) {
		t.Fatalf("commit result = %+v", result)
	}

	var version, lastSequence, eventCount, outboxCount int
	var turnStatus, inboxStatus, inboxOwner string
	if err := st.pool.QueryRow(ctx, `
		SELECT s.version, s.last_event_sequence,
			(SELECT count(*) FROM session_turn_events AS e
			 WHERE e.app_name=s.app_name AND e.user_id=s.user_id AND e.session_id=s.session_id),
			t.status,
			(SELECT status FROM runtime_inbox WHERE inbox_id=$4),
			(SELECT lease_owner FROM runtime_inbox WHERE inbox_id=$4),
			(SELECT count(*) FROM runtime_outbox)
		FROM session_turn_sessions AS s
		JOIN session_turns AS t USING (app_name, user_id, session_id)
		WHERE s.app_name=$1 AND s.user_id=$2 AND s.session_id=$3 AND t.turn_id=$5`,
		key.AppName, key.UserID, key.SessionID, inbox.InboxID, turnID,
	).Scan(&version, &lastSequence, &eventCount, &turnStatus, &inboxStatus, &inboxOwner, &outboxCount); err != nil {
		t.Fatal(err)
	}
	if version != 1 || lastSequence != 1 || eventCount != 1 || turnStatus != "committed" ||
		inboxStatus != InboxProcessed || inboxOwner != "" || outboxCount != len(plan) {
		t.Fatalf("bundle snapshot: version=%d sequence=%d events=%d turn=%s inbox=%s owner=%q outboxes=%d",
			version, lastSequence, eventCount, turnStatus, inboxStatus, inboxOwner, outboxCount)
	}
	rows, err := st.pool.Query(ctx, `SELECT part_index, payload_bytes FROM runtime_outbox ORDER BY queue_sequence`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var gotIndex int
		var payload []byte
		if err := rows.Scan(&gotIndex, &payload); err != nil {
			t.Fatal(err)
		}
		if gotIndex != index || !bytes.Equal(payload, plan[index].Payload) {
			t.Fatalf("Outbox part %d = index %d payload %s", index, gotIndex, payload)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresIntegrationAtomicFenceRejectsPipelineAndTransactionDatabase(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	other := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	inbox := newPostgresInbox("atomic-identity-fence", "lane-atomic-identity-fence")
	markPostgresInboxAtomic(st, inbox)
	if err := st.InsertInbox(ctx, inbox, time.Now()); err != nil {
		t.Fatal(err)
	}
	const owner = "atomic-identity-owner"
	leased, err := st.LeaseInbox(ctx, owner, time.Now(), time.Hour, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease Inbox: %+v %v", leased, err)
	}
	plan := newPostgresOutboxPlan("atomic-identity-fence", inbox.PartitionKey, 1)

	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	badFence := postgresInboxFence(leased[0], owner)
	badFence.DatabaseIdentity = "postgres-binding-v1:tampered"
	if err := st.CompleteInboxBatchTx(ctx, tx, badFence, plan); !errors.Is(err, ErrInboxIdentityMismatch) {
		_ = tx.Rollback(ctx)
		t.Fatalf("tampered database fence = %v, want ErrInboxIdentityMismatch", err)
	}
	_ = tx.Rollback(ctx)

	otherTx, err := other.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteInboxBatchTx(ctx, otherTx, postgresInboxFence(leased[0], owner), plan); !errors.Is(err, ErrInboxIdentityMismatch) {
		_ = otherTx.Rollback(ctx)
		t.Fatalf("foreign transaction database = %v, want ErrInboxIdentityMismatch", err)
	}
	_ = otherTx.Rollback(ctx)

	var status string
	var outboxes int
	if err := st.pool.QueryRow(ctx, `SELECT status FROM runtime_inbox WHERE inbox_id=$1`, inbox.InboxID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_outbox`).Scan(&outboxes); err != nil {
		t.Fatal(err)
	}
	if status != InboxProcessing || outboxes != 0 {
		t.Fatalf("identity fence changed state: inbox=%s outboxes=%d", status, outboxes)
	}
}

func TestPostgresIntegrationAtomicOutboxFailureRollsBackTurnAndInbox(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	turns, err := sessionturn.NewPostgres(st.pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := turns.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inbox := newPostgresInbox("atomic-rollback", "lane-atomic-rollback")
	markPostgresInboxAtomic(st, inbox)
	if err := st.InsertInbox(ctx, inbox, time.Now()); err != nil {
		t.Fatal(err)
	}
	const owner = "atomic-rollback-owner"
	leased, err := st.LeaseInbox(ctx, owner, time.Now(), time.Hour, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease Inbox: %+v %v", leased, err)
	}
	key := session.Key{AppName: "app-rollback", UserID: "user-rollback", SessionID: "session-rollback"}
	begun, err := turns.Begin(ctx, sessionturn.BeginRequest{Key: key, TurnID: "turn-rollback"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `
		CREATE FUNCTION reject_atomic_second_part() RETURNS trigger
		LANGUAGE plpgsql AS $body$
		BEGIN
			IF NEW.part_index = 1 THEN
				RAISE EXCEPTION 'injected atomic bundle failure';
			END IF;
			RETURN NEW;
		END
		$body$;
		CREATE TRIGGER reject_atomic_second_part
		BEFORE INSERT ON runtime_outbox
		FOR EACH ROW EXECUTE FUNCTION reject_atomic_second_part()`); err != nil {
		t.Fatal(err)
	}
	plan := newPostgresOutboxPlan("atomic-rollback", inbox.PartitionKey, 3)
	_, err = turns.CommitWithParticipant(ctx, sessionturn.CommitRequest{
		Handle: begun.Handle,
		Events: []event.Event{{ID: "must-rollback", Author: "agent"}},
		State:  session.StateMap{"must-not": []byte("persist")},
		Replay: []byte("must-not-replay"),
	}, func(participantCtx context.Context, tx pgx.Tx, _ []byte) error {
		return st.CompleteInboxBatchTx(participantCtx, tx, postgresInboxFence(leased[0], owner), plan)
	})
	if err == nil {
		t.Fatal("atomic bundle with injected Outbox failure committed")
	}

	var version, sequence, eventCount, outboxCount int
	var stateText, turnStatus, inboxStatus, inboxOwner string
	if err := st.pool.QueryRow(ctx, `
		SELECT s.version, s.last_event_sequence, s.state::text,
			(SELECT count(*) FROM session_turn_events), t.status,
			(SELECT status FROM runtime_inbox WHERE inbox_id=$4),
			(SELECT lease_owner FROM runtime_inbox WHERE inbox_id=$4),
			(SELECT count(*) FROM runtime_outbox)
		FROM session_turn_sessions AS s
		JOIN session_turns AS t USING (app_name, user_id, session_id)
		WHERE s.app_name=$1 AND s.user_id=$2 AND s.session_id=$3 AND t.turn_id='turn-rollback'`,
		key.AppName, key.UserID, key.SessionID, inbox.InboxID,
	).Scan(&version, &sequence, &stateText, &eventCount, &turnStatus, &inboxStatus, &inboxOwner, &outboxCount); err != nil {
		t.Fatal(err)
	}
	if version != 0 || sequence != 0 || stateText != `{}` || eventCount != 0 ||
		turnStatus != "active" || inboxStatus != InboxProcessing || inboxOwner != owner || outboxCount != 0 {
		t.Fatalf("partial bundle escaped rollback: version=%d sequence=%d state=%s events=%d turn=%s inbox=%s owner=%q outboxes=%d",
			version, sequence, stateText, eventCount, turnStatus, inboxStatus, inboxOwner, outboxCount)
	}
}

func TestPostgresIntegrationAtomicParticipantUsesCommittedCanonicalReplay(t *testing.T) {
	st := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	turns, err := sessionturn.NewPostgres(st.pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := turns.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	inbox := newPostgresInbox("atomic-legacy-replay", "lane-atomic-legacy")
	markPostgresInboxAtomic(st, inbox)
	if err := st.InsertInbox(ctx, inbox, time.Now()); err != nil {
		t.Fatal(err)
	}
	const owner = "atomic-legacy-owner"
	leased, err := st.LeaseInbox(ctx, owner, time.Now(), time.Hour, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease Inbox: %+v %v", leased, err)
	}
	key := session.Key{AppName: "app-legacy", UserID: "user-legacy", SessionID: "session-legacy"}
	begun, err := turns.Begin(ctx, sessionturn.BeginRequest{Key: key, TurnID: "turn-legacy"})
	if err != nil {
		t.Fatal(err)
	}
	canonical := []byte("canonical-A")
	if _, err := turns.Commit(ctx, sessionturn.CommitRequest{
		Handle: begun.Handle,
		Events: []event.Event{{ID: "winner-event", Author: "agent"}},
		State:  session.StateMap{"winner": []byte("A")},
		Replay: canonical,
	}); err != nil {
		t.Fatal(err)
	}
	var participantReplay []byte
	result, err := turns.CommitWithParticipant(ctx, sessionturn.CommitRequest{
		Handle: begun.Handle,
		Events: []event.Event{{ID: "loser-event", Author: "agent"}},
		State:  session.StateMap{"loser": []byte("B")},
		Replay: []byte("loser-B"),
	}, func(participantCtx context.Context, tx pgx.Tx, replay []byte) error {
		participantReplay = append([]byte(nil), replay...)
		plan := newPostgresOutboxPlan(string(replay), inbox.PartitionKey, 1)
		plan[0].Payload = []byte(fmt.Sprintf(`{"target":"chat","text":%q}`, replay))
		return st.CompleteInboxBatchTx(participantCtx, tx, postgresInboxFence(leased[0], owner), plan)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Replayed || string(result.Replay) != string(canonical) || string(participantReplay) != string(canonical) {
		t.Fatalf("duplicate result=%+v participant replay=%q", result, participantReplay)
	}
	var version, eventCount, outboxCount int
	var inboxStatus string
	var payload []byte
	if err := st.pool.QueryRow(ctx, `
		SELECT s.version,
			(SELECT count(*) FROM session_turn_events),
			(SELECT status FROM runtime_inbox WHERE inbox_id=$4),
			(SELECT count(*) FROM runtime_outbox),
			(SELECT payload_bytes FROM runtime_outbox LIMIT 1)
		FROM session_turn_sessions AS s
		WHERE s.app_name=$1 AND s.user_id=$2 AND s.session_id=$3`,
		key.AppName, key.UserID, key.SessionID, inbox.InboxID,
	).Scan(&version, &eventCount, &inboxStatus, &outboxCount, &payload); err != nil {
		t.Fatal(err)
	}
	if version != 1 || eventCount != 1 || inboxStatus != InboxProcessed || outboxCount != 1 ||
		!strings.Contains(string(payload), "canonical-A") {
		t.Fatalf("canonical replay bundle: version=%d events=%d inbox=%s outboxes=%d payload=%s",
			version, eventCount, inboxStatus, outboxCount, payload)
	}
}
