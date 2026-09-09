package storage_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// uniqueUUID returns a valid, collision-free uuid for fixture rows.
func uniqueUUID(prefix string) string {
	return prefix[:8] + "-0000-0000-0000-" + fmt.Sprintf("%012x", time.Now().UnixNano()%(1<<48))
}

// A non-positive observation window falls back to the 24h default.
func TestNewMigratorDefaults(t *testing.T) {
	_, pool := pgSessionService(t)
	m := storage.NewMigrator(pool, nil, nil, 0)
	if m.ObserveWindow != 24*time.Hour {
		t.Fatalf("default observe window must be 24h, got %s", m.ObserveWindow)
	}
	if m.Interval != 5*time.Second || m.BatchSize != 50 {
		t.Fatalf("unexpected cadence defaults: %s %d", m.Interval, m.BatchSize)
	}
}

// A postgres→redis migration exercises the mirror image of the redis→postgres
// case: PG enumeration, the redis copy path (create + append the missing
// tail), the tenant session count over SQL, and the read switch with the
// invalidation broadcast. BatchSize=1 forces the multi-batch backfill. A
// dedicated tenant keeps the enumeration scoped to this test's sessions.
func TestMigratorPostgresToRedis(t *testing.T) {
	pgSvc, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	ctx := context.Background()

	// Dedicated tenant + app: enumeratePG pages the whole tenant, so sharing
	// the fixture tenant with other tests would make batch boundaries random.
	tenantID := uniqueUUID("fffffff0")
	appID := uniqueUUID("fffffffe")
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-pg2redis', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'migrate-pg2redis', 'llm', '{}', 1, 'published')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	key1 := session.Key{AppName: appID, UserID: "u-pg", SessionID: "dm:mock:migr-1-" + t.Name()}
	key2 := session.Key{AppName: appID, UserID: "u-pg", SessionID: "dm:mock:migr-2-" + t.Name()}
	for _, key := range []sessionKeyList{{k: key1, evts: 2}, {k: key2, evts: 1}} {
		cleanupSession(t, pool, key.k)
		t.Cleanup(func() { cleanupSession(t, pool, key.k) })
		t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key.k) })

		sess, err := pgSvc.CreateSession(ctx, key.k, session.StateMap{})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < key.evts; i++ {
			if err := pgSvc.AppendEvent(ctx, sess,
				textEvent("pgm-"+key.k.SessionID[:12]+string(rune('a'+i)), "user", "迁移消息")); err != nil {
				t.Fatal(err)
			}
		}
	}

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'postgres', 'redis', 'dual_write') RETURNING id`,
		tenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, tenantID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"postgres": pgSvc, "redis": redisSvc}, 50*time.Millisecond)
	m.BatchSize = 1

	// dual_write → backfilling.
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// Two backfill ticks copy one session each (batch size 1), persisting
	// progress between them.
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// The third backfill tick finds nothing left of the two sessions: the
	// consistency check passes and the read switch flips the tenant to redis.
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var phase string
	if err := pool.QueryRow(ctx,
		`SELECT phase FROM storage_migration WHERE id = $1`, migID).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if phase != tenant.PhaseObserving {
		t.Fatalf("want phase observing after the read switch, got %s", phase)
	}

	var storageCfg string
	if err := pool.QueryRow(ctx,
		`SELECT storage_config::text FROM tenant WHERE id = $1`, tenantID).Scan(&storageCfg); err != nil {
		t.Fatal(err)
	}
	if storageCfg != `{"session": {"type": "redis"}}` {
		t.Fatalf("tenant storage_config not switched: %s", storageCfg)
	}

	// Observation window is 50ms; the next tick finishes the migration.
	time.Sleep(80 * time.Millisecond)
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT phase FROM storage_migration WHERE id = $1`, migID).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if phase != tenant.PhaseDone {
		t.Fatalf("want phase done, got %s", phase)
	}

	// Both sessions (with their events) live on the redis target now.
	for _, want := range []sessionKeyList{{k: key1, evts: 2}, {k: key2, evts: 1}} {
		got, err := redisSvc.GetSession(ctx, want.k)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || len(got.Events) != want.evts {
			t.Fatalf("redis target session %s must carry %d events, got %+v",
				want.k.SessionID, want.evts, got)
		}
	}
}

type sessionKeyList struct {
	k    session.Key
	evts int
}

// Two apps of one tenant may share a session_key — uk_session_app_key is
// (app_id, session_key), not session_key alone. A cursor paginating on
// session_key only ties those rows together: with BatchSize=1 the first batch
// copies one of them and stores the shared key as the cursor, the next page's
// "session_key > cursor" then matches neither, and the second app's session is
// never copied. The consistency check enumerates the same way so it never sees
// the gap, the read switch flips, and that app's history silently disappears.
func TestMigratorPagesTwoAppsSharingOneSessionKey(t *testing.T) {
	pgSvc, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	ctx := context.Background()

	// Distinct prefixes keep the three ids apart even if the nanosecond suffix
	// lands on the same value twice.
	tenantID := uniqueUUID("fffffffa")
	appA := uniqueUUID("fffffffb")
	appB := uniqueUUID("fffffffc")
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-shared-key', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	for i, appID := range []string{appA, appB} {
		// uk_agent_app_version is (tenant_id, name, version): each app needs
		// its own name.
		if _, err := pool.Exec(ctx,
			`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
			 VALUES ($1, $2, $3, 'llm', '{}', 1, 'published')`,
			appID, tenantID, fmt.Sprintf("migrate-shared-key-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = ANY($1)`, []string{appA, appB})
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	// One session_key, two apps — the case the single-dimension cursor ties.
	shared := "dm:mock:shared-" + t.Name()
	keyA := session.Key{AppName: appA, UserID: "u-shared", SessionID: shared}
	keyB := session.Key{AppName: appB, UserID: "u-shared", SessionID: shared}
	for _, want := range []sessionKeyList{{k: keyA, evts: 1}, {k: keyB, evts: 1}} {
		cleanupSession(t, pool, want.k)
		t.Cleanup(func() { cleanupSession(t, pool, want.k) })
		t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), want.k) })

		sess, err := pgSvc.CreateSession(ctx, want.k, session.StateMap{})
		if err != nil {
			t.Fatal(err)
		}
		if err := pgSvc.AppendEvent(ctx, sess,
			textEvent("shared-"+want.k.AppName[:8], "user", "两个应用同一个 key")); err != nil {
			t.Fatal(err)
		}
	}

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'postgres', 'redis', 'dual_write') RETURNING id`,
		tenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, tenantID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"postgres": pgSvc, "redis": redisSvc}, 50*time.Millisecond)
	m.BatchSize = 1

	// One session per tick, so the page boundary falls between the two apps.
	var phase string
	for i := 0; i < 8; i++ {
		if err := m.Tick(ctx); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT phase FROM storage_migration WHERE id = $1`, migID).Scan(&phase); err != nil {
			t.Fatal(err)
		}
		if phase != tenant.PhaseDualWrite && phase != tenant.PhaseBackfilling {
			break
		}
	}
	if phase != tenant.PhaseObserving {
		t.Fatalf("want phase observing after the read switch, got %s", phase)
	}

	// Both apps' sessions must have crossed over, not just the one the cursor
	// happened to land on.
	for _, want := range []sessionKeyList{{k: keyA, evts: 1}, {k: keyB, evts: 1}} {
		got, err := redisSvc.GetSession(ctx, want.k)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || len(got.Events) != want.evts {
			t.Fatalf("app %s session %s was skipped by the cursor: want %d events, got %+v",
				want.k.AppName, want.k.SessionID, want.evts, got)
		}
	}
}

// The redis enumerator is separate code from the SQL row comparison — SCAN,
// sort, then a cursor filter — so it needs the same proof: two apps sharing one
// session_key must both cross over when BatchSize=1 puts the page boundary
// between them.
func TestMigratorRedisPagesTwoAppsSharingOneSessionKey(t *testing.T) {
	_, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	pgSvc := storage.NewPGSessionService(pool)
	ctx := context.Background()

	tenantID := uniqueUUID("fffffff9")
	appA := uniqueUUID("fffffff8")
	appB := uniqueUUID("fffffff7")
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-shared-key-redis', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	for i, appID := range []string{appA, appB} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
			 VALUES ($1, $2, $3, 'llm', '{}', 1, 'published')`,
			appID, tenantID, fmt.Sprintf("migrate-shared-key-redis-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = ANY($1)`, []string{appA, appB})
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	shared := "dm:mock:shared-redis-" + t.Name()
	keyA := session.Key{AppName: appA, UserID: "u-shared", SessionID: shared}
	keyB := session.Key{AppName: appB, UserID: "u-shared", SessionID: shared}
	for _, key := range []session.Key{keyA, keyB} {
		cleanupSession(t, pool, key)
		t.Cleanup(func() { cleanupSession(t, pool, key) })
		t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key) })

		sess, err := redisSvc.CreateSession(ctx, key, session.StateMap{})
		if err != nil {
			t.Fatal(err)
		}
		if err := redisSvc.AppendEvent(ctx, sess,
			textEvent("shared-redis-"+key.AppName[:8], "user", "两个应用同一个 key")); err != nil {
			t.Fatal(err)
		}
	}

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'redis', 'postgres', 'dual_write') RETURNING id`,
		tenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, tenantID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"redis": redisSvc, "postgres": pgSvc}, 50*time.Millisecond)
	m.BatchSize = 1

	var phase string
	for i := 0; i < 8; i++ {
		if err := m.Tick(ctx); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT phase FROM storage_migration WHERE id = $1`, migID).Scan(&phase); err != nil {
			t.Fatal(err)
		}
		if phase != tenant.PhaseDualWrite && phase != tenant.PhaseBackfilling {
			break
		}
	}
	if phase != tenant.PhaseObserving {
		t.Fatalf("want phase observing after the read switch, got %s", phase)
	}

	for _, key := range []session.Key{keyA, keyB} {
		got, err := pgSvc.GetSession(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || len(got.Events) != 1 {
			t.Fatalf("app %s session %s was skipped by the redis cursor: want 1 event, got %+v",
				key.AppName, key.SessionID, got)
		}
	}
}

// GetSession is not the journal: it replays only the events after the summary
// cursor, and only those the archive sweep has left in the hot table. A
// postgres source must be copied through the full journal instead; a copy
// through GetSession loses the summarized and archived history on the new
// backend with no error anywhere.
func TestMigratorCopiesTheFullJournalPastSummaryAndArchive(t *testing.T) {
	_, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	ctx := context.Background()

	tenantID := uniqueUUID("fffffff6")
	appID := uniqueUUID("fffffff5")
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-journal', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'migrate-journal', 'llm', '{}', 1, 'published')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	// The summarizer is what truncates GetSession, so the migrator gets the
	// service a runner would use rather than a bare one.
	pgSvc := storage.NewPGSessionService(pool, storage.WithSummarizer(&fakeSummarizer{}))
	t.Cleanup(func() { _ = pgSvc.Close() })

	key := session.Key{AppName: appID, UserID: "u-journal", SessionID: "dm:mock:journal-" + t.Name()}
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })
	t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key) })
	// Registered last so it runs first: archived rows outlive their session row.
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM session_event_archive WHERE session_id IN
			   (SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`,
			key.AppName, key.SessionID)
	})

	sess, err := pgSvc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"jr-1", "jr-2", "jr-3"} {
		if err := pgSvc.AppendEvent(ctx, sess, textEvent(id, "user", "被摘要的旧消息")); err != nil {
			t.Fatal(err)
		}
	}
	if err := pgSvc.CreateSessionSummary(ctx, sess, "assistant", true); err != nil {
		t.Fatal(err)
	}
	if err := pgSvc.AppendEvent(ctx, sess, textEvent("jr-4", "user", "摘要之后的新消息")); err != nil {
		t.Fatal(err)
	}

	var sessID string
	if err := pool.QueryRow(ctx,
		`SELECT id FROM session WHERE app_id=$1 AND session_key=$2`,
		key.AppName, key.SessionID).Scan(&sessID); err != nil {
		t.Fatal(err)
	}
	// Age the covered events out of the hot table; jr-4 stays.
	if _, err := pool.Exec(ctx,
		`UPDATE session_event SET created_at = $1 WHERE session_id = $2 AND event_seq <= 3`,
		time.Now().Add(-40*24*time.Hour), sessID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := storage.NewArchiver(pool, 30*24*time.Hour, time.Hour).ArchiveOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var hot, archived int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM session_event WHERE session_id=$1`, sessID).Scan(&hot); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM session_event_archive WHERE session_id=$1`, sessID).Scan(&archived); err != nil {
		t.Fatal(err)
	}
	if hot != 1 || archived != 3 {
		t.Fatalf("want 1 hot + 3 archived events, got %d/%d", hot, archived)
	}

	// The truncated view the copy must not mistake for the journal.
	view, err := pgSvc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Events) != 1 || view.Events[0].ID != "jr-4" {
		t.Fatalf("precondition: GetSession must expose only the uncovered hot tail, got %+v", view.Events)
	}

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'postgres', 'redis', 'dual_write') RETURNING id`,
		tenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, tenantID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"postgres": pgSvc, "redis": redisSvc}, 50*time.Millisecond)
	m.BatchSize = 1

	var phase, failureText string
	for i := 0; i < 8; i++ {
		if err := m.Tick(ctx); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT phase, COALESCE(error, '') FROM storage_migration WHERE id = $1`,
			migID).Scan(&phase, &failureText); err != nil {
			t.Fatal(err)
		}
		if phase != tenant.PhaseDualWrite && phase != tenant.PhaseBackfilling {
			break
		}
	}
	// Counting the truncated view against a complete copy is a mismatch, so a
	// half-fix parks here instead of reaching the read switch.
	if phase != tenant.PhaseObserving {
		t.Fatalf("want phase observing after the read switch, got %s (%s)", phase, failureText)
	}

	got, err := redisSvc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Events) != 4 {
		t.Fatalf("the whole journal must cross over, got %+v", got)
	}
	for i, want := range []string{"jr-1", "jr-2", "jr-3", "jr-4"} {
		if got.Events[i].ID != want {
			t.Fatalf("event %d: want %s, got %s", i, want, got.Events[i].ID)
		}
	}
}

// A migration whose target backend is not wired parks the row in "failed"
// with the reason, and Run exits cleanly on cancellation.
func TestMigratorRunMarksBadBackendPairFailed(t *testing.T) {
	pgSvc, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	ctx := context.Background()

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'postgres', 'backend-missing', 'backfilling') RETURNING id`,
		testTenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"postgres": pgSvc}, time.Hour)
	m.Interval = 20 * time.Millisecond

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		m.Run(runCtx)
		close(done)
	}()

	// The tick fails the migration; Run keeps ticking until cancel.
	deadline := time.Now().Add(5 * time.Second)
	var phase, failure string
	for {
		if err := pool.QueryRow(ctx,
			`SELECT phase, COALESCE(error, '') FROM storage_migration WHERE id = $1`,
			migID).Scan(&phase, &failure); err != nil {
			t.Fatal(err)
		}
		if phase == tenant.PhaseFailed {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("migration never failed, last phase %s", phase)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
	if !strings.Contains(failure, "backend pair") {
		t.Fatalf("the failure reason must name the backend pair, got %q", failure)
	}
}

// roleTextEvent is textEvent with the message role under the test's control —
// ApplyEventFiltering keys on it, so a journal that opens without a user
// message needs genuinely non-user events to prove anything.
func roleTextEvent(id string, role model.Role, content string) *event.Event {
	return &event.Event{
		ID:     id,
		Author: string(role),
		Response: &model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: role, Content: content},
		}}},
	}
}

// Dual write is live the whole time a migration runs, and the fanout writes
// the SECONDARY first — so by the time the backfill reaches a session, the
// target already holds the source's newest events at seqs 1..k. A copy that
// upserts the source's i-th event at seq i+1 with ON CONFLICT DO NOTHING
// would drop the source's first k events (their seqs are taken) and leave the
// tail duplicated at both ends. The copy must reconcile by event ID and
// reorder the target into the source's order instead.
func TestMigratorReconcilesDualWriteTailByEventID(t *testing.T) {
	_, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	pgSvc := storage.NewPGSessionService(pool)
	ctx := context.Background()

	tenantID := uniqueUUID("fffffff4")
	appID := uniqueUUID("fffffff3")
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-dual-write', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'migrate-dual-write', 'llm', '{}', 1, 'published')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	key := session.Key{AppName: appID, UserID: "u-tail", SessionID: "dm:mock:tail-" + t.Name()}
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })
	t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key) })

	// History the session already had when the migration row was created.
	rSess, err := redisSvc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"tail-e1", "tail-e2"} {
		if err := redisSvc.AppendEvent(ctx, rSess, textEvent(id, "user", "迁移前的历史")); err != nil {
			t.Fatal(err)
		}
	}

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'redis', 'postgres', 'dual_write') RETURNING id`,
		tenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, tenantID)
	})

	// The live tail during dual write: the fanout writes the secondary (the PG
	// target) FIRST with the same event object, so the target holds the
	// source's newest events before the backfill ever reaches this session.
	pSess, err := pgSvc.CreateSession(ctx, key, rSess.State)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []*event.Event{
		textEvent("tail-e3", "user", "迁移中的新消息"),
		textEvent("tail-e4", "user", "迁移中的更新消息"),
	} {
		if err := pgSvc.AppendEvent(ctx, pSess, e); err != nil {
			t.Fatal(err)
		}
		if err := redisSvc.AppendEvent(ctx, rSess, e); err != nil {
			t.Fatal(err)
		}
	}

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"redis": redisSvc, "postgres": pgSvc}, 50*time.Millisecond)

	var phase, failureText string
	for i := 0; i < 8; i++ {
		if err := m.Tick(ctx); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT phase, COALESCE(error, '') FROM storage_migration WHERE id = $1`,
			migID).Scan(&phase, &failureText); err != nil {
			t.Fatal(err)
		}
		if phase != tenant.PhaseDualWrite && phase != tenant.PhaseBackfilling {
			break
		}
	}
	if phase != tenant.PhaseObserving {
		t.Fatalf("want phase observing after the read switch, got %s (%s)", phase, failureText)
	}

	// The target journal must be the source journal, event by event, in seq
	// order: prefix restored, tail de-duplicated, nothing parked in between.
	journal, err := pgSvc.FullJournal(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tail-e1", "tail-e2", "tail-e3", "tail-e4"}
	if len(journal) != len(want) {
		t.Fatalf("target journal must hold %d events, got %d: %+v", len(want), len(journal), journal)
	}
	for i, id := range want {
		if journal[i].ID != id {
			t.Fatalf("target event %d must be %s, got %s (journal %+v)", i, id, journal[i].ID, journal)
		}
	}
}

// The redis source journal must be read past GetSession, not through it:
// every read applies ApplyEventFiltering, which keeps only the newest
// sessionEventLimit events (1000 in production) and then anchors the head to
// the first user message. Copying that view leaves the head behind while the
// copy still looks complete to anything that counts it — the mirror image of
// the postgres summary/archive truncation.
func TestMigratorCopiesRedisJournalPastGetSessionTruncation(t *testing.T) {
	_, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	pgSvc := storage.NewPGSessionService(pool)
	ctx := context.Background()

	tenantID := uniqueUUID("ffffffef")
	appID := uniqueUUID("ffffffee")
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-redis-trunc', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'migrate-redis-trunc', 'llm', '{}', 1, 'published')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	key := session.Key{AppName: appID, UserID: "u-trunc", SessionID: "dm:mock:trunc-" + t.Name()}
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })
	t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key) })

	rSess, err := redisSvc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	// A journal that opens without a user message — channel adapters can
	// write welcome/system events before the first user turn. Timestamps are
	// explicit: the redis zset is scored by event timestamp, and zero-value
	// timestamps would reorder the journal by event ID.
	base := time.Now()
	for i, e := range []*event.Event{
		roleTextEvent("tr-a1", model.RoleAssistant, "欢迎语，任何用户消息之前"),
		roleTextEvent("tr-u1", model.RoleUser, "第一条用户消息"),
		roleTextEvent("tr-a2", model.RoleAssistant, "回复"),
	} {
		e.Timestamp = base.Add(time.Duration(i) * time.Second)
		if err := redisSvc.AppendEvent(ctx, rSess, e); err != nil {
			t.Fatal(err)
		}
	}

	// Premise: GetSession really does drop the head — the stored journal holds
	// three events, the read returns two.
	view, err := redisSvc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Events) != 2 || view.Events[0].ID != "tr-u1" {
		t.Fatalf("precondition: GetSession must expose only [tr-u1, tr-a2], got %+v", view.Events)
	}

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'redis', 'postgres', 'dual_write') RETURNING id`,
		tenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, tenantID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"redis": redisSvc, "postgres": pgSvc}, 50*time.Millisecond)

	var phase, failureText string
	for i := 0; i < 8; i++ {
		if err := m.Tick(ctx); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT phase, COALESCE(error, '') FROM storage_migration WHERE id = $1`,
			migID).Scan(&phase, &failureText); err != nil {
			t.Fatal(err)
		}
		if phase != tenant.PhaseDualWrite && phase != tenant.PhaseBackfilling {
			break
		}
	}
	if phase != tenant.PhaseObserving {
		t.Fatalf("want phase observing after the read switch, got %s (%s)", phase, failureText)
	}

	journal, err := pgSvc.FullJournal(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tr-a1", "tr-u1", "tr-a2"}
	if len(journal) != len(want) {
		t.Fatalf("the whole journal must cross over, got %+v", journal)
	}
	for i, id := range want {
		if journal[i].ID != id {
			t.Fatalf("event %d: want %s, got %s", i, id, journal[i].ID)
		}
	}
}

// An event that reached the target but never the source (the fanout writes the
// secondary first; if the primary write then fails, the shadow stays) must
// park the migration in "failed" with the extra named, not flip reads onto a
// journal the source cannot reproduce.
func TestMigratorFailsClosedOnTargetOnlyEvent(t *testing.T) {
	_, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	pgSvc := storage.NewPGSessionService(pool)
	ctx := context.Background()

	tenantID := uniqueUUID("ffffffed")
	appID := uniqueUUID("ffffffec")
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-extra-event', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'migrate-extra-event', 'llm', '{}', 1, 'published')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	key := session.Key{AppName: appID, UserID: "u-extra", SessionID: "dm:mock:extra-" + t.Name()}
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })
	t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key) })

	rSess, err := redisSvc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	pSess, err := pgSvc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	// e1 made it to both sides; x2 only to the target.
	if err := redisSvc.AppendEvent(ctx, rSess, textEvent("extra-e1", "user", "两边都有")); err != nil {
		t.Fatal(err)
	}
	if err := pgSvc.AppendEvent(ctx, pSess, textEvent("extra-e1", "user", "两边都有")); err != nil {
		t.Fatal(err)
	}
	if err := pgSvc.AppendEvent(ctx, pSess, textEvent("extra-x2", "user", "只在目标端")); err != nil {
		t.Fatal(err)
	}

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'redis', 'postgres', 'dual_write') RETURNING id`,
		tenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, tenantID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"redis": redisSvc, "postgres": pgSvc}, 50*time.Millisecond)

	var phase, failureText string
	for i := 0; i < 8; i++ {
		if err := m.Tick(ctx); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT phase, COALESCE(error, '') FROM storage_migration WHERE id = $1`,
			migID).Scan(&phase, &failureText); err != nil {
			t.Fatal(err)
		}
		if phase != tenant.PhaseDualWrite && phase != tenant.PhaseBackfilling {
			break
		}
	}
	if phase != tenant.PhaseFailed {
		t.Fatalf("an event only the target holds must fail the migration, got phase %s", phase)
	}
	if !strings.Contains(failureText, "extra-x2") {
		t.Fatalf("the failure must name the extra event, got %q", failureText)
	}

	// The read switch must not have fired.
	var storageCfg string
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(storage_config::text, '') FROM tenant WHERE id = $1`, tenantID).Scan(&storageCfg); err != nil {
		t.Fatal(err)
	}
	if storageCfg != "" {
		t.Fatalf("reads must stay on the old backend after a failed check, got %s", storageCfg)
	}
}
