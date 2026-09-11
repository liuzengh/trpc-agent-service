package postgresadapter_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

func setup(t *testing.T) (*postgresadapter.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set GATEWAY_TEST_DATABASE_URL for real PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	random := make([]byte, 8)
	if _, err = rand.Read(random); err != nil {
		t.Fatal(err)
	}
	schema := "gateway_routing_" + hex.EncodeToString(random)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = quoted
	config.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return postgresadapter.NewStore(pool), pool
}
func source(last uint64) domain.ReplaySource {
	first := uint64(1)
	if last == 0 {
		first = 0
	}
	return domain.ReplaySource{StreamName: "CONTROL_ROUTES", StreamID: "2026-09-05T00:00:00Z", FirstSequence: first, LastSequence: last, MessageCount: last}
}
func position(seq uint64) domain.StreamPosition {
	s := source(0)
	return domain.StreamPosition{StreamName: s.StreamName, StreamID: s.StreamID, Sequence: seq}
}
func event(id string, generation int64) domain.RouteEvent {
	return domain.RouteEvent{EventID: id, SchemaVersion: 1, Enabled: true, Route: domain.RouteSnapshot{Provider: "telegram", AccountID: "account-1", TenantID: "tenant-1", BindingID: "binding-1", Generation: generation, DeploymentRevisionID: "revision-1", ManifestRef: "manifest/revision-1", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}}
}
func mustBegin(t *testing.T, s *postgresadapter.Store, last uint64) {
	t.Helper()
	if err := s.BeginReplay(context.Background(), source(last)); err != nil {
		t.Fatal(err)
	}
}
func mustApply(t *testing.T, s *postgresadapter.Store, seq uint64, e domain.RouteEvent) {
	t.Helper()
	if err := s.ApplyFromStream(context.Background(), position(seq), e); err != nil {
		t.Fatal(err)
	}
}
func health(t *testing.T, s *postgresadapter.Store) domain.ProjectionHealth {
	t.Helper()
	h, err := s.QueryProjectionHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func count(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func insertProjection(t *testing.T, pool *pgxpool.Pool, e domain.RouteEvent) {
	t.Helper()
	data, _ := json.Marshal(e.Route)
	if _, err := pool.Exec(context.Background(), `INSERT INTO gateway_route_projections(provider,account_id,generation,enabled,snapshot,digest) VALUES($1,$2,$3,$4,$5,$6)`, e.Route.Provider, e.Route.AccountID, e.Route.Generation, e.Enabled, data, e.ProjectionDigest()); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationExistingRowsDoNotBypassInitialization(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	insertProjection(t, pool, event("old", 1))
	if h := health(t, store); h.Initialized {
		t.Fatal("arbitrary old row initialized routing")
	}
	if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrNotInitialized) {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = store.VerifyGeneration(ctx, tx, "telegram", "account-1", 1); !errors.Is(err, domain.ErrNotInitialized) {
		t.Fatal(err)
	}
	if err = store.Apply(ctx, event("unsequenced", 2)); !errors.Is(err, domain.ErrStreamPositionRequired) {
		t.Fatal(err)
	}
}

func TestIntegrationOutOfOrderReplayRequiresContiguousWatermark(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 3)
	third := event("evt-3", 3)
	third.Route.BindingID = "binding-3"
	mustApply(t, store, 3, third)
	h := health(t, store)
	if h.Initialized || h.ContiguousSequence != 0 || h.TargetSequence != 3 {
		t.Fatalf("premature initialized: %#v", h)
	}
	if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrNotInitialized) {
		t.Fatal(err)
	}
	mustApply(t, store, 1, event("evt-1", 1))
	if h = health(t, store); h.ContiguousSequence != 1 || h.Initialized {
		t.Fatalf("hole skipped: %#v", h)
	}
	mustApply(t, store, 2, event("evt-2", 2))
	h = health(t, store)
	if !h.Initialized || h.ContiguousSequence != 3 {
		t.Fatalf("complete replay not initialized: %#v", h)
	}
	route, err := store.Resolve(ctx, "telegram", "account-1")
	if err != nil || route != third.Route {
		t.Fatalf("route=%#v %v", route, err)
	}
	mustApply(t, store, 2, event("evt-2", 2))
	if n := count(t, pool, "gateway_route_stream_receipts"); n != 3 {
		t.Fatalf("duplicate receipt: %d", n)
	}
	// A fresh replica captures a newer startup target; old rows are insufficient.
	mustBegin(t, store, 4)
	if health(t, store).Initialized {
		t.Fatal("restart skipped newly captured target")
	}
	mustApply(t, store, 4, event("evt-4", 4))
	if !health(t, store).Initialized {
		t.Fatal("restart replay failed")
	}
}

func TestIntegrationEmptyStreamAndStableReplayIdentity(t *testing.T) {
	store, pool := setup(t)
	mustBegin(t, store, 0)
	if h := health(t, store); !h.Initialized || h.TargetSequence != 0 {
		t.Fatalf("empty stream health=%#v", h)
	}
	e := event("evt-1", 1)
	mustApply(t, store, 1, e)
	mustApply(t, store, 2, e)
	if n := count(t, pool, "gateway_route_receipts"); n != 1 {
		t.Fatalf("event identity differs from stream position: %d", n)
	}
	if h := health(t, store); h.ContiguousSequence != 2 || !h.Initialized {
		t.Fatalf("health=%#v", h)
	}
}

func TestIntegrationConflictQuarantineIsDurableAndBlocksAdmission(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 0)
	mustApply(t, store, 1, event("evt-1", 1))
	conflicting := event("evt-conflict", 1)
	conflicting.Route.BindingID = "different-binding"
	if err := store.ApplyFromStream(ctx, position(2), conflicting); !errors.Is(err, domain.ErrProjectionBlocked) {
		t.Fatal(err)
	}
	h := health(t, store)
	if h.Initialized || h.BlockedReason != "route_generation_conflict" || h.ContiguousSequence != 1 {
		t.Fatalf("health=%#v", h)
	}
	if n := count(t, pool, "gateway_route_receipts"); n != 1 {
		t.Fatalf("rejected event receipt committed: %d", n)
	}
	if n := count(t, pool, "gateway_route_quarantines"); n != 1 {
		t.Fatalf("quarantine missing: %d", n)
	}
	if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrProjectionBlocked) {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = store.VerifyGeneration(ctx, tx, "telegram", "account-1", 1); !errors.Is(err, domain.ErrProjectionBlocked) {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.BeginReplay(ctx, source(2)); !errors.Is(err, domain.ErrProjectionBlocked) {
		t.Fatal("restart erased quarantine", err)
	}
}

func TestIntegrationSequenceConflictAndInvalidSchemaQuarantine(t *testing.T) {
	for _, kind := range []string{"sequence", "schema"} {
		t.Run(kind, func(t *testing.T) {
			store, pool := setup(t)
			ctx := context.Background()
			mustBegin(t, store, 0)
			mustApply(t, store, 1, event("evt-1", 1))
			var err error
			if kind == "sequence" {
				err = store.ApplyFromStream(ctx, position(1), event("different-event", 2))
			} else {
				bad := event("bad", 2)
				bad.SchemaVersion = 2
				err = store.ApplyFromStream(ctx, position(2), bad)
			}
			if !errors.Is(err, domain.ErrProjectionBlocked) {
				t.Fatal(err)
			}
			if count(t, pool, "gateway_route_quarantines") != 1 || health(t, store).Initialized {
				t.Fatal("permanent fault not durably isolated")
			}
		})
	}
}

func TestIntegrationSourceHistoryLossAndRecreationFailClosed(t *testing.T) {
	for _, kind := range []string{"trimmed", "hole", "recreated"} {
		t.Run(kind, func(t *testing.T) {
			store, _ := setup(t)
			ctx := context.Background()
			s := source(3)
			if kind == "trimmed" {
				s.FirstSequence = 2
				s.MessageCount = 2
			} else if kind == "hole" {
				s.MessageCount = 2
			} else {
				mustBegin(t, store, 0)
				s = source(0)
				s.StreamID = "2026-09-05T00:00:01Z"
			}
			if err := store.BeginReplay(ctx, s); !errors.Is(err, domain.ErrProjectionBlocked) {
				t.Fatal(err)
			}
			if h := health(t, store); h.Initialized || h.BlockedReason == "" {
				t.Fatalf("health=%#v", h)
			}
		})
	}
}

func TestIntegrationConcurrentReplayAdvancesCompletePrefix(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	const total = 12
	mustBegin(t, store, total)
	var group sync.WaitGroup
	results := make(chan error, total)
	for i := 1; i <= total; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			results <- store.ApplyFromStream(ctx, position(uint64(i)), event(fmt.Sprintf("evt-%d", i), int64(i)))
		}(i)
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	h := health(t, store)
	if !h.Initialized || h.ContiguousSequence != total {
		t.Fatalf("health=%#v", h)
	}
	if count(t, pool, "gateway_route_stream_receipts") != total || count(t, pool, "gateway_route_receipts") != total {
		t.Fatal("lost replay receipt")
	}
	route, err := store.Resolve(ctx, "telegram", "account-1")
	if err != nil || route.Generation != total {
		t.Fatalf("route=%#v %v", route, err)
	}
}

func TestIntegrationGenerationFenceAndCancelledApplyRollback(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 0)
	mustApply(t, store, 1, event("evt-1", 1))
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = store.VerifyGeneration(ctx, tx, "telegram", "account-1", 1); err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	err = store.ApplyFromStream(blocked, position(2), event("evt-2", 2))
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("apply crossed admission fence: %v", err)
	}
	blocked, cancel = context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = pool.Exec(blocked, `UPDATE gateway_route_projections SET enabled=false WHERE provider='telegram' AND account_id='account-1'`)
	cancel()
	if err == nil {
		t.Fatal("FOR SHARE missing")
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if count(t, pool, "gateway_route_stream_receipts") != 1 {
		t.Fatal("cancelled transaction leaked receipt")
	}
	mustApply(t, store, 2, event("evt-2", 2))
	if h := health(t, store); h.ContiguousSequence != 2 {
		t.Fatalf("health=%#v", h)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = store.VerifyGeneration(ctx, tx, "telegram", "account-1", 1); !errors.Is(err, domain.ErrGenerationConflict) {
		t.Fatal(err)
	}
}

func TestIntegrationTransientDatabaseFailureLeavesNoReceiptOrCheckpoint(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 1)
	_, err := pool.Exec(ctx, `CREATE FUNCTION fail_route_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected transient failure'; END $$; CREATE TRIGGER fail_route BEFORE INSERT OR UPDATE ON gateway_route_projections FOR EACH ROW EXECUTE FUNCTION fail_route_write()`)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ApplyFromStream(ctx, position(1), event("evt-1", 1)); err == nil {
		t.Fatal("injected fault ignored")
	}
	if count(t, pool, "gateway_route_receipts") != 0 || count(t, pool, "gateway_route_stream_receipts") != 0 || count(t, pool, "gateway_route_quarantines") != 0 {
		t.Fatal("transient failure changed durable receipt/quarantine")
	}
	if h := health(t, store); h.Initialized || h.ContiguousSequence != 0 {
		t.Fatalf("health=%#v", h)
	}
	if _, err = pool.Exec(ctx, `DROP TRIGGER fail_route ON gateway_route_projections`); err != nil {
		t.Fatal(err)
	}
	mustApply(t, store, 1, event("evt-1", 1))
	if !health(t, store).Initialized {
		t.Fatal("retry did not initialize")
	}
}

func TestIntegrationOldAndLiveProjectionCorruptionPersistsQuarantine(t *testing.T) {
	for _, when := range []string{"startup", "resolve"} {
		t.Run(when, func(t *testing.T) {
			store, pool := setup(t)
			ctx := context.Background()
			if when == "startup" {
				insertProjection(t, pool, event("old", 1))
			} else {
				mustBegin(t, store, 0)
				mustApply(t, store, 1, event("evt-1", 1))
			}
			if _, err := pool.Exec(ctx, `UPDATE gateway_route_projections SET snapshot=jsonb_set(snapshot,'{binding_id}','"tampered"')`); err != nil {
				t.Fatal(err)
			}
			if when == "startup" {
				if err := store.BeginReplay(ctx, source(0)); !errors.Is(err, domain.ErrProjectionBlocked) {
					t.Fatal(err)
				}
			} else {
				if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrProjectionBlocked) {
					t.Fatal(err)
				}
			}
			if h := health(t, store); h.Initialized || h.BlockedReason != "corrupt_projection" {
				t.Fatalf("health=%#v", h)
			}
			if count(t, pool, "gateway_route_quarantines") != 1 {
				t.Fatal("corruption quarantine missing")
			}
		})
	}
}

func TestIntegrationTransportObservationStalenessAndRefresh(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 0)
	mustApply(t, store, 1, event("evt-1", 1))
	if _, err := pool.Exec(ctx, `UPDATE gateway_route_replay_state SET last_observed_at=now()-interval '6 minutes'`); err != nil {
		t.Fatal(err)
	}
	h := health(t, store)
	if !h.Stale || h.Initialized || h.LastObservedAt.IsZero() {
		t.Fatalf("health=%#v", h)
	}
	if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrProjectionStale) {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = store.VerifyGeneration(ctx, tx, "telegram", "account-1", 1); !errors.Is(err, domain.ErrProjectionStale) {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	delayed := source(1)
	delayed.ObservedAt = time.Now().Add(-6 * time.Minute)
	if err = store.ObserveSource(ctx, delayed); err != nil {
		t.Fatal(err)
	}
	if !health(t, store).Stale {
		t.Fatal("delayed observation falsely refreshed grace window")
	}
	if err = store.ObserveSource(ctx, source(1)); err != nil {
		t.Fatal(err)
	}
	h = health(t, store)
	if h.Stale || !h.Initialized || h.TargetSequence != 0 {
		t.Fatalf("refresh redefined target or stayed stale: %#v", h)
	}
	// A live history hole is quarantined rather than extending the grace window.
	broken := source(2)
	broken.MessageCount = 1
	if err = store.ObserveSource(ctx, broken); !errors.Is(err, domain.ErrProjectionBlocked) {
		t.Fatal(err)
	}
}

func TestIntegrationDisabledAccountAndProviderScope(t *testing.T) {
	store, _ := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 0)
	mustApply(t, store, 1, event("evt-1", 1))
	other := event("evt-wecom", 1)
	other.Route.Provider = "wecom"
	mustApply(t, store, 2, other)
	tombstone := domain.RouteEvent{EventID: "disabled", SchemaVersion: 1, Route: domain.RouteSnapshot{Provider: "telegram", AccountID: "account-1", Generation: 2}}
	mustApply(t, store, 3, tombstone)
	mustApply(t, store, 4, event("stale", 1))
	if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := store.Resolve(ctx, "wecom", "account-1"); err != nil {
		t.Fatal(err)
	}
	mustApply(t, store, 5, event("reenabled", 3))
	if _, err := store.Resolve(ctx, "telegram", "account-1"); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationUnboundLegacyProjectionNeverBecomesAuthoritative(t *testing.T) {
	store, pool := setup(t)
	ctx := context.Background()
	insertProjection(t, pool, event("unbound-legacy", 100))
	if err := store.BeginReplay(ctx, source(1)); !errors.Is(err, domain.ErrProjectionBlocked) {
		t.Fatal(err)
	}
	if h := health(t, store); h.Initialized || h.BlockedReason != "unbound_projection" {
		t.Fatalf("health=%#v", h)
	}
	if count(t, pool, "gateway_route_quarantines") != 1 {
		t.Fatal("unbound legacy projection not isolated")
	}
}
func TestIntegrationOlderReplicaObservationDoesNotQuarantineOrLowerTarget(t *testing.T) {
	store, _ := setup(t)
	ctx := context.Background()
	mustBegin(t, store, 2)
	mustApply(t, store, 1, event("evt-1", 1))
	mustApply(t, store, 2, event("evt-2", 2))
	if err := store.ObserveSource(ctx, source(1)); err != nil {
		t.Fatalf("delayed observation quarantined healthy stream: %v", err)
	}
	if err := store.BeginReplay(ctx, source(1)); err != nil {
		t.Fatalf("delayed replica startup quarantined healthy stream: %v", err)
	}
	h := health(t, store)
	if !h.Initialized || h.TargetSequence != 2 || h.HighestSequence != 2 || h.BlockedReason != "" {
		t.Fatalf("health=%#v", h)
	}
	// Stop-time disable is included in the next captured startup target, so a
	// previously initialized old enabled row cannot admit until it is applied.
	mustBegin(t, store, 3)
	if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrNotInitialized) {
		t.Fatal(err)
	}
	disabled := domain.RouteEvent{EventID: "evt-disabled", SchemaVersion: 1, Route: domain.RouteSnapshot{Provider: "telegram", AccountID: "account-1", Generation: 3}}
	mustApply(t, store, 3, disabled)
	if _, err := store.Resolve(ctx, "telegram", "account-1"); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatal(err)
	}
	if !health(t, store).Initialized {
		t.Fatal("disable replay did not finish startup target")
	}
}
