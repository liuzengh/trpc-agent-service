package catalogpostgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	p "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/catalogpostgres"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
	"os"
	"testing"
	"time"
)

func setup(t *testing.T) *pgxpool.Pool {
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
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"gateway_connection_" + hex.EncodeToString(random[:])}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

const sourceEpoch = "00000000-0000-4000-8000-000000000001"
const boot = "00000000-0000-4000-8000-000000000002"

func store(t *testing.T, pool *pgxpool.Pool, instance string) *p.Store {
	t.Helper()
	s, e := p.New(pool, "pool", sourceEpoch, instance, boot)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func snapshot(rev int64) c.Snapshot {
	s := c.Snapshot{SchemaVersion: 1, ScopeID: "pool", SourceEpoch: sourceEpoch, Revision: rev, Complete: true, Accounts: []c.Account{{TenantID: "tnt_test", ID: "cha_test", Provider: "telegram", ProviderAccountID: "123", Revision: 1, ConnectionRevision: 1, Enabled: true, MinRouteGeneration: 1, Config: c.Config{WebhookPath: "/v1/telegram/cha_test"}, Credentials: []c.Credential{{Purpose: "telegram.bot_token", ID: "ccr_token", Version: 1, Configured: true}, {Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1, Configured: true}}}}}
	seal(&s)
	return s
}
func seal(s *c.Snapshot) { s.Digest, _ = s.ComputedDigest() }
func apply(t *testing.T, s *p.Store, snap c.Snapshot) (p.Poll, p.Qualification) {
	t.Helper()
	ticket, e := s.BeginPoll(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	_, q, e := s.Apply(context.Background(), ticket, snap)
	if e != nil {
		t.Fatal(e)
	}
	return ticket, q
}
func permit(t *testing.T, s *p.Store, ticket p.Poll, q p.Qualification, a c.Account, kind string) *c.Permit {
	t.Helper()
	v, e := s.Issue(context.Background(), ticket, q, a, kind, 1)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(v.Revoke)
	return v
}
func TestPGSnapshotSupersededVersusRollback(t *testing.T) {
	pool := setup(t)
	a, b := store(t, pool, "a"), store(t, pool, "b")
	snap := snapshot(10)
	_, initial := apply(t, a, snap)
	ticket, e := a.BeginPoll(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	next := snapshot(11)
	next.Accounts[0].MinRouteGeneration = 2
	seal(&next)
	apply(t, b, next)
	class, q, e := a.Apply(context.Background(), ticket, snap)
	if e != nil || class != c.Superseded || q.Generation != 0 {
		t.Fatalf("classification %s: %v", class, e)
	}
	var until time.Time
	if e = pool.QueryRow(context.Background(), `SELECT valid_until FROM gateway_account_qualifications WHERE instance_id='a'`).Scan(&until); e != nil || !until.Equal(initial.ValidUntil) {
		t.Fatal("superseded renewed qualification")
	}
	ticket, e = a.BeginPoll(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = a.Apply(context.Background(), ticket, snap); !errors.Is(e, c.ErrIntegrity) {
		t.Fatal("rollback accepted")
	}
	if _, e = b.BeginPoll(context.Background()); !errors.Is(e, c.ErrIntegrity) {
		t.Fatal("quarantine not durable")
	}
}
func TestPGGuardSerializesDisableAndPreservesHistory(t *testing.T) {
	pool := setup(t)
	s := store(t, pool, "a")
	snap := snapshot(1)
	ticket, q := apply(t, s, snap)
	token := permit(t, s, ticket, q, snap.Accounts[0], "telegram_delivery")
	ctx := context.Background()
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if e = s.Guard(ctx, tx, token, nil); e != nil {
		t.Fatal(e)
	}
	update := snapshot(2)
	update.Accounts[0].Enabled = false
	update.Accounts[0].Revision = 2
	update.Accounts[0].ConnectionRevision = 2
	seal(&update)
	poll, e := s.BeginPoll(ctx)
	if e != nil {
		t.Fatal(e)
	}
	finished := make(chan error, 1)
	go func() { _, _, e := s.Apply(ctx, poll, update); finished <- e }()
	select {
	case e := <-finished:
		t.Fatalf("disable passed active share guard: %v", e)
	case <-time.After(80 * time.Millisecond):
	}
	if e = s.RecheckExpiry(ctx, tx, token); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-finished; e != nil {
		t.Fatal(e)
	}
	tx, e = pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if !errors.Is(s.Guard(ctx, tx, token, nil), c.ErrUnauthorized) {
		t.Fatal("disabled account passed guard")
	}
	// History is retained; guard failure does not delete ledger/replica evidence.
	var n int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM gateway_account_directory WHERE account_id='cha_test'`).Scan(&n); e != nil || n != 1 {
		t.Fatal("identity evidence deleted")
	}
}
func TestPGFloorAffectsNewAdmissionNotHistoricalDelivery(t *testing.T) {
	pool := setup(t)
	s := store(t, pool, "a")
	snap := snapshot(1)
	ticket, q := apply(t, s, snap)
	ingress := permit(t, s, ticket, q, snap.Accounts[0], "telegram_webhook")
	delivery := permit(t, s, ticket, q, snap.Accounts[0], "telegram_delivery")
	next := snapshot(2)
	next.Accounts[0].MinRouteGeneration = 4
	seal(&next)
	apply(t, s, next)
	ctx := context.Background()
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	generation := int64(1)
	if !errors.Is(s.Guard(ctx, tx, ingress, &generation), c.ErrVersion) {
		t.Fatal("old route admitted")
	}
	generation = 4
	if e = s.Guard(ctx, tx, ingress, &generation); e != nil {
		t.Fatal(e)
	}
	if e = s.Guard(ctx, tx, delivery, nil); e != nil {
		t.Fatal("route floor broke old reply:", e)
	}
}
func TestPGPerInstanceFailureAndExpiry(t *testing.T) {
	pool := setup(t)
	a, b := store(t, pool, "a"), store(t, pool, "b")
	snap := snapshot(1)
	pa, qa := apply(t, a, snap)
	pb, qb := apply(t, b, snap)
	ta := permit(t, a, pa, qa, snap.Accounts[0], "telegram_delivery")
	tb := permit(t, b, pb, qb, snap.Accounts[0], "telegram_delivery")
	ctx := context.Background()
	if e := a.Invalidate(ctx); e != nil {
		t.Fatal(e)
	}
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if !errors.Is(a.Guard(ctx, tx, ta, nil), c.ErrUnauthorized) {
		t.Fatal("invalidated incarnation allowed")
	}
	_ = tx.Rollback(ctx)
	tx, e = pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Guard(ctx, tx, tb, nil); e != nil {
		t.Fatal("healthy other replica revoked", e)
	}
	_ = tx.Rollback(ctx)
	if _, e = pool.Exec(ctx, `UPDATE gateway_account_qualifications SET valid_until=clock_timestamp()-interval '1 second' WHERE instance_id='b'`); e != nil {
		t.Fatal(e)
	}
	tx, e = pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if !errors.Is(b.Guard(ctx, tx, tb, nil), c.ErrUnauthorized) {
		t.Fatal("DB-expired qualified")
	}
}
func TestPGConflictDoesNotPartiallyApply(t *testing.T) {
	pool := setup(t)
	s := store(t, pool, "a")
	snap := snapshot(1)
	apply(t, s, snap)
	next := snapshot(2)
	next.Accounts[0].Credentials[0].Version = 2 // impossible without connection_revision
	seal(&next)
	ticket, e := s.BeginPoll(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.Apply(context.Background(), ticket, next); !errors.Is(e, c.ErrIntegrity) {
		t.Fatal("same version mutation accepted")
	}
	var raw []byte
	var revision int64
	var blocked bool
	e = pool.QueryRow(context.Background(), `SELECT revision,blocked,snapshot_json FROM gateway_account_catalogs`).Scan(&revision, &blocked, &raw)
	if e != nil {
		t.Fatal(e)
	}
	var persisted c.Snapshot
	if json.Unmarshal(raw, &persisted) != nil || revision != 1 || persisted.Digest != snap.Digest || !blocked {
		t.Fatal("conflict partially applied or not quarantined")
	}
}
func TestPGDuplicateSnapshotDigestAndExpiredPoll(t *testing.T) {
	pool := setup(t)
	s := store(t, pool, "a")
	snap := snapshot(1)
	apply(t, s, snap)
	ticket, e := s.BeginPoll(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	ticket.LocalStarted = time.Now().Add(-6 * time.Second)
	if _, _, e = s.Apply(context.Background(), ticket, snap); !errors.Is(e, c.ErrExpired) {
		t.Fatal("late response renewed")
	}
	ticket, e = s.BeginPoll(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	snap.Accounts[0].MinRouteGeneration = 2
	seal(&snap)
	if _, _, e = s.Apply(context.Background(), ticket, snap); !errors.Is(e, c.ErrIntegrity) {
		t.Fatal("same revision changed digest")
	}
}

func TestPGEndpointSwitchClearsRemoteCursor(t *testing.T) {
	pool := setup(t)
	s := store(t, pool, "endpoint-test")
	snap := snapshot(1)
	apply(t, s, snap)
	_, e := pool.Exec(context.Background(), `INSERT INTO gateway_telegram_receivers(bot_id,scope_id,account_id,source_epoch,connection_revision,next_offset,managed_url,pending_url) VALUES('123','pool','cha_test',$1,1,99999,'https://old.example/callback','https://old.example/pending')`, sourceEpoch)
	if e != nil {
		t.Fatal(e)
	}
	snap.Revision = 2
	snap.Accounts[0].Revision = 2
	snap.Accounts[0].ConnectionRevision = 2
	snap.Accounts[0].Enabled = false
	snap.Accounts[0].Config.EndpointProfile = "test"
	seal(&snap)
	apply(t, s, snap)
	var offset int64
	var managed, pending string
	e = pool.QueryRow(context.Background(), `SELECT next_offset,managed_url,pending_url FROM gateway_telegram_receivers WHERE bot_id='123'`).Scan(&offset, &managed, &pending)
	if e != nil || offset != 0 || managed != "" || pending != "" {
		t.Fatal(offset, managed, pending, e)
	}
}
