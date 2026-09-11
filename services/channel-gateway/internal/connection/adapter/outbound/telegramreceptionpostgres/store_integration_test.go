package telegramreceptionpostgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	p "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/catalogpostgres"
	r "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/telegramreceptionpostgres"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/telegramreception"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
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
	s := c.Snapshot{SchemaVersion: 1, ScopeID: "pool", SourceEpoch: sourceEpoch, Revision: rev, Complete: true, Accounts: []c.Account{{TenantID: "tnt_test", ID: "cha_test", Provider: "telegram", ProviderAccountID: "123", Revision: 1, ConnectionRevision: 1, Enabled: true, MinRouteGeneration: 1, Config: c.Config{ReceiveMode: "long_polling", WebhookPath: "/v1/telegram/cha_test"}, Credentials: []c.Credential{{Purpose: "telegram.bot_token", ID: "ccr_token", Version: 1, Configured: true}, {Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1, Configured: true}}}}}
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
func receipt(t *testing.T, pool *pgxpool.Pool, id int64) {
	t.Helper()
	_, e := pool.Exec(context.Background(), `INSERT INTO gateway_inbox(provider,account_id,event_id,source_digest,receipt,received_at) VALUES('telegram','cha_test',$1,repeat('a',64),'{"decision":"ignore"}',clock_timestamp()) ON CONFLICT DO NOTHING`, strconv.FormatInt(id, 10))
	if e != nil {
		t.Fatal(e)
	}
}
func TestPGRestartedInstanceTakesOverItsOwnLiveLease(t *testing.T) {
	pool := setup(t)
	const first, second = "00000000-0000-4000-8000-00000000000a", "00000000-0000-4000-8000-00000000000b"
	a, e := p.New(pool, "pool", sourceEpoch, "gw", first)
	if e != nil {
		t.Fatal(e)
	}
	snap := snapshot(1)
	ta, qa := apply(t, a, snap)
	pa := permit(t, a, ta, qa, snap.Accounts[0], "telegram_receiver")
	ctx := context.Background()
	if _, yes, e := r.New(pool, a).Acquire(ctx, pa, "123"); e != nil || !yes {
		t.Fatal("first acquisition", yes, e)
	}
	// The same logical instance restarted before its 30 second lease expired.
	b, e := p.New(pool, "pool", sourceEpoch, "gw", second)
	if e != nil {
		t.Fatal(e)
	}
	tb, qb := apply(t, b, snap)
	pb := permit(t, b, tb, qb, snap.Accounts[0], "telegram_receiver")
	lease, yes, e := r.New(pool, b).Acquire(ctx, pb, "123")
	if e != nil || !yes {
		t.Fatal("restarted instance must take over its own live lease", yes, e)
	}
	if lease.InstanceEpoch != second {
		t.Fatalf("lease instance epoch = %q, want %q", lease.InstanceEpoch, second)
	}
	if lease.Epoch <= 1 {
		t.Fatal("takeover must advance the owner epoch to fence the old process")
	}
}

func TestPGPhysicalOwnerCallWindowAndDurableCursor(t *testing.T) {
	pool := setup(t)
	a, b := store(t, pool, "a"), store(t, pool, "b")
	snap := snapshot(1)
	ta, qa := apply(t, a, snap)
	tb, qb := apply(t, b, snap)
	pa, pb := permit(t, a, ta, qa, snap.Accounts[0], "telegram_receiver"), permit(t, b, tb, qb, snap.Accounts[0], "telegram_receiver")
	ra, rb := r.New(pool, a), r.New(pool, b)
	ctx := context.Background()
	first, yes, e := ra.Acquire(ctx, pa, "123")
	if e != nil || !yes {
		t.Fatal(yes, e)
	}
	if _, yes, e := rb.Acquire(ctx, pb, "123"); e != nil || yes {
		t.Fatal("two owners", yes, e)
	}
	if e = ra.CommitCursor(ctx, pa, first, 0, 102); e == nil {
		t.Fatal("cursor advanced without durable receipt")
	}
	receipt(t, pool, 101)
	if e = ra.CommitCursor(ctx, pa, first, 0, 102); e != nil {
		t.Fatal(e)
	}
	call, e := ra.BeginCall(ctx, pa, first)
	if e != nil {
		t.Fatal(e)
	}
	if !call.Deadline.After(time.Now()) || time.Until(call.Deadline) > d.CallBudget+time.Second {
		t.Fatal("unbounded request")
	}
	if _, e = pool.Exec(ctx, `UPDATE gateway_telegram_receivers SET lease_until=clock_timestamp()-interval '1 second'`); e != nil {
		t.Fatal(e)
	}
	if _, yes, e = rb.Acquire(ctx, pb, "123"); e != nil || yes {
		t.Fatal("takeover ignored in-flight window", yes, e)
	}
	if e = ra.FinishCall(ctx, pa, first, call, false); e != nil {
		t.Fatal(e)
	}
	var future bool
	if e = pool.QueryRow(ctx, `SELECT call_until>clock_timestamp() FROM gateway_telegram_receivers`).Scan(&future); e != nil || !future {
		t.Fatal("uncertain window cleared", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE gateway_telegram_receivers SET call_until=clock_timestamp()-interval '1 second'`); e != nil {
		t.Fatal(e)
	}
	second, yes, e := rb.Acquire(ctx, pb, "123")
	if e != nil || !yes || second.Epoch <= first.Epoch || second.NextOffset != 102 {
		t.Fatal(second, yes, e)
	}
	receipt(t, pool, 102)
	if e = ra.CommitCursor(ctx, pa, first, 102, 103); e == nil {
		t.Fatal("stale owner advanced")
	}
	if e = rb.CommitCursor(ctx, pb, second, 102, 103); e != nil {
		t.Fatal(e)
	}
	if e = rb.CommitCursor(ctx, pb, second, 102, 104); e == nil {
		t.Fatal("old cursor CAS accepted")
	}
}
func TestPGIdleResetRequiresAgeAndReceipt(t *testing.T) {
	pool := setup(t)
	a := store(t, pool, "a")
	snap := snapshot(1)
	ticket, q := apply(t, a, snap)
	p := permit(t, a, ticket, q, snap.Accounts[0], "telegram_receiver")
	receiver := r.New(pool, a)
	ctx := context.Background()
	l, ok, e := receiver.Acquire(ctx, p, "123")
	if e != nil || !ok {
		t.Fatal(e)
	}
	receipt(t, pool, 10000)
	if e = receiver.CommitCursor(ctx, p, l, 0, 10001); e != nil {
		t.Fatal(e)
	}
	receipt(t, pool, 42)
	if e = receiver.CommitCursor(ctx, p, l, 10001, 43); e == nil {
		t.Fatal("fresh cursor moved backward")
	}
	if _, e = pool.Exec(ctx, `UPDATE gateway_telegram_receivers SET last_update_at=clock_timestamp()-interval '7 days'`); e != nil {
		t.Fatal(e)
	}
	if e = receiver.CommitCursor(ctx, p, l, 10001, 43); e != nil {
		t.Fatal(e)
	}
	var offset int64
	if e = pool.QueryRow(ctx, `SELECT next_offset FROM gateway_telegram_receivers`).Scan(&offset); e != nil || offset != 43 {
		t.Fatal(offset, e)
	}
}
func TestPGModeChangeWaitsForOldOwnerAndRejectsOldPollingFence(t *testing.T) {
	pool := setup(t)
	a := store(t, pool, "a")
	snap := snapshot(1)
	ticket, q := apply(t, a, snap)
	old := permit(t, a, ticket, q, snap.Accounts[0], "telegram_receiver")
	receiver := r.New(pool, a)
	ctx := context.Background()
	l, yes, e := receiver.Acquire(ctx, old, "123")
	if e != nil || !yes {
		t.Fatal(e)
	}
	if _, e = receiver.BeginCall(ctx, old, l); e != nil {
		t.Fatal(e)
	}
	snap.Revision++
	snap.Accounts[0].Revision++
	snap.Accounts[0].ConnectionRevision++
	snap.Accounts[0].Config.ReceiveMode = "webhook"
	seal(&snap)
	ticket, q = apply(t, a, snap)
	next := permit(t, a, ticket, q, snap.Accounts[0], "telegram_receiver")
	if _, yes, e = receiver.Acquire(ctx, next, "123"); e != nil || yes {
		t.Fatal("mode change skipped old lease", yes, e)
	}
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if e = receiver.VerifyPolling(ctx, tx, "pool", l.AccountID, l.InstanceID, l.InstanceEpoch, l.Epoch, l.Revision); e == nil {
		t.Fatal("old polling fence survived mode change")
	}
}
