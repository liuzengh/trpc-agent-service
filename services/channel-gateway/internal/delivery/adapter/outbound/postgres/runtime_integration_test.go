package postgresadapter_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

func runtimePrepared(id, provider, account string) domain.Prepared {
	p := prepared(id)
	p.Target.Provider = provider
	p.Target.AccountID = account
	if provider == "wecom" {
		p.Target.CallbackRequestID = "callback-1"
		p.Target.Origin = &domain.ReplyOrigin{InstanceID: "owner-1", Epoch: 1, Revision: 1, SocketGeneration: 1}
	}
	refresh(&p)
	return p
}
func runtimeExec(t *testing.T, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeDueAccountsKeysetAndEligibility(t *testing.T) {
	s, _, pool := ledgers(t, pg.Options{})
	due, _ := runtimePorts(t, s)
	ctx := context.Background()
	for _, item := range []struct{ id, account string }{{"a1", "account-a"}, {"a2", "account-a"}, {"future", "account-b"}, {"blocked", "account-c"}, {"prep-exhausted", "account-d"}, {"call-exhausted", "account-e"}, {"expired", "account-f"}, {"z", "account-z"}} {
		p := runtimePrepared(item.id, "telegram", item.account)
		if item.id == "blocked" {
			p.Intent.Text = strings.Repeat("x", 4097)
			refresh(&p)
		}
		accept(t, s, p)
	}
	accept(t, s, runtimePrepared("wecom-no-owner", "wecom", "account-wecom"))
	runtimeExec(t, pool, `UPDATE gateway_delivery_parts SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE intent_id='future'`)
	runtimeExec(t, pool, `UPDATE gateway_delivery_parts SET state='UNKNOWN',next_attempt_at=NULL WHERE intent_id='blocked' AND part_index=0`)
	runtimeExec(t, pool, `UPDATE gateway_delivery_parts SET preparation_attempts=3 WHERE intent_id='prep-exhausted'`)
	runtimeExec(t, pool, `UPDATE gateway_delivery_parts SET attempt_number=3 WHERE intent_id='call-exhausted'`)
	runtimeExec(t, pool, `UPDATE gateway_delivery_intents SET deadline=clock_timestamp()-interval '1 second' WHERE intent_id='expired'`)
	page, err := due.ListDueAccounts(ctx, app.DueAccountQuery{Provider: "telegram", Limit: 1})
	if err != nil || !reflect.DeepEqual(page.Accounts, []app.AccountKey{{Provider: "telegram", AccountID: "account-a"}}) || page.Exhausted || page.NextAccountID != "account-a" {
		t.Fatalf("first page=%+v err=%v", page, err)
	}
	page, err = due.ListDueAccounts(ctx, app.DueAccountQuery{Provider: "telegram", AfterAccountID: page.NextAccountID, Limit: 1})
	if err != nil || !reflect.DeepEqual(page.Accounts, []app.AccountKey{{Provider: "telegram", AccountID: "account-z"}}) || !page.Exhausted {
		t.Fatalf("last page=%+v err=%v", page, err)
	}
	page, err = due.ListDueAccounts(ctx, app.DueAccountQuery{Provider: "wecom", Limit: 1000})
	if err != nil || !reflect.DeepEqual(page.Accounts, []app.AccountKey{{Provider: "wecom", AccountID: "account-wecom"}}) || !page.Exhausted {
		t.Fatalf("owner-independent page=%+v err=%v", page, err)
	}
	page, err = due.ListDueAccounts(ctx, app.DueAccountQuery{Provider: "telegram", AfterAccountID: "zzzz", Limit: 2})
	if err != nil || len(page.Accounts) != 0 || !page.Exhausted {
		t.Fatalf("empty page=%+v err=%v", page, err)
	}
	t.Log("DUE_VERIFIED: distinct account keyset, provider isolation, deadline/due/attempt/preparation/prior-part predicates, WeCom without owner")
}

func TestRuntimeExpirePendingWithoutOwnerOrPredecessorAndPreserveFacts(t *testing.T) {
	s, _, pool := ledgers(t, pg.Options{MaxIntents: 7})
	_, maintenance := runtimePorts(t, s)
	ids := []string{"wecom-expired", "blocked-later", "terminal-not-sent", "accepted", "calling", "claimed", "retryable"}
	for _, id := range ids {
		provider := "telegram"
		if id == "wecom-expired" {
			provider = "wecom"
		}
		p := runtimePrepared(id, provider, id)
		if id == "blocked-later" {
			p.Intent.Text = strings.Repeat("x", 4097)
			refresh(&p)
		}
		accept(t, s, p)
	}
	for _, id := range []string{"terminal-not-sent", "accepted", "calling", "claimed", "retryable"} {
		r := claimRequest()
		r.AccountID = id
		c := oneClaim(t, s, r)
		switch id {
		case "terminal-not-sent":
			if err := s.FinishPreparation(context.Background(), c, domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorPermanent}); err != nil {
				t.Fatal(err)
			}
		case "accepted":
			a := calling(t, s, c)
			if err := s.Finish(context.Background(), a, acceptedResult()); err != nil {
				t.Fatal(err)
			}
		case "calling":
			calling(t, s, c)
		case "retryable":
			if err := s.FinishPreparation(context.Background(), c, domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorTemporary}); err != nil {
				t.Fatal(err)
			}
		}
	}
	runtimeExec(t, pool, `UPDATE gateway_delivery_parts SET state='UNKNOWN',next_attempt_at=NULL WHERE intent_id='blocked-later' AND part_index=0`)
	runtimeExec(t, pool, `INSERT INTO gateway_connection_accounts(account_id,bot_id,credential_ref,revision,enabled) VALUES('wecom-expired','disabled-bot','FIXTURE',1,false)`)
	runtimeExec(t, pool, `UPDATE gateway_delivery_intents SET deadline=clock_timestamp()-interval '1 second'`)
	before := map[string]string{}
	for _, id := range ids {
		_, digest, found, err := s.Find(context.Background(), id)
		if err != nil || !found {
			t.Fatal(err)
		}
		before[id] = digest
	}
	total := 0
	for n := 0; n < 4; n++ {
		count, err := maintenance.ExpirePending(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		total += count
		if count == 0 {
			break
		}
	}
	if total != 3 {
		t.Fatalf("expired=%d want3", total)
	}
	for _, item := range []struct {
		id    string
		index int
		state domain.State
	}{{"wecom-expired", 0, domain.Expired}, {"blocked-later", 0, domain.Unknown}, {"blocked-later", 1, domain.Expired}, {"terminal-not-sent", 0, domain.NotSent}, {"accepted", 0, domain.Accepted}, {"calling", 0, domain.Calling}, {"claimed", 0, domain.Claimed}, {"retryable", 0, domain.Expired}} {
		if got := partState(t, s, item.id, item.index); got.State != item.state {
			t.Fatalf("%s[%d]=%s want%s", item.id, item.index, got.State, item.state)
		}
	}
	for _, id := range ids {
		receipt, digest, found, err := s.Find(context.Background(), id)
		if err != nil || !found || digest != before[id] || receipt.IntentID != id {
			t.Fatalf("receipt changed %s: %v", id, err)
		}
	}
	assertCount(t, pool, "gateway_delivery_intents", 7)
	assertCount(t, pool, "gateway_delivery_parts", 8)
	assertCount(t, pool, "gateway_delivery_attempts", 2)
	if _, err := s.Accept(context.Background(), prepared("capacity-still-full")); !errors.Is(err, domain.ErrCapacity) {
		t.Fatalf("expiry released total-row capacity: %v", err)
	}
	t.Log("EXPIRY_VERIFIED: disabled ownerless WeCom and UNKNOWN-blocked later parts expire; terminal NOT_SENT/CALLING/CLAIMED/accepted facts untouched; receipts and capacity retained")
}

func TestRuntimeObservedKeysetIncludesConflictsWithoutChangingState(t *testing.T) {
	s, _, _ := ledgers(t, pg.Options{})
	_, maintenance := runtimePorts(t, s)
	attempts := make([]domain.Attempt, 0, 4)
	for n := 0; n < 6; n++ {
		id := fmt.Sprintf("observed-%d", n)
		accept(t, s, runtimePrepared(id, "telegram", id))
		r := claimRequest()
		r.AccountID = id
		a := calling(t, s, oneClaim(t, s, r))
		if n != 5 {
			if err := s.Finish(context.Background(), a, domain.Result{Certainty: domain.CertaintyUnknown}); err != nil {
				t.Fatal(err)
			}
		}
		if n < 4 {
			attempts = append(attempts, a)
		}
		if n == 5 {
			if err := s.Observe(context.Background(), observation(a, "live-calling-observation")); err != nil {
				t.Fatal(err)
			}
		}
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].ID < attempts[j].ID })
	for n, a := range attempts {
		obs := observation(a, fmt.Sprintf("candidate-%d", n))
		if n == 2 {
			obs.Result = domain.Result{Certainty: domain.CertaintyUnknown}
		}
		if n == 3 {
			obs.Result = domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorTemporary}
		}
		if err := s.Observe(context.Background(), obs); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			obs.ID = "conflicting-evidence"
			obs.Result = domain.Result{Certainty: domain.CertaintyRejected, ErrorClass: domain.ErrorPermanent}
			if err := s.Observe(context.Background(), obs); err != nil {
				t.Fatal(err)
			}
		}
	}
	var got []string
	after := ""
	for n := 0; n < 5; n++ {
		page, err := maintenance.ListResolvableObserved(context.Background(), app.ObservedAttemptQuery{AfterAttemptID: after, Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page.AttemptIDs...)
		if page.Exhausted {
			break
		}
		if page.NextAttemptID <= after {
			t.Fatalf("cursor did not advance %+v", page)
		}
		after = page.NextAttemptID
	}
	want := []string{attempts[0].ID, attempts[1].ID, attempts[2].ID, attempts[3].ID}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("observed IDs=%v want=%v", got, want)
	}
	for _, a := range attempts {
		if got := partState(t, s, a.Intent.ID, 0); got.State != domain.Unknown {
			t.Fatalf("query changed state: %+v", got)
		}
	}
	if changed, err := maintenance.ResolveObserved(context.Background(), attempts[0].ID); err != nil || changed {
		t.Fatalf("conflict changed=%v err=%v", changed, err)
	}
	if changed, err := maintenance.ResolveObserved(context.Background(), attempts[1].ID); err != nil || !changed {
		t.Fatalf("consistent late result changed=%v err=%v", changed, err)
	}
	page, err := maintenance.ListResolvableObserved(context.Background(), app.ObservedAttemptQuery{Limit: 1000})
	if err != nil || len(page.AttemptIDs) != 3 || !page.Exhausted {
		t.Fatalf("post-resolution page=%+v err=%v", page, err)
	}
	t.Log("OBSERVED_VERIFIED: keyset reaches consistent evidence past first conflicting candidate; pending CALLING/no-evidence omitted; reads do not mutate UNKNOWN")
}

func TestRuntimeExpirySkipsLockedRowsAndClaimCannotCrossExpiry(t *testing.T) {
	s, other, pool := ledgers(t, pg.Options{})
	_, maintenance := runtimePorts(t, s)
	accept(t, s, prepared("locked-expiry"))
	accept(t, s, prepared("free-expiry"))
	runtimeExec(t, pool, `UPDATE gateway_delivery_intents SET deadline=clock_timestamp()-interval '1 second'`)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT part_id FROM gateway_delivery_parts WHERE intent_id='locked-expiry' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	if n, err := maintenance.ExpirePending(ctx, 10); err != nil || n != 1 {
		t.Fatalf("SKIP LOCKED n=%d err=%v", n, err)
	}
	if got := partState(t, other, "locked-expiry", 0); got.State != domain.Pending {
		t.Fatalf("locked row changed: %+v", got)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// A fixture advisory latch stops expiry after it owns the real part lock.
	runtimeExec(t, pool, `CREATE FUNCTION runtime_expiry_latch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='EXPIRED' THEN PERFORM pg_advisory_xact_lock(98177401); END IF; RETURN NEW; END $$; CREATE TRIGGER runtime_expiry_latch BEFORE UPDATE ON gateway_delivery_parts FOR EACH ROW EXECUTE FUNCTION runtime_expiry_latch()`)
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err = lock.Exec(ctx, `SELECT pg_advisory_xact_lock(98177401)`); err != nil {
		t.Fatal(err)
	}
	var pid int32
	if err = lock.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		n, e := maintenance.ExpirePending(ctx, 1)
		if e == nil && n != 1 {
			e = fmt.Errorf("expired=%d", n)
		}
		done <- e
	}()
	waitDeliveryBlocked(t, ctx, pool, pid)
	claims, err := other.ClaimDue(ctx, claimRequest())
	if err != nil || len(claims) != 0 {
		t.Fatalf("claim crossed expiry row lock: %v %v", claims, err)
	}
	if err = lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if got := partState(t, s, "locked-expiry", 0); got.State != domain.Expired {
		t.Fatalf("expiry result=%+v", got)
	}
	t.Log("EXPIRY_LOCK_VERIFIED: locked row skipped, independent work completes, competing ClaimDue cannot claim a row locked by expiry")
}

func TestRuntimeExpirySamplesDatabaseClockAfterLocksAndConcurrentCAS(t *testing.T) {
	s, other, pool := ledgers(t, pg.Options{})
	_, maintenance := runtimePorts(t, s)
	_, second := runtimePorts(t, other)
	for n := 0; n < 12; n++ {
		accept(t, s, prepared(fmt.Sprintf("expire-cas-%d", n)))
	}
	runtimeExec(t, pool, `UPDATE gateway_delivery_intents SET deadline=clock_timestamp()-interval '1 second'`)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `LOCK TABLE gateway_delivery_parts IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	var pid int32
	if err = tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		n, e := maintenance.ExpirePending(ctx, 1)
		if e == nil && n != 1 {
			e = fmt.Errorf("expired=%d", n)
		}
		done <- e
	}()
	waitDeliveryBlocked(t, ctx, pool, pid)
	var releasedAt time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&releasedAt); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	var afterLock bool
	if err = pool.QueryRow(ctx, `SELECT bool_and(updated_at >= $1) FROM gateway_delivery_parts WHERE state='EXPIRED'`, releasedAt).Scan(&afterLock); err != nil || !afterLock {
		t.Fatalf("clock was sampled before lock: %v %v", afterLock, err)
	}
	counts := make(chan int, 16)
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m := maintenance
			if n%2 == 1 {
				m = second
			}
			count, e := m.ExpirePending(ctx, 1)
			counts <- count
			errs <- e
		}(n)
	}
	wg.Wait()
	close(counts)
	close(errs)
	total := 1
	for count := range counts {
		total += count
	}
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if total != 12 {
		t.Fatalf("concurrent expiry counted duplicates/lost rows: %d", total)
	}
	for n := 0; n < 12; n++ {
		if got := partState(t, s, fmt.Sprintf("expire-cas-%d", n), 0); got.State != domain.Expired {
			t.Fatal(got)
		}
	}
	t.Log("EXPIRY_CAS_VERIFIED: post-lock PG timestamp, two pools/16 concurrent batches count each of 12 parts exactly once")
}

func TestRuntimeQueriesHaveIndependentBoundedTimeouts(t *testing.T) {
	s, _, pool := ledgers(t, pg.Options{})
	due, maintenance := runtimePorts(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `LOCK TABLE gateway_delivery_intents,gateway_delivery_parts,gateway_delivery_attempts IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 3)
	start := time.Now()
	go func() {
		_, e := due.ListDueAccounts(context.Background(), app.DueAccountQuery{Provider: "telegram", Limit: 1})
		errs <- e
	}()
	go func() { _, e := maintenance.ExpirePending(context.Background(), 1); errs <- e }()
	go func() {
		_, e := maintenance.ListResolvableObserved(context.Background(), app.ObservedAttemptQuery{Limit: 1})
		errs <- e
	}()
	for range 3 {
		select {
		case e := <-errs:
			if !errors.Is(e, context.DeadlineExceeded) || !errors.Is(e, domain.ErrUnavailable) {
				t.Fatalf("unbounded/wrong query error: %v", e)
			}
		case <-ctx.Done():
			t.Fatal("background query exceeded its independent deadline")
		}
	}
	if elapsed := time.Since(start); elapsed < 4*time.Second || elapsed > 8*time.Second {
		t.Fatalf("independent query deadline elapsed=%v", elapsed)
	}
	short, stop := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer stop()
	start = time.Now()
	_, err = due.ListDueAccounts(short, app.DueAccountQuery{Provider: "telegram", Limit: 1})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("parent deadline extended: %v", err)
	}
	t.Log("TIMEOUT_VERIFIED: every runtime query bounded to 5s with Background; shorter caller deadline retained")
}

func TestRuntimeAccountCursorUsesASCIIOrder(t *testing.T) {
	s, _, _ := ledgers(t, pg.Options{})
	due, _ := runtimePorts(t, s)
	want := []string{"A", "a", "a-1", "a.1", "a1", "a:1", "a_1"}
	for n, id := range want {
		accept(t, s, runtimePrepared(fmt.Sprintf("ordered-%d", n), "telegram", id))
	}
	var got []string
	cursor := ""
	for n := 0; n < 10; n++ {
		page, err := due.ListDueAccounts(context.Background(), app.DueAccountQuery{Provider: "telegram", AfterAccountID: cursor, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range page.Accounts {
			if a.AccountID <= cursor {
				t.Fatalf("non-forward cursor: %q <= %q", a.AccountID, cursor)
			}
			cursor = a.AccountID
			got = append(got, cursor)
		}
		if len(page.Accounts) > 0 && page.NextAccountID != cursor {
			t.Fatal(page)
		}
		if page.Exhausted {
			break
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("account order=%v want=%v", got, want)
	}
}
