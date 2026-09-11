package postgresadapter_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"strings"
	"sync"
	"testing"
	"time"
)

func prepared(id string) domain.Prepared {
	p := domain.Prepared{Intent: domain.Intent{ID: id, AdmissionID: "admission-1", RunID: "run-" + id, AttemptID: "execution-attempt-1", CompletionID: "completion-1", ExecutionGeneration: 1, Sequence: 1, Text: "hello", Deadline: time.Now().Add(time.Hour).UTC()}, Digest: strings.Repeat("a", 64), Target: domain.Target{TenantID: "tenant-1", Provider: "telegram", AccountID: "account-1", ManifestDigest: "sha256:" + strings.Repeat("b", 64), ConversationID: "123", SourceEventID: "event-1", ReceivedAt: time.Now().UTC()}, Parts: []string{"hello"}}
	p.Digest, _ = domain.IntentDigest(p.Intent)
	return p
}
func TestAcceptPersistsLogicalFinal(t *testing.T) {
	pool, _ := deliveryDatabase(t)
	store, err := pg.NewStore(pool, nil, pg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	in := prepared("intent-1")
	got, err := store.Accept(context.Background(), in)
	if err != nil {
		t.Fatalf("persistent Final acceptance failed: %v", err)
	}
	if got.IntentID != in.Intent.ID || got.RunID != in.Intent.RunID || got.PartCount != 1 {
		t.Fatalf("receipt=%+v", got)
	}
}

func ledgers(t *testing.T, options pg.Options) (*pg.Store, *pg.Store, *pgxpool.Pool) {
	t.Helper()
	p1, p2 := deliveryDatabase(t)
	s1, e := pg.NewStore(p1, nil, options)
	if e != nil {
		t.Fatal(e)
	}
	s2, e := pg.NewStore(p2, nil, options)
	if e != nil {
		t.Fatal(e)
	}
	return s1, s2, p1
}
func refresh(p *domain.Prepared) {
	p.Digest, _ = domain.IntentDigest(p.Intent)
	p.Parts, _ = domain.PlanText(p.Target, p.Intent.Text)
}
func accept(t *testing.T, s *pg.Store, p domain.Prepared) {
	t.Helper()
	if _, e := s.Accept(context.Background(), p); e != nil {
		t.Fatal(e)
	}
}
func claimRequest() domain.ClaimRequest {
	return domain.ClaimRequest{Provider: "telegram", AccountID: "account-1", InstanceID: "instance-1", Limit: 100, Lease: time.Second}
}
func oneClaim(t *testing.T, s *pg.Store, r domain.ClaimRequest) domain.Claim {
	t.Helper()
	claims, e := s.ClaimDue(context.Background(), r)
	if e != nil || len(claims) != 1 {
		t.Fatalf("claims=%d err=%v", len(claims), e)
	}
	return claims[0]
}
func callRequest(c domain.Claim) domain.CallingRequest {
	digest, _ := domain.RequestDigest(c)
	id := "request-" + c.Token
	if c.Target.Provider == "wecom" {
		id = c.Target.CallbackRequestID
	}
	return domain.CallingRequest{Claim: c, RequestID: id, RequestDigest: digest, Timeout: time.Second}
}
func calling(t *testing.T, s *pg.Store, c domain.Claim) domain.Attempt {
	t.Helper()
	a, e := s.MarkCalling(context.Background(), callRequest(c))
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func partState(t *testing.T, s *pg.Store, id string, index int) domain.Part {
	t.Helper()
	got, e := s.Get(context.Background(), id)
	if e != nil {
		t.Fatal(e)
	}
	return got.Parts[index]
}
func acceptedResult() domain.Result {
	return domain.Result{Certainty: domain.CertaintyAccepted, ProviderMessageID: "provider-message-1"}
}
func observation(a domain.Attempt, id string) domain.Observation {
	return domain.Observation{ID: id, AttemptID: a.ID, EvidenceToken: a.EvidenceToken, ProviderRequestID: a.RequestID, RequestDigest: a.RequestDigest, Result: acceptedResult()}
}
func assertCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	var n int
	if e := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); e != nil || n != want {
		t.Fatalf("%s count=%d want=%d err=%v", table, n, want, e)
	}
}

func TestConcurrentAcceptAndRunFinalBarrier(t *testing.T) {
	a, b, pool := ledgers(t, pg.Options{})
	in := prepared("dedup")
	start := make(chan struct{})
	results := make(chan error, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			<-start
			s := a
			if i%2 == 1 {
				s = b
			}
			_, e := s.Accept(context.Background(), in)
			results <- e
		})
	}
	close(start)
	wg.Wait()
	close(results)
	for e := range results {
		if e != nil {
			t.Fatal(e)
		}
	}
	assertCount(t, pool, "gateway_delivery_intents", 1)
	assertCount(t, pool, "gateway_delivery_parts", 1)
	other := in
	other.Intent.ID = "other-final"
	refresh(&other)
	if _, e := b.Accept(context.Background(), other); !errors.Is(e, domain.ErrConflict) {
		t.Fatalf("second Final=%v", e)
	}
	changed := in
	changed.Intent.Text = "changed"
	refresh(&changed)
	if _, e := a.Accept(context.Background(), changed); !errors.Is(e, domain.ErrConflict) {
		t.Fatalf("conflicting same ID=%v", e)
	}
	// Age only scheduling time; immutable payload/digest and prior Receipt stay fixed.
	if _, e := pool.Exec(context.Background(), `UPDATE gateway_delivery_intents SET deadline=clock_timestamp()-interval '1 second'`); e != nil {
		t.Fatal(e)
	}
	if _, e := b.Accept(context.Background(), in); e != nil {
		t.Fatalf("replay rechecked deadline: %v", e)
	}
	expired := prepared("expired")
	expired.Intent.Deadline = time.Now().Add(-time.Second)
	refresh(&expired)
	if _, e := a.Accept(context.Background(), expired); !errors.Is(e, domain.ErrExpired) {
		t.Fatalf("new expired Final=%v", e)
	}
}

func TestCapacityAtomicAndDuplicateBypassesCapacity(t *testing.T) {
	a, b, pool := ledgers(t, pg.Options{MaxIntents: 1, MaxParts: 1})
	in := prepared("capacity-one")
	accept(t, a, in)
	if _, e := b.Accept(context.Background(), prepared("capacity-two")); !errors.Is(e, domain.ErrCapacity) {
		t.Fatalf("capacity=%v", e)
	}
	accept(t, b, in)
	assertCount(t, pool, "gateway_delivery_intents", 1)
	assertCount(t, pool, "gateway_delivery_parts", 1)
}

func TestClaimAndCallingHaveSingleWinnerAndOrderedParts(t *testing.T) {
	a, b, pool := ledgers(t, pg.Options{})
	in := prepared("ordered")
	in.Intent.Text = strings.Repeat("a", 4097)
	refresh(&in)
	accept(t, a, in)
	var wg sync.WaitGroup
	claims := make(chan domain.Claim, 16)
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Go(func() {
			s := a
			if i%2 == 1 {
				s = b
			}
			got, e := s.ClaimDue(context.Background(), claimRequest())
			errs <- e
			for _, c := range got {
				claims <- c
			}
		})
	}
	wg.Wait()
	close(claims)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var claim domain.Claim
	count := 0
	for c := range claims {
		claim = c
		count++
	}
	if count != 1 || claim.Part.Index != 0 {
		t.Fatalf("A1 winners=%d index=%d", count, claim.Part.Index)
	}
	assertCount(t, pool, "gateway_delivery_attempts", 0)
	tampered := claim
	tampered.Target.ConversationID = "456"
	if _, e := b.MarkCalling(context.Background(), callRequest(tampered)); !errors.Is(e, domain.ErrConflict) {
		t.Fatalf("altered authoritative target=%v", e)
	}
	attempts := make(chan domain.Attempt, 16)
	errs = make(chan error, 16)
	for i := range 16 {
		wg.Go(func() {
			s := a
			if i%2 == 1 {
				s = b
			}
			at, e := s.MarkCalling(context.Background(), callRequest(claim))
			if e == nil {
				attempts <- at
			} else {
				errs <- e
			}
		})
	}
	wg.Wait()
	close(attempts)
	close(errs)
	var attempt domain.Attempt
	count = 0
	for at := range attempts {
		attempt = at
		count++
	}
	if count != 1 {
		t.Fatalf("A2 winners=%d", count)
	}
	for e := range errs {
		if !errors.Is(e, domain.ErrClaimLost) {
			t.Fatal(e)
		}
	}
	if attempt.Target.ConversationID != "123" || attempt.Text != in.Parts[0] || attempt.EvidenceToken == "" {
		t.Fatal("A2 did not return authoritative payload/capability")
	}
	var hash string
	if e := pool.QueryRow(context.Background(), `SELECT evidence_hash FROM gateway_delivery_attempts WHERE attempt_id=$1`, attempt.ID).Scan(&hash); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256([]byte(attempt.EvidenceToken))
	if hash != hex.EncodeToString(sum[:]) || hash == attempt.EvidenceToken {
		t.Fatal("stored capability is not its hash")
	}
	if got, e := a.ClaimDue(context.Background(), claimRequest()); e != nil || len(got) != 0 {
		t.Fatalf("part2 passed CALLING predecessor: %v %v", got, e)
	}
	if e := a.Finish(context.Background(), attempt, acceptedResult()); e != nil {
		t.Fatal(e)
	}
	if e := b.Finish(context.Background(), attempt, acceptedResult()); e != nil {
		t.Fatalf("repeat result=%v", e)
	}
	if e := b.Finish(context.Background(), attempt, domain.Result{Certainty: domain.CertaintyUnknown}); !errors.Is(e, domain.ErrConflict) {
		t.Fatalf("conflicting finish=%v", e)
	}
	next := oneClaim(t, b, claimRequest())
	if next.Part.Index != 1 {
		t.Fatalf("next index=%d", next.Part.Index)
	}
}

func TestClaimExpiryReclaimsWithoutAttemptAndRejectsLateA2(t *testing.T) {
	a, b, pool := ledgers(t, pg.Options{})
	in := prepared("expiry")
	accept(t, a, in)
	r := claimRequest()
	r.Lease = 100 * time.Millisecond
	c := oneClaim(t, a, r)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `SELECT 1 FROM gateway_delivery_parts WHERE part_id=$1 FOR UPDATE`, c.Part.ID); e != nil {
		t.Fatal(e)
	}
	var pid int32
	if e = tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := b.MarkCalling(ctx, callRequest(c)); done <- e }()
	waitDeliveryBlocked(t, ctx, pool, pid)
	time.Sleep(120 * time.Millisecond)
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-done; !errors.Is(e, domain.ErrClaimLost) {
		t.Fatalf("A2 used time before lock wait: %v", e)
	}
	if n, e := a.RecoverExpiredClaims(ctx, 100); e != nil || n != 1 {
		t.Fatalf("recover=%d %v", n, e)
	}
	newClaim := oneClaim(t, b, claimRequest())
	if newClaim.Token == c.Token {
		t.Fatal("claim token reused")
	}
	if _, e = a.MarkCalling(ctx, callRequest(c)); !errors.Is(e, domain.ErrClaimLost) {
		t.Fatalf("old token reentered A2: %v", e)
	}
	assertCount(t, pool, "gateway_delivery_attempts", 0)
}

func TestCallingRecoveryPreservesUnknownAndLateEvidenceOnlyAppends(t *testing.T) {
	a, b, pool := ledgers(t, pg.Options{})
	in := prepared("late-evidence")
	accept(t, a, in)
	at := calling(t, a, oneClaim(t, a, claimRequest()))
	if _, e := pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET calling_until=clock_timestamp()-interval '1 second' WHERE part_id=$1`, at.PartID); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Go(func() { _, e := a.RecoverStaleCalling(context.Background(), 100); errs <- e })
	wg.Go(func() { errs <- b.Observe(context.Background(), observation(at, "observation-1")) })
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if p := partState(t, a, in.Intent.ID, 0); p.State != domain.Unknown {
		t.Fatalf("late evidence advanced state: %+v", p)
	}
	if e := b.Finish(context.Background(), at, acceptedResult()); e == nil {
		t.Fatal("late Finish overwrote recovery")
	}
	if got, e := a.ClaimDue(context.Background(), claimRequest()); e != nil || len(got) != 0 {
		t.Fatalf("UNKNOWN retried: %v %v", got, e)
	}
	if e := a.Observe(context.Background(), observation(at, "observation-1")); e != nil {
		t.Fatal(e)
	}
	bad := observation(at, "observation-forged")
	bad.EvidenceToken = "not-the-capability"
	if e := a.Observe(context.Background(), bad); !errors.Is(e, domain.ErrUnauthorized) {
		t.Fatalf("forged capability=%v", e)
	}
	bad = observation(at, "observation-forged")
	bad.ProviderRequestID = "wrong-request"
	if e := a.Observe(context.Background(), bad); !errors.Is(e, domain.ErrUnauthorized) {
		t.Fatalf("wrong req=%v", e)
	}
	bad = observation(at, "observation-1")
	bad.Result = domain.Result{Certainty: domain.CertaintyRejected, ErrorClass: domain.ErrorPermanent}
	if e := a.Observe(context.Background(), bad); !errors.Is(e, domain.ErrConflict) {
		t.Fatalf("observation conflict=%v", e)
	}
	got, e := b.Observations(context.Background(), at.ID)
	if e != nil || len(got) != 1 || got[0].Result != acceptedResult() {
		t.Fatalf("observations=%v %v", got, e)
	}
}

func TestNotSentRetryAndPreparationFailureAreSeparatelyBounded(t *testing.T) {
	for _, preparation := range []bool{false, true} {
		t.Run(fmt.Sprintf("preparation=%v", preparation), func(t *testing.T) {
			a, _, pool := ledgers(t, pg.Options{})
			in := prepared("bounded")
			accept(t, a, in)
			result := domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorTemporary}
			for index := 1; index <= 3; index++ {
				c := oneClaim(t, a, claimRequest())
				var e error
				if preparation {
					e = a.FinishPreparation(context.Background(), c, result)
				} else {
					at := calling(t, a, c)
					e = a.Finish(context.Background(), at, result)
				}
				if e != nil {
					t.Fatal(e)
				}
				var due *time.Time
				var attempts int64
				if e = pool.QueryRow(context.Background(), `SELECT next_attempt_at,attempt_number FROM gateway_delivery_parts WHERE part_id=$1`, c.Part.ID).Scan(&due, &attempts); e != nil {
					t.Fatal(e)
				}
				if index < 3 {
					if due == nil || time.Until(*due) < time.Duration(index)*time.Second-250*time.Millisecond {
						t.Fatalf("missing bounded backoff at %d: %v", index, due)
					}
				} else if due != nil {
					t.Fatal("third failure scheduled an unlimited retry")
				}
				if preparation && attempts != 0 {
					t.Fatal("Reserve failure invented a Provider attempt")
				}
				if index < 3 {
					if _, e = pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE part_id=$1`, c.Part.ID); e != nil {
						t.Fatal(e)
					}
				}
			}
			if got, e := a.ClaimDue(context.Background(), claimRequest()); e != nil || len(got) != 0 {
				t.Fatalf("fourth retry=%v %v", got, e)
			}
			want := 3
			if preparation {
				want = 0
			}
			assertCount(t, pool, "gateway_delivery_attempts", want)
		})
	}
}

func TestCancellationBeforeCommitDoesNotConsumeClaimOrAttempt(t *testing.T) {
	a, _, pool := ledgers(t, pg.Options{})
	in := prepared("cancel")
	accept(t, a, in)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := a.ClaimDue(ctx, claimRequest()); e == nil {
		t.Fatal("canceled claim succeeded")
	}
	c := oneClaim(t, a, claimRequest())
	if _, e := a.MarkCalling(ctx, callRequest(c)); e == nil {
		t.Fatal("canceled A2 succeeded")
	}
	if p := partState(t, a, in.Intent.ID, 0); p.State != domain.Claimed {
		t.Fatalf("canceled A2 changed state: %+v", p)
	}
	assertCount(t, pool, "gateway_delivery_attempts", 0)
}

func TestFinishWaitsForLockThenRechecksCallingDeadline(t *testing.T) {
	a, b, pool := ledgers(t, pg.Options{})
	in := prepared("finish-deadline")
	accept(t, a, in)
	c := oneClaim(t, a, claimRequest())
	r := callRequest(c)
	r.Timeout = 100 * time.Millisecond
	at, e := a.MarkCalling(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `SELECT 1 FROM gateway_delivery_parts WHERE part_id=$1 FOR UPDATE`, at.PartID); e != nil {
		t.Fatal(e)
	}
	var pid int32
	if e = tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- b.Finish(ctx, at, acceptedResult()) }()
	waitDeliveryBlocked(t, ctx, pool, pid)
	time.Sleep(120 * time.Millisecond)
	// Recovery skips the locked row, rather than making the in-flight Finish and
	// recovery hold locks in opposing orders.
	if n, e := a.RecoverStaleCalling(ctx, 100); e != nil || n != 0 {
		t.Fatalf("recovery crossed held part lock: %d %v", n, e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-done; !errors.Is(e, domain.ErrClaimLost) {
		t.Fatalf("Finish reused time before waiting: %v", e)
	}
	if n, e := a.RecoverStaleCalling(ctx, 100); e != nil || n != 1 {
		t.Fatalf("post-lock recovery=%d %v", n, e)
	}
	if e = a.Observe(ctx, observation(at, "late-finish-ack")); e != nil {
		t.Fatal(e)
	}
	if changed, e := a.ResolveObserved(ctx, at.ID); e != nil || !changed {
		t.Fatalf("late result recovery=%v %v", changed, e)
	}
}

func TestCompletedFinishAndCallingGateAreNotReversedByRecovery(t *testing.T) {
	s, _, pool := ledgers(t, pg.Options{})
	in := prepared("finished-first")
	accept(t, s, in)
	c := oneClaim(t, s, claimRequest())
	a := calling(t, s, c)
	if _, e := pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET claim_until=clock_timestamp()-interval '1 second' WHERE part_id=$1`, a.PartID); e != nil {
		t.Fatal(e)
	}
	if n, e := s.RecoverExpiredClaims(context.Background(), 100); e != nil || n != 0 {
		t.Fatalf("claim recovery reversed A2: %d %v", n, e)
	}
	if e := s.Finish(context.Background(), a, acceptedResult()); e != nil {
		t.Fatal(e)
	}
	if _, e := pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET calling_until=clock_timestamp()-interval '1 second' WHERE part_id=$1`, a.PartID); e != nil {
		t.Fatal(e)
	}
	if n, e := s.RecoverStaleCalling(context.Background(), 100); e != nil || n != 0 {
		t.Fatalf("recovery reversed completed Finish: %d %v", n, e)
	}
	if partState(t, s, in.Intent.ID, 0).State != domain.Accepted {
		t.Fatal("accepted fact changed")
	}
}

func TestExpiredPreparationHasNoCallingWindow(t *testing.T) {
	s, _, pool := ledgers(t, pg.Options{})
	in := prepared("prepare-deadline")
	accept(t, s, in)
	c := oneClaim(t, s, claimRequest())
	if e := s.FinishPreparation(context.Background(), c, domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorDeadline}); e != nil {
		t.Fatal(e)
	}
	if partState(t, s, in.Intent.ID, 0).State != domain.Expired {
		t.Fatal("known deadline failure not terminal EXPIRED")
	}
	if got, e := s.ClaimDue(context.Background(), claimRequest()); e != nil || len(got) != 0 {
		t.Fatalf("expired preparation reclaimed=%v %v", got, e)
	}
	assertCount(t, pool, "gateway_delivery_attempts", 0)
}
