package application_test

import (
	"context"
	"errors"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"testing"
	"time"
)

type dispatchLedger struct {
	app.Ledger
	claims                                  []d.Claim
	markErr, finishErr, observeErr          error
	marked, finished, observed, preparation int
	result                                  d.Result
	finishCtxErr                            error
	events                                  []string
}

func (l *dispatchLedger) ClaimDue(context.Context, d.ClaimRequest) ([]d.Claim, error) {
	return l.claims, nil
}
func (l *dispatchLedger) MarkCalling(_ context.Context, r d.CallingRequest) (d.Attempt, error) {
	l.marked++
	l.events = append(l.events, "mark")
	if l.markErr != nil {
		return d.Attempt{}, l.markErr
	}
	return d.Attempt{ID: "send-1", PartID: r.Claim.Part.ID, IntentID: r.Claim.Intent.ID, Number: 1, ClaimToken: r.Claim.Token, InstanceID: r.Claim.InstanceID, Owner: r.Claim.Owner, RequestID: r.RequestID, RequestDigest: r.RequestDigest, EvidenceToken: "private-evidence", CallingUntil: time.Now().Add(time.Second), Intent: r.Claim.Intent, Target: r.Claim.Target, Text: r.Claim.Part.Text}, nil
}
func (l *dispatchLedger) FinishPreparation(_ context.Context, _ d.Claim, r d.Result) error {
	l.preparation++
	l.result = r
	return nil
}
func (l *dispatchLedger) Observe(_ context.Context, o d.Observation) error {
	l.observed++
	l.events = append(l.events, "observe")
	l.result = o.Result
	return l.observeErr
}
func (l *dispatchLedger) Finish(ctx context.Context, _ d.Attempt, r d.Result) error {
	l.finished++
	l.events = append(l.events, "finish")
	l.result = r
	l.finishCtxErr = ctx.Err()
	return l.finishErr
}

type senders struct {
	handle   *reserved
	err      error
	reserves int
}

func (s *senders) Reserve(context.Context, app.SendRequest) (app.ReservedSender, error) {
	s.reserves++
	if s.err != nil {
		return nil, s.err
	}
	return s.handle, nil
}

type reserved struct {
	calls, releases int
	result          d.Result
	during          func()
}

func (s *reserved) SendFinal(context.Context, d.Attempt) d.Result {
	s.calls++
	if s.during != nil {
		s.during()
	}
	return s.result
}
func (s *reserved) Release() { s.releases++ }
func claim() d.Claim {
	i := input()
	return d.Claim{Part: d.Part{ID: d.PartID(i.ID, 0), IntentID: i.ID, Index: 0, Text: i.Text, State: d.Claimed}, Intent: i, Target: target(), Token: "claim-1", InstanceID: "instance-1", ExpiresAt: time.Now().Add(time.Minute)}
}
func TestDispatcherNeverSendsWhenA2CommitFails(t *testing.T) {
	l := &dispatchLedger{claims: []d.Claim{claim()}, markErr: d.ErrUnavailable}
	h := &reserved{result: d.Result{Certainty: d.CertaintyAccepted}}
	s, err := app.NewDispatcher(l, &senders{handle: h}, app.DispatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DispatchAccount(context.Background(), d.ClaimRequest{}); !errors.Is(err, d.ErrUnavailable) {
		t.Fatalf("lost commit result: %v", err)
	}
	if h.calls != 0 || h.releases != 1 || l.observed != 0 {
		t.Fatalf("A2 failure invoked provider: %+v %+v", h, l)
	}
}
func TestDispatcherPersistsACKBeforeReleasingReservationAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := &dispatchLedger{claims: []d.Claim{claim()}}
	h := &reserved{result: d.Result{Certainty: d.CertaintyAccepted, ProviderMessageID: "42"}, during: cancel}
	s, _ := app.NewDispatcher(l, &senders{handle: h}, app.DispatchOptions{})
	n, err := s.DispatchAccount(ctx, d.ClaimRequest{})
	if err != nil || n != 1 || h.calls != 1 || h.releases != 1 || l.finished != 1 || l.observed != 1 || l.finishCtxErr != nil {
		t.Fatalf("lost ACK on cancellation: %d %v %+v", n, err, l)
	}
	if len(l.events) != 3 || l.events[0] != "mark" || l.events[1] != "observe" || l.events[2] != "finish" {
		t.Fatalf("wrong persistence order: %v", l.events)
	}
}
func TestLateACKObservationSurvivesFinishOwnerLoss(t *testing.T) {
	l := &dispatchLedger{claims: []d.Claim{claim()}, finishErr: d.ErrClaimLost}
	h := &reserved{result: d.Result{Certainty: d.CertaintyAccepted}}
	s, _ := app.NewDispatcher(l, &senders{handle: h}, app.DispatchOptions{})
	if _, err := s.DispatchAccount(context.Background(), d.ClaimRequest{}); !errors.Is(err, d.ErrClaimLost) {
		t.Fatal(err)
	}
	if l.observed != 1 || l.result.Certainty != d.CertaintyAccepted || h.calls != 1 || h.releases != 1 {
		t.Fatalf("lost late ACK: %+v", l)
	}
}
func TestPreparationFailureDoesNotOpenSendGate(t *testing.T) {
	for _, failure := range []error{d.ErrUnavailable, d.ErrNotFound, d.ErrUnsupported} {
		l := &dispatchLedger{claims: []d.Claim{claim()}}
		s, _ := app.NewDispatcher(l, &senders{err: failure}, app.DispatchOptions{})
		if _, err := s.DispatchAccount(context.Background(), d.ClaimRequest{}); err != nil {
			t.Fatal(err)
		}
		if l.marked != 0 || l.preparation != 1 || l.result.Certainty != d.CertaintyNotSent {
			t.Fatalf("preparation failure entered CALLING: %+v", l)
		}
	}
}
func TestMalformedSenderResultIsUnknownNeverSuccess(t *testing.T) {
	l := &dispatchLedger{claims: []d.Claim{claim()}}
	h := &reserved{result: d.Result{Certainty: "fictional_success"}}
	s, _ := app.NewDispatcher(l, &senders{handle: h}, app.DispatchOptions{})
	if _, err := s.DispatchAccount(context.Background(), d.ClaimRequest{}); err != nil {
		t.Fatal(err)
	}
	if l.result.Certainty != d.CertaintyUnknown {
		t.Fatalf("invalid certainty accepted: %+v", l.result)
	}
}
func TestEvidenceFailureDoesNotPretendFinishSucceeded(t *testing.T) {
	l := &dispatchLedger{claims: []d.Claim{claim()}, observeErr: d.ErrUnavailable}
	h := &reserved{result: d.Result{Certainty: d.CertaintyAccepted}}
	s, _ := app.NewDispatcher(l, &senders{handle: h}, app.DispatchOptions{EvidenceTimeout: time.Second})
	if _, err := s.DispatchAccount(context.Background(), d.ClaimRequest{}); !errors.Is(err, d.ErrUnavailable) {
		t.Fatal(err)
	}
	if l.observed != 3 || l.finished != 0 || h.calls != 1 || h.releases != 1 {
		t.Fatalf("unbounded or fabricated persistence: %+v", l)
	}
}

// reserveFunc permits anomalous port outcomes explicitly; the ordinary sender
// fixture deliberately returns nil on error and cannot exercise partial setup.
type reserveFunc func(context.Context, app.SendRequest) (app.ReservedSender, error)

func (f reserveFunc) Reserve(ctx context.Context, r app.SendRequest) (app.ReservedSender, error) {
	return f(ctx, r)
}

func TestDispatcherReleasesPartialReservationOnPreparationFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reserveErr error
		class      d.ErrorClass
	}{
		{"temporary", d.ErrUnavailable, d.ErrorTemporary},
		{"stale origin", d.ErrNotFound, d.ErrorStaleOrigin},
		{"unsupported", d.ErrUnsupported, d.ErrorPermanent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &dispatchLedger{claims: []d.Claim{claim()}}
			h := &reserved{result: d.Result{Certainty: d.CertaintyAccepted}}
			provider := reserveFunc(func(context.Context, app.SendRequest) (app.ReservedSender, error) { return h, tc.reserveErr })
			s, err := app.NewDispatcher(l, provider, app.DispatchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			n, err := s.DispatchAccount(context.Background(), d.ClaimRequest{})
			if err != nil || n != 1 || h.releases != 1 || h.calls != 0 || l.preparation != 1 || l.marked != 0 || l.observed != 0 || l.finished != 0 {
				t.Fatalf("partial reservation cleanup/preparation: processed=%d err=%v handle=%+v ledger=%+v", n, err, h, l)
			}
			if l.result != (d.Result{Certainty: d.CertaintyNotSent, ErrorClass: tc.class}) {
				t.Fatalf("changed preparation certainty: %+v", l.result)
			}
		})
	}
}

// The persistence port owns the preparation budget. This fixture exposes the
// same externally visible contract: charged attempts eventually stop claiming.
type preparationBudgetLedger struct {
	*dispatchLedger
	limit int
}

func (l *preparationBudgetLedger) ClaimDue(context.Context, d.ClaimRequest) ([]d.Claim, error) {
	if l.preparation >= l.limit {
		return nil, nil
	}
	return l.claims, nil
}
func TestDispatcherNilReservationConsumesPreparationBudget(t *testing.T) {
	l := &preparationBudgetLedger{dispatchLedger: &dispatchLedger{claims: []d.Claim{claim()}}, limit: 3}
	reserves := 0
	provider := reserveFunc(func(context.Context, app.SendRequest) (app.ReservedSender, error) { reserves++; return nil, nil })
	s, err := app.NewDispatcher(l, provider, app.DispatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 5; attempt++ {
		n, err := s.DispatchAccount(context.Background(), d.ClaimRequest{})
		want := 0
		if attempt < 3 {
			want = 1
		}
		if err != nil || n != want {
			t.Fatalf("nil reservation failed to settle preparation budget: attempt=%d processed=%d err=%v", attempt, n, err)
		}
	}
	if reserves != 3 || l.preparation != 3 || l.marked != 0 || l.observed != 0 || l.finished != 0 || l.result != (d.Result{Certainty: d.CertaintyNotSent, ErrorClass: d.ErrorTemporary}) {
		t.Fatalf("nil reservation left endlessly reclaimable work: reserves=%d ledger=%+v", reserves, l.dispatchLedger)
	}
}

type failedPreparationLedger struct{ *dispatchLedger }

func (l failedPreparationLedger) FinishPreparation(ctx context.Context, c d.Claim, r d.Result) error {
	_ = l.dispatchLedger.FinishPreparation(ctx, c, r)
	return d.ErrUnavailable
}
func TestDispatcherReleasesPartialReservationWhenPreparationCommitFails(t *testing.T) {
	l := failedPreparationLedger{&dispatchLedger{claims: []d.Claim{claim()}}}
	h := &reserved{}
	provider := reserveFunc(func(context.Context, app.SendRequest) (app.ReservedSender, error) { return h, d.ErrUnavailable })
	s, err := app.NewDispatcher(l, provider, app.DispatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.DispatchAccount(context.Background(), d.ClaimRequest{})
	if n != 0 || !errors.Is(err, d.ErrUnavailable) || h.releases != 1 || h.calls != 0 || l.preparation != 1 || l.marked != 0 || l.observed != 0 || l.finished != 0 {
		t.Fatalf("partial setup leaked after failed preparation commit: n=%d err=%v releases=%d calls=%d preparation=%d", n, err, h.releases, h.calls, l.preparation)
	}
}
