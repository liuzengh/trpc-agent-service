package wecomadapter_test

import (
	"context"
	adapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/wecomadapter"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type source struct{ s *session }

func (s source) ReserveOriginal(context.Context, d.Target) (adapter.Session, error) { return s.s, nil }

type session struct{ calls, releases atomic.Int32 }

func (s *session) SendFinal(context.Context, string, string) d.Result {
	s.calls.Add(1)
	return d.Result{Certainty: d.CertaintyAccepted}
}
func (s *session) Release() { s.releases.Add(1) }
func request() (app.SendRequest, d.Attempt) {
	i := d.Intent{ID: "i", AdmissionID: "a", RunID: "r", AttemptID: "e", CompletionID: "c", ExecutionGeneration: 1, Sequence: 1, Text: "hello", Deadline: time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)}
	t := d.Target{TenantID: "tenant", Provider: "wecom", AccountID: "account", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "user", SourceEventID: "message", CallbackRequestID: "request", ReceivedAt: time.Now().UTC(), Origin: &d.ReplyOrigin{InstanceID: "instance", Epoch: 1, Revision: 1, SocketGeneration: 1}}
	c := d.Claim{Part: d.Part{ID: d.PartID(i.ID, 0), IntentID: i.ID, Index: 0, Text: i.Text, State: d.Claimed}, Intent: i, Target: t, Token: "claim", InstanceID: "instance", Owner: &d.OwnerFence{InstanceID: "instance", Epoch: 1, Revision: 1}}
	digest, _ := d.RequestDigest(c)
	r := app.SendRequest{Claim: c, RequestID: t.CallbackRequestID, RequestDigest: digest}
	a := d.Attempt{ID: "attempt", PartID: c.Part.ID, IntentID: i.ID, ClaimToken: c.Token, InstanceID: c.InstanceID, Number: 1, Owner: c.Owner, RequestID: r.RequestID, RequestDigest: digest, EvidenceToken: "evidence", Intent: i, Target: t, Text: i.Text}
	return r, a
}
func TestReservationHasNoSendAndOneConcurrentFinal(t *testing.T) {
	s := &session{}
	p, _ := adapter.NewProvider(source{s})
	r, a := request()
	h, err := p.Reserve(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if s.calls.Load() != 0 {
		t.Fatal("reserve sent")
	}
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if h.SendFinal(context.Background(), a).Certainty == d.CertaintyAccepted {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	h.Release()
	h.Release()
	if accepted.Load() != 1 || s.calls.Load() != 1 || s.releases.Load() != 1 {
		t.Fatal("not one-shot")
	}
}
func TestChangedAttemptCannotUseOriginalReservation(t *testing.T) {
	changes := []func(*d.Attempt){func(a *d.Attempt) { a.ClaimToken = "new-claim" }, func(a *d.Attempt) { a.Text = "changed" }, func(a *d.Attempt) { a.Target.CallbackRequestID = "new-request" }, func(a *d.Attempt) { a.RequestDigest = "sha256:" + strings.Repeat("b", 64) }, func(a *d.Attempt) { a.EvidenceToken = "" }}
	for _, change := range changes {
		s := &session{}
		p, _ := adapter.NewProvider(source{s})
		r, a := request()
		h, err := p.Reserve(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		change(&a)
		got := h.SendFinal(context.Background(), a)
		h.Release()
		if got.Certainty != d.CertaintyNotSent || s.calls.Load() != 0 {
			t.Fatalf("changed Attempt sent: %+v", got)
		}
	}
}
func TestReleaseOrCancellationBeforeSendIsNotSent(t *testing.T) {
	for _, release := range []bool{true, false} {
		s := &session{}
		p, _ := adapter.NewProvider(source{s})
		r, a := request()
		h, _ := p.Reserve(context.Background(), r)
		ctx, cancel := context.WithCancel(context.Background())
		if release {
			h.Release()
		} else {
			cancel()
		}
		got := h.SendFinal(ctx, a)
		cancel()
		h.Release()
		if got.Certainty != d.CertaintyNotSent || s.calls.Load() != 0 {
			t.Fatal("released/canceled handle sent")
		}
	}
}
func TestResultMappingKeepsUnknownAndPoisonedNonRetryable(t *testing.T) {
	for _, tc := range []struct {
		certainty, code string
		want            d.Certainty
		retry           bool
	}{{"ACCEPTED", "", d.CertaintyAccepted, false}, {"UNKNOWN", "canceled", d.CertaintyUnknown, false}, {"NOT_SENT", "request_poisoned", d.CertaintyNotSent, false}, {"NOT_SENT", "final_already_attempted", d.CertaintyNotSent, false}, {"NOT_SENT", "not_ready", d.CertaintyNotSent, true}, {"ACCEPTED", "invalid_request", d.CertaintyUnknown, false}} {
		got := adapter.FromConnectionResult(tc.certainty, tc.code, nil)
		_, retry := d.RetryDelay(got, 1, time.Now(), time.Now().Add(time.Hour))
		if got.Validate() != nil || got.Certainty != tc.want || retry != tc.retry {
			t.Fatalf("bad certainty mapping %+v -> %+v", tc, got)
		}
	}
}

func TestResultMappingDoesNotDiscardContradictoryProviderEvidence(t *testing.T) {
	zero, rejection := int64(0), int64(45009)
	for _, tc := range []struct {
		name, certainty, code string
		providerCode          *int64
		want                  d.Result
		retry                 bool
	}{
		{"accepted-zero", "ACCEPTED", "", &zero, d.Result{Certainty: d.CertaintyAccepted}, false},
		{"accepted-rejection-code", "ACCEPTED", "", &rejection, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
		{"not-sent-success-code", "NOT_SENT", "not_ready", &zero, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
		{"not-sent-rejection-code", "NOT_SENT", "capacity_exceeded", &rejection, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
		{"not-sent-no-ack", "NOT_SENT", "not_ready", nil, d.Result{Certainty: d.CertaintyNotSent, ErrorClass: d.ErrorTemporary}, true},
		{"accepted-with-failure-class", "ACCEPTED", "invalid_request", &zero, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
		{"rejected-nonzero", "REJECTED", "provider_rejected", &rejection, d.Result{Certainty: d.CertaintyRejected, ErrorClass: d.ErrorPermanent}, false},
		{"unknown-with-provider-code", "UNKNOWN", "canceled", &rejection, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
		{"unrecognized-code-is-not-propagated", "NOT_SENT", "synthetic-raw-error-not-for-domain", &rejection, d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := adapter.FromConnectionResult(tc.certainty, tc.code, tc.providerCode)
			now := time.Now()
			_, retry := d.RetryDelay(got, 1, now, now.Add(time.Hour))
			if got.Validate() != nil || got != tc.want || retry != tc.retry {
				t.Fatalf("result=%+v retry=%t; want=%+v retry=%t", got, retry, tc.want, tc.retry)
			}
		})
	}
}
