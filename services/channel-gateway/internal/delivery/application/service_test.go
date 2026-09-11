package application_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type acceptanceLedger struct {
	app.Ledger
	receipt  d.Receipt
	digest   string
	found    bool
	accepted int
	prepared d.Prepared
	findErr  error
}

func (l *acceptanceLedger) Find(context.Context, string) (d.Receipt, string, bool, error) {
	return l.receipt, l.digest, l.found, l.findErr
}
func (l *acceptanceLedger) Accept(_ context.Context, p d.Prepared) (d.Receipt, error) {
	l.accepted++
	l.prepared = p
	return d.Receipt{IntentID: p.Intent.ID, RunID: p.Intent.RunID, PartCount: len(p.Parts)}, nil
}

type reader struct {
	target d.Target
	calls  int
	err    error
}

func (r *reader) ReadReplyTarget(context.Context, string, string) (d.Target, error) {
	r.calls++
	return r.target, r.err
}

type verifier struct {
	auth  app.FinalAuthorization
	err   error
	calls int
}

func (v *verifier) VerifyCommittedFinal(context.Context, d.Intent, string) (app.FinalAuthorization, error) {
	v.calls++
	return v.auth, v.err
}
func input() d.Intent {
	return d.Intent{ID: "intent-1", AdmissionID: "admission-1", RunID: "run-1", AttemptID: "attempt-1", CompletionID: "completion-1", ExecutionGeneration: 1, Sequence: 1, Text: "hello", Deadline: time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func target() d.Target {
	return d.Target{TenantID: "tenant-1", Provider: "telegram", AccountID: "account-1", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "123", SourceEventID: "1", SourceMessageID: "2", ReceivedAt: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)}
}
func authorized(i d.Intent, digest string, t d.Target) app.FinalAuthorization {
	return app.FinalAuthorization{IntentID: i.ID, Digest: digest, AdmissionID: i.AdmissionID, RunID: i.RunID, AttemptID: i.AttemptID, CompletionID: i.CompletionID, ExecutionGeneration: i.ExecutionGeneration, Sequence: i.Sequence, TenantID: t.TenantID, ManifestDigest: t.ManifestDigest}
}

func TestAcceptorRequiresCommittedExecutionVerifier(t *testing.T) {
	if _, err := app.NewAcceptor(&acceptanceLedger{}, &reader{}, nil, app.AcceptOptions{}); err == nil {
		t.Fatal("missing execution owner was treated as authorization")
	}
}
func TestAcceptorRejectsUncommittedFinalWithoutWriting(t *testing.T) {
	l := &acceptanceLedger{}
	r := &reader{target: target()}
	v := &verifier{err: d.ErrUnauthorized}
	s, err := app.NewAcceptor(l, r, v, app.AcceptOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.AcceptReplyIntent(context.Background(), input()); !errors.Is(err, d.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
	if l.accepted != 0 {
		t.Fatal("unverified Final created delivery facts")
	}
}

func TestAcceptorUsesAdmissionTargetAndExactCommittedProof(t *testing.T) {
	i := input()
	digest, err := d.IntentDigest(i)
	if err != nil {
		t.Fatal(err)
	}
	l := &acceptanceLedger{}
	r := &reader{target: target()}
	v := &verifier{auth: authorized(i, digest, r.target)}
	s, err := app.NewAcceptor(l, r, v, app.AcceptOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.AcceptReplyIntent(context.Background(), i)
	if err != nil {
		t.Fatal(err)
	}
	if got.IntentID != i.ID || got.PartCount != 1 || l.accepted != 1 || l.prepared.Target.ConversationID != "123" || l.prepared.Digest != digest {
		t.Fatalf("wrong immutable preparation: %+v %+v", got, l.prepared)
	}
}
func TestAcceptorChecksEveryProofIdentity(t *testing.T) {
	changes := []func(*app.FinalAuthorization){func(p *app.FinalAuthorization) { p.IntentID = "other" }, func(p *app.FinalAuthorization) { p.Digest = strings.Repeat("b", 64) }, func(p *app.FinalAuthorization) { p.AdmissionID = "other" }, func(p *app.FinalAuthorization) { p.RunID = "other" }, func(p *app.FinalAuthorization) { p.AttemptID = "other" }, func(p *app.FinalAuthorization) { p.CompletionID = "other" }, func(p *app.FinalAuthorization) { p.ExecutionGeneration++ }, func(p *app.FinalAuthorization) { p.Sequence++ }, func(p *app.FinalAuthorization) { p.TenantID = "other" }, func(p *app.FinalAuthorization) { p.ManifestDigest = "sha256:" + strings.Repeat("b", 64) }}
	for n, change := range changes {
		t.Run(string(rune('a'+n)), func(t *testing.T) {
			i := input()
			digest, _ := d.IntentDigest(i)
			p := authorized(i, digest, target())
			change(&p)
			l := &acceptanceLedger{}
			s, _ := app.NewAcceptor(l, &reader{target: target()}, &verifier{auth: p}, app.AcceptOptions{})
			if _, err := s.AcceptReplyIntent(context.Background(), i); !errors.Is(err, d.ErrUnauthorized) || l.accepted != 0 {
				t.Fatalf("proof mismatch accepted: %v", err)
			}
		})
	}
}
func TestDurableReceiptReplaysWithoutDynamicDependenciesEvenAfterStop(t *testing.T) {
	i := input()
	i.Deadline = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	digest, err := d.IntentDigest(i)
	if err != nil {
		t.Fatal(err)
	}
	l := &acceptanceLedger{found: true, digest: digest, receipt: d.Receipt{IntentID: i.ID, RunID: i.RunID, PartCount: 1}}
	r := &reader{err: d.ErrUnavailable}
	v := &verifier{err: d.ErrUnavailable}
	s, _ := app.NewAcceptor(l, r, v, app.AcceptOptions{})
	s.Stop()
	if _, err = s.AcceptReplyIntent(context.Background(), i); err != nil || r.calls != 0 || v.calls != 0 || l.accepted != 0 {
		t.Fatalf("durable replay touched mutable dependencies: %v", err)
	}
	i.Text = "different"
	if _, err = s.AcceptReplyIntent(context.Background(), i); !errors.Is(err, d.ErrConflict) {
		t.Fatalf("changed intent reused receipt: %v", err)
	}
}
func TestReceiptLookupFailureIsNotConvertedToSuccess(t *testing.T) {
	l := &acceptanceLedger{found: true, findErr: d.ErrUnavailable}
	s, _ := app.NewAcceptor(l, &reader{}, &verifier{}, app.AcceptOptions{})
	if _, err := s.AcceptReplyIntent(context.Background(), input()); !errors.Is(err, d.ErrUnavailable) || l.accepted != 0 {
		t.Fatalf("database failure hidden: %v", err)
	}
}
