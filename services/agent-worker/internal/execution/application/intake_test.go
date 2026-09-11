package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type intakeRecorder struct {
	Ledger
	limits domain.IntakeLimits
	calls  int
}

func (l *intakeRecorder) Accept(_ context.Context, r domain.Requested, _ domain.Policy, limits domain.IntakeLimits) (domain.Receipt, error) {
	l.limits = limits
	l.calls++
	return domain.Receipt{EventID: r.EventID, RunID: r.RunID}, nil
}
func TestAcceptorRequiresAndPassesExplicitIntakeLimits(t *testing.T) {
	ledger := &intakeRecorder{}
	policy := domain.Policy{Version: "test", MaxRunAge: time.Minute, MaxReplyAge: time.Minute, MaxFutureSkew: time.Second, LeaseTTL: time.Second, RenewalInterval: time.Millisecond, RetryBackoff: time.Millisecond, MaxAttempts: 2}
	for _, limits := range []domain.IntakeLimits{{}, {MaxQueuedRuns: 1}, {MaxRetainedRuns: 1}} {
		if _, err := NewAcceptor(ledger, policy, limits); !errors.Is(err, domain.ErrInvalid) {
			t.Fatal("invalid intake limits accepted", err)
		}
	}
	limits := domain.IntakeLimits{MaxQueuedRuns: 2, MaxRetainedRuns: 7}
	acceptor, err := NewAcceptor(ledger, policy, limits)
	if err != nil {
		t.Fatal(err)
	}
	req := domain.Requested{EventID: "event", RunID: "run", AdmissionID: "admission", EventDigest: domain.Digest([]byte("event")), RunDigest: domain.Digest([]byte("run")), Route: domain.Route{TenantID: "tenant", Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{SenderID: "43", ConversationID: "conversation", Text: "hello", ReceivedAt: time.Now()}}
	receipt, err := acceptor.Accept(context.Background(), req)
	if err != nil || receipt.RunID != req.RunID || ledger.calls != 1 || ledger.limits != limits {
		t.Fatalf("receipt=%+v calls=%d limits=%+v err=%v", receipt, ledger.calls, ledger.limits, err)
	}
}
