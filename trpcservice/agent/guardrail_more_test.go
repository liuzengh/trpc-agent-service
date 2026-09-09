package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// errAuditor fails every synchronous write.
type errAuditor struct {
	fakeAuditor
	syncErr error
}

func (e *errAuditor) LogSync(context.Context, storage.AuditEvent) error { return e.syncErr }

type errBudget struct{}

func (errBudget) Allow(context.Context, string, int64) (bool, error) {
	return false, errors.New("budget store down")
}
func (errBudget) Record(context.Context, string, int64) {}

// A failing approval lookup must abort processing with the error, keeping the
// message pending for redelivery (never silently answering).
func TestGuardedApproverAnswerErrorAborts(t *testing.T) {
	rdb := testenv.Redis(t)
	_ = rdb.Close() // closed client: every approval read fails

	ap := NewApprover(rdb, testRegistry(), time.Minute)
	g := &Guarded{Inner: EchoProcessor{}, Approver: ap}
	_, err := g.Process(context.Background(), testMsg("确认"))
	if err == nil {
		t.Fatal("a failed approval lookup must abort processing")
	}
	if !strings.Contains(err.Error(), "read pending approval") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A consumed approval answer runs the output checks and audits the decision
// synchronously, with the tool result as the reply.
func TestGuardedApprovalAnswerRunsOutputChecks(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:guarded-answer:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(approvalScope{"test-app", sessionKey})) })

	if _, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`)); err != nil {
		t.Fatal(err)
	}

	aud := &fakeAuditor{}
	redacted := false
	g := &Guarded{
		Inner:    EchoProcessor{},
		Approver: ap,
		Auditor:  aud,
		Output: []OutputChecker{func(_ context.Context, _ channels.InboundMessage, text string) string {
			redacted = strings.Contains(text, "已执行")
			return text + " [checked]"
		}},
	}
	msg := testMsg("确认")
	msg.SessionKey = sessionKey
	out, err := g.Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !redacted {
		t.Fatalf("output checks must run on the tool-result reply: %q", out.Text)
	}
	if !strings.HasSuffix(out.Text, "[checked]") {
		t.Fatalf("output checker result must be kept: %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "allow" || evs[0].ToolName != "op_a" {
		t.Fatalf("want sync allow/op_a audit, got %+v", evs)
	}
}

// A failing budget check fails open: the message proceeds instead of wedging.
func TestGuardedBudgetCheckErrorFailsOpen(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:     usageProcessor{},
		Auditor:   aud,
		Budget:    errBudget{},
		PolicyFor: policyStub(tenant.GuardrailPolicy{MaxTokensPerDay: 1000}),
	}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "ok" {
		t.Fatalf("budget check failure must fail open, got %q", out.Text)
	}
	if n := len(aud.asyncDecisions()); n != 1 {
		t.Fatalf("want the routine allow audit, got %d", n)
	}
}

// A failing tenant-policy lookup degrades to the platform baseline.
func TestGuardedPolicyLookupErrorDegradesToBaseline(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		PolicyFor: func(context.Context, string) (tenant.GuardrailPolicy, error) {
			return tenant.GuardrailPolicy{}, errors.New("policy store down")
		},
		Input: []InputChecker{SensitiveWordInput(DefaultBlockedWords)},
	}
	// The baseline words still apply; the (unresolvable) tenant word list is
	// simply unavailable.
	if _, err := g.Process(context.Background(), testMsg("来赌博吧")); err != nil {
		t.Fatal(err)
	}
	if n := len(aud.syncDecisions()); n != 1 {
		t.Fatalf("baseline deny must still fire, got %d sync events", n)
	}
}

// A recall without a state-mark sink still audits and answers empty.
func TestGuardedRecallWithoutStateMark(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{Inner: EchoProcessor{}, Auditor: aud}
	msg := testMsg("")
	msg.Type = channels.TypeRecall
	msg.MsgID = "recall:m-7"
	out, err := g.Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "" {
		t.Fatalf("recall must not produce a reply, got %q", out.Text)
	}
	if evs := aud.syncDecisions(); len(evs) != 1 || evs[0].Decision != "recall" {
		t.Fatalf("want sync recall audit, got %+v", evs)
	}
}

// A failing state mark is logged and does not fail the recall.
func TestGuardedRecallStateMarkErrorIsLogged(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		StateMark: func(context.Context, channels.InboundMessage, string, []byte) error {
			return errors.New("session store down")
		},
	}
	msg := testMsg("")
	msg.Type = channels.TypeRecall
	msg.MsgID = "recall:m-8"
	if _, err := g.Process(context.Background(), msg); err != nil {
		t.Fatalf("state-mark failure must not fail the recall, got %v", err)
	}
	if evs := aud.syncDecisions(); len(evs) != 1 || evs[0].Decision != "recall" {
		t.Fatalf("recall audit must survive the state-mark failure, got %+v", evs)
	}
}

// syncAudit with an empty decision (nothing to audit) writes nothing.
func TestSyncAuditSkipsEmptyDecision(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{Inner: EchoProcessor{}, Auditor: aud}
	g.syncAudit(testMsg("hello"), auditDecision{})
	if n := len(aud.synced) + len(aud.async); n != 0 {
		t.Fatalf("empty decision must audit nothing, got %d events", n)
	}
}

// A failed synchronous audit write is logged, never dropped silently.
func TestSyncAuditLogsWriteErrors(t *testing.T) {
	aud := &errAuditor{syncErr: errors.New("audit store down")}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		Input:   []InputChecker{SensitiveWordInput(DefaultBlockedWords)},
	}
	// Must not panic; the deny path continues to return its reply.
	out, err := g.Process(context.Background(), testMsg("来赌博吧"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "受限内容") {
		t.Fatalf("deny reply must still be produced, got %q", out.Text)
	}
}

// asyncModelAudit without an auditor is a no-op.
func TestAsyncModelAuditWithoutAuditor(t *testing.T) {
	g := &Guarded{Inner: EchoProcessor{}}
	g.asyncModelAudit(testMsg("hello"), channels.OutboundMessage{}, time.Now(),
		&ModelError{Err: errors.New("boom")})
}

// A non-timeout model failure is audited as model_error (vs model_timeout).
func TestAsyncModelAuditErrorType(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{Inner: EchoProcessor{}, Auditor: aud}
	g.asyncModelAudit(testMsg("hello"), channels.OutboundMessage{}, time.Now(),
		&ModelError{Err: errors.New("rate limited")})
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].ErrorType != "model_error" {
		t.Fatalf("want model_error audit, got %+v", evs)
	}
}
