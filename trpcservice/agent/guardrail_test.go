package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// fakeAuditor captures audit events without PG.
type fakeAuditor struct {
	mu     sync.Mutex
	async  []storage.AuditEvent
	synced []storage.AuditEvent
}

func (f *fakeAuditor) LogAsync(ev storage.AuditEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.async = append(f.async, ev)
}

func (f *fakeAuditor) LogSync(_ context.Context, ev storage.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synced = append(f.synced, ev)
	return nil
}

func (f *fakeAuditor) syncDecisions() []storage.AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storage.AuditEvent(nil), f.synced...)
}

func (f *fakeAuditor) asyncDecisions() []storage.AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storage.AuditEvent(nil), f.async...)
}

func testMsg(text string) channels.InboundMessage {
	return channels.InboundMessage{
		Channel: "mock", MsgID: "m1", SessionKey: "dm:mock:u1", UserID: "u1",
		Text: text, TenantID: "t1", AppID: "test-app", TraceID: "trace-1",
	}
}

// testRegistry builds two dangerous tools (op_a / op_b) and one safe tool.
func testRegistry() *tool.Registry {
	mk := func(name string) ttool.Tool {
		return function.NewFunctionTool(
			func(_ context.Context, in struct {
				X string `json:"x"`
			}) (map[string]string, error) {
				return map[string]string{"tool": name, "x": in.X}, nil
			},
			function.WithName(name), function.WithDescription("test tool "+name))
	}
	return tool.NewRegistry(
		tool.Tool{Tool: mk("op_a"), Dangerous: true},
		tool.Tool{Tool: mk("op_b"), Dangerous: true},
		tool.Tool{Tool: mk("safe")},
	)
}

func TestGuardedInputDeny(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		Input:   []InputChecker{SensitiveWordInput([]string{"赌博"})},
	}
	out, err := g.Process(context.Background(), testMsg("来赌博吧"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "受限内容") {
		t.Fatalf("want denial reply, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "deny" || evs[0].ErrorType != "sensitive_input" {
		t.Fatalf("want sync deny/sensitive_input, got %+v", evs)
	}
	if evs[0].TenantID != "t1" {
		t.Fatalf("tenant not propagated to audit: %+v", evs[0])
	}
	if n := len(aud.asyncDecisions()); n != 0 {
		t.Fatalf("denied message must not get an allow event, got %d", n)
	}
}

func TestGuardedOutputRedact(t *testing.T) {
	g := &Guarded{
		Inner:  EchoProcessor{},
		Output: []OutputChecker{RedactOutput()},
	}
	out, err := g.Process(context.Background(), testMsg("打我电话 13800138000"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.Text, "13800138000") {
		t.Fatalf("phone number not redacted: %q", out.Text)
	}
}

func TestGuardedPassthroughAuditsAllow(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{Inner: EchoProcessor{}, Auditor: aud}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "echo: hello" {
		t.Fatalf("unexpected reply: %q", out.Text)
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].Decision != "allow" || evs[0].TenantID != "t1" {
		t.Fatalf("want async allow with tenant, got %+v", evs)
	}
}

func TestGuardedSignalReplacesReply(t *testing.T) {
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0) // nil rdb: store disabled, signals work
	ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"1"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{Inner: EchoProcessor{}, Approver: ap, Auditor: aud}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "危险操作") || !strings.Contains(out.Text, "op_a") {
		t.Fatalf("want confirmation notice, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "review" || evs[0].ToolName != "op_a" {
		t.Fatalf("want sync review for op_a, got %+v", evs)
	}
}

// failProcessor always fails with the given error.
type failProcessor struct{ err error }

func (f failProcessor) Process(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
	return channels.OutboundMessage{}, f.err
}

func TestGuardedModelTimeoutDegradesToBusyReply(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   failProcessor{err: &ModelError{Err: context.DeadlineExceeded}},
		Auditor: aud,
	}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatalf("model failures degrade to a reply, got err %v", err)
	}
	if out.Text != degradedReply {
		t.Fatalf("want busy reply, got %q", out.Text)
	}
	// The degraded reply must keep the routing fields for the sender.
	if out.SessionKey != "dm:mock:u1" || out.UserID != "u1" {
		t.Fatalf("routing fields lost: %+v", out)
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].Decision != "allow" || evs[0].ErrorType != "model_timeout" {
		t.Fatalf("want async allow/model_timeout, got %+v", evs)
	}
}

func TestGuardedModelErrorDegradesToBusyReply(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   failProcessor{err: &ModelError{Err: errors.New("boom")}},
		Auditor: aud,
	}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != degradedReply {
		t.Fatalf("want busy reply, got %q", out.Text)
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].ErrorType != "model_error" {
		t.Fatalf("want model_error audit, got %+v", evs)
	}
}

func TestGuardedInfraErrorPropagates(t *testing.T) {
	// Non-model failures keep the message pending for redelivery: the error
	// propagates to the worker, which does not ack.
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   failProcessor{err: errors.New("pg down")},
		Auditor: aud,
	}
	_, err := g.Process(context.Background(), testMsg("hello"))
	if err == nil {
		t.Fatal("infra errors must propagate")
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].ErrorType != "process_error" {
		t.Fatalf("want process_error audit, got %+v", evs)
	}
}

func TestGuardedModelErrorWithPendingSignalDeliversConfirmation(t *testing.T) {
	// A dangerous call was intercepted during the failed run: the pending
	// approval is real, so the confirmation notice wins over the busy reply.
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0)
	ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"1"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{
		Inner:    failProcessor{err: &ModelError{Err: errors.New("boom")}},
		Approver: ap,
		Auditor:  aud,
	}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "危险操作") || !strings.Contains(out.Text, "op_a") {
		t.Fatalf("want confirmation notice, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "review" {
		t.Fatalf("want sync review, got %+v", evs)
	}
}

// usageProcessor returns a fixed reply with token usage, as RunnerProcessor
// does after a real run.
type usageProcessor struct{}

func (usageProcessor) Process(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	out := channels.OutboundMessage{
		Channel: msg.Channel, MsgID: msg.MsgID, SessionKey: msg.SessionKey,
		UserID: msg.UserID, ChatID: msg.ChatID, TenantID: msg.TenantID,
		Text: "ok", PromptTokens: 1000, CompletionTokens: 500, Model: "m-test",
	}
	return out, nil
}

func TestGuardedAuditCarriesUsageAndCost(t *testing.T) {
	SetModelPricing(map[string][2]float64{"m-test": {1.0, 2.0}}) // $1/$2 per 1M tokens
	t.Cleanup(func() { SetModelPricing(nil) })

	aud := &fakeAuditor{}
	g := &Guarded{Inner: usageProcessor{}, Auditor: aud}
	if _, err := g.Process(context.Background(), testMsg("hello")); err != nil {
		t.Fatal(err)
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 {
		t.Fatalf("want 1 audit event, got %d", len(evs))
	}
	ev := evs[0]
	if ev.PromptTokens != 1000 || ev.CompletionTokens != 500 {
		t.Fatalf("token usage missing from audit: %+v", ev)
	}
	// 1000*$1 + 500*$2 per 1M = $0.002
	if ev.Cost < 0.0019 || ev.Cost > 0.0021 {
		t.Fatalf("cost miscalculated: %+v", ev)
	}
}

// A recall event is audited (sync, with the recalled message id in detail)
// and marked in session state; it never reaches the inner processor and the
// reply is empty (the worker acks without an outbound hop).
func TestGuardedRecall(t *testing.T) {
	aud := &fakeAuditor{}
	marked := map[string][]byte{}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		StateMark: func(_ context.Context, msg channels.InboundMessage, key string, value []byte) error {
			marked[key] = value
			return nil
		},
	}
	msg := testMsg("")
	msg.Type = channels.TypeRecall
	msg.MsgID = "recall:m-42"
	out, err := g.Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "" {
		t.Fatalf("recall must not produce a reply, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "recall" {
		t.Fatalf("want sync recall audit, got %+v", evs)
	}
	if !strings.Contains(string(evs[0].Detail), "m-42") {
		t.Fatalf("audit detail must carry the recalled message id: %s", evs[0].Detail)
	}
	if _, ok := marked["recalled:m-42"]; !ok {
		t.Fatalf("session state not marked: %v", marked)
	}
	if n := len(aud.asyncDecisions()); n != 0 {
		t.Fatalf("recall must not get a routine allow event, got %d", n)
	}
}

// fakeBudget is a BudgetGate stub.
type fakeBudget struct {
	allowed  bool
	recorded map[string]int64
}

func (f *fakeBudget) Allow(context.Context, string, int64) (bool, error) { return f.allowed, nil }
func (f *fakeBudget) Record(_ context.Context, tenantID string, tokens int64) {
	f.recorded[tenantID] += tokens
}

// policyStub returns one fixed policy.
func policyStub(p tenant.GuardrailPolicy) func(context.Context, string) (tenant.GuardrailPolicy, error) {
	return func(context.Context, string) (tenant.GuardrailPolicy, error) { return p, nil }
}

func TestGuardedUserAllowlist(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		PolicyFor: policyStub(tenant.GuardrailPolicy{
			InputAllowUsers: []string{"vip-user"},
		}),
	}
	// A user outside the allowlist is denied before the model runs.
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "权限") {
		t.Fatalf("want permission denial, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "deny" || evs[0].ErrorType != "user_not_allowed" {
		t.Fatalf("want sync deny/user_not_allowed, got %+v", evs)
	}

	// The allowed user passes.
	msg := testMsg("hello")
	msg.UserID = "vip-user"
	if _, err := g.Process(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if n := len(aud.asyncDecisions()); n != 1 {
		t.Fatalf("allowed user must get the allow event, got %d", n)
	}
}

func TestGuardedTenantDenyWordsReplaceBaseline(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		// Baseline blocks the platform word; the tenant list replaces it with a custom word.
		Input:     []InputChecker{SensitiveWordInput(DefaultBlockedWords)},
		PolicyFor: policyStub(tenant.GuardrailPolicy{InputDenyWords: []string{"芒果"}}),
	}
	if _, err := g.Process(context.Background(), testMsg("来赌博吧")); err != nil {
		t.Fatal(err)
	}
	if n := len(aud.syncDecisions()); n != 0 {
		t.Fatal("baseline word must NOT fire when the tenant overrides the list")
	}
	out, err := g.Process(context.Background(), testMsg("想吃芒果"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "受限内容") {
		t.Fatalf("tenant deny word must block, got %q", out.Text)
	}
}

func TestGuardedOutputDenyWords(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:     EchoProcessor{},
		Auditor:   aud,
		PolicyFor: policyStub(tenant.GuardrailPolicy{OutputDenyWords: []string{"内部"}}),
	}
	out, err := g.Process(context.Background(), testMsg("这是内部消息"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "受限内容") {
		t.Fatalf("output deny word must replace the reply, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].ErrorType != "sensitive_output" {
		t.Fatalf("want sync deny/sensitive_output, got %+v", evs)
	}
}

func TestGuardedBudgetGate(t *testing.T) {
	aud := &fakeAuditor{}
	fb := &fakeBudget{allowed: false, recorded: map[string]int64{}}
	g := &Guarded{
		Inner:     usageProcessor{},
		Auditor:   aud,
		Budget:    fb,
		PolicyFor: policyStub(tenant.GuardrailPolicy{MaxTokensPerDay: 1000}),
	}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "上限") {
		t.Fatalf("want budget denial, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].ErrorType != "budget_exceeded" {
		t.Fatalf("want sync deny/budget_exceeded, got %+v", evs)
	}

	// Under budget: the run proceeds and tokens are recorded.
	fb.allowed = true
	if _, err := g.Process(context.Background(), testMsg("hello")); err != nil {
		t.Fatal(err)
	}
	if fb.recorded["t1"] != 1500 {
		t.Fatalf("want 1500 tokens recorded for t1, got %v", fb.recorded)
	}
}

// Exactly one terminal audit per message: a denied reply must carry exactly
// one sync deny row and no async allow row.
func TestGuardedOutputDenySingleAudit(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:     EchoProcessor{},
		Auditor:   aud,
		PolicyFor: policyStub(tenant.GuardrailPolicy{OutputDenyWords: []string{"内部"}}),
	}
	out, err := g.Process(context.Background(), testMsg("这是内部消息"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "受限内容") {
		t.Fatalf("output deny word must replace the reply, got %q", out.Text)
	}
	if evs := aud.syncDecisions(); len(evs) != 1 || evs[0].Decision != "deny" {
		t.Fatalf("want exactly one sync deny, got %+v", evs)
	}
	if evs := aud.asyncDecisions(); len(evs) != 0 {
		t.Fatalf("deny path must not leave an async allow audit, got %+v", evs)
	}
}

// The interception notice embeds the raw tool arguments, which no model-side
// filter ever saw, so it rides the platform redaction checkers like any reply.
func TestGuardedSignalReplyIsRedacted(t *testing.T) {
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0) // nil rdb: store disabled, signals work
	ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"13800138000"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{
		Inner: EchoProcessor{}, Approver: ap, Auditor: aud,
		Output: []OutputChecker{RedactOutput()},
	}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "危险操作") || !strings.Contains(out.Text, "op_a") {
		t.Fatalf("want the confirmation notice, got %q", out.Text)
	}
	if strings.Contains(out.Text, "13800138000") {
		t.Fatalf("the phone number in the tool args must be redacted, got %q", out.Text)
	}
	// The interception is still audited as a review.
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "review" || evs[0].ToolName != "op_a" {
		t.Fatalf("want one sync review for op_a, got %+v", evs)
	}
}

// The notice also rides the tenant's output denylist. A hit replaces it and
// becomes the message's single terminal audit, naming the intercepted tool:
// the review row would otherwise be written for a reply the user never got.
func TestGuardedSignalReplyHitsOutputDenyWords(t *testing.T) {
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0)
	ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"内部名单"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{
		Inner: EchoProcessor{}, Approver: ap, Auditor: aud,
		PolicyFor: policyStub(tenant.GuardrailPolicy{OutputDenyWords: []string{"内部"}}),
	}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != outputDeniedReply {
		t.Fatalf("the notice must trip the tenant denylist, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "deny" ||
		evs[0].ErrorType != "sensitive_output" || evs[0].ToolName != "op_a" {
		t.Fatalf("want one sync deny/sensitive_output naming op_a, got %+v", evs)
	}
	if n := len(aud.asyncDecisions()); n != 0 {
		t.Fatalf("the deny path must not leave an async allow row, got %d", n)
	}
}

// The degraded path composes the same notice after a model failure, so it is
// filtered the same way.
func TestGuardedModelErrorSignalReplyIsFiltered(t *testing.T) {
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0)
	ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"内部名单"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{
		Inner:     failProcessor{err: &ModelError{Err: errors.New("boom")}},
		Approver:  ap,
		Auditor:   aud,
		PolicyFor: policyStub(tenant.GuardrailPolicy{OutputDenyWords: []string{"内部"}}),
	}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != outputDeniedReply {
		t.Fatalf("the degraded-path notice must be filtered too, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "deny" || evs[0].ToolName != "op_a" {
		t.Fatalf("want one sync deny naming op_a, got %+v", evs)
	}
}

// The approval answer embeds the tool's raw result, so it rides the tenant
// output denylist as well as the platform checkers. The tool really ran, so the
// execution decision stays the audit row and the redaction folds into it
// instead of adding a second terminal audit for one message.
func TestGuardedApprovalAnswerHitsOutputDenyWords(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:approval:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(approvalScope{"test-app", sessionKey})) })

	if _, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"内部名单"}`)); err != nil {
		t.Fatal(err)
	}
	_, _ = ap.TakeSignal(approvalScope{"test-app", sessionKey})

	aud := &fakeAuditor{}
	g := &Guarded{
		Inner: EchoProcessor{}, Approver: ap, Auditor: aud,
		PolicyFor: policyStub(tenant.GuardrailPolicy{OutputDenyWords: []string{"内部"}}),
	}
	msg := testMsg("确认")
	msg.SessionKey = sessionKey
	out, err := g.Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != outputDeniedReply {
		t.Fatalf("the tool result must not reach the chat, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "allow" ||
		evs[0].ToolName != "op_a" || evs[0].ErrorType != "sensitive_output" {
		t.Fatalf("want one sync allow/op_a row carrying the redaction, got %+v", evs)
	}
}

// A user dropped from the tenant allowlist must not be able to confirm a
// dangerous call intercepted while they were still on it: the answer runs
// behind the allowlist gate.
func TestGuardedAllowlistBlocksApprovalAnswer(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:approval:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(approvalScope{"test-app", sessionKey})) })

	if _, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`)); err != nil {
		t.Fatal(err)
	}
	_, _ = ap.TakeSignal(approvalScope{"test-app", sessionKey})

	aud := &fakeAuditor{}
	g := &Guarded{
		Inner: EchoProcessor{}, Approver: ap, Auditor: aud,
		PolicyFor: policyStub(tenant.GuardrailPolicy{InputAllowUsers: []string{"vip-user"}}),
	}
	msg := testMsg("确认")
	msg.SessionKey = sessionKey
	out, err := g.Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "权限") {
		t.Fatalf("a de-allowlisted user must be refused, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "deny" || evs[0].ErrorType != "user_not_allowed" {
		t.Fatalf("want one sync deny/user_not_allowed, got %+v", evs)
	}
	// Answer consumes the pending record before executing, so its survival is
	// the proof that the tool never ran.
	p, err := ap.pending(context.Background(), approvalScope{"test-app", sessionKey})
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("the pending approval must survive a refused answer")
	}
}

// A model failure can still burn prompt tokens before dying: the degraded
// path must charge them to the daily budget, or a sustained model outage
// becomes a free-usage window.
func TestGuardedBudgetRecordsTokensOnModelFailure(t *testing.T) {
	aud := &fakeAuditor{}
	fb := &fakeBudget{allowed: true, recorded: map[string]int64{}}
	g := &Guarded{
		Inner:   usageFailProcessor{},
		Auditor: aud,
		Budget:  fb,
	}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "繁忙") {
		t.Fatalf("want the degraded busy reply, got %q", out.Text)
	}
	if fb.recorded["t1"] != 1500 {
		t.Fatalf("model-failure usage must still be charged, got %v", fb.recorded)
	}
}

// usageFailProcessor fails with a model error but reports the usage the run
// already consumed, as the runner does when a generation dies mid-flight.
type usageFailProcessor struct{}

func (usageFailProcessor) Process(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	out := channels.OutboundMessage{
		Channel: msg.Channel, MsgID: msg.MsgID, SessionKey: msg.SessionKey,
		TenantID: msg.TenantID, PromptTokens: 1000, CompletionTokens: 500,
	}
	return out, &ModelError{Err: errors.New("upstream 502")}
}

// A fresh dangerous-tool interception attaches a card to the confirmation
// notice for card-capable channels (wecom direct chats); channels without
// cards read Text, which carries the complete confirmation notice.
func TestGuardedSignalCreatedAttachesCard(t *testing.T) {
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0)
	ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"1"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{Inner: EchoProcessor{}, Approver: ap, Auditor: aud}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	// The Text fallback is the untouched confirmation notice.
	if !strings.Contains(out.Text, "危险操作") || !strings.Contains(out.Text, "op_a") ||
		!strings.Contains(out.Text, "确认") || !strings.Contains(out.Text, "拒绝") {
		t.Fatalf("the text fallback must be the unchanged notice, got %q", out.Text)
	}
	if out.Card == nil {
		t.Fatal("a created signal must attach a card")
	}
	if !strings.Contains(out.Card.Title, "危险操作") {
		t.Fatalf("unexpected card title: %q", out.Card.Title)
	}
	// The desc reuses the filtered notice text: tool, args summary, deadline
	// hint and the confirm/reject instruction all ride it.
	if out.Card.Desc != out.Text {
		t.Fatalf("the card desc must carry the filtered notice verbatim, got %q vs %q", out.Card.Desc, out.Text)
	}
	// The interception is still audited as a review, exactly once.
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "review" || evs[0].ToolName != "op_a" {
		t.Fatalf("want sync review for op_a, got %+v", evs)
	}
}

// Conflict and timeout notices carry no pending action to highlight, so they
// stay plain text.
func TestGuardedSignalConflictAndTimeoutHaveNoCard(t *testing.T) {
	for _, sig := range []Signal{
		{Kind: "conflict", ToolName: "op_b", Pending: "op_a"},
		{Kind: "timeout", ToolName: "op_a"},
	} {
		t.Run(sig.Kind, func(t *testing.T) {
			ap := NewApprover(nil, testRegistry(), 0)
			ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, sig)
			g := &Guarded{Inner: EchoProcessor{}, Approver: ap}
			out, err := g.Process(context.Background(), testMsg("执行操作"))
			if err != nil {
				t.Fatal(err)
			}
			if out.Card != nil {
				t.Fatalf("a %s notice must not carry a card: %+v", sig.Kind, out.Card)
			}
			if out.Text == "" {
				t.Fatalf("a %s notice must keep its text, got %q", sig.Kind, out.Text)
			}
		})
	}
}

// The card desc must never smuggle content the output filters stripped: when
// the notice trips the tenant denylist, the reply is the redacted denial and
// no card is attached.
func TestGuardedSignalCardDroppedOnOutputDeny(t *testing.T) {
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0)
	ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"内部名单"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{
		Inner: EchoProcessor{}, Approver: ap, Auditor: aud,
		PolicyFor: policyStub(tenant.GuardrailPolicy{OutputDenyWords: []string{"内部"}}),
	}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != outputDeniedReply {
		t.Fatalf("the notice must trip the tenant denylist, got %q", out.Text)
	}
	if out.Card != nil {
		t.Fatalf("a denied notice must not carry a card leaking the raw args: %+v", out.Card)
	}
}

// The platform redaction also reaches the card: the desc reuses the filtered
// text, so a phone number in the tool args is masked there too.
func TestGuardedSignalCardDescIsRedacted(t *testing.T) {
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0)
	ap.setSignal(approvalScope{"test-app", "dm:mock:u1"}, Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"13800138000"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{
		Inner: EchoProcessor{}, Approver: ap, Auditor: aud,
		Output: []OutputChecker{RedactOutput()},
	}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Card == nil {
		t.Fatal("a created signal must attach a card")
	}
	if strings.Contains(out.Card.Desc, "13800138000") {
		t.Fatalf("the phone number must be masked in the card desc too: %q", out.Card.Desc)
	}
}
