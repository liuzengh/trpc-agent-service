package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// A nil rdb disables the approval store: dangerous calls are blocked outright
// instead of being pended for a confirmation that could never be stored.
func TestBeforeToolBlocksWhenStoreUnavailable(t *testing.T) {
	ap := NewApprover(nil, testRegistry(), 0)
	res, err := ap.BeforeTool(invocationCtx("test:approval:nostore", "u1"), toolArgs("c1", "op_a", `{"x":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.CustomResult == nil || !strings.Contains(fmt.Sprintf("%v", res.CustomResult), "审批存储不可用") {
		t.Fatalf("dangerous call must be blocked without a store, got %+v", res)
	}
}

// Without session context the approval cannot be scoped, so the dangerous
// call is blocked hard instead of being pended globally.
func TestBeforeToolBlocksWithoutInvocation(t *testing.T) {
	ap, rdb := approverForTest(t)
	res, err := ap.BeforeTool(context.Background(), toolArgs("c1", "op_a", `{"x":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.CustomResult == nil || !strings.Contains(fmt.Sprintf("%v", res.CustomResult), "无法确认会话上下文") {
		t.Fatalf("unscoped dangerous call must be blocked, got %+v", res)
	}
	_ = rdb // nothing stored
}

// A failing approval store read must block the dangerous call, never let it
// through unconfirmed.
func TestBeforeToolBlocksWhenStoreReadFails(t *testing.T) {
	rdb := testenv.Redis(t)
	_ = rdb.Close()
	ap := NewApprover(rdb, testRegistry(), time.Minute)
	res, err := ap.BeforeTool(invocationCtx("test:approval:readdir", "u1"), toolArgs("c1", "op_a", `{"x":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.CustomResult == nil || !strings.Contains(fmt.Sprintf("%v", res.CustomResult), "审批存储读取失败") {
		t.Fatalf("unreadable store must block the call, got %+v", res)
	}
}

// A corrupted pending record is treated as an unreadable store: block rather
// than execute an unconfirmed dangerous tool.
func TestBeforeToolBlocksOnCorruptPendingRecord(t *testing.T) {
	ap, rdb := approverForTest(t)
	ctx := context.Background()
	sessionKey := "test:approval:" + t.Name()
	key := approvalKey(approvalScope{"test-app", sessionKey})
	t.Cleanup(func() { rdb.Del(ctx, key) })
	if err := rdb.Set(ctx, key, []byte("{not json"), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	res, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.CustomResult == nil || !strings.Contains(fmt.Sprintf("%v", res.CustomResult), "审批存储读取失败") {
		t.Fatalf("corrupt record must block the call, got %+v", res)
	}
}

// Lazy timeout detection in BeforeTool: an expired pending record is deleted,
// audited as a timeout signal, and the new call is told to re-confirm.
func TestBeforeToolTimesOutStalePending(t *testing.T) {
	ap, rdb := approverForTest(t)
	ctx := context.Background()
	sessionKey := "test:approval:" + t.Name()
	scope := approvalScope{"test-app", sessionKey}
	t.Cleanup(func() { rdb.Del(ctx, approvalKey(scope)) })

	if err := ap.set(ctx, scope, PendingApproval{
		CallID: "c0", ToolName: "op_a", Arguments: []byte(`{"x":"0"}`),
		Requester: "u1", Deadline: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	res, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.CustomResult == nil || !strings.Contains(fmt.Sprintf("%v", res.CustomResult), "超时作废") {
		t.Fatalf("stale pending must block with a timeout notice, got %+v", res)
	}
	sig, ok := ap.TakeSignal(scope)
	if !ok || sig.Kind != "timeout" || sig.ToolName != "op_a" {
		t.Fatalf("want timeout signal naming the stale tool, got %+v (ok=%v)", sig, ok)
	}
	if p, _ := ap.pending(ctx, scope); p != nil {
		t.Fatalf("stale record must be deleted, got %+v", p)
	}
}

// A confirmed tool that fails at execution time reports the failure to the
// user and audits allow/tool_error — the confirmation was still valid.
func TestAnswerToolCallError(t *testing.T) {
	ap, rdb := approverForTest(t)
	ctx := context.Background()
	sessionKey := "test:approval:" + t.Name()
	scope := approvalScope{"test-app", sessionKey}
	t.Cleanup(func() { rdb.Del(ctx, approvalKey(scope)) })

	// "no-such-tool" is unknown to the registry, so the confirmed call errors.
	if err := ap.set(ctx, scope, PendingApproval{
		CallID: "c1", ToolName: "no-such-tool", Arguments: []byte(`{"x":"1"}`),
		Requester: "u1", Deadline: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	msg := testMsg("确认")
	msg.SessionKey = sessionKey
	handled, out, dec, err := ap.Answer(ctx, msg)
	if err != nil || !handled {
		t.Fatalf("confirm of a failing tool must be handled, got handled=%v err=%v", handled, err)
	}
	if dec.decision != "allow" || dec.errorType != "tool_error" || dec.toolName != "no-such-tool" {
		t.Fatalf("want allow/tool_error, got %+v", dec)
	}
	if !strings.Contains(out.Text, "执行失败") || !strings.Contains(out.Text, "no-such-tool") {
		t.Fatalf("want execution failure notice, got %q", out.Text)
	}
}

// Argument and result summaries are capped so confirmation messages stay
// within IM length limits; unmarshalable results fall back to fmt formatting.
func TestSummarizeTruncationAndFallback(t *testing.T) {
	args := json.RawMessage(strings.Repeat("a", 250))
	got := summarizeArgs(args)
	if len(got) != 203 || !strings.HasSuffix(got, "…") || !strings.HasPrefix(got, strings.Repeat("a", 200)) {
		t.Fatalf("long args must truncate to 200 chars plus ellipsis, got %d bytes %q", len(got), got)
	}
	if s := summarizeArgs(json.RawMessage("short")); s != "short" {
		t.Fatalf("short args must pass through, got %q", s)
	}

	long := summarizeResult(strings.Repeat("b", 600))
	if len(long) != 503 || !strings.HasSuffix(long, "…") {
		t.Fatalf("long result must truncate to 500 bytes plus ellipsis, got %d bytes", len(long))
	}
	if s := summarizeResult("tiny"); s != `"tiny"` {
		t.Fatalf("short result must stay JSON, got %q", s)
	}

	ch := make(chan int)
	if got, want := summarizeResult(ch), fmt.Sprintf("%v", ch); got != want {
		t.Fatalf("unmarshalable result must fall back to %%v, got %q want %q", got, want)
	}
}

// The summaries are embedded in IM replies, so a truncation must never split a
// multi-byte rune: cutting on a byte offset left invalid UTF-8 in the notice
// the whole group chat receives.
func TestSummarizeTruncatesOnRuneBoundaries(t *testing.T) {
	cases := []json.RawMessage{
		json.RawMessage(strings.Repeat("汉", 100)),                  // 300 bytes: the cap lands mid-rune
		json.RawMessage(strings.Repeat("a", 199) + "😀"),            // 4-byte rune straddling the cap
		json.RawMessage(`{"x":"` + strings.Repeat("界", 80) + `"}`), // a cut inside a JSON string
	}
	for i, args := range cases {
		got := summarizeArgs(args)
		if !utf8.ValidString(got) {
			t.Fatalf("case %d: truncated args must stay valid UTF-8, got %q", i, got)
		}
		if want := 200 + len("…"); len(got) > want {
			t.Fatalf("case %d: summary must stay within the cap, got %d bytes", i, len(got))
		}
		if !strings.HasSuffix(got, "…") {
			t.Fatalf("case %d: a truncated summary must carry the ellipsis, got %q", i, got)
		}
	}
	if got := summarizeResult(strings.Repeat("汉", 300)); !utf8.ValidString(got) {
		t.Fatalf("truncated result must stay valid UTF-8, got %q", got)
	}
	// A short input passes through untouched.
	if got := summarizeArgs(json.RawMessage("汉字")); got != "汉字" {
		t.Fatalf("a short summary must pass through, got %q", got)
	}
}

// truncate steps back off a partial rune instead of emitting half of one.
func TestTruncateWalksBackOffAPartialRune(t *testing.T) {
	const s = "abc汉def" // the multibyte rune occupies bytes 3..5
	for _, tc := range []struct {
		max  int
		want string
	}{
		{len(s), s},  // no truncation
		{200, s},     // cap above the length
		{6, "abc汉…"}, // cut on a rune boundary
		{5, "abc…"},  // cut inside the multibyte rune: walks back
		{4, "abc…"},  // cut inside the multibyte rune: walks back
		{3, "abc…"},  // cut exactly before the multibyte rune
	} {
		got := truncate(s, tc.max)
		if got != tc.want {
			t.Fatalf("truncate(%q, %d) = %q, want %q", s, tc.max, got, tc.want)
		}
		if !utf8.ValidString(got) {
			t.Fatalf("truncate(%q, %d) produced invalid UTF-8: %q", s, tc.max, got)
		}
	}
}
