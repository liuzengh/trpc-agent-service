package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	tagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func invCtx(id string) context.Context {
	return tagent.NewInvocationContext(context.Background(), &tagent.Invocation{InvocationID: id})
}

// A before/after pair records once and hands the key back: the map returns to
// empty. A second After for the same key (a multi-response stream) is a no-op.
func TestModelTimerBeforeAfter(t *testing.T) {
	mt := &modelTimer{tenantID: "t1", model: "m1"}
	ctx := invCtx("inv-1")

	res, err := mt.before(ctx, &model.BeforeModelArgs{})
	if err != nil || res != nil {
		t.Fatalf("timing before must be transparent: %v, %v", res, err)
	}
	if mt.starts.pending() != 1 {
		t.Fatalf("before must start the clock, pending=%d", mt.starts.pending())
	}
	out, err := mt.after(ctx, &model.AfterModelArgs{})
	if err != nil || out != nil {
		t.Fatalf("timing after must be transparent: %v, %v", out, err)
	}
	if mt.starts.pending() != 0 {
		t.Fatalf("after must delete the start entry, pending=%d", mt.starts.pending())
	}
	// Error results record under result=error and clean up the same way.
	if _, err := mt.before(ctx, &model.BeforeModelArgs{}); err != nil {
		t.Fatal(err)
	}
	if _, err := mt.after(ctx, &model.AfterModelArgs{Error: errors.New("model boom")}); err != nil {
		t.Fatal(err)
	}
	if mt.starts.pending() != 0 {
		t.Fatalf("error after must delete the start entry, pending=%d", mt.starts.pending())
	}
	// After without a matching Before (already recorded, or never began).
	if _, err := mt.after(ctx, &model.AfterModelArgs{}); err != nil {
		t.Fatal(err)
	}
}

// Without an invocation in the context the callbacks cannot key the call:
// they pass through without timing instead of guessing.
func TestModelTimerWithoutInvocation(t *testing.T) {
	mt := &modelTimer{tenantID: "t1", model: "m1"}
	if _, err := mt.before(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatal(err)
	}
	if _, err := mt.after(context.Background(), &model.AfterModelArgs{}); err != nil {
		t.Fatal(err)
	}
	if mt.starts.pending() != 0 {
		t.Fatalf("no invocation means no timing, pending=%d", mt.starts.pending())
	}
}

// modelTimingCallbacks must register both callbacks so the framework's
// RunBeforeModel/RunAfterModel drive the timer.
func TestModelTimingCallbacksRegistered(t *testing.T) {
	cbs := modelTimingCallbacks("t1", "m1")
	ctx := invCtx("inv-9")
	if _, err := cbs.RunBeforeModel(ctx, &model.BeforeModelArgs{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cbs.RunAfterModel(ctx, &model.AfterModelArgs{}); err != nil {
		t.Fatal(err)
	}
}

// Concurrent invocations time independently and the map drains back to zero.
func TestModelTimerConcurrent(t *testing.T) {
	mt := &modelTimer{tenantID: "t1", model: "m1"}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := invCtx(fmt.Sprintf("inv-%d", i))
			if _, err := mt.before(ctx, &model.BeforeModelArgs{}); err != nil {
				t.Error(err)
			}
			if _, err := mt.after(ctx, &model.AfterModelArgs{}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if mt.starts.pending() != 0 {
		t.Fatalf("all starts must be consumed, pending=%d", mt.starts.pending())
	}
}

// A Before whose After never fires (the call failed before any response)
// leaves a stale entry; the sweep in begin reaps it once it ages out.
func TestCallTimerSweepsStaleStarts(t *testing.T) {
	ct := &callTimer{}
	ct.starts.Store("stale", time.Now().Add(-2*timingStaleTTL))
	ct.begin("fresh")
	if ct.pending() != 1 {
		t.Fatalf("stale entry must be reaped, pending=%d", ct.pending())
	}
	if _, ok := ct.end("stale"); ok {
		t.Fatal("stale entry must be gone")
	}
	if _, ok := ct.end("fresh"); !ok {
		t.Fatal("fresh entry must survive the sweep")
	}
	if ct.pending() != 0 {
		t.Fatalf("pending=%d after end", ct.pending())
	}
}

// Tool timing records ok and error results and hands the ToolCallID key back.
func TestToolTimerBeforeAfter(t *testing.T) {
	tt := &toolTimer{tenantID: "t1"}
	ctx := context.Background()

	if _, err := tt.before(ctx, &ttool.BeforeToolArgs{ToolCallID: "call-1", ToolName: "search"}); err != nil {
		t.Fatal(err)
	}
	if tt.starts.pending() != 1 {
		t.Fatalf("before must start the clock, pending=%d", tt.starts.pending())
	}
	res, err := tt.after(ctx, &ttool.AfterToolArgs{ToolCallID: "call-1", ToolName: "search"})
	if err != nil || res != nil {
		t.Fatalf("timing after must be transparent: %v, %v", res, err)
	}
	if tt.starts.pending() != 0 {
		t.Fatalf("after must delete the start entry, pending=%d", tt.starts.pending())
	}
	// Error result: recorded under result=error, cleaned up the same way.
	if _, err := tt.before(ctx, &ttool.BeforeToolArgs{ToolCallID: "call-2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tt.after(ctx, &ttool.AfterToolArgs{ToolCallID: "call-2", Error: errors.New("tool boom")}); err != nil {
		t.Fatal(err)
	}
	// After for an unknown key (e.g. a call the approver blocked before the
	// timer started) is a no-op.
	if _, err := tt.after(ctx, &ttool.AfterToolArgs{ToolCallID: "never-began"}); err != nil {
		t.Fatal(err)
	}
	if tt.starts.pending() != 0 {
		t.Fatalf("pending=%d", tt.starts.pending())
	}
}

// The timing callbacks append to the approver's callbacks on a CLONE: the
// shared instance keeps exactly its original registrations, the approver
// still runs first and its block short-circuits before the timer starts.
func TestToolTimingCallbacksCoexistWithApprover(t *testing.T) {
	approver := ttool.BeforeToolCallbackStructured(
		func(_ context.Context, args *ttool.BeforeToolArgs) (*ttool.BeforeToolResult, error) {
			if args.ToolName == "danger" {
				return &ttool.BeforeToolResult{CustomResult: "blocked"}, nil
			}
			return nil, nil
		})
	base := ttool.NewCallbacks().RegisterBeforeTool(approver)

	cbs := toolTimingCallbacks(base, "t1")
	if len(base.BeforeTool) != 1 || len(base.AfterTool) != 0 {
		t.Fatalf("shared callbacks must not be mutated: %+v", base)
	}
	if len(cbs.BeforeTool) != 2 || len(cbs.AfterTool) != 1 {
		t.Fatalf("timing callbacks must append to the clone: %+v", cbs)
	}

	// Blocked call: the approver's CustomResult passes through unchanged.
	res, err := cbs.RunBeforeTool(context.Background(), &ttool.BeforeToolArgs{ToolCallID: "c1", ToolName: "danger"})
	if err != nil || res == nil || res.CustomResult != "blocked" {
		t.Fatalf("approval interception semantics changed: %v, %v", res, err)
	}
	// The timing Before sits behind the approver, so a blocked call never
	// started the clock; its (never-sent) After would be a no-op.
	if _, err := cbs.RunAfterTool(context.Background(), &ttool.AfterToolArgs{ToolCallID: "c1", ToolName: "danger"}); err != nil {
		t.Fatal(err)
	}

	// Allowed call: Before is transparent, After is transparent.
	res, err = cbs.RunBeforeTool(context.Background(), &ttool.BeforeToolArgs{ToolCallID: "c2", ToolName: "search"})
	if err != nil {
		t.Fatal(err)
	}
	if res != nil && res.CustomResult != nil {
		t.Fatalf("timing before must never produce a result: %+v", res)
	}
	if _, err := cbs.RunAfterTool(context.Background(), &ttool.AfterToolArgs{ToolCallID: "c2", ToolName: "search"}); err != nil {
		t.Fatal(err)
	}
}

// A nil base (no approver configured) still yields working timing callbacks.
func TestToolTimingCallbacksNilBase(t *testing.T) {
	cbs := toolTimingCallbacks(nil, "t1")
	if _, err := cbs.RunBeforeTool(context.Background(), &ttool.BeforeToolArgs{ToolCallID: "c1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cbs.RunAfterTool(context.Background(), &ttool.AfterToolArgs{ToolCallID: "c1"}); err != nil {
		t.Fatal(err)
	}
}
