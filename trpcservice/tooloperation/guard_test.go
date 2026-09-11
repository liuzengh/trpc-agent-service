package tooloperation

import (
	"context"
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestRunGuardDeniedPermissionLeavesNoOperation(t *testing.T) {
	ledger := NewMemory()
	guard := newTestGuard(t, ledger)
	before, err := guard.beforeTool(context.Background(), &tool.BeforeToolArgs{
		ToolCallID: "call-denied", ToolName: "calendar_create", Arguments: []byte(`{"title":"private"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := guard.WrapPermissionPolicy(tool.PermissionPolicyFunc(func(
		context.Context, *tool.PermissionRequest,
	) (tool.PermissionDecision, error) {
		return tool.DenyPermission("not approved"), nil
	}))
	decision, err := policy.CheckToolPermission(before.Context, &tool.PermissionRequest{
		ToolCallID: "call-denied", ToolName: "calendar_create", Arguments: []byte(`{"title":"private"}`),
	})
	if err != nil || decision.Action != tool.PermissionActionDeny {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	key, ok := OperationKeyFromContext(before.Context)
	if !ok {
		t.Fatal("before callback did not expose operation key")
	}
	if _, err := ledger.Get(context.Background(), "tenant-a", key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("denied call persisted an operation: %v", err)
	}
}

func TestRunGuardConfirmsAndReplaysSemanticIntentAcrossCallIDs(t *testing.T) {
	ledger := NewMemory()
	guard := newTestGuard(t, ledger)
	ctx := executeAllowedGuardCall(t, guard, "call-1", []byte(`{"b":2,"a":1}`))
	if _, err := guard.afterTool(ctx, &tool.AfterToolArgs{
		ToolCallID: "call-1", ToolName: "calendar_create",
		Arguments: []byte(`{"b":2,"a":1}`), Result: map[string]any{"created": true},
	}); err != nil {
		t.Fatal(err)
	}
	message := model.NewToolMessage("call-1", "calendar_create", `{"created":true}`)
	if _, err := guard.afterToolMessages(ctx, &plugin.AfterToolMessagesArgs{
		ToolCalls: []model.ToolCall{{ID: "call-1"}}, ToolResultMessages: []model.Message{message},
	}); err != nil {
		t.Fatal(err)
	}
	key, _ := OperationKeyFromContext(ctx)
	record, err := ledger.Get(context.Background(), "tenant-a", key)
	if err != nil || record.State != StateConfirmed || record.Confirmation == nil {
		t.Fatalf("confirmed record=%+v err=%v", record, err)
	}

	// A crash/retry may cause the model to issue a different call ID and JSON
	// field order. Semantic turn+tool+arguments identity must still replay.
	retry := newTestGuard(t, ledger)
	replayed, err := retry.beforeTool(context.Background(), &tool.BeforeToolArgs{
		ToolCallID: "different-call-id", ToolName: "calendar_create", Arguments: []byte(`{"a":1,"b":2}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed == nil || replayed.CustomResult == nil {
		t.Fatal("confirmed semantic intent did not skip real tool execution")
	}
	replayedKey, ok := OperationKeyFromContext(replayed.Context)
	if !ok || replayedKey != key {
		t.Fatalf("replayed key=%q want=%q", replayedKey, key)
	}
}

func TestRunGuardOrdinaryToolErrorBecomesUnknown(t *testing.T) {
	ledger := NewMemory()
	guard := newTestGuard(t, ledger)
	ctx := executeAllowedGuardCall(t, guard, "call-unknown", []byte(`{"id":"opaque"}`))
	_, err := guard.afterTool(ctx, &tool.AfterToolArgs{
		ToolCallID: "call-unknown", ToolName: "calendar_create",
		Arguments: []byte(`{"id":"opaque"}`), Error: errors.New("network timeout with sensitive response"),
	})
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("after error=%v, want unknown", err)
	}
	key, _ := OperationKeyFromContext(ctx)
	record, getErr := ledger.Get(context.Background(), "tenant-a", key)
	if getErr != nil || record.State != StateUnknown || record.OutcomeCode != "tool_execution_error" {
		t.Fatalf("unknown record=%+v err=%v", record, getErr)
	}
	retry := newTestGuard(t, ledger)
	if _, err := retry.beforeTool(context.Background(), &tool.BeforeToolArgs{
		ToolCallID: "new-call", ToolName: "calendar_create", Arguments: []byte(`{"id":"opaque"}`),
	}); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("unknown operation was not blocked: %v", err)
	}
}

func TestRunGuardOnlyTypedNotAppliedErrorCanRetry(t *testing.T) {
	ledger := NewMemory()
	guard := newTestGuard(t, ledger)
	base := guard.now()
	ctx := executeAllowedGuardCall(t, guard, "call-safe", []byte(`{"id":"safe"}`))
	_, err := guard.afterTool(ctx, &tool.AfterToolArgs{
		ToolCallID: "call-safe", ToolName: "calendar_create",
		Arguments: []byte(`{"id":"safe"}`), Error: provedNotAppliedError{},
	})
	if err == nil || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("typed not-applied error=%v", err)
	}
	key, _ := OperationKeyFromContext(ctx)
	record, _ := ledger.Get(context.Background(), "tenant-a", key)
	if record.State != StateRetryableNotApplied {
		t.Fatalf("state=%s, want retryable_not_applied", record.State)
	}

	retry := newTestGuard(t, ledger)
	retry.now = func() time.Time { return base.Add(2 * time.Second) }
	retryCtx := executeAllowedGuardCall(t, retry, "call-safe-retry", []byte(`{"id":"safe"}`))
	retryRecord, _ := ledger.Get(context.Background(), "tenant-a", key)
	if retryRecord.State != StateExecuting || retryRecord.AttemptCount != 2 {
		t.Fatalf("retry record=%+v", retryRecord)
	}
	if retryKey, _ := OperationKeyFromContext(retryCtx); retryKey != key {
		t.Fatalf("retry operation key=%q want=%q", retryKey, key)
	}
}

func TestRunGuardMissingToolMessageFailsClosed(t *testing.T) {
	ledger := NewMemory()
	guard := newTestGuard(t, ledger)
	ctx := executeAllowedGuardCall(t, guard, "call-no-message", []byte(`{"id":"x"}`))
	if _, err := guard.afterTool(ctx, &tool.AfterToolArgs{
		ToolCallID: "call-no-message", ToolName: "calendar_create",
		Arguments: []byte(`{"id":"x"}`), Result: map[string]bool{"ok": true},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := guard.afterToolMessages(ctx, &plugin.AfterToolMessagesArgs{
		ToolCalls: []model.ToolCall{{ID: "call-no-message"}},
	})
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("missing message error=%v", err)
	}
}

func TestCanonicalJSONPreservesDistinctLargeIntegerArguments(t *testing.T) {
	first := canonicalJSON([]byte(`{"account":9007199254740992,"enabled":true}`))
	second := canonicalJSON([]byte(`{"enabled":true,"account":9007199254740993}`))
	if string(first) == string(second) {
		t.Fatalf("distinct large integer arguments collapsed to %s", first)
	}

	reordered := canonicalJSON([]byte(" { \"enabled\" : true, \"account\" : 9007199254740992 } "))
	if string(first) != string(reordered) {
		t.Fatalf("equivalent JSON was not canonicalized: %s != %s", first, reordered)
	}
}

func TestIntentKeyIsConfigurationRevisionScoped(t *testing.T) {
	payloadHash := HashPayload(canonicalJSON([]byte(`{"resource_id":"invoice-7"}`)))
	scope := GuardScope{
		TenantID: "tenant-a", AppNamespace: "ta_test", SessionID: "session-1",
		TurnID: "turn-1", ConfigRevision: "revision-1",
	}
	first, err := deriveIntentKey(scope, "calendar.create", payloadHash)
	if err != nil {
		t.Fatal(err)
	}
	scope.ConfigRevision = "revision-2"
	second, err := deriveIntentKey(scope, "calendar.create", payloadHash)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("tool operation intent crossed a configuration revision boundary")
	}
}

type provedNotAppliedError struct{}

func (provedNotAppliedError) Error() string                  { return "provider rejected before acceptance" }
func (provedNotAppliedError) ToolSideEffectNotApplied() bool { return true }

func newTestGuard(t *testing.T, ledger Ledger) *RunGuard {
	t.Helper()
	guard, err := NewRunGuard(ledger, GuardScope{
		TenantID: "tenant-a", AppNamespace: "ta_test", SessionID: "session-1",
		TurnID: "turn-1", ConfigRevision: "rev-1", SideEffects: []string{"calendar_create"},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	guard.now = func() time.Time { return now }
	return guard
}

func executeAllowedGuardCall(t *testing.T, guard *RunGuard, callID string, arguments []byte) context.Context {
	t.Helper()
	before, err := guard.beforeTool(context.Background(), &tool.BeforeToolArgs{
		ToolCallID: callID, ToolName: "calendar_create", Arguments: arguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := guard.WrapPermissionPolicy(tool.PermissionPolicyFunc(func(
		context.Context, *tool.PermissionRequest,
	) (tool.PermissionDecision, error) {
		return tool.AllowPermission(), nil
	}))
	decision, err := policy.CheckToolPermission(before.Context, &tool.PermissionRequest{
		ToolCallID: callID, ToolName: "calendar_create", Arguments: arguments,
	})
	if err != nil || decision.Action != tool.PermissionActionAllow {
		t.Fatalf("allow decision=%+v err=%v", decision, err)
	}
	return before.Context
}
