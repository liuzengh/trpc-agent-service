package tool

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type stubExecutionLedger struct {
	decision      ExecutionDecision
	beginErr      error
	completeErr   error
	failErr       error
	unknownErr    error
	beginCalls    int
	completeCalls int
	failCalls     int
	unknownCalls  int
}

func (l *stubExecutionLedger) Begin(context.Context, ExecutionRequest) (ExecutionDecision, error) {
	l.beginCalls++
	return l.decision, l.beginErr
}

func (l *stubExecutionLedger) Complete(context.Context, string, string, any) error {
	l.completeCalls++
	return l.completeErr
}

func (l *stubExecutionLedger) Fail(context.Context, string, string, string) error {
	l.failCalls++
	return l.failErr
}

func (l *stubExecutionLedger) MarkOutcomeUnknown(context.Context, string, string, string) error {
	l.unknownCalls++
	return l.unknownErr
}

type failingAuditSink struct{ err error }

func (s failingAuditSink) RecordToolAudit(context.Context, governance.ToolAuditEvent) error {
	return s.err
}

func governanceCallbacksWithLedger(t *testing.T, ledger ExecutionLedger, audit governance.AuditSink) *agenttool.Callbacks {
	t.Helper()
	callbacks, err := NewGovernanceCallbacks(
		governance.NewStaticToolPolicy([]string{"support.lookup"}, nil), audit, ledger, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	return callbacks
}

func governanceTestContext(callBudget int, units *governance.UnitBudget) context.Context {
	invocation := contextInvocation(callBudget)
	invocation.Execution.Role = "member"
	invocation.Units = units
	return governance.WithInvocation(context.Background(), invocation)
}

func TestNewGovernanceCallbacksValidatesDependencies(t *testing.T) {
	policy := governance.NewStaticToolPolicy([]string{"support.lookup"}, nil)
	audit := &recordingAuditSink{}
	ledger := NewMemoryExecutionLedger()
	tests := []struct {
		name    string
		policy  governance.ToolPolicy
		audit   governance.AuditSink
		ledger  ExecutionLedger
		timeout time.Duration
		want    string
	}{
		{name: "policy", audit: audit, ledger: ledger, timeout: time.Second, want: "policy"},
		{name: "audit", policy: policy, ledger: ledger, timeout: time.Second, want: "audit"},
		{name: "ledger", policy: policy, audit: audit, timeout: time.Second, want: "ledger"},
		{name: "timeout", policy: policy, audit: audit, ledger: ledger, want: "timeout"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewGovernanceCallbacks(test.policy, test.audit, test.ledger, test.timeout)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestGovernanceCallbacksHandleDurableLedgerDecisions(t *testing.T) {
	tests := []struct {
		name       string
		decision   ExecutionDecision
		wantErr    error
		wantText   string
		wantResult any
	}{
		{
			name:       "completed replay",
			decision:   ExecutionDecision{Status: ExecutionCompleted, IdempotencyKey: "key", Result: "cached"},
			wantResult: "cached",
		},
		{
			name:     "unknown outcome",
			decision: ExecutionDecision{Status: ExecutionOutcomeUnknown, IdempotencyKey: "key"},
			wantErr:  ErrToolOutcomeUnknown,
		},
		{
			name:     "already running",
			decision: ExecutionDecision{Status: ExecutionRunning, IdempotencyKey: "key"},
			wantErr:  ErrToolExecutionInProgress,
		},
		{
			name:     "unsupported",
			decision: ExecutionDecision{Status: ExecutionStatus("paused"), IdempotencyKey: "key"},
			wantText: "unsupported tool execution decision",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := &stubExecutionLedger{decision: test.decision}
			audit := &recordingAuditSink{}
			callbacks := governanceCallbacksWithLedger(t, ledger, audit)
			result, err := callbacks.BeforeTool[0](governanceTestContext(2, nil), &agenttool.BeforeToolArgs{
				ToolName: "support.lookup", ToolCallID: "call-1", Arguments: []byte(`{"order_id":"42"}`),
			})
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if test.wantText != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantText) {
					t.Fatalf("error = %v, want containing %q", err, test.wantText)
				}
				return
			}
			if err != nil || result == nil || result.CustomResult != test.wantResult {
				t.Fatalf("BeforeTool() = %#v, %v", result, err)
			}
			if _, err := callbacks.AfterTool[0](result.Context, &agenttool.AfterToolArgs{ToolName: "support.lookup"}); err != nil {
				t.Fatalf("AfterTool(replay) error = %v", err)
			}
			if ledger.completeCalls != 0 || ledger.unknownCalls != 0 {
				t.Fatalf("replay finalized ledger again: complete=%d unknown=%d", ledger.completeCalls, ledger.unknownCalls)
			}
		})
	}
}

func TestGovernanceCallbacksReplayPresentedCardIntoCurrentRun(t *testing.T) {
	ledger := NewMemoryExecutionLedger()
	callbacks, err := NewGovernanceCallbacks(
		governance.NewStaticToolPolicy([]string{PresentCardToolName}, nil),
		&recordingAuditSink{}, ledger, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	arguments := []byte(`{"title":"订单信息","body":"已找到订单","actions":[{"label":"查看","url":"https://support.example.test/orders/42","style":"primary"}]}`)

	firstRecorder := NewPresentedCardRecorder()
	firstContext := WithPresentedCardRecorder(governanceTestContext(2, nil), firstRecorder)
	before, err := callbacks.BeforeTool[0](firstContext, &agenttool.BeforeToolArgs{
		ToolName: PresentCardToolName, ToolCallID: "call-card", Arguments: arguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewPresentCardTool().Call(before.Context, arguments)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := callbacks.AfterTool[0](before.Context, &agenttool.AfterToolArgs{
		ToolName: PresentCardToolName, ToolCallID: "call-card", Arguments: arguments, Result: result,
	}); err != nil {
		t.Fatal(err)
	}
	if card := firstRecorder.Snapshot(); card == nil || card.Title != "订单信息" || len(card.Actions) != 1 {
		t.Fatalf("first presented card = %#v", card)
	}

	replayRecorder := NewPresentedCardRecorder()
	replayContext := WithPresentedCardRecorder(governanceTestContext(2, nil), replayRecorder)
	replayed, err := callbacks.BeforeTool[0](replayContext, &agenttool.BeforeToolArgs{
		ToolName: PresentCardToolName, ToolCallID: "call-card", Arguments: arguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := replayed.CustomResult.(map[string]any); !ok || got["presented"] != true {
		t.Fatalf("replayed model result = %#v", replayed.CustomResult)
	}
	if card := replayRecorder.Snapshot(); card == nil || card.Title != "订单信息" || card.Body != "已找到订单" || len(card.Actions) != 1 || card.Actions[0].URL != "https://support.example.test/orders/42" {
		t.Fatalf("replayed presented card = %#v", card)
	}
	if _, err := callbacks.AfterTool[0](replayed.Context, &agenttool.AfterToolArgs{ToolName: PresentCardToolName}); err != nil {
		t.Fatalf("AfterTool(replay) error = %v", err)
	}
}

func TestGovernanceCallbacksSurfaceLedgerAndAuditFailures(t *testing.T) {
	t.Run("begin", func(t *testing.T) {
		ledger := &stubExecutionLedger{beginErr: errors.New("ledger unavailable")}
		callbacks := governanceCallbacksWithLedger(t, ledger, &recordingAuditSink{})
		_, err := callbacks.BeforeTool[0](governanceTestContext(2, nil), &agenttool.BeforeToolArgs{ToolName: "support.lookup", Arguments: []byte(`{}`)})
		if err == nil || !strings.Contains(err.Error(), "ledger unavailable") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("denied audit", func(t *testing.T) {
		callbacks, err := NewGovernanceCallbacks(
			governance.NewStaticToolPolicy([]string{"other"}, nil),
			failingAuditSink{err: errors.New("audit unavailable")},
			NewMemoryExecutionLedger(), time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}
		_, err = callbacks.BeforeTool[0](governanceTestContext(2, nil), &agenttool.BeforeToolArgs{ToolName: "support.lookup", Arguments: []byte(`{}`)})
		if !errors.Is(err, governance.ErrToolDenied) || !strings.Contains(err.Error(), "audit unavailable") {
			t.Fatalf("joined error = %v", err)
		}
	})

	t.Run("completed replay audit", func(t *testing.T) {
		ledger := &stubExecutionLedger{decision: ExecutionDecision{Status: ExecutionCompleted, IdempotencyKey: "key", Result: "cached"}}
		callbacks := governanceCallbacksWithLedger(t, ledger, failingAuditSink{err: errors.New("audit unavailable")})
		_, err := callbacks.BeforeTool[0](governanceTestContext(2, nil), &agenttool.BeforeToolArgs{ToolName: "support.lookup"})
		if err == nil || !strings.Contains(err.Error(), "audit unavailable") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestGovernanceCallbacksFailReservationWhenBudgetsReject(t *testing.T) {
	t.Run("call budget", func(t *testing.T) {
		ledger := &stubExecutionLedger{decision: ExecutionDecision{Created: true, Status: ExecutionRunning, IdempotencyKey: "key"}}
		callbacks := governanceCallbacksWithLedger(t, ledger, &recordingAuditSink{})
		_, err := callbacks.BeforeTool[0](governanceTestContext(0, nil), &agenttool.BeforeToolArgs{ToolName: "support.lookup", Arguments: []byte(`{}`)})
		if !errors.Is(err, governance.ErrBudgetExceeded) || ledger.failCalls != 1 {
			t.Fatalf("error=%v failCalls=%d", err, ledger.failCalls)
		}
	})

	t.Run("unit budget", func(t *testing.T) {
		ledger := &stubExecutionLedger{decision: ExecutionDecision{Created: true, Status: ExecutionRunning, IdempotencyKey: "key"}}
		callbacks := governanceCallbacksWithLedger(t, ledger, &recordingAuditSink{})
		_, err := callbacks.BeforeTool[0](governanceTestContext(2, governance.NewUnitBudget(0)), &agenttool.BeforeToolArgs{ToolName: "support.lookup", Arguments: []byte(`{}`)})
		if !errors.Is(err, governance.ErrBudgetUnitsExceeded) || ledger.failCalls != 1 {
			t.Fatalf("error=%v failCalls=%d", err, ledger.failCalls)
		}
	})
}

func TestGovernanceCallbacksAfterToolFinalizationBoundaries(t *testing.T) {
	t.Run("missing invocation is ignored", func(t *testing.T) {
		ledger := &stubExecutionLedger{}
		callbacks := governanceCallbacksWithLedger(t, ledger, &recordingAuditSink{})
		if _, err := callbacks.AfterTool[0](context.Background(), &agenttool.AfterToolArgs{ToolName: "support.lookup"}); err != nil {
			t.Fatalf("AfterTool() error = %v", err)
		}
	})

	t.Run("missing execution key", func(t *testing.T) {
		ledger := &stubExecutionLedger{}
		audit := &recordingAuditSink{}
		callbacks := governanceCallbacksWithLedger(t, ledger, audit)
		_, err := callbacks.AfterTool[0](governanceTestContext(2, nil), &agenttool.AfterToolArgs{ToolName: "support.lookup"})
		if err == nil || !strings.Contains(err.Error(), "execution key is missing") || len(audit.events) != 1 {
			t.Fatalf("AfterTool() error=%v audit=%v", err, audit.events)
		}
	})

	t.Run("complete failure", func(t *testing.T) {
		ledger := &stubExecutionLedger{
			decision:    ExecutionDecision{Created: true, Status: ExecutionRunning, IdempotencyKey: "key"},
			completeErr: errors.New("complete failed"),
		}
		callbacks := governanceCallbacksWithLedger(t, ledger, &recordingAuditSink{})
		before, err := callbacks.BeforeTool[0](governanceTestContext(2, nil), &agenttool.BeforeToolArgs{ToolName: "support.lookup", Arguments: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		_, err = callbacks.AfterTool[0](before.Context, &agenttool.AfterToolArgs{ToolName: "support.lookup", Result: "ok"})
		if err == nil || !strings.Contains(err.Error(), "complete failed") || ledger.completeCalls != 1 {
			t.Fatalf("AfterTool() error=%v completeCalls=%d", err, ledger.completeCalls)
		}
	})

	t.Run("unknown failure", func(t *testing.T) {
		ledger := &stubExecutionLedger{
			decision:   ExecutionDecision{Created: true, Status: ExecutionRunning, IdempotencyKey: "key"},
			unknownErr: errors.New("unknown persistence failed"),
		}
		callbacks := governanceCallbacksWithLedger(t, ledger, &recordingAuditSink{})
		before, err := callbacks.BeforeTool[0](governanceTestContext(2, nil), &agenttool.BeforeToolArgs{ToolName: "support.lookup", Arguments: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		_, err = callbacks.AfterTool[0](before.Context, &agenttool.AfterToolArgs{ToolName: "support.lookup", Error: errors.New("remote timeout")})
		if err == nil || !strings.Contains(err.Error(), "unknown persistence failed") || ledger.unknownCalls != 1 {
			t.Fatalf("AfterTool() error=%v unknownCalls=%d", err, ledger.unknownCalls)
		}
	})
}
