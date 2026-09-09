package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
)

type unavailableOutcomeAudit struct{ calls int }

func (s *unavailableOutcomeAudit) Write([]byte) (int, error) {
	s.calls++
	return 0, errors.New("audit sink unavailable")
}

func TestDetailedAuditAdmissionFailsBeforeModelAndBudgetDispatch(t *testing.T) {
	r := &scriptedRunner{events: completedModelEvents("provider-response", "answer", nil)}
	budget := &recordingBudgetController{reservation: tokenReservationForWorker()}
	w := newBudgetProcessWorker(t, r, budget)
	w.tenant.Governance.AuditLevel = "detailed"
	w.collector = telemetry.NewCollectorWithAuditSink(&unavailableOutcomeAudit{})
	response, err := w.Process(context.Background(), budgetProcessRequest())
	if response != nil || err == nil || !errors.Is(err, ErrExecutionPreflight) || r.runs != 0 || budget.dispatched != 0 {
		t.Fatalf("detailed admission audit must precede execution: response=%+v err=%v runs=%d dispatch=%d", response, err, r.runs, budget.dispatched)
	}
	if budget.released != 1 || errors.Is(err, ErrWorkerExecutionOutcomeUnknown) {
		t.Fatalf("unstarted work must release reservation and remain retry-safe: released=%d err=%v", budget.released, err)
	}
}

func TestAuditLevelsKeepOutcomesAndDetailedAddsAdmission(t *testing.T) {
	for _, level := range []string{"basic", "detailed"} {
		t.Run(level, func(t *testing.T) {
			var buffer bytes.Buffer
			r := &scriptedRunner{events: completedModelEvents("provider-response", "answer", nil)}
			w := newBudgetProcessWorker(t, r, &recordingBudgetController{reservation: tokenReservationForWorker()})
			w.tenant.Governance.AuditLevel = level
			w.collector = telemetry.NewCollectorWithAuditSink(&buffer)
			if _, err := w.Process(context.Background(), budgetProcessRequest()); err != nil {
				t.Fatal(err)
			}
			var decisions []string
			decoder := json.NewDecoder(&buffer)
			for decoder.More() {
				var entry telemetry.AuditLog
				if err := decoder.Decode(&entry); err != nil {
					t.Fatal(err)
				}
				decisions = append(decisions, entry.Decision)
			}
			if level == "basic" && (len(decisions) != 1 || decisions[0] != "allowed") {
				t.Fatalf("basic decisions=%v", decisions)
			}
			if level == "detailed" && (len(decisions) != 2 || decisions[0] != "execution_admitted" || decisions[1] != "allowed") {
				t.Fatalf("detailed decisions=%v", decisions)
			}
		})
	}
}

func TestProcessDoesNotConfirmSuccessWithoutOutcomeAudit(t *testing.T) {
	tokens := 30
	r := &scriptedRunner{events: completedModelEvents("provider-response", "answer", &tokens)}
	w := newBudgetProcessWorker(t, r, &recordingBudgetController{reservation: tokenReservationForWorker()})
	sink := &unavailableOutcomeAudit{}
	w.collector = telemetry.NewCollectorWithAuditSinkAndIdentityKey(sink, []byte("test-only-audit-key"))
	response, err := w.Process(context.Background(), budgetProcessRequest())
	if sink.calls == 0 || r.runs != 1 {
		t.Fatalf("probe missed execution boundary: writes=%d runs=%d", sink.calls, r.runs)
	}
	if response != nil || !errors.Is(err, ErrWorkerExecutionOutcomeUnknown) {
		t.Fatalf("audit failure after execution must require reconciliation: response=%+v err=%v", response, err)
	}
	if errors.Is(err, ErrExecutionPreflight) {
		t.Fatal("completed model call must not be marked safe to replay")
	}
}
