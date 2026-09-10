package tenant

import (
	"strings"
	"testing"
	"time"
)

func TestCanaryRuleWaitsForMinimumSamples(t *testing.T) {
	maxErrorRate := 0.1
	rule := CanaryRule{
		MinimumSamples: 20,
		MaxErrorRate:   &maxErrorRate,
		Action:         CanaryActionRollback,
	}
	decision, err := rule.Evaluate(CanaryObservations{Samples: 19, ErrorRate: 1})
	if err != nil {
		t.Fatalf("evaluate canary rule: %v", err)
	}
	if decision.Action != CanaryActionNone || len(decision.Reasons) != 0 {
		t.Fatalf("decision with insufficient samples = %#v, want NONE", decision)
	}
}

func TestCanaryRuleReturnsAllBreachedSignals(t *testing.T) {
	maxErrorRate := 0.1
	maxP95Latency := 2 * time.Second
	maxBudgetRejections := int64(0)
	rule := CanaryRule{
		MinimumSamples:      20,
		MaxErrorRate:        &maxErrorRate,
		MaxP95Latency:       &maxP95Latency,
		MaxBudgetRejections: &maxBudgetRejections,
		Action:              CanaryActionPause,
	}
	decision, err := rule.Evaluate(CanaryObservations{
		Samples:          20,
		ErrorRate:        0.2,
		P95Latency:       3 * time.Second,
		BudgetRejections: 1,
	})
	if err != nil {
		t.Fatalf("evaluate canary rule: %v", err)
	}
	if decision.Action != CanaryActionPause {
		t.Fatalf("decision action = %q, want PAUSE", decision.Action)
	}
	if got := decision.AuditReason(); got != "error_rate_p95_latency_budget_rejections" {
		t.Fatalf("audit reason = %q, want all breached signals", got)
	}
}

func TestCanaryRuleRejectsUnsafeConfiguration(t *testing.T) {
	maxErrorRate := 1.1
	_, err := (CanaryRule{
		MinimumSamples: 1,
		MaxErrorRate:   &maxErrorRate,
		Action:         CanaryActionRollback,
	}).Evaluate(CanaryObservations{Samples: 1})
	if err == nil || !strings.Contains(err.Error(), "max error rate") {
		t.Fatalf("unsafe canary rule error = %v", err)
	}

	_, err = (CanaryRule{
		MinimumSamples: 1,
		Action:         CanaryActionRollback,
	}).Evaluate(CanaryObservations{Samples: 1})
	if err == nil || !strings.Contains(err.Error(), "at least one threshold") {
		t.Fatalf("empty canary rule error = %v", err)
	}
}
