package capacity

import (
	"slices"
	"testing"
	"time"
)

func acceptedReport() Report {
	start := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	return Report{SchemaVersion: 1, ScenarioID: "worker-step-100rps", ServiceVersion: "sha256:build", Environment: "staging",
		StartedAt: start, EndedAt: start.Add(10 * time.Minute), Criteria: Criteria{MinimumDurationSeconds: 600,
			MinimumOfferedRPS: 100, MinimumSustainedRPS: 95, MaximumErrorRatio: .01, MaximumP95LatencyMillis: 2000,
			MaximumBrokerLagSeconds: 5, MaximumDrainSeconds: 30, MinimumMetricCoverageRatio: .99,
			MaximumAuditLagSeconds: 30, RequireWorkerScaleUp: true}, OfferedRequests: 60000, AdmittedRequests: 60000,
		CompletedRequests: 59400, FailedRequests: 600, P95LatencyMillis: 1500, PeakBrokerLagSeconds: 4,
		DrainSeconds: 20, MetricCoverageRatio: 1, InitialWorkerReplicas: 3, PeakWorkerReplicas: 8,
		EvidenceRefs: []string{"trace://capacity/run-1", "audit://capacity/run-1"}}
}

func TestEvaluateAcceptsCompleteEvidence(t *testing.T) {
	result := Evaluate(acceptedReport())
	if !result.Accepted || result.SustainedRPS != 99 || result.ErrorRatio != .01 || result.Error() != nil {
		t.Fatalf("result=%+v", result)
	}
}

func TestEvaluateRejectsBacklogStaleMetricsAndMissingScaleUp(t *testing.T) {
	report := acceptedReport()
	report.FinalBrokerBacklog = 3
	report.MetricCoverageRatio = .8
	report.PeakWorkerReplicas = report.InitialWorkerReplicas
	result := Evaluate(report)
	for _, expected := range []string{"broker_not_drained", "metric_coverage_incomplete", "worker_scale_up_missing"} {
		if !slices.Contains(result.Violations, expected) {
			t.Fatalf("missing %q in %+v", expected, result)
		}
	}
	if result.Accepted || result.Error() == nil {
		t.Fatalf("result=%+v", result)
	}
}

func TestEvaluateRejectsExampleEvidence(t *testing.T) {
	report := acceptedReport()
	report.EvidenceRefs = []string{"example://trace", "example://audit"}
	result := Evaluate(report)
	if result.Accepted || !slices.Contains(result.Violations, "evidence_incomplete") {
		t.Fatalf("result=%+v", result)
	}
}
