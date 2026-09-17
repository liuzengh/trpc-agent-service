// Package capacity evaluates reproducible load-test evidence against explicit
// release criteria. It does not generate traffic or infer success from HPA state.
package capacity

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const SchemaVersion = 1

type Criteria struct {
	MinimumDurationSeconds     float64 `json:"minimum_duration_seconds"`
	MinimumOfferedRPS          float64 `json:"minimum_offered_rps"`
	MinimumSustainedRPS        float64 `json:"minimum_sustained_rps"`
	MaximumErrorRatio          float64 `json:"maximum_error_ratio"`
	MaximumP95LatencyMillis    float64 `json:"maximum_p95_latency_millis"`
	MaximumBrokerLagSeconds    float64 `json:"maximum_broker_lag_seconds"`
	MaximumDrainSeconds        float64 `json:"maximum_drain_seconds"`
	MinimumMetricCoverageRatio float64 `json:"minimum_metric_coverage_ratio"`
	MaximumAuditLagSeconds     float64 `json:"maximum_audit_lag_seconds"`
	RequireWorkerScaleUp       bool    `json:"require_worker_scale_up"`
}

type Report struct {
	SchemaVersion         int       `json:"schema_version"`
	ScenarioID            string    `json:"scenario_id"`
	ServiceVersion        string    `json:"service_version"`
	Environment           string    `json:"environment"`
	StartedAt             time.Time `json:"started_at"`
	EndedAt               time.Time `json:"ended_at"`
	Criteria              Criteria  `json:"criteria"`
	OfferedRequests       int64     `json:"offered_requests"`
	AdmittedRequests      int64     `json:"admitted_requests"`
	CompletedRequests     int64     `json:"completed_requests"`
	FailedRequests        int64     `json:"failed_requests"`
	P95LatencyMillis      float64   `json:"p95_latency_millis"`
	PeakBrokerLagSeconds  float64   `json:"peak_broker_lag_seconds"`
	FinalBrokerBacklog    int64     `json:"final_broker_backlog"`
	DrainSeconds          float64   `json:"drain_seconds"`
	MetricCoverageRatio   float64   `json:"metric_coverage_ratio"`
	AuditDeadLetter       int64     `json:"audit_dead_letter"`
	PeakAuditLagSeconds   float64   `json:"peak_audit_lag_seconds"`
	InitialWorkerReplicas int       `json:"initial_worker_replicas"`
	PeakWorkerReplicas    int       `json:"peak_worker_replicas"`
	EvidenceRefs          []string  `json:"evidence_refs"`
}

type Result struct {
	Accepted     bool     `json:"accepted"`
	SustainedRPS float64  `json:"sustained_rps"`
	ErrorRatio   float64  `json:"error_ratio"`
	Violations   []string `json:"violations,omitempty"`
}

func Evaluate(report Report) Result {
	result := Result{}
	duration := report.EndedAt.Sub(report.StartedAt).Seconds()
	if duration > 0 {
		result.SustainedRPS = float64(report.CompletedRequests) / duration
	}
	if report.AdmittedRequests > 0 {
		result.ErrorRatio = float64(report.FailedRequests) / float64(report.AdmittedRequests)
	}
	add := func(code string, condition bool) {
		if condition {
			result.Violations = append(result.Violations, code)
		}
	}
	add("invalid_schema", report.SchemaVersion != SchemaVersion)
	add("missing_identity", !text(report.ScenarioID) || !text(report.ServiceVersion) || !text(report.Environment))
	add("invalid_time_window", duration <= 0)
	add("invalid_criteria", !validCriteria(report.Criteria))
	add("request_accounting_mismatch", report.OfferedRequests < 0 || report.AdmittedRequests < 0 || report.CompletedRequests < 0 || report.FailedRequests < 0 ||
		report.AdmittedRequests > report.OfferedRequests || report.CompletedRequests+report.FailedRequests != report.AdmittedRequests)
	if duration > 0 && validCriteria(report.Criteria) {
		add("duration_below_target", duration+epsilon < report.Criteria.MinimumDurationSeconds)
		add("offered_rate_below_target", float64(report.OfferedRequests)/duration+epsilon < report.Criteria.MinimumOfferedRPS)
		add("sustained_rate_below_target", result.SustainedRPS+epsilon < report.Criteria.MinimumSustainedRPS)
		add("error_ratio_exceeded", result.ErrorRatio-epsilon > report.Criteria.MaximumErrorRatio)
		add("p95_latency_exceeded", report.P95LatencyMillis-epsilon > report.Criteria.MaximumP95LatencyMillis)
		add("broker_lag_exceeded", report.PeakBrokerLagSeconds-epsilon > report.Criteria.MaximumBrokerLagSeconds)
		add("drain_deadline_exceeded", report.DrainSeconds-epsilon > report.Criteria.MaximumDrainSeconds)
		add("metric_coverage_incomplete", report.MetricCoverageRatio+epsilon < report.Criteria.MinimumMetricCoverageRatio)
		add("audit_lag_exceeded", report.PeakAuditLagSeconds-epsilon > report.Criteria.MaximumAuditLagSeconds)
	}
	add("negative_observation", !finite(report.P95LatencyMillis) || !finite(report.PeakBrokerLagSeconds) || !finite(report.DrainSeconds) ||
		!finite(report.MetricCoverageRatio) || !finite(report.PeakAuditLagSeconds) || report.P95LatencyMillis < 0 || report.PeakBrokerLagSeconds < 0 || report.FinalBrokerBacklog < 0 ||
		report.DrainSeconds < 0 || report.MetricCoverageRatio < 0 || report.MetricCoverageRatio > 1 || report.AuditDeadLetter < 0 ||
		report.PeakAuditLagSeconds < 0 || report.InitialWorkerReplicas < 1 || report.PeakWorkerReplicas < report.InitialWorkerReplicas)
	add("broker_not_drained", report.FinalBrokerBacklog != 0)
	add("audit_dead_letter_present", report.AuditDeadLetter != 0)
	add("worker_scale_up_missing", report.Criteria.RequireWorkerScaleUp && report.PeakWorkerReplicas <= report.InitialWorkerReplicas)
	add("evidence_incomplete", len(report.EvidenceRefs) < 2 || invalidEvidence(report.EvidenceRefs))
	sort.Strings(result.Violations)
	result.Accepted = len(result.Violations) == 0
	return result
}

const epsilon = 1e-9

func validCriteria(value Criteria) bool {
	values := []float64{value.MinimumDurationSeconds, value.MinimumOfferedRPS, value.MinimumSustainedRPS,
		value.MaximumP95LatencyMillis, value.MaximumBrokerLagSeconds, value.MaximumDrainSeconds,
		value.MinimumMetricCoverageRatio, value.MaximumAuditLagSeconds}
	for _, item := range values {
		if math.IsNaN(item) || math.IsInf(item, 0) || item <= 0 {
			return false
		}
	}
	return finite(value.MaximumErrorRatio) && value.MaximumErrorRatio >= 0 && value.MaximumErrorRatio < 1 && value.MinimumMetricCoverageRatio <= 1
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func invalidEvidence(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !text(value) || len(value) > 512 || strings.HasPrefix(value, "example://") {
			return true
		}
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func text(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func (r Result) Error() error {
	if r.Accepted {
		return nil
	}
	return fmt.Errorf("capacity acceptance failed: %s", strings.Join(r.Violations, ","))
}
