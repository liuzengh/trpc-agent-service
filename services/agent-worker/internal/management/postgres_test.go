package management

import (
	"testing"
	"time"

	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
)

func TestUsageStatusDoesNotTreatMissingRowsAsZeroUsage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		run     managementv1.RunSummary
		records int64
		want    string
	}{
		{"missing", managementv1.RunSummary{Status: "SUCCEEDED", Attempts: 1}, 0, "UNAVAILABLE"},
		{"running", managementv1.RunSummary{Status: "RUNNING", Attempts: 1}, 1, "PARTIAL"},
		{"retry gap", managementv1.RunSummary{Status: "SUCCEEDED", Attempts: 2}, 1, "PARTIAL"},
		{"complete", managementv1.RunSummary{Status: "SUCCEEDED", Attempts: 1}, 1, "COMPLETE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := usageStatus(tt.run, tt.records); got != tt.want {
				t.Fatalf("usageStatus() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestTimelineUsesEventStateRatherThanFinalAttemptState(t *testing.T) {
	t.Parallel()
	created := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	started, ended := created.Add(time.Minute), created.Add(2*time.Minute)
	events := timeline(managementv1.RunDetail{
		RunSummary: managementv1.RunSummary{AcceptedAt: created.Add(-time.Minute)},
		AttemptsLog: []managementv1.Attempt{{
			AttemptID: "attempt", WorkerID: "worker", Generation: 1,
			Status: "FAILED", Reason: "MODEL_UNAVAILABLE",
			CreatedAt: created, StartedAt: &started, EndedAt: &ended,
		}},
	})
	if len(events) != 4 {
		t.Fatalf("events = %#v", events)
	}
	if events[1].Category != "ATTEMPT_CREATED" || events[1].Status != "CREATED" || events[1].Reason != "" {
		t.Fatalf("created event = %#v", events[1])
	}
	if events[2].Category != "AGENT_STARTED" || events[2].Status != "RUNNING" || events[2].Reason != "" {
		t.Fatalf("started event = %#v", events[2])
	}
	if events[3].Category != "ATTEMPT_ENDED" || events[3].Status != "FAILED" || events[3].Reason != "MODEL_UNAVAILABLE" {
		t.Fatalf("ended event = %#v", events[3])
	}
}
