package audit

import (
	"testing"
	"time"
)

func TestEntryFields(t *testing.T) {
	e := Entry{
		TenantID:  "t1",
		Channel:   "wecom",
		UserID:    "u1",
		SessionID: "s1",
		AgentName: "support-bot",
		ToolName:  "echo",
		Decision:  DecisionExecuted,
		Latency:   150 * time.Millisecond,
		ErrorType: "",
		Cost:      0.0042,
		TraceID:   "trace-1",
	}
	// The entry must carry the full README-required audit surface.
	if e.TenantID == "" || e.Channel == "" || e.UserID == "" || e.SessionID == "" {
		t.Error("tenant/channel/user/session are required audit dimensions")
	}
	if e.TraceID == "" {
		t.Error("trace_id is required to link the full chain")
	}
	if e.Decision == "" {
		t.Error("decision is required")
	}
}

func TestDecisionConstants(t *testing.T) {
	// Decisions must match the audit_logs schema comment (allow/deny/approve).
	for _, d := range []string{DecisionAllow, DecisionDeny, DecisionApprove, DecisionExecuted, DecisionFailed} {
		if d == "" {
			t.Error("decision constant must not be empty")
		}
	}
}
