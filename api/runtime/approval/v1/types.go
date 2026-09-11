// Package approvalv1 defines the bounded Control/Worker contract for human
// confirmation of one supported side-effecting tool capability.
package approvalv1

import (
	"regexp"
	"time"
)

const (
	CapabilityTestTicketStatusUpdate = "test.ticket.status.update"
	MaxPageSize                      = 100
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func ValidDigest(value string) bool { return digestPattern.MatchString(value) }

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusApproved  Status = "APPROVED"
	StatusRejected  Status = "REJECTED"
	StatusExpired   Status = "EXPIRED"
	StatusExecuting Status = "EXECUTING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusUnknown   Status = "UNKNOWN"
)

type Operation struct {
	OperationID         string     `json:"operation_id"`
	TenantID            string     `json:"tenant_id"`
	RunID               string     `json:"run_id"`
	AttemptID           string     `json:"attempt_id"`
	NodeID              string     `json:"node_id"`
	ToolName            string     `json:"tool_name"`
	ToolResource        string     `json:"tool_resource"`
	Capability          string     `json:"capability"`
	Target              string     `json:"target"`
	ParameterSummary    string     `json:"parameter_summary"`
	ArgumentsDigest     string     `json:"arguments_digest"`
	Status              Status     `json:"status"`
	RequestedAt         time.Time  `json:"requested_at"`
	ExpiresAt           time.Time  `json:"expires_at"`
	DecidedBy           string     `json:"decided_by,omitempty"`
	DecidedAt           *time.Time `json:"decided_at,omitempty"`
	DecisionReason      string     `json:"decision_reason,omitempty"`
	ExecutionStartedAt  *time.Time `json:"execution_started_at,omitempty"`
	ExecutionFinishedAt *time.Time `json:"execution_finished_at,omitempty"`
	ResultSummary       string     `json:"result_summary,omitempty"`
	ResultDigest        string     `json:"result_digest,omitempty"`
}

type Page struct {
	Operations []Operation `json:"operations"`
	Offset     int         `json:"offset"`
	Limit      int         `json:"limit"`
	Total      int         `json:"total"`
}

type DecisionRequest struct {
	Action                  string `json:"action"`
	Reason                  string `json:"reason,omitempty"`
	ExpectedArgumentsDigest string `json:"expected_arguments_digest"`
}

// WorkerDecisionRequest adds the authenticated Control actor to the internal
// mTLS request. It is never accepted from the public Web route verbatim.
type WorkerDecisionRequest struct {
	ActorID                 string `json:"actor_id"`
	Action                  string `json:"action"`
	Reason                  string `json:"reason,omitempty"`
	ExpectedArgumentsDigest string `json:"expected_arguments_digest"`
}

type DecisionResponse struct {
	Operation Operation `json:"operation"`
	Outcome   string    `json:"outcome"`
}
