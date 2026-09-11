// Package managementv1 defines the read-only business observation contract
// shared by runtime owners and Control. It contains no credentials or payloads.
package managementv1

import "time"

const (
	WorkerRunsPath = "/internal/v1/management/tenants/{tenant_id}/runs"
	MaxPageSize    = 100
)

type RunSummary struct {
	RunID          string     `json:"run_id"`
	SessionID      string     `json:"session_id"`
	Status         string     `json:"status"`
	Stage          string     `json:"stage"`
	WaitReason     string     `json:"wait_reason,omitempty"`
	FailureReason  string     `json:"failure_reason,omitempty"`
	Attempts       int        `json:"attempts"`
	UsageStatus    string     `json:"usage_status"`
	InputTokens    int64      `json:"input_tokens"`
	OutputTokens   int64      `json:"output_tokens"`
	TotalTokens    int64      `json:"total_tokens"`
	MemoryStatus   string     `json:"memory_status,omitempty"`
	ReplyStatus    string     `json:"reply_status"`
	AcceptedAt     time.Time  `json:"accepted_at"`
	ExecutionUntil *time.Time `json:"execution_deadline,omitempty"`
}

type Attempt struct {
	AttemptID  string     `json:"attempt_id"`
	WorkerID   string     `json:"worker_id"`
	Generation int64      `json:"generation"`
	Status     string     `json:"status"`
	Reason     string     `json:"reason,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
}

type TimelineEvent struct {
	Source     string         `json:"source"`
	Category   string         `json:"category"`
	Status     string         `json:"status"`
	Reason     string         `json:"reason,omitempty"`
	OccurredAt time.Time      `json:"occurred_at"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type RunDetail struct {
	RunSummary
	AdmissionID    string          `json:"admission_id"`
	ManifestRef    string          `json:"manifest_ref,omitempty"`
	ManifestDigest string          `json:"manifest_digest,omitempty"`
	SessionHead    string          `json:"session_head,omitempty"`
	AttemptsLog    []Attempt       `json:"attempt_log"`
	Timeline       []TimelineEvent `json:"timeline"`
	Coverage       []string        `json:"coverage"`
}

type RunPage struct {
	Runs   []RunSummary `json:"runs"`
	Offset int          `json:"offset"`
	Limit  int          `json:"limit"`
	Total  int          `json:"total"`
}

type AuditEvent struct {
	EventID      string         `json:"event_id"`
	Source       string         `json:"source"`
	Category     string         `json:"category"`
	Action       string         `json:"action"`
	Outcome      string         `json:"outcome"`
	ActorID      string         `json:"actor_id,omitempty"`
	ResourceType string         `json:"resource_type"`
	ResourceID   string         `json:"resource_id"`
	Reason       string         `json:"reason,omitempty"`
	OccurredAt   time.Time      `json:"occurred_at"`
	Attributes   map[string]any `json:"attributes,omitempty"`
}

type AuditPage struct {
	Events []AuditEvent `json:"events"`
	Offset int          `json:"offset"`
	Limit  int          `json:"limit"`
	Total  int          `json:"total"`
}
