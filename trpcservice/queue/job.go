package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	SchemaVersion       = 2
	DefaultJobMaxAge    = 24 * time.Hour
	defaultJobMaxAge    = DefaultJobMaxAge
	maxJobIDLength      = 128
	maxMessageLength    = 1 << 20
	maxHistoryMessages  = 100
	maxHistoryBytes     = 256 << 10
	maxNackReasonLength = 256
)

var (
	ErrInvalidJob       = errors.New("queue: invalid job")
	ErrInvalidEnvelope  = errors.New("queue: invalid job envelope")
	ErrInvalidDelivery  = errors.New("queue: invalid delivery")
	ErrDeliveryExpired  = errors.New("queue: delivery expired")
	ErrDeliveryFinished = errors.New("queue: delivery already finished")
	ErrQueueClosed      = errors.New("queue: queue is closed")
	ErrNotAccepted      = errors.New("queue: receipt was not accepted")
)

// TenantContextDTO is the transport-safe projection of tenant.TenantContext.
// It intentionally contains no context.Context or runtime-owned values.
type TenantContextDTO struct {
	TenantID         string               `json:"tenant_id"`
	AgentAppID       string               `json:"agent_app_id"`
	BindingID        string               `json:"binding_id"`
	Channel          string               `json:"channel"`
	ExternalUser     string               `json:"external_user,omitempty"`
	ExternalChat     string               `json:"external_chat,omitempty"`
	ExternalThreadID string               `json:"external_thread_id,omitempty"`
	InternalUser     string               `json:"internal_user,omitempty"`
	SessionID        string               `json:"session_id"`
	RequestID        string               `json:"request_id"`
	MessageID        string               `json:"message_id"`
	TraceID          string               `json:"trace_id"`
	ConfigVersion    int64                `json:"config_version"`
	Permissions      []string             `json:"permissions,omitempty"`
	BackendPolicy    tenant.BackendPolicy `json:"backend_policy"`
}

func TenantContextDTOFromContext(tc tenant.TenantContext) TenantContextDTO {
	return TenantContextDTO{
		TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, BindingID: tc.BindingID,
		Channel: tc.Channel, ExternalUser: tc.ExternalUser, ExternalChat: tc.ExternalChat, ExternalThreadID: tc.ExternalThreadID,
		InternalUser: tc.InternalUser, SessionID: tc.SessionID, RequestID: tc.RequestID,
		MessageID: tc.MessageID, TraceID: tc.TraceID, ConfigVersion: tc.ConfigVersion,
		Permissions: append([]string(nil), tc.Permissions...), BackendPolicy: tc.BackendPolicy,
	}
}

func (dto TenantContextDTO) Restore() (tenant.TenantContext, error) {
	tc := tenant.TenantContext{
		TenantID: dto.TenantID, AgentAppID: dto.AgentAppID, BindingID: dto.BindingID,
		Channel: dto.Channel, ExternalUser: dto.ExternalUser, ExternalChat: dto.ExternalChat, ExternalThreadID: dto.ExternalThreadID,
		InternalUser: dto.InternalUser, SessionID: dto.SessionID, RequestID: dto.RequestID,
		MessageID: dto.MessageID, TraceID: dto.TraceID, ConfigVersion: dto.ConfigVersion,
		Permissions: append([]string(nil), dto.Permissions...), BackendPolicy: dto.BackendPolicy,
	}
	if err := tc.Validate(); err != nil {
		return tenant.TenantContext{}, fmt.Errorf("%w: tenant context: %v", ErrInvalidJob, err)
	}
	return tc, nil
}

// TraceContextDTO carries correlation identifiers across the queue boundary.
type TraceContextDTO struct {
	TraceID     string `json:"trace_id"`
	RequestID   string `json:"request_id"`
	MessageID   string `json:"message_id"`
	ExecutionID string `json:"execution_id"`
}

// AgentRefDTO identifies an immutable agent release without transporting its
// provider, secret, runner, or mutable configuration.
type AgentRefDTO struct {
	TenantID   string `json:"tenant_id"`
	AgentAppID string `json:"agent_app_id"`
	Version    int64  `json:"version"`
}

type ToolCallDTO struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments,omitempty"`
}

// MessageDTO is the queue-safe projection of a platform message.
type MessageDTO struct {
	ID        string        `json:"id"`
	Role      string        `json:"role"`
	Content   string        `json:"content"`
	CreatedAt time.Time     `json:"created_at"`
	ToolID    string        `json:"tool_id,omitempty"`
	ToolName  string        `json:"tool_name,omitempty"`
	ToolCalls []ToolCallDTO `json:"tool_calls,omitempty"`
}

// AgentJob is the versioned, runtime-independent unit of asynchronous work.
type AgentJob struct {
	SchemaVersion int              `json:"schema_version"`
	JobID         string           `json:"job_id"`
	ExecutionID   string           `json:"execution_id"`
	Tenant        TenantContextDTO `json:"tenant"`
	Agent         AgentRefDTO      `json:"agent"`
	History       []MessageDTO     `json:"history,omitempty"`
	Message       MessageDTO       `json:"message"`
	Trace         TraceContextDTO  `json:"trace"`
	CreatedAt     time.Time        `json:"created_at"`
	Deadline      time.Time        `json:"deadline"`
	Attempt       int              `json:"attempt"`
}

// JobEnvelope is the stable wire representation stored by a queue.
type JobEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	JobID         string          `json:"job_id"`
	Payload       json.RawMessage `json:"payload"`
}

func (j AgentJob) Validate() error { return j.ValidateAt(time.Now().UTC(), defaultJobMaxAge) }

func (j AgentJob) ValidateAt(now time.Time, maxAge time.Duration) error {
	if j.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: unsupported schema version %d", ErrInvalidJob, j.SchemaVersion)
	}
	if !validID(j.JobID) || !validID(j.ExecutionID) {
		return fmt.Errorf("%w: job and execution IDs are required", ErrInvalidJob)
	}
	if !validID(j.Tenant.TenantID) || !validID(j.Tenant.AgentAppID) || !validID(j.Tenant.BindingID) {
		return fmt.Errorf("%w: tenant IDs are required", ErrInvalidJob)
	}
	if j.Agent.TenantID != j.Tenant.TenantID || j.Agent.AgentAppID != j.Tenant.AgentAppID || j.Agent.Version < 1 {
		return fmt.Errorf("%w: agent reference does not match tenant context", ErrInvalidJob)
	}
	if j.Trace.TraceID != j.Tenant.TraceID || j.Trace.RequestID != j.Tenant.RequestID || j.Trace.MessageID != j.Tenant.MessageID || j.Trace.ExecutionID != j.ExecutionID {
		return fmt.Errorf("%w: trace identifiers do not match job", ErrInvalidJob)
	}
	if !validID(j.Tenant.SessionID) || !validID(j.Tenant.RequestID) || !validID(j.Tenant.MessageID) || !validID(j.Tenant.TraceID) {
		return fmt.Errorf("%w: session and correlation IDs are required", ErrInvalidJob)
	}
	if !validID(j.Trace.TraceID) || !validID(j.Trace.RequestID) || !validID(j.Trace.MessageID) || !validID(j.Trace.ExecutionID) {
		return fmt.Errorf("%w: trace identifiers are required", ErrInvalidJob)
	}
	if j.Message.ID != j.Tenant.MessageID || !validID(j.Message.ID) || strings.TrimSpace(j.Message.Content) == "" {
		return fmt.Errorf("%w: message does not match tenant context", ErrInvalidJob)
	}
	if j.Attempt < 1 {
		return fmt.Errorf("%w: attempt must be positive", ErrInvalidJob)
	}
	if j.CreatedAt.IsZero() || j.Deadline.IsZero() || j.Deadline.Location() == nil {
		return fmt.Errorf("%w: created_at and deadline are required", ErrInvalidJob)
	}
	if maxAge <= 0 {
		return fmt.Errorf("%w: max job age must be positive", ErrInvalidJob)
	}
	now = now.UTC()
	deadline := j.Deadline.UTC()
	if !deadline.After(now) || deadline.After(now.Add(maxAge)) {
		return fmt.Errorf("%w: deadline is outside the allowed window", ErrInvalidJob)
	}
	if err := validateMessages(j.History, "history"); err != nil {
		return err
	}
	if err := validateMessages([]MessageDTO{j.Message}, "message"); err != nil {
		return err
	}
	if historyPayload, err := json.Marshal(j.History); err != nil {
		return fmt.Errorf("%w: history cannot be encoded", ErrInvalidJob)
	} else if len(historyPayload) > maxHistoryBytes {
		return fmt.Errorf("%w: history exceeds serialized size limit", ErrInvalidJob)
	}
	restoredTenant, err := j.Tenant.Restore()
	if err != nil {
		return err
	}
	if restoredTenant.TenantID != j.Tenant.TenantID || restoredTenant.SessionID != j.Tenant.SessionID {
		return fmt.Errorf("%w: restored tenant context does not match job", ErrInvalidJob)
	}
	return nil
}

func EncodeJob(j AgentJob) (JobEnvelope, error) {
	if err := j.Validate(); err != nil {
		return JobEnvelope{}, err
	}
	payload, err := marshalJob(j)
	if err != nil {
		return JobEnvelope{}, err
	}
	return JobEnvelope{SchemaVersion: SchemaVersion, JobID: j.JobID, Payload: payload}, nil
}

func marshalJob(j AgentJob) ([]byte, error) {
	payload, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("%w: encode job: %v", ErrInvalidJob, err)
	}
	if len(payload) > maxHistoryBytes+maxMessageLength {
		return nil, fmt.Errorf("%w: encoded job exceeds size limit", ErrInvalidJob)
	}
	return payload, nil
}

func validateMessages(messages []MessageDTO, field string) error {
	if field == "history" && len(messages) > maxHistoryMessages {
		return fmt.Errorf("%w: history exceeds message count limit", ErrInvalidJob)
	}
	for _, message := range messages {
		if !validID(message.ID) || strings.TrimSpace(message.Content) == "" {
			return fmt.Errorf("%w: %s message is invalid", ErrInvalidJob, field)
		}
		if len(message.Content) > maxMessageLength {
			return fmt.Errorf("%w: %s content exceeds limit", ErrInvalidJob, field)
		}
		for _, call := range message.ToolCalls {
			if !validID(call.ID) || strings.TrimSpace(call.Name) == "" || len(call.Arguments) > maxMessageLength {
				return fmt.Errorf("%w: %s tool call is invalid", ErrInvalidJob, field)
			}
		}
	}
	return nil
}

func (e JobEnvelope) Validate() error {
	if e.SchemaVersion != SchemaVersion || !validID(e.JobID) || len(e.Payload) == 0 {
		return ErrInvalidEnvelope
	}
	return nil
}

func DecodeJob(e JobEnvelope) (AgentJob, error) {
	job, err := DecodeJobAt(e, time.Now().UTC(), defaultJobMaxAge)
	if err != nil {
		return AgentJob{}, err
	}
	return job, nil
}

func DecodeJobAt(e JobEnvelope, now time.Time, maxAge time.Duration) (AgentJob, error) {
	if err := e.Validate(); err != nil {
		return AgentJob{}, err
	}
	var j AgentJob
	if err := json.Unmarshal(e.Payload, &j); err != nil {
		return AgentJob{}, fmt.Errorf("%w: decode payload: %v", ErrInvalidEnvelope, err)
	}
	if j.JobID != e.JobID || j.SchemaVersion != e.SchemaVersion {
		return AgentJob{}, fmt.Errorf("%w: envelope and payload disagree", ErrInvalidEnvelope)
	}
	if err := j.ValidateAt(now, maxAge); err != nil {
		return AgentJob{}, err
	}
	return j, nil
}

// QueueReceipt records whether a queue accepted a Job.
type QueueReceipt struct {
	Accepted  bool   `json:"accepted"`
	ReceiptID string `json:"receipt_id"`
	JobID     string `json:"job_id"`
	Attempt   int    `json:"attempt"`
}

type Delivery struct {
	DeliveryID   string
	Envelope     JobEnvelope
	Job          AgentJob
	Attempt      int
	ReceivedAt   time.Time
	VisibleUntil time.Time
}

type NackOptions struct {
	Requeue    bool
	RetryAfter time.Duration
	Reason     string
}

func (o NackOptions) Validate() error {
	if o.RetryAfter < 0 || o.RetryAfter > DefaultJobMaxAge {
		return fmt.Errorf("%w: invalid retry delay", ErrInvalidDelivery)
	}
	if len(o.Reason) > maxNackReasonLength {
		return fmt.Errorf("%w: nack reason is too long", ErrInvalidDelivery)
	}
	return nil
}

func validID(id string) bool {
	if len(id) == 0 || len(id) > maxJobIDLength {
		return false
	}
	for _, r := range id {
		if r == '-' || r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}
