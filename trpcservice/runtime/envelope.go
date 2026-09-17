// Package runtime contains backend-neutral execution contracts.
package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const CurrentEnvelopeSchemaVersion uint16 = 1

// ExecutionEnvelope is the only cross-process execution payload. Large or
// sensitive data is represented by references.
type ExecutionEnvelope struct {
	SchemaVersion      uint16          `json:"schema_version"`
	TenantID           string          `json:"tenant_id"`
	TenantVersion      int64           `json:"tenant_version"`
	AgentAppID         string          `json:"agent_app_id"`
	AgentAppVersion    int64           `json:"agent_app_version"`
	AgentAppRevision   int64           `json:"agent_app_revision"`
	AgentContentDigest string          `json:"agent_content_digest"`
	ConfigVersion      int64           `json:"config_version"`
	PolicyVersion      int64           `json:"policy_version"`
	ExecutionBudget    ExecutionBudget `json:"execution_budget"`
	RequestID          string          `json:"request_id"`
	SessionID          string          `json:"session_id"`
	UserID             string          `json:"user_id"`
	Channel            string          `json:"channel"`
	InputSeq           uint64          `json:"input_seq"`
	PayloadRef         string          `json:"payload_ref"`
	TraceParent        string          `json:"traceparent,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
}

// MarshalJSON preserves schema-v1 compatibility for deployments that have
// not enabled execution budgets. An older worker rejects unknown JSON fields,
// so the additive budget field must be absent, rather than encoded as {},
// until a non-zero budget is deliberately published after that fleet is
// upgraded. New workers treat an absent field as the all-zero (unlimited)
// budget.
func (e ExecutionEnvelope) MarshalJSON() ([]byte, error) {
	type wireEnvelope struct {
		SchemaVersion      uint16           `json:"schema_version"`
		TenantID           string           `json:"tenant_id"`
		TenantVersion      int64            `json:"tenant_version"`
		AgentAppID         string           `json:"agent_app_id"`
		AgentAppVersion    int64            `json:"agent_app_version"`
		AgentAppRevision   int64            `json:"agent_app_revision"`
		AgentContentDigest string           `json:"agent_content_digest"`
		ConfigVersion      int64            `json:"config_version"`
		PolicyVersion      int64            `json:"policy_version"`
		ExecutionBudget    *ExecutionBudget `json:"execution_budget,omitempty"`
		RequestID          string           `json:"request_id"`
		SessionID          string           `json:"session_id"`
		UserID             string           `json:"user_id"`
		Channel            string           `json:"channel"`
		InputSeq           uint64           `json:"input_seq"`
		PayloadRef         string           `json:"payload_ref"`
		TraceParent        string           `json:"traceparent,omitempty"`
		CreatedAt          time.Time        `json:"created_at"`
	}
	var budget *ExecutionBudget
	if e.ExecutionBudget != (ExecutionBudget{}) {
		value := e.ExecutionBudget
		budget = &value
	}
	return json.Marshal(wireEnvelope{
		SchemaVersion: e.SchemaVersion, TenantID: e.TenantID, TenantVersion: e.TenantVersion,
		AgentAppID: e.AgentAppID, AgentAppVersion: e.AgentAppVersion, AgentAppRevision: e.AgentAppRevision,
		AgentContentDigest: e.AgentContentDigest, ConfigVersion: e.ConfigVersion, PolicyVersion: e.PolicyVersion,
		ExecutionBudget: budget, RequestID: e.RequestID, SessionID: e.SessionID, UserID: e.UserID,
		Channel: e.Channel, InputSeq: e.InputSeq, PayloadRef: e.PayloadRef, TraceParent: e.TraceParent,
		CreatedAt: e.CreatedAt,
	})
}

// Validate performs structural validation only. Trust is established by
// comparing the envelope with the authoritative task record.
func (e ExecutionEnvelope) Validate() error {
	if e.SchemaVersion != CurrentEnvelopeSchemaVersion {
		return fmt.Errorf("%w: %d", ErrUnsupportedSchema, e.SchemaVersion)
	}
	if e.TenantID == "" || e.TenantVersion < 1 || e.AgentAppID == "" ||
		e.AgentAppVersion < 1 || e.AgentAppRevision < 1 ||
		e.AgentContentDigest == "" || e.ConfigVersion < 1 || e.PolicyVersion < 1 ||
		e.RequestID == "" || e.SessionID == "" || e.UserID == "" ||
		e.Channel == "" || e.InputSeq < 1 || e.PayloadRef == "" || e.CreatedAt.IsZero() {
		return ErrInvalidEnvelope
	}
	if err := e.ExecutionBudget.Validate(); err != nil {
		return err
	}
	return nil
}

// MarshalEnvelope encodes the current schema after validation.
func MarshalEnvelope(e ExecutionEnvelope) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// UnmarshalEnvelope rejects unknown schemas and unknown fields. Compatible
// optional fields can be introduced with an explicit codec revision.
func UnmarshalEnvelope(data []byte) (ExecutionEnvelope, error) {
	var e ExecutionEnvelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return ExecutionEnvelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return ExecutionEnvelope{}, ErrInvalidEnvelope
		}
		return ExecutionEnvelope{}, fmt.Errorf("decode envelope trailing data: %w", err)
	}
	if err := e.Validate(); err != nil {
		return ExecutionEnvelope{}, err
	}
	return e, nil
}
