package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func validEnvelope() ExecutionEnvelope {
	return ExecutionEnvelope{SchemaVersion: 1, TenantID: "t_01", TenantVersion: 1, AgentAppID: "app_01", AgentAppVersion: 2, AgentAppRevision: 3, AgentContentDigest: "digest", ConfigVersion: 4, PolicyVersion: 5, RequestID: "req_01", SessionID: "s_01", UserID: "u_01", Channel: "fake", InputSeq: 1, PayloadRef: "payload://1", CreatedAt: time.Unix(1, 0).UTC()}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	in := validEnvelope()
	data, err := MarshalEnvelope(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round trip mismatch: %#v", out)
	}
}
func TestEnvelopeRejectsUnknownSchema(t *testing.T) {
	in := validEnvelope()
	in.SchemaVersion = 2
	_, err := MarshalEnvelope(in)
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("got %v", err)
	}
}
func TestEnvelopeRejectsUnknownField(t *testing.T) {
	data := []byte(`{"schema_version":1,"unknown":true}`)
	if _, err := UnmarshalEnvelope(data); err == nil {
		t.Fatal("expected unknown field rejection")
	}
}

func TestEnvelopeOmitsUnsetBudgetForSchemaV1Workers(t *testing.T) {
	data, err := MarshalEnvelope(validEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(`"execution_budget"`)) {
		t.Fatalf("unset budget must not add a field to schema-v1 payload: %s", data)
	}
	// This mirrors the pre-budget decoder: unknown fields remain fatal, so an
	// unchanged worker can consume the zero-budget payload during rollout.
	var legacy struct {
		SchemaVersion      uint16    `json:"schema_version"`
		TenantID           string    `json:"tenant_id"`
		TenantVersion      int64     `json:"tenant_version"`
		AgentAppID         string    `json:"agent_app_id"`
		AgentAppVersion    int64     `json:"agent_app_version"`
		AgentAppRevision   int64     `json:"agent_app_revision"`
		AgentContentDigest string    `json:"agent_content_digest"`
		ConfigVersion      int64     `json:"config_version"`
		PolicyVersion      int64     `json:"policy_version"`
		RequestID          string    `json:"request_id"`
		SessionID          string    `json:"session_id"`
		UserID             string    `json:"user_id"`
		Channel            string    `json:"channel"`
		InputSeq           uint64    `json:"input_seq"`
		PayloadRef         string    `json:"payload_ref"`
		TraceParent        string    `json:"traceparent,omitempty"`
		CreatedAt          time.Time `json:"created_at"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&legacy); err != nil {
		t.Fatalf("legacy worker could not decode zero-budget envelope: %v", err)
	}
}

func TestEnvelopeIncludesConfiguredBudget(t *testing.T) {
	in := validEnvelope()
	in.ExecutionBudget = ExecutionBudget{MaxLLMCalls: 2}
	data, err := MarshalEnvelope(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"execution_budget"`)) {
		t.Fatalf("configured budget missing from payload: %s", data)
	}
	out, err := UnmarshalEnvelope(data)
	if err != nil || out.ExecutionBudget != in.ExecutionBudget {
		t.Fatalf("out=%#v err=%v", out, err)
	}
}
