// Code generated from api/events/execution/v1/run-requested.schema.json; DO NOT EDIT.

// Package executionv1 contains transport DTOs, not Gateway or Worker domain entities.
package executionv1

// RunRequested is defined by the versioned execution JSON Schema.
type RunRequested struct {
	AdmissionID   string        `json:"admission_id"`
	EventID       string        `json:"event_id"`
	Input         Inbound       `json:"input"`
	Route         RouteSnapshot `json:"route"`
	RunID         string        `json:"run_id"`
	SchemaVersion int64         `json:"schema_version"`
	UsagePolicy   UsagePolicy   `json:"usage_policy,omitempty"`
}

// EventKey is defined by the versioned execution JSON Schema.
type EventKey struct {
	AccountID string `json:"account_id"`
	EventID   string `json:"event_id"`
	Provider  string `json:"provider"`
}

// Inbound is defined by the versioned execution JSON Schema.
type Inbound struct {
	ConversationID string        `json:"conversation_id"`
	Key            EventKey      `json:"key"`
	Kind           string        `json:"kind"`
	ReceivedAt     string        `json:"received_at"`
	ReplyContext   *ReplyContext `json:"reply_context"`
	SenderID       string        `json:"sender_id"`
	SourceDigest   string        `json:"source_digest"`
	Text           string        `json:"text"`
	ThreadID       string        `json:"thread_id,omitempty"`
}

// ReplyContext is defined by the versioned execution JSON Schema.
type ReplyContext struct {
	CallbackReqID   string `json:"callback_req_id,omitempty"`
	ChatID          string `json:"chat_id,omitempty"`
	ChatType        string `json:"chat_type,omitempty"`
	ChatIDOrUserID  string `json:"chatid_or_userid,omitempty"`
	MessageThreadID string `json:"message_thread_id,omitempty"`
	ReceivedAt      string `json:"received_at,omitempty"`
	SourceMessageID string `json:"source_message_id,omitempty"`
}

// RouteSnapshot is defined by the versioned execution JSON Schema.
type RouteSnapshot struct {
	AccountID            string `json:"account_id"`
	BindingID            string `json:"binding_id"`
	DeploymentRevisionID string `json:"deployment_revision_id"`
	Generation           int64  `json:"generation"`
	ManifestDigest       string `json:"manifest_digest"`
	ManifestRef          string `json:"manifest_ref"`
	Provider             string `json:"provider"`
	TenantID             string `json:"tenant_id"`
}

// UsagePolicy is defined by the versioned execution JSON Schema.
type UsagePolicy struct {
	Enabled                      bool  `json:"enabled"`
	InputMicrosPerMillionTokens  int64 `json:"input_micros_per_million_tokens"`
	MaxConcurrentRuns            int64 `json:"max_concurrent_runs"`
	OutputMicrosPerMillionTokens int64 `json:"output_micros_per_million_tokens"`
	Revision                     int64 `json:"revision"`
	TokenLimit                   int64 `json:"token_limit"`
	TokenPeriodSeconds           int64 `json:"token_period_seconds"`
	TokenReservationPerRun       int64 `json:"token_reservation_per_run"`
}
