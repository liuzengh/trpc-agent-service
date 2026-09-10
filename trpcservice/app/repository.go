package app

import (
	"context"
	"time"
)

// Repository is the tenant-scoped control-plane contract for Agent Apps.
// Every operation keeps both tenant and app identity explicit.
type Repository interface {
	Create(context.Context, CreateInput) (*App, error)
	Get(context.Context, string, string) (*App, error)
	UpdateMetadata(context.Context, UpdateMetadataInput) (*App, error)
	CreateDraft(context.Context, CreateDraftInput) (*Revision, error)
	UpdateDraft(context.Context, UpdateDraftInput) (*Revision, error)
	GetRevision(context.Context, string, string, int64) (*Revision, error)
	Publish(context.Context, PublishInput) (*App, *Revision, ChangeEvent, error)
	Rollback(context.Context, RollbackInput) (*App, ChangeEvent, error)
	SetCanary(context.Context, SetCanaryInput) (*App, ChangeEvent, error)
	TransitionStatus(context.Context, TransitionStatusInput) (*App, ChangeEvent, error)
}

// UpdateMetadataInput replaces mutable App display metadata under an
// optimistic lock. Stable identity and app_key are immutable.
type UpdateMetadataInput struct {
	TenantID        string
	AppID           string
	ExpectedVersion int64
	DisplayName     string
	Description     string
}

// CreateDraftInput requests allocation of the next App-local revision number.
// ExpectedAppVersion prevents creating new work from stale lifecycle state.
type CreateDraftInput struct {
	TenantID           string
	AppID              string
	ExpectedAppVersion int64
	Kind               Kind
	SchemaVersion      int
	Configuration      DraftConfiguration
}

// UpdateDraftInput replaces a complete mutable draft configuration. Both the
// App root and draft carry independent optimistic-lock versions.
type UpdateDraftInput struct {
	TenantID             string
	AppID                string
	Revision             int64
	ExpectedAppVersion   int64
	ExpectedDraftVersion int64
	Configuration        DraftConfiguration
}

// ChangeMetadata is mandatory trusted audit context for publication,
// rollback, and lifecycle transitions.
type ChangeMetadata struct {
	ActorType     string
	ActorID       string
	Reason        string
	CorrelationID string
}

// ChangeEventType identifies a control-plane mutation requiring audit/outbox
// persistence in a durable Repository.
type ChangeEventType string

const (
	// ChangePublished records a revision publication.
	ChangePublished ChangeEventType = "published"
	// ChangeRolledBack records a rollback to an earlier revision.
	ChangeRolledBack ChangeEventType = "rolled_back"
	// ChangeSuspended records an application suspension.
	ChangeSuspended ChangeEventType = "suspended"
	// ChangeResumed records an application resume.
	ChangeResumed ChangeEventType = "resumed"
	// ChangeDisabled records an application disablement.
	ChangeDisabled ChangeEventType = "disabled"
	// ChangeCanaryStarted records candidate selection.
	ChangeCanaryStarted ChangeEventType = "canary_started"
	// ChangeCanaryStopped records candidate clearing.
	ChangeCanaryStopped ChangeEventType = "canary_stopped"
)

// ChangeEvent is the complete immutable audit handoff returned atomically with
// every publication, rollback, or lifecycle transition.
type ChangeEvent struct {
	EventType        ChangeEventType
	TenantID         string
	AppID            string
	PreviousRevision *int64
	CurrentRevision  *int64
	ContentDigest    string
	PreviousStatus   Status
	CurrentStatus    Status
	ActorType        string
	ActorID          string
	Reason           string
	CorrelationID    string
	PreviousVersion  int64
	NextVersion      int64
	OccurredAt       time.Time
}

// Clone returns a defensive copy of an audit event.
func (e ChangeEvent) Clone() ChangeEvent {
	clone := e
	clone.PreviousRevision = cloneInt64(e.PreviousRevision)
	clone.CurrentRevision = cloneInt64(e.CurrentRevision)
	return clone
}

// PublishInput requests atomic draft publication and App pointer movement.
// TenantActive is a trusted control-plane gate supplied from the Tenant root.
type PublishInput struct {
	TenantID             string
	AppID                string
	Revision             int64
	ExpectedAppVersion   int64
	ExpectedDraftVersion int64
	TenantActive         bool
	Metadata             ChangeMetadata
}

// RollbackInput selects an existing immutable published Revision without
// copying or renumbering it.
type RollbackInput struct {
	TenantID           string
	AppID              string
	TargetRevision     int64
	ExpectedAppVersion int64
	Metadata           ChangeMetadata
}

// SetCanaryInput selects or clears a published candidate revision.
type SetCanaryInput struct {
	TenantID           string
	AppID              string
	CandidateRevision  *int64
	ExpectedAppVersion int64
	TenantActive       bool
	Metadata           ChangeMetadata
}

// TransitionStatusInput requests a lifecycle transition under an optimistic
// App version check.
type TransitionStatusInput struct {
	TenantID        string
	AppID           string
	ExpectedVersion int64
	NextStatus      Status
	Metadata        ChangeMetadata
}
