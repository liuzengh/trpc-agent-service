package agentapp

import (
	"context"
	"errors"
)

var (
	ErrNotFound        = errors.New("agent app not found")
	ErrVersionConflict = errors.New("agent app version conflict")
	ErrInvalid         = errors.New("invalid agent app")
	ErrImmutable       = errors.New("published revision is immutable")
	ErrStatusConflict  = errors.New("agent app status conflict")
)

type ChangeMetadata struct {
	ActorType, ActorID, Reason, CorrelationID, TraceID string
}

func (m ChangeMetadata) Validate() error {
	if m.ActorType == "" || m.ActorID == "" || m.Reason == "" || m.CorrelationID == "" || m.TraceID == "" {
		return ErrInvalid
	}
	return nil
}

type CreateInput struct {
	App AgentApp
	ChangeMetadata
}

type CreateDraftInput struct {
	TenantID, AgentAppID string
	ExpectedAppVersion   int64
	Revision             Revision
	ChangeMetadata
}

type UpdateDraftInput struct {
	Revision             Revision
	ExpectedDraftVersion int64
	ChangeMetadata
}

type PublishInput struct {
	TenantID, AgentAppID string
	Revision             int64
	ExpectedAppVersion   int64
	ExpectedDraftVersion int64
	ChangeMetadata
}

type PublishResult struct {
	App      AgentApp
	Revision Revision
}

type RollbackInput struct {
	TenantID, AgentAppID string
	TargetRevision       int64
	ExpectedAppVersion   int64
	ChangeMetadata
}

type RollbackResult struct{ App AgentApp }

type TransitionStatusInput struct {
	TenantID, AgentAppID string
	NextStatus           Status
	ExpectedAppVersion   int64
	ChangeMetadata
}

type ChangeResult struct{ App AgentApp }

type Repository interface {
	Create(context.Context, CreateInput) (AgentApp, error)
	Get(context.Context, string, string) (AgentApp, error)
	CreateDraft(context.Context, CreateDraftInput) (Revision, error)
	UpdateDraft(context.Context, UpdateDraftInput) (Revision, error)
	GetRevision(context.Context, string, string, int64) (Revision, error)
	Publish(context.Context, PublishInput) (PublishResult, error)
	Rollback(context.Context, RollbackInput) (RollbackResult, error)
	TransitionStatus(context.Context, TransitionStatusInput) (ChangeResult, error)
}
