package tenant

import (
	"context"
	"errors"
)

var (
	ErrNotFound        = errors.New("tenant not found")
	ErrInvalid         = errors.New("invalid tenant")
	ErrVersionConflict = errors.New("tenant version conflict")
	ErrStatusConflict  = errors.New("tenant status conflict")
	ErrKeyConflict     = errors.New("tenant key conflict")
)

type ChangeMetadata struct {
	ActorType, ActorID, ReasonCode, ReasonRef, CorrelationID, TraceID string
}

func (m ChangeMetadata) Validate() error {
	if m.ActorType == "" || m.ActorID == "" || m.ReasonCode == "" || m.CorrelationID == "" || m.TraceID == "" {
		return ErrInvalid
	}
	return nil
}

type CreateInput struct {
	Tenant         Tenant
	ChangeMetadata ChangeMetadata
}
type UpdateConfigurationInput struct {
	Tenant          Tenant
	ExpectedVersion int64
	ChangeMetadata  ChangeMetadata
}
type TransitionStatusInput struct {
	TenantID        string
	ExpectedVersion int64
	NextStatus      Status
	ChangeMetadata  ChangeMetadata
}
type ChangeResult struct{ Tenant Tenant }

type Repository interface {
	Create(context.Context, CreateInput) (Tenant, error)
	Get(context.Context, string) (Tenant, error)
	UpdateConfiguration(context.Context, UpdateConfigurationInput) (ChangeResult, error)
	TransitionStatus(context.Context, TransitionStatusInput) (ChangeResult, error)
}

type ChangeFact struct {
	TenantID, Kind, PreviousStatus, NextStatus string
	PreviousVersion, NextVersion               int64
	Metadata                                   ChangeMetadata
}
type OutboxFact struct {
	TenantID, Kind, IdempotencyKey string
	Version                        int64
}
