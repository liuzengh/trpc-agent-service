package channels

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// EventType identifies one committed Binding mutation.
type EventType string

const (
	// EventCreated identifies a newly stored Binding.
	EventCreated EventType = "created"
	// EventConfigurationUpdated identifies a complete mutable configuration replacement.
	EventConfigurationUpdated EventType = "configuration_updated"
	// EventActivated identifies draft or suspended admission into inbound routing.
	EventActivated EventType = "activated"
	// EventSuspended identifies a transition that removes a Binding from routing.
	EventSuspended EventType = "suspended"
	// EventResumed identifies a suspended Binding becoming active again.
	EventResumed EventType = "resumed"
	// EventDisabled identifies the terminal lifecycle transition.
	EventDisabled EventType = "disabled"
)

// ChangeMetadata is trusted audit context required for a control-plane change.
type ChangeMetadata struct {
	ActorType     string
	ActorID       string
	Reason        string
	CorrelationID string
}

// ChangeEvent is the immutable audit handoff for one atomic Binding change.
// It intentionally contains no route key or credential value.
type ChangeEvent struct {
	EventType       EventType
	TenantID        string
	BindingID       string
	PreviousStatus  Status
	CurrentStatus   Status
	PreviousDigest  string
	CurrentDigest   string
	ActorType       string
	ActorID         string
	Reason          string
	CorrelationID   string
	PreviousVersion int64
	NextVersion     int64
	OccurredAt      time.Time
}

// UpdateConfigurationInput replaces every mutable Binding configuration under
// an optimistic lock. Binding identity, key, channel, and lifecycle remain
// unchanged.
type UpdateConfigurationInput struct {
	TenantID             string
	BindingID            string
	ExpectedVersion      int64
	ProviderAccountID    string
	PublicRouteKeyDigest string
	AppID                string
	SecretRef            string
	Protocol             ProtocolConfiguration
	Metadata             ChangeMetadata
}

// TransitionStatusInput requests one expected-version protected lifecycle
// transition. Use the named Repository helpers when possible so callers do not
// accidentally request the wrong transition.
type TransitionStatusInput struct {
	TenantID        string
	BindingID       string
	ExpectedVersion int64
	NextStatus      Status
	Metadata        ChangeMetadata
}

// Repository is the explicit tenant-scoped Channel Binding control-plane
// contract. Every lookup and mutation carries tenant_id and binding_id.
type Repository interface {
	Create(context.Context, CreateInput) (*Binding, ChangeEvent, error)
	Get(context.Context, string, string) (*Binding, error)
	UpdateConfiguration(context.Context, UpdateConfigurationInput) (*Binding, ChangeEvent, error)
	TransitionStatus(context.Context, TransitionStatusInput) (*Binding, ChangeEvent, error)
	Activate(context.Context, TransitionStatusInput) (*Binding, ChangeEvent, error)
	Suspend(context.Context, TransitionStatusInput) (*Binding, ChangeEvent, error)
	Resume(context.Context, TransitionStatusInput) (*Binding, ChangeEvent, error)
	Disable(context.Context, TransitionStatusInput) (*Binding, ChangeEvent, error)
}

// CandidateIndex is the restricted public-route lookup boundary. It returns
// opaque candidate contexts rather than Tenant, App, SecretRef, or full
// Binding data.
type CandidateIndex interface {
	LookupCandidates(context.Context, Channel, string) ([]CandidateBindingContext, error)
}

// CandidateConsumer is the private control-plane surface needed by the
// package-owned offline verifier. Keeping candidate consumption behind this
// interface lets the verifier mint proof-bearing results without exposing a
// public shape-only VerifiedBinding constructor.
type CandidateConsumer interface {
	CandidateIndex
	Get(context.Context, string, string) (*Binding, error)
	ConsumeCandidate(context.Context, CandidateBindingContext) (*Binding, error)
}

// PrepareCreatedChange validates the initial Binding and builds its audit
// event without mutating repository state.
func PrepareCreatedChange(binding Binding, metadata ChangeMetadata) (ChangeEvent, error) {
	if err := binding.Validate(); err != nil {
		return ChangeEvent{}, err
	}
	if binding.Status == StatusDisabled {
		return ChangeEvent{}, fmt.Errorf("%w: created binding cannot be disabled", ErrInvalid)
	}
	metadata, err := normalizeChangeMetadata(metadata)
	if err != nil {
		return ChangeEvent{}, err
	}
	if binding.Version != 1 || binding.CreatedAt.IsZero() || binding.CreatedAt.Location() != time.UTC || !binding.UpdatedAt.Equal(binding.CreatedAt) {
		return ChangeEvent{}, fmt.Errorf("%w: created binding is not initialized", ErrInvalid)
	}
	return newChangeEvent(EventCreated, binding, Status(""), binding.Status, "", binding.ConfigDigest, 0, binding.Version, binding.CreatedAt, metadata), nil
}

// PrepareConfigurationChange constructs a validated Binding and change event
// without mutating repository state. The Repository commits both atomically.
func PrepareConfigurationChange(current Binding, input UpdateConfigurationInput, occurredAt time.Time) (Binding, ChangeEvent, error) {
	if current.TenantID != input.TenantID || current.BindingID != input.BindingID {
		return Binding{}, ChangeEvent{}, ErrNotFound
	}
	if err := current.Validate(); err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	if current.Status == StatusDisabled {
		return Binding{}, ChangeEvent{}, ErrDisabled
	}
	if input.ExpectedVersion != current.Version {
		return Binding{}, ChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", ErrConflict, input.ExpectedVersion, current.Version)
	}
	metadata, err := normalizeChangeMetadata(input.Metadata)
	if err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	providerAccountID, err := normalizeRequiredValue(input.ProviderAccountID, maxProviderAccountLength, "provider account id")
	if err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	routeDigest, err := normalizeRouteKeyDigest(input.PublicRouteKeyDigest)
	if err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	if err := validateAppID(input.AppID); err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	secretRef, err := normalizeRequiredValue(input.SecretRef, maxSecretRefLength, "secret reference")
	if err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	protocol, err := normalizeProtocolConfiguration(current.Channel, input.Protocol)
	if err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	if occurredAt.IsZero() || occurredAt.Location() != time.UTC || occurredAt.Before(current.UpdatedAt) {
		return Binding{}, ChangeEvent{}, fmt.Errorf("%w: mutation time must be UTC and monotonic", ErrInvalid)
	}
	updated := current.Clone()
	updated.ProviderAccountID = providerAccountID
	updated.PublicRouteKeyDigest = routeDigest
	updated.AppID = input.AppID
	updated.SecretRef = secretRef
	updated.Protocol = protocol
	updated.Version++
	updated.UpdatedAt = occurredAt
	updated.ConfigDigest, err = updated.computeConfigDigest()
	if err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	if err := updated.Validate(); err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	event := newChangeEvent(EventConfigurationUpdated, updated, current.Status, updated.Status, current.ConfigDigest, updated.ConfigDigest, current.Version, updated.Version, occurredAt, metadata)
	return updated, event, nil
}

// PrepareStatusChange constructs a validated lifecycle transition and its
// event without mutating repository state.
func PrepareStatusChange(current Binding, input TransitionStatusInput, occurredAt time.Time) (Binding, ChangeEvent, error) {
	if current.TenantID != input.TenantID || current.BindingID != input.BindingID {
		return Binding{}, ChangeEvent{}, ErrNotFound
	}
	if err := current.Validate(); err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	if current.Status == StatusDisabled {
		return Binding{}, ChangeEvent{}, ErrDisabled
	}
	if input.ExpectedVersion != current.Version {
		return Binding{}, ChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", ErrConflict, input.ExpectedVersion, current.Version)
	}
	if !current.CanTransitionTo(input.NextStatus) {
		return Binding{}, ChangeEvent{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current.Status, input.NextStatus)
	}
	metadata, err := normalizeChangeMetadata(input.Metadata)
	if err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	if occurredAt.IsZero() || occurredAt.Location() != time.UTC || occurredAt.Before(current.UpdatedAt) {
		return Binding{}, ChangeEvent{}, fmt.Errorf("%w: mutation time must be UTC and monotonic", ErrInvalid)
	}
	updated := current.Clone()
	updated.Status = input.NextStatus
	updated.Version++
	updated.UpdatedAt = occurredAt
	if err := updated.Validate(); err != nil {
		return Binding{}, ChangeEvent{}, err
	}
	eventType := EventActivated
	switch input.NextStatus {
	case StatusSuspended:
		eventType = EventSuspended
	case StatusActive:
		if current.Status == StatusSuspended {
			eventType = EventResumed
		}
	case StatusDisabled:
		eventType = EventDisabled
	}
	event := newChangeEvent(eventType, updated, current.Status, updated.Status, current.ConfigDigest, updated.ConfigDigest, current.Version, updated.Version, occurredAt, metadata)
	return updated, event, nil
}

func normalizeChangeMetadata(metadata ChangeMetadata) (ChangeMetadata, error) {
	metadata.ActorType = strings.TrimSpace(metadata.ActorType)
	metadata.ActorID = strings.TrimSpace(metadata.ActorID)
	metadata.Reason = strings.TrimSpace(metadata.Reason)
	metadata.CorrelationID = strings.TrimSpace(metadata.CorrelationID)
	if metadata.ActorType == "" || metadata.ActorID == "" || metadata.Reason == "" || metadata.CorrelationID == "" {
		return ChangeMetadata{}, fmt.Errorf("%w: change metadata requires actor, reason, and correlation ID", ErrInvalid)
	}
	if len([]rune(metadata.Reason)) > 1000 || hasControl(metadata.ActorType) || hasControl(metadata.ActorID) || hasControl(metadata.Reason) || hasControl(metadata.CorrelationID) {
		return ChangeMetadata{}, fmt.Errorf("%w: change metadata contains invalid text", ErrInvalid)
	}
	return metadata, nil
}

func newChangeEvent(eventType EventType, binding Binding, previousStatus, currentStatus Status, previousDigest, currentDigest string, previousVersion, nextVersion int64, occurredAt time.Time, metadata ChangeMetadata) ChangeEvent {
	return ChangeEvent{
		EventType: eventType, TenantID: binding.TenantID, BindingID: binding.BindingID,
		PreviousStatus: previousStatus, CurrentStatus: currentStatus,
		PreviousDigest: previousDigest, CurrentDigest: currentDigest,
		ActorType: metadata.ActorType, ActorID: metadata.ActorID, Reason: metadata.Reason,
		CorrelationID: metadata.CorrelationID, PreviousVersion: previousVersion,
		NextVersion: nextVersion, OccurredAt: occurredAt,
	}
}
