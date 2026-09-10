package backend

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// EventType identifies a committed Backend Profile change.
type EventType string

const (
	// EventCreated records initial profile creation.
	EventCreated EventType = "created"
	// EventConfigurationUpdated records a profile configuration change.
	EventConfigurationUpdated EventType = "configuration_updated"
	// EventSuspended records a profile suspension.
	EventSuspended EventType = "suspended"
	// EventResumed records a profile resume.
	EventResumed EventType = "resumed"
	// EventDisabled records a profile disablement.
	EventDisabled EventType = "disabled"
)

// ChangeMetadata is the trusted audit context required for every mutation.
type ChangeMetadata struct {
	ActorType     string
	ActorID       string
	Reason        string
	CorrelationID string
}

// ChangeEvent describes one atomically committed Profile mutation.
type ChangeEvent struct {
	EventType       EventType
	TenantID        string
	ProfileID       string
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

// UpdateConfigurationInput replaces all mutable Profile configuration.
// ProfileKey and lifecycle Status remain unchanged.
type UpdateConfigurationInput struct {
	TenantID        string
	ProfileID       string
	ExpectedVersion int64
	DisplayName     string
	Description     string
	SchemaVersion   int
	Bindings        []CapabilityBinding
	Metadata        ChangeMetadata
}

// TransitionStatusInput requests an optimistic-lock protected lifecycle change.
type TransitionStatusInput struct {
	TenantID        string
	ProfileID       string
	ExpectedVersion int64
	NextStatus      Status
	Metadata        ChangeMetadata
}

// Repository is the tenant-scoped Backend Profile persistence contract.
type Repository interface {
	Create(context.Context, CreateInput) (*Profile, ChangeEvent, error)
	Get(context.Context, string, string) (*Profile, error)
	UpdateConfiguration(context.Context, UpdateConfigurationInput) (*Profile, ChangeEvent, error)
	TransitionStatus(context.Context, TransitionStatusInput) (*Profile, ChangeEvent, error)
}

// PrepareCreatedChange builds the audit event that must commit with a new Profile.
func PrepareCreatedChange(profile Profile, catalog *ProviderCatalog, metadata ChangeMetadata) (ChangeEvent, error) {
	if err := profile.Validate(catalog); err != nil {
		return ChangeEvent{}, err
	}
	if profile.Status != StatusActive && profile.Status != StatusSuspended {
		return ChangeEvent{}, fmt.Errorf("%w: created profile cannot be disabled", ErrInvalid)
	}
	metadata, err := normalizeChangeMetadata(metadata)
	if err != nil {
		return ChangeEvent{}, err
	}
	if profile.Version != 1 || profile.CreatedAt.IsZero() || profile.CreatedAt.Location() != time.UTC || !profile.UpdatedAt.Equal(profile.CreatedAt) {
		return ChangeEvent{}, fmt.Errorf("%w: created profile is not initialized", ErrInvalid)
	}
	return newChangeEvent(
		EventCreated, profile, "", profile.Status, "", profile.ContentDigest,
		0, profile.Version, profile.CreatedAt, metadata,
	), nil
}

// PrepareConfigurationChange constructs a validated Profile and event without
// mutating repository state. Adapters commit both values atomically.
func PrepareConfigurationChange(current Profile, input UpdateConfigurationInput, catalog *ProviderCatalog, occurredAt time.Time) (Profile, ChangeEvent, error) {
	if current.TenantID != input.TenantID || current.ProfileID != input.ProfileID {
		return Profile{}, ChangeEvent{}, ErrNotFound
	}
	if err := current.Validate(catalog); err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	if current.Status == StatusDisabled {
		return Profile{}, ChangeEvent{}, ErrDisabled
	}
	if input.ExpectedVersion != current.Version {
		return Profile{}, ChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", ErrConflict, input.ExpectedVersion, current.Version)
	}
	metadata, err := normalizeChangeMetadata(input.Metadata)
	if err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	displayName, description, err := normalizeMetadata(input.DisplayName, input.Description)
	if err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	bindings, digest, err := normalizeConfiguration(input.SchemaVersion, input.Bindings, catalog)
	if err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	if err := validateMutationTime(current, occurredAt); err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	updated := current.Clone()
	updated.DisplayName = displayName
	updated.Description = description
	updated.SchemaVersion = input.SchemaVersion
	updated.Bindings = bindings
	updated.ContentDigest = digest
	updated.Version++
	updated.UpdatedAt = occurredAt
	if err := updated.Validate(catalog); err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	event := newChangeEvent(
		EventConfigurationUpdated, updated, current.Status, updated.Status,
		current.ContentDigest, updated.ContentDigest, current.Version, updated.Version,
		occurredAt, metadata,
	)
	return updated, event, nil
}

// PrepareStatusChange constructs a validated lifecycle change and its event
// without mutating repository state.
func PrepareStatusChange(current Profile, input TransitionStatusInput, catalog *ProviderCatalog, occurredAt time.Time) (Profile, ChangeEvent, error) {
	if current.TenantID != input.TenantID || current.ProfileID != input.ProfileID {
		return Profile{}, ChangeEvent{}, ErrNotFound
	}
	if err := current.Validate(catalog); err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	if current.Status == StatusDisabled {
		return Profile{}, ChangeEvent{}, ErrDisabled
	}
	if input.ExpectedVersion != current.Version {
		return Profile{}, ChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", ErrConflict, input.ExpectedVersion, current.Version)
	}
	if !current.CanTransitionTo(input.NextStatus) {
		return Profile{}, ChangeEvent{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current.Status, input.NextStatus)
	}
	metadata, err := normalizeChangeMetadata(input.Metadata)
	if err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	if err := validateMutationTime(current, occurredAt); err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	updated := current.Clone()
	updated.Status = input.NextStatus
	updated.Version++
	updated.UpdatedAt = occurredAt
	if err := updated.Validate(catalog); err != nil {
		return Profile{}, ChangeEvent{}, err
	}
	eventType := EventSuspended
	switch input.NextStatus {
	case StatusActive:
		eventType = EventResumed
	case StatusDisabled:
		eventType = EventDisabled
	}
	event := newChangeEvent(
		eventType, updated, current.Status, updated.Status,
		current.ContentDigest, updated.ContentDigest, current.Version, updated.Version,
		occurredAt, metadata,
	)
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
	if len([]rune(metadata.Reason)) > 1000 {
		return ChangeMetadata{}, fmt.Errorf("%w: change reason must contain at most 1000 characters", ErrInvalid)
	}
	return metadata, nil
}

func validateMutationTime(current Profile, occurredAt time.Time) error {
	if occurredAt.IsZero() || occurredAt.Location() != time.UTC || occurredAt.Before(current.CreatedAt) || occurredAt.Before(current.UpdatedAt) {
		return fmt.Errorf("%w: mutation time must be UTC and monotonic", ErrInvalid)
	}
	return nil
}

func newChangeEvent(eventType EventType, profile Profile, previousStatus, currentStatus Status, previousDigest, currentDigest string, previousVersion, nextVersion int64, occurredAt time.Time, metadata ChangeMetadata) ChangeEvent {
	return ChangeEvent{
		EventType: eventType, TenantID: profile.TenantID, ProfileID: profile.ProfileID,
		PreviousStatus: previousStatus, CurrentStatus: currentStatus,
		PreviousDigest: previousDigest, CurrentDigest: currentDigest,
		ActorType: metadata.ActorType, ActorID: metadata.ActorID, Reason: metadata.Reason,
		CorrelationID: metadata.CorrelationID, PreviousVersion: previousVersion,
		NextVersion: nextVersion, OccurredAt: occurredAt,
	}
}
