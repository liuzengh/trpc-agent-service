package model

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPrepareProfileChangesValidateAuditAndLifecycleEvents(t *testing.T) {
	catalog := modelTestCatalog(t)
	profile, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: "prepared", DisplayName: "Prepared",
		Configuration: Configuration{Provider: "fake", Model: "deterministic"},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ChangeMetadata{ActorType: " admin ", ActorID: " user-1 ", Reason: " test change ", CorrelationID: " corr-1 "}
	assertPreparedCreatedChange(t, profile, catalog, metadata)
	assertPreparedConfigurationChange(t, profile, catalog, metadata)
	assertPreparedStatusChange(t, profile, catalog, metadata)
}

func assertPreparedCreatedChange(t *testing.T, profile *Profile, catalog *ProviderCatalog, metadata ChangeMetadata) {
	t.Helper()
	createdEvent, err := PrepareCreatedChange(*profile, catalog, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if createdEvent.EventType != EventCreated || createdEvent.CurrentStatus != StatusActive || createdEvent.NextVersion != 1 || createdEvent.ActorType != "admin" || createdEvent.Reason != "test change" {
		t.Fatalf("created event = %+v", createdEvent)
	}
	if createdEvent.Clone() != createdEvent {
		t.Fatal("ChangeEvent.Clone changed event value")
	}

	badCreated := profile.Clone()
	badCreated.Status = StatusDisabled
	if _, err := PrepareCreatedChange(badCreated, catalog, metadata); !errors.Is(err, ErrInvalid) {
		t.Fatalf("disabled created profile error = %v", err)
	}
	if _, err := PrepareCreatedChange(*profile, catalog, ChangeMetadata{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("incomplete created metadata error = %v", err)
	}
	longReason := strings.Repeat("x", 1001)
	if _, err := PrepareCreatedChange(*profile, catalog, ChangeMetadata{ActorType: "admin", ActorID: "user", Reason: longReason, CorrelationID: "corr"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("long created reason error = %v", err)
	}
	badCreated.Version = 2
	if _, err := PrepareCreatedChange(badCreated, catalog, metadata); !errors.Is(err, ErrInvalid) {
		t.Fatalf("uninitialized created profile error = %v", err)
	}
}

func assertPreparedConfigurationChange(t *testing.T, profile *Profile, catalog *ProviderCatalog, metadata ChangeMetadata) {
	t.Helper()
	updateInput := UpdateConfigurationInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		DisplayName: "Updated", Description: "Updated description", SchemaVersion: SchemaVersionV1,
		Configuration: Configuration{Provider: "fake", Model: "deterministic", Options: map[string]string{"mode": "fast"}}, Metadata: metadata,
	}
	updated, updateEvent, err := PrepareConfigurationChange(*profile, updateInput, catalog, profile.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.Configuration.Options["mode"] != "fast" || updateEvent.EventType != EventConfigurationUpdated || updateEvent.PreviousVersion != 1 || updateEvent.NextVersion != 2 {
		t.Fatalf("configuration change = profile=%+v event=%+v", updated, updateEvent)
	}
	if profile.Version != 1 {
		t.Fatal("PrepareConfigurationChange mutated current profile")
	}

	wrongScope := updateInput
	wrongScope.TenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if _, _, err := PrepareConfigurationChange(*profile, wrongScope, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong-scope update error = %v", err)
	}
	stale := updateInput
	stale.ExpectedVersion = 99
	if _, _, err := PrepareConfigurationChange(*profile, stale, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	if _, _, err := PrepareConfigurationChange(*profile, updateInput, catalog, time.Time{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero update time error = %v", err)
	}
	if _, _, err := PrepareConfigurationChange(*profile, updateInput, catalog, profile.UpdatedAt.Add(-time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-monotonic update time error = %v", err)
	}
	if _, _, err := PrepareConfigurationChange(*profile, updateInput, catalog, profile.UpdatedAt.In(time.FixedZone("test", 3600))); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-UTC update time error = %v", err)
	}
	missingMetadata := updateInput
	missingMetadata.Metadata = ChangeMetadata{}
	if _, _, err := PrepareConfigurationChange(*profile, missingMetadata, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing update metadata error = %v", err)
	}
	badDisplay := updateInput
	badDisplay.DisplayName = ""
	if _, _, err := PrepareConfigurationChange(*profile, badDisplay, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad update metadata error = %v", err)
	}
	badConfig := updateInput
	badConfig.Configuration.Provider = "unknown"
	if _, _, err := PrepareConfigurationChange(*profile, badConfig, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad update configuration error = %v", err)
	}
	disabled := profile.Clone()
	disabled.Status = StatusDisabled
	if _, _, err := PrepareConfigurationChange(disabled, updateInput, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled update error = %v", err)
	}
}

func assertPreparedStatusChange(t *testing.T, profile *Profile, catalog *ProviderCatalog, metadata ChangeMetadata) {
	t.Helper()
	disabled := profile.Clone()
	disabled.Status = StatusDisabled
	transitionInput := TransitionStatusInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		NextStatus: StatusSuspended, Metadata: metadata,
	}
	suspended, suspendedEvent, err := PrepareStatusChange(*profile, transitionInput, catalog, profile.UpdatedAt.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if suspended.Status != StatusSuspended || suspended.Version != 2 || suspendedEvent.EventType != EventSuspended {
		t.Fatalf("suspension = profile=%+v event=%+v", suspended, suspendedEvent)
	}
	resumed, resumedEvent, err := PrepareStatusChange(suspended, TransitionStatusInput{
		TenantID: suspended.TenantID, ProfileID: suspended.ProfileID, ExpectedVersion: suspended.Version,
		NextStatus: StatusActive, Metadata: metadata,
	}, catalog, suspended.UpdatedAt.Add(time.Second))
	if err != nil || resumed.Status != StatusActive || resumedEvent.EventType != EventResumed {
		t.Fatalf("resume = profile=%+v event=%+v err=%v", resumed, resumedEvent, err)
	}
	_, disabledEvent, err := PrepareStatusChange(suspended, TransitionStatusInput{
		TenantID: suspended.TenantID, ProfileID: suspended.ProfileID, ExpectedVersion: suspended.Version,
		NextStatus: StatusDisabled, Metadata: metadata,
	}, catalog, suspended.UpdatedAt.Add(2*time.Second))
	if err != nil || disabledEvent.EventType != EventDisabled {
		t.Fatalf("disable event = %+v err=%v", disabledEvent, err)
	}

	wrongTransitionScope := transitionInput
	wrongTransitionScope.ProfileID = "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if _, _, err := PrepareStatusChange(*profile, wrongTransitionScope, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong-scope transition error = %v", err)
	}
	staleTransition := transitionInput
	staleTransition.ExpectedVersion = 99
	if _, _, err := PrepareStatusChange(*profile, staleTransition, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale transition error = %v", err)
	}
	invalidTransition := transitionInput
	invalidTransition.NextStatus = StatusActive
	if _, _, err := PrepareStatusChange(*profile, invalidTransition, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("invalid transition error = %v", err)
	}
	disabledTransition := transitionInput
	disabledTransition.ExpectedVersion = 1
	if _, _, err := PrepareStatusChange(disabled, disabledTransition, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled transition error = %v", err)
	}
	if _, _, err := PrepareStatusChange(*profile, TransitionStatusInput{TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, NextStatus: StatusSuspended}, catalog, profile.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing transition metadata error = %v", err)
	}
	if _, _, err := PrepareStatusChange(*profile, transitionInput, catalog, profile.UpdatedAt.Add(-time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-monotonic transition time error = %v", err)
	}
}
