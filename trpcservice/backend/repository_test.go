package backend

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPrepareCreatedChange(t *testing.T) {
	catalog := newTestCatalog(t)
	profile := newTestProfile(t, catalog)
	event, err := PrepareCreatedChange(*profile, catalog, repositoryMetadata())
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != EventCreated || event.TenantID != profile.TenantID || event.ProfileID != profile.ProfileID ||
		event.PreviousStatus != "" || event.CurrentStatus != profile.Status || event.PreviousDigest != "" ||
		event.CurrentDigest != profile.ContentDigest || event.PreviousVersion != 0 || event.NextVersion != 1 ||
		event.ActorType != "admin" || event.ActorID != "user-1" || event.Reason != "test" ||
		event.CorrelationID != "request-1" || event.OccurredAt != profile.CreatedAt {
		t.Fatalf("unexpected created event: %+v", event)
	}
	corrupt := profile.Clone()
	corrupt.ContentDigest = "bad"
	if _, err := PrepareCreatedChange(corrupt, catalog, repositoryMetadata()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("corrupt Profile error = %v", err)
	}
	mutated := profile.Clone()
	mutated.UpdatedAt = mutated.CreatedAt.Add(time.Second)
	if _, err := PrepareCreatedChange(mutated, catalog, repositoryMetadata()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("already-mutated creation error = %v", err)
	}
	disabled := profile.Clone()
	disabled.Status = StatusDisabled
	if _, err := PrepareCreatedChange(disabled, catalog, repositoryMetadata()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("created disabled Profile error = %v", err)
	}
	metadata := repositoryMetadata()
	metadata.ActorID = " "
	if _, err := PrepareCreatedChange(*profile, catalog, metadata); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid metadata error = %v", err)
	}
}

func TestPrepareConfigurationChange(t *testing.T) {
	catalog := newTestCatalog(t)
	current := newTestProfile(t, catalog)
	input := repositoryUpdateInput(current)
	input.Bindings = append(input.Bindings, CapabilityBinding{
		Capability: CapabilityAudit, Provider: "inmemory", Options: map[string]string{"namespace": "audit"},
	})
	occurredAt := current.UpdatedAt.Add(time.Second)
	updated, event, err := PrepareConfigurationChange(*current, input, catalog, occurredAt)
	if err != nil {
		t.Fatal(err)
	}
	if updated.DisplayName != "Updated" || updated.Description != "description" || updated.ProfileKey != current.ProfileKey ||
		updated.Version != current.Version+1 || updated.UpdatedAt != occurredAt || updated.ContentDigest == current.ContentDigest {
		t.Fatalf("unexpected updated Profile: %+v", updated)
	}
	if event.EventType != EventConfigurationUpdated || event.PreviousVersion != current.Version || event.NextVersion != updated.Version ||
		event.PreviousDigest != current.ContentDigest || event.CurrentDigest != updated.ContentDigest ||
		event.PreviousStatus != current.Status || event.CurrentStatus != current.Status {
		t.Fatalf("unexpected configuration event: %+v", event)
	}
	input.Bindings[0].Options["database"] = "caller-mutated"
	if updated.Bindings[0].Options["database"] == "caller-mutated" {
		t.Fatal("prepared Profile retained caller-owned options")
	}
}

func TestPrepareConfigurationChangeRejectsInvalidBoundaries(t *testing.T) {
	catalog := newTestCatalog(t)
	profile := newTestProfile(t, catalog)
	valid := repositoryUpdateInput(profile)
	now := profile.UpdatedAt.Add(time.Second)
	tests := []struct {
		name   string
		mutate func(*Profile, *UpdateConfigurationInput, *time.Time)
		want   error
	}{
		{name: "scope", mutate: func(_ *Profile, input *UpdateConfigurationInput, _ *time.Time) {
			input.TenantID = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
		}, want: ErrNotFound},
		{name: "corrupt current", mutate: func(profile *Profile, _ *UpdateConfigurationInput, _ *time.Time) { profile.ContentDigest = "bad" }, want: ErrInvalid},
		{name: "disabled", mutate: func(profile *Profile, _ *UpdateConfigurationInput, _ *time.Time) { profile.Status = StatusDisabled }, want: ErrDisabled},
		{name: "version", mutate: func(_ *Profile, input *UpdateConfigurationInput, _ *time.Time) { input.ExpectedVersion++ }, want: ErrConflict},
		{name: "metadata", mutate: func(_ *Profile, input *UpdateConfigurationInput, _ *time.Time) { input.Metadata.Reason = " " }, want: ErrInvalid},
		{name: "display", mutate: func(_ *Profile, input *UpdateConfigurationInput, _ *time.Time) { input.DisplayName = " " }, want: ErrInvalid},
		{name: "schema", mutate: func(_ *Profile, input *UpdateConfigurationInput, _ *time.Time) { input.SchemaVersion = 2 }, want: ErrInvalid},
		{name: "active without Session", mutate: func(_ *Profile, input *UpdateConfigurationInput, _ *time.Time) {
			input.Bindings = []CapabilityBinding{{Capability: CapabilityMemory, Provider: "inmemory"}}
		}, want: ErrInvalid},
		{name: "zero time", mutate: func(_ *Profile, _ *UpdateConfigurationInput, occurredAt *time.Time) { *occurredAt = time.Time{} }, want: ErrInvalid},
		{name: "non UTC", mutate: func(_ *Profile, _ *UpdateConfigurationInput, occurredAt *time.Time) {
			*occurredAt = occurredAt.In(time.FixedZone("UTC+1", 3600))
		}, want: ErrInvalid},
		{name: "time reversal", mutate: func(profile *Profile, _ *UpdateConfigurationInput, occurredAt *time.Time) {
			*occurredAt = profile.UpdatedAt.Add(-time.Second)
		}, want: ErrInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := profile.Clone()
			input := valid
			input.Bindings = cloneBindings(valid.Bindings)
			occurredAt := now
			test.mutate(&current, &input, &occurredAt)
			_, _, err := PrepareConfigurationChange(current, input, catalog, occurredAt)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPrepareStatusChangeAndBoundaries(t *testing.T) {
	catalog := newTestCatalog(t)
	profile := newTestProfile(t, catalog)
	input := repositoryTransitionInput(profile, StatusSuspended)
	occurredAt := profile.UpdatedAt.Add(time.Second)
	updated, event, err := PrepareStatusChange(*profile, input, catalog, occurredAt)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != StatusSuspended || updated.ContentDigest != profile.ContentDigest || event.EventType != EventSuspended ||
		event.PreviousDigest != event.CurrentDigest || event.PreviousStatus != StatusActive || event.CurrentStatus != StatusSuspended {
		t.Fatalf("unexpected status change: %+v %+v", updated, event)
	}

	tests := []struct {
		name   string
		mutate func(*Profile, *TransitionStatusInput, *time.Time)
		want   error
	}{
		{name: "scope", mutate: func(_ *Profile, input *TransitionStatusInput, _ *time.Time) {
			input.ProfileID = "bp_01J1K9ZQTVE4PAWF1TSB2WMHNP"
		}, want: ErrNotFound},
		{name: "corrupt current", mutate: func(profile *Profile, _ *TransitionStatusInput, _ *time.Time) { profile.ContentDigest = "bad" }, want: ErrInvalid},
		{name: "disabled", mutate: func(profile *Profile, _ *TransitionStatusInput, _ *time.Time) { profile.Status = StatusDisabled }, want: ErrDisabled},
		{name: "version", mutate: func(_ *Profile, input *TransitionStatusInput, _ *time.Time) { input.ExpectedVersion++ }, want: ErrConflict},
		{name: "invalid transition", mutate: func(_ *Profile, input *TransitionStatusInput, _ *time.Time) { input.NextStatus = StatusActive }, want: ErrInvalidTransition},
		{name: "metadata", mutate: func(_ *Profile, input *TransitionStatusInput, _ *time.Time) { input.Metadata.CorrelationID = "" }, want: ErrInvalid},
		{name: "time", mutate: func(profile *Profile, _ *TransitionStatusInput, occurredAt *time.Time) {
			*occurredAt = profile.UpdatedAt.Add(-time.Second)
		}, want: ErrInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := profile.Clone()
			input := repositoryTransitionInput(profile, StatusSuspended)
			at := occurredAt
			test.mutate(&current, &input, &at)
			_, _, err := PrepareStatusChange(current, input, catalog, at)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPrepareStatusChangeMapsTerminalEventsAndResumeRequiresSession(t *testing.T) {
	catalog := newTestCatalog(t)
	active := newTestProfile(t, catalog)
	disabled, event, err := PrepareStatusChange(
		*active, repositoryTransitionInput(active, StatusDisabled), catalog, active.UpdatedAt.Add(time.Second),
	)
	if err != nil || disabled.Status != StatusDisabled || event.EventType != EventDisabled {
		t.Fatalf("disable result = %+v %+v, error = %v", disabled, event, err)
	}
	suspended, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "no-session", DisplayName: "No Session", Status: StatusSuspended,
		Bindings: []CapabilityBinding{{Capability: CapabilityMemory, Provider: "inmemory"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := PrepareStatusChange(
		*suspended, repositoryTransitionInput(suspended, StatusActive), catalog, suspended.UpdatedAt.Add(time.Second),
	); !errors.Is(err, ErrInvalid) {
		t.Fatalf("resume without Session error = %v", err)
	}
	resumable := active.Clone()
	resumable.Status = StatusSuspended
	resumed, event, err := PrepareStatusChange(
		resumable, repositoryTransitionInput(&resumable, StatusActive), catalog, resumable.UpdatedAt.Add(time.Second),
	)
	if err != nil || resumed.Status != StatusActive || event.EventType != EventResumed {
		t.Fatalf("resume result = %+v %+v, error = %v", resumed, event, err)
	}
}

func TestDisabledProfileWithoutBindingsRemainsTerminal(t *testing.T) {
	catalog := newTestCatalog(t)
	disabled := newTestProfile(t, catalog)
	disabled.Status = StatusDisabled
	disabled.Bindings = nil
	disabled.ContentDigest = contentDigest(disabled.SchemaVersion, disabled.Bindings)

	update := repositoryUpdateInput(disabled)
	if _, _, err := PrepareConfigurationChange(*disabled, update, catalog, disabled.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrDisabled) {
		t.Fatalf("configuration mutation error = %v, want ErrDisabled", err)
	}
	transition := repositoryTransitionInput(disabled, StatusActive)
	if _, _, err := PrepareStatusChange(*disabled, transition, catalog, disabled.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrDisabled) {
		t.Fatalf("status mutation error = %v, want ErrDisabled", err)
	}
}

func TestChangeMetadataReasonLimit(t *testing.T) {
	metadata := repositoryMetadata()
	metadata.Reason = strings.Repeat("界", 1001)
	if _, err := normalizeChangeMetadata(metadata); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized reason error = %v", err)
	}
}

func TestChangeMetadataNormalizesUnicodeWhitespace(t *testing.T) {
	metadata := ChangeMetadata{
		ActorType: "\u00a0admin\u2003", ActorID: "\u202fuser-1\u3000",
		Reason: "\u205freason\u00a0", CorrelationID: "\u1680request-1\u0085",
	}
	normalized, err := normalizeChangeMetadata(metadata)
	if err != nil {
		t.Fatalf("Unicode metadata error = %v", err)
	}
	want := ChangeMetadata{ActorType: "admin", ActorID: "user-1", Reason: "reason", CorrelationID: "request-1"}
	if normalized != want {
		t.Fatalf("normalized metadata = %#v, want %#v", normalized, want)
	}

	metadata.Reason = "\u00a0"
	if _, err := normalizeChangeMetadata(metadata); !errors.Is(err, ErrInvalid) {
		t.Fatalf("all-Unicode-whitespace reason error = %v", err)
	}
}

func repositoryMetadata() ChangeMetadata {
	return ChangeMetadata{ActorType: " admin ", ActorID: " user-1 ", Reason: " test ", CorrelationID: " request-1 "}
}

func repositoryUpdateInput(profile *Profile) UpdateConfigurationInput {
	return UpdateConfigurationInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		DisplayName: " Updated ", Description: " description ", SchemaVersion: 1,
		Bindings: cloneBindings(profile.Bindings), Metadata: repositoryMetadata(),
	}
}

func repositoryTransitionInput(profile *Profile, next Status) TransitionStatusInput {
	return TransitionStatusInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		NextStatus: next, Metadata: repositoryMetadata(),
	}
}
