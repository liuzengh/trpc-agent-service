package app

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const validTenantID = "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"

func validAppInput(key string) CreateInput {
	return CreateInput{
		TenantID:    validTenantID,
		AppKey:      key,
		DisplayName: " Example App ",
		Description: " Example description ",
	}
}

func TestNewAppNormalizesStableIdentityAndDefaults(t *testing.T) {
	app, err := NewApp(validAppInput(" Example-App "))
	if err != nil {
		t.Fatal(err)
	}
	if app.TenantID != validTenantID || app.AppKey != "example-app" {
		t.Fatalf("unexpected identity: %+v", app)
	}
	if err := validateAppID(app.AppID); err != nil {
		t.Fatalf("generated app id is invalid: %v", err)
	}
	if app.DisplayName != "Example App" || app.Description != "Example description" {
		t.Fatalf("metadata was not normalized: %+v", app)
	}
	if app.Status != StatusDraft || app.CurrentRevision != nil || app.Version != 1 {
		t.Fatalf("unexpected new app state: %+v", app)
	}
	if app.CreatedAt.IsZero() || !app.CreatedAt.Equal(app.UpdatedAt) || app.CreatedAt.Location() != time.UTC {
		t.Fatalf("unexpected timestamps: %+v", app)
	}
	if app.CanAcceptExecution() {
		t.Fatal("draft app must not accept execution")
	}
	if err := app.Validate(); err != nil {
		t.Fatalf("new app must validate: %v", err)
	}
}

func TestAppCanaryValidationAndClone(t *testing.T) {
	current, candidate := int64(1), int64(2)
	app := App{TenantID: validTenantID, AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppKey: "app", DisplayName: "App", Status: StatusActive, CurrentRevision: &current, CanaryRevision: &candidate, Version: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	clone := app.Clone()
	*clone.CanaryRevision = 3
	if *app.CanaryRevision != 2 {
		t.Fatal("clone leaked canary pointer")
	}
	if err := app.Validate(); err != nil {
		t.Fatalf("valid canary app rejected: %v", err)
	}
	app.CanaryRevision = &current
	if err := app.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("same canary/current accepted: %v", err)
	}
}

func TestNewAppRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*CreateInput)
	}{
		{name: "tenant id", mutate: func(input *CreateInput) { input.TenantID = "tenant" }},
		{name: "short key", mutate: func(input *CreateInput) { input.AppKey = "a" }},
		{name: "invalid key", mutate: func(input *CreateInput) { input.AppKey = "app_key" }},
		{name: "blank display name", mutate: func(input *CreateInput) { input.DisplayName = " " }},
		{name: "long display name", mutate: func(input *CreateInput) { input.DisplayName = strings.Repeat("界", 201) }},
		{name: "long description", mutate: func(input *CreateInput) { input.Description = strings.Repeat("界", 2001) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validAppInput("valid-key")
			test.mutate(&input)
			if _, err := NewApp(input); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}

func TestAppValidateRejectsMalformedRootState(t *testing.T) {
	app, err := NewApp(validAppInput("validate-app"))
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(1)
	tests := []struct {
		name   string
		mutate func(*App)
	}{
		{name: "tenant id", mutate: func(app *App) { app.TenantID = "bad" }},
		{name: "app id", mutate: func(app *App) { app.AppID = "bad" }},
		{name: "unnormalized key", mutate: func(app *App) { app.AppKey = "UPPER" }},
		{name: "unnormalized name", mutate: func(app *App) { app.DisplayName = " padded " }},
		{name: "unknown status", mutate: func(app *App) { app.Status = Status("unknown") }},
		{name: "draft current revision", mutate: func(app *App) { app.CurrentRevision = &revision }},
		{name: "active without revision", mutate: func(app *App) { app.Status = StatusActive }},
		{name: "suspended without revision", mutate: func(app *App) { app.Status = StatusSuspended }},
		{name: "disabled invalid revision", mutate: func(app *App) { zero := int64(0); app.Status = StatusDisabled; app.CurrentRevision = &zero }},
		{name: "zero version", mutate: func(app *App) { app.Version = 0 }},
		{name: "zero timestamp", mutate: func(app *App) { app.CreatedAt = time.Time{} }},
		{name: "reversed timestamps", mutate: func(app *App) { app.UpdatedAt = app.CreatedAt.Add(-time.Second) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := app.Clone()
			test.mutate(&invalid)
			if !errors.Is(invalid.Validate(), ErrInvalid) {
				t.Fatalf("expected malformed root rejection: %+v", invalid)
			}
		})
	}
}

func TestAppCloneAndLifecycleContracts(t *testing.T) {
	app, err := NewApp(validAppInput("lifecycle-app"))
	if err != nil {
		t.Fatal(err)
	}
	if !app.CanTransitionTo(StatusDisabled) || app.CanTransitionTo(StatusActive) {
		t.Fatal("unexpected draft transition set")
	}

	revision := int64(2)
	active := app.Clone()
	active.Status = StatusActive
	active.CurrentRevision = &revision
	if !active.CanAcceptExecution() || !active.CanTransitionTo(StatusSuspended) || !active.CanTransitionTo(StatusDisabled) || active.CanTransitionTo(StatusDraft) {
		t.Fatal("unexpected active lifecycle behavior")
	}
	clone := active.Clone()
	*clone.CurrentRevision = 9
	if *active.CurrentRevision != 2 {
		t.Fatal("clone leaked current revision pointer")
	}

	suspended := active.Clone()
	suspended.Status = StatusSuspended
	if suspended.CanAcceptExecution() || !suspended.CanTransitionTo(StatusActive) || !suspended.CanTransitionTo(StatusDisabled) {
		t.Fatal("unexpected suspended lifecycle behavior")
	}

	disabled := active.Clone()
	disabled.Status = StatusDisabled
	if disabled.CanAcceptExecution() || disabled.CanTransitionTo(StatusActive) || disabled.CanTransitionTo(StatusDisabled) {
		t.Fatal("disabled app must be terminal")
	}
}

func TestIdentityValidationBoundaries(t *testing.T) {
	for _, id := range []string{
		"app_01J1K9ZQTVE4PAWF1TSB2WMHNP",
		"app_71J1K9ZQTVE4PAWF1TSB2WMHNP",
	} {
		if err := validateAppID(id); err != nil {
			t.Fatalf("expected valid app id %q: %v", id, err)
		}
	}
	for _, id := range []string{
		"app_81J1K9ZQTVE4PAWF1TSB2WMHNP",
		"app_01J1K9ZQTVE4PAWF1TSB2WMHN!",
		"t_01J1K9ZQTVE4PAWF1TSB2WMHNP",
	} {
		if !errors.Is(validateAppID(id), ErrInvalid) {
			t.Fatalf("expected invalid app id %q", id)
		}
	}
}
