package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestModelRepositoryGetDecodesStoredProfile(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: model.StatusActive,
		SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	options, generation, err := encodeModelJSON(profile.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "provider", "model", "endpoint",
		"options", "secret_ref", "generation", "content_digest", "version", "created_at", "updated_at",
	}).AddRow(
		profile.TenantID, profile.ProfileID, profile.ProfileKey, profile.DisplayName, profile.Description, string(profile.Status), profile.SchemaVersion,
		profile.Configuration.Provider, profile.Configuration.Model, profile.Configuration.Endpoint, options, profile.Configuration.SecretRef, generation,
		profile.ContentDigest, profile.Version, profile.CreatedAt, profile.UpdatedAt,
	))

	stored, err := NewRepository(db, catalog).Get(context.Background(), profile.TenantID, profile.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ProfileID != profile.ProfileID || stored.Configuration.Options["mode"] != "safe" {
		t.Fatalf("stored model profile = %+v", stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestModelRepositoryRejectsInvalidCreationMetadata(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, _, err = NewRepository(db, catalog).Create(context.Background(), model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "invalid-metadata", DisplayName: "Invalid metadata", Status: model.StatusActive,
		SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
	})
	if !errors.Is(err, model.ErrInvalid) {
		t.Fatalf("invalid creation metadata error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestModelRepositoryListsProfiles(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: model.StatusActive,
		SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`FROM public\.model_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).WillReturnRows(testModelProfileRows(t, profile))

	items, next, err := NewRepository(db, catalog).List(context.Background(), profile.TenantID, "primary", string(profile.Status), "", 50)
	if err != nil || len(items) != 1 || items[0].ProfileID != profile.ProfileID || next != "" {
		t.Fatalf("listed model profiles = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestModelRepositoryUpdatesConfigurationAndReturnsEvent(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "update", DisplayName: "Update", Status: model.StatusActive,
		SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "initial"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input := model.UpdateConfigurationInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, DisplayName: "Updated", SchemaVersion: profile.SchemaVersion,
		Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "updated"}},
		Metadata:      model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "update", CorrelationID: "model-update"},
	}
	updated, _, err := model.PrepareConfigurationChange(*profile, input, catalog, profile.UpdatedAt.Add(1))
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, profile))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(3)))
	mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, &updated))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "profile_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow("configuration_updated", profile.TenantID, profile.ProfileID, string(profile.Status), string(updated.Status), profile.ContentDigest, updated.ContentDigest, "test", "user", "update", "model-update", profile.Version, updated.Version, updated.UpdatedAt))
	mock.ExpectCommit()

	stored, event, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if stored.DisplayName != updated.DisplayName || stored.Version != updated.Version || event.EventType != model.EventConfigurationUpdated || event.NextVersion != updated.Version {
		t.Fatalf("updated model profile = %+v, event = %+v", stored, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestModelRepositoryTransitionsStatusAndReturnsEvent(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "transition", DisplayName: "Transition", Status: model.StatusActive,
		SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input := model.TransitionStatusInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, NextStatus: model.StatusSuspended,
		Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "suspend", CorrelationID: "model-transition"},
	}
	updated, _, err := model.PrepareStatusChange(*profile, input, catalog, profile.UpdatedAt.Add(1))
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, profile))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(3)))
	mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, &updated))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "profile_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow("suspended", profile.TenantID, profile.ProfileID, string(profile.Status), string(updated.Status), profile.ContentDigest, updated.ContentDigest, "test", "user", "suspend", "model-transition", profile.Version, updated.Version, updated.UpdatedAt))
	mock.ExpectCommit()

	stored, event, err := NewRepository(db, catalog).TransitionStatus(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.StatusSuspended || stored.Version != updated.Version || event.EventType != model.EventSuspended || event.NextVersion != updated.Version {
		t.Fatalf("transitioned model profile = %+v, event = %+v", stored, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestModelRepositoryRequiresStorage(t *testing.T) {
	repository := NewRepository(nil, nil)
	ctx := context.Background()
	if _, _, err := repository.Create(ctx, model.CreateInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create nil-storage error = %v", err)
	}
	if _, err := repository.Get(ctx, "tenant", "profile"); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get nil-storage error = %v", err)
	}
	if _, _, err := repository.List(ctx, "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("List nil-storage error = %v", err)
	}
	if _, _, err := repository.UpdateConfiguration(ctx, model.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateConfiguration nil-storage error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, model.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("TransitionStatus nil-storage error = %v", err)
	}
}

func testModelProfileRows(t *testing.T, profiles ...*model.Profile) *sqlmock.Rows {
	t.Helper()
	rows := sqlmock.NewRows([]string{
		"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "provider", "model", "endpoint",
		"options", "secret_ref", "generation", "content_digest", "version", "created_at", "updated_at",
	})
	for _, profile := range profiles {
		options, generation, err := encodeModelJSON(profile.Configuration)
		if err != nil {
			t.Fatal(err)
		}
		rows.AddRow(
			profile.TenantID, profile.ProfileID, profile.ProfileKey, profile.DisplayName, profile.Description, string(profile.Status), profile.SchemaVersion,
			profile.Configuration.Provider, profile.Configuration.Model, profile.Configuration.Endpoint, options, profile.Configuration.SecretRef, generation,
			profile.ContentDigest, profile.Version, profile.CreatedAt, profile.UpdatedAt,
		)
	}
	return rows
}
