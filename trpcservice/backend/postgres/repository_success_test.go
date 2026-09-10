package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

func TestBackendRepositoryGetDecodesBindings(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := backend.NewProfile(backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: backend.StatusActive,
		SchemaVersion: 1,
		Bindings:      []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	options, err := encodeJSON(profile.Bindings[0].Options)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "content_digest", "version", "created_at", "updated_at",
	}).AddRow(
		profile.TenantID, profile.ProfileID, profile.ProfileKey, profile.DisplayName, profile.Description, string(profile.Status), profile.SchemaVersion,
		profile.ContentDigest, profile.Version, profile.CreatedAt, profile.UpdatedAt,
	))
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(sqlmock.NewRows([]string{
		"capability", "provider", "endpoint", "options", "secret_ref",
	}).AddRow(string(profile.Bindings[0].Capability), profile.Bindings[0].Provider, "", options, ""))

	stored, err := NewRepository(db, catalog).Get(context.Background(), profile.TenantID, profile.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Bindings) != 1 || stored.Bindings[0].Options["namespace"] != "safe" {
		t.Fatalf("stored backend profile = %+v", stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepositoryRejectsInvalidCreationMetadata(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, _, err = NewRepository(db, catalog).Create(context.Background(), backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "invalid-metadata", DisplayName: "Invalid metadata", Status: backend.StatusActive,
		SchemaVersion: 1, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
	})
	if !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("invalid creation metadata error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepositoryListsProfiles(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := backend.NewProfile(backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: backend.StatusActive,
		SchemaVersion: 1, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT profile_id FROM public\.backend_profile WHERE tenant_id=\$1`).WithArgs(profile.TenantID).WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID))
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(testBackendRootRows(t, profile))
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(testBackendBindingRows(t, profile))

	items, next, err := NewRepository(db, catalog).List(context.Background(), profile.TenantID, "primary", string(profile.Status), "", 50)
	if err != nil || len(items) != 1 || items[0].ProfileID != profile.ProfileID || next != "" {
		t.Fatalf("listed backend profiles = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepositoryUpdatesConfigurationAndReturnsEvent(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := backend.NewProfile(backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "update", DisplayName: "Update", Status: backend.StatusActive,
		SchemaVersion: 1,
		Bindings:      []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "initial"}}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input := backend.UpdateConfigurationInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, DisplayName: "Updated", SchemaVersion: profile.SchemaVersion,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "updated"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "update", CorrelationID: "backend-update"},
	}
	updated, _, err := backend.PrepareConfigurationChange(*profile, input, catalog, profile.UpdatedAt.Add(1))
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, profile))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, profile))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(3)))
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, &updated))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, &updated))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "profile_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow("configuration_updated", profile.TenantID, profile.ProfileID, string(profile.Status), string(updated.Status), profile.ContentDigest, updated.ContentDigest, "test", "user", "update", "backend-update", profile.Version, updated.Version, updated.UpdatedAt))
	mock.ExpectCommit()

	stored, event, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if stored.DisplayName != updated.DisplayName || stored.Version != updated.Version || event.EventType != backend.EventConfigurationUpdated || event.NextVersion != updated.Version {
		t.Fatalf("updated backend profile = %+v, event = %+v", stored, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepositoryTransitionsStatusAndReturnsEvent(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := backend.NewProfile(backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "transition", DisplayName: "Transition", Status: backend.StatusActive,
		SchemaVersion: 1,
		Bindings:      []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input := backend.TransitionStatusInput{
		TenantID: profile.TenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, NextStatus: backend.StatusSuspended,
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "suspend", CorrelationID: "backend-transition"},
	}
	updated, _, err := backend.PrepareStatusChange(*profile, input, catalog, profile.UpdatedAt.Add(1))
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, profile))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, profile))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"next_version"}).AddRow(updated.Version))
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, &updated))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, &updated))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "profile_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow("suspended", profile.TenantID, profile.ProfileID, string(profile.Status), string(updated.Status), profile.ContentDigest, updated.ContentDigest, "test", "user", "suspend", "backend-transition", profile.Version, updated.Version, updated.UpdatedAt))
	mock.ExpectCommit()

	stored, event, err := NewRepository(db, catalog).TransitionStatus(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != backend.StatusSuspended || stored.Version != updated.Version || event.EventType != backend.EventSuspended || event.NextVersion != updated.Version {
		t.Fatalf("transitioned backend profile = %+v, event = %+v", stored, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepositoryRequiresStorage(t *testing.T) {
	repository := NewRepository(nil, nil)
	ctx := context.Background()
	if _, _, err := repository.Create(ctx, backend.CreateInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create nil-storage error = %v", err)
	}
	if _, err := repository.Get(ctx, "tenant", "profile"); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get nil-storage error = %v", err)
	}
	if _, _, err := repository.List(ctx, "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("List nil-storage error = %v", err)
	}
	if _, _, err := repository.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateConfiguration nil-storage error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, backend.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("TransitionStatus nil-storage error = %v", err)
	}
}

func testBackendRootRows(t *testing.T, profile *backend.Profile) *sqlmock.Rows {
	t.Helper()
	return sqlmock.NewRows([]string{
		"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "content_digest", "version", "created_at", "updated_at",
	}).AddRow(profile.TenantID, profile.ProfileID, profile.ProfileKey, profile.DisplayName, profile.Description, string(profile.Status), profile.SchemaVersion, profile.ContentDigest, profile.Version, profile.CreatedAt, profile.UpdatedAt)
}

func testBackendBindingRows(t *testing.T, profile *backend.Profile) *sqlmock.Rows {
	t.Helper()
	options, err := encodeJSON(profile.Bindings[0].Options)
	if err != nil {
		t.Fatal(err)
	}
	return sqlmock.NewRows([]string{"capability", "provider", "endpoint", "options", "secret_ref"}).AddRow(
		string(profile.Bindings[0].Capability), profile.Bindings[0].Provider, profile.Bindings[0].Endpoint, options, profile.Bindings[0].SecretRef,
	)
}
