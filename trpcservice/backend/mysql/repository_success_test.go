package mysql

import (
	"context"
	"database/sql"
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
	mock.ExpectQuery(`SELECT profile_id FROM backend_profile WHERE tenant_id = \? ORDER BY profile_id`).WithArgs(profile.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"profile_id"}).AddRow(profile.ProfileID))
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

func TestBackendRepositoryListBoundaries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := NewRepository(nil, nil).List(ctx, "tenant", "", "", "", 50); err == nil {
		t.Fatal("canceled backend list was accepted")
	}
	if _, _, err := NewRepository(nil, nil).List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil backend list error = %v", err)
	}
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{}})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository := NewRepository(db, catalog)
	if _, _, err := repository.List(context.Background(), "tenant", "", "", "bad", 50); err == nil {
		t.Fatal("invalid backend cursor was accepted")
	}
	mock.ExpectQuery(`SELECT profile_id FROM backend_profile WHERE tenant_id = \? ORDER BY profile_id`).WithArgs("tenant").WillReturnError(errors.New("query down"))
	if _, _, err := repository.List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend query error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	scanDB, scanMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanDB.Close() })
	scanMock.ExpectQuery(`SELECT profile_id FROM backend_profile WHERE tenant_id = \? ORDER BY profile_id`).WithArgs("tenant").WillReturnRows(sqlmock.NewRows([]string{"profile_id", "extra"}).AddRow("profile", "bad"))
	if _, _, err := NewRepository(scanDB, catalog).List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend scan error = %v", err)
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(3, 1))
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(3, 1))
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
	if _, _, err := repository.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateConfiguration nil-storage error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, backend.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("TransitionStatus nil-storage error = %v", err)
	}
}

func TestBackendRepositoryCreatesProfileAndEvent(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "created", DisplayName: "Created", Status: backend.StatusActive,
		SchemaVersion: 1, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "create", CorrelationID: "backend-create"},
	}
	value, err := backend.NewProfile(input, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(9, 1))
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "profile_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow("created", value.TenantID, value.ProfileID, nil, string(value.Status), nil, value.ContentDigest, "test", "user", "create", "backend-create", 0, value.Version, value.CreatedAt))
	mock.ExpectCommit()
	created, event, err := NewRepository(db, catalog).Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if created.ProfileID != value.ProfileID || event.EventType != backend.EventCreated {
		t.Fatalf("created profile = %+v, event = %+v", created, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepositoryDatabaseAndConflictErrors(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden,
		SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	validCreate := backend.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "error-path", DisplayName: "Error Path", Status: backend.StatusActive,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "error", CorrelationID: "backend-error"},
	}
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock
	}
	for _, operation := range []string{"Create", "Update", "Transition"} {
		t.Run(operation+" begin", func(t *testing.T) {
			db, mock := newDB(t)
			mock.ExpectBegin().WillReturnError(errors.New("begin"))
			var err error
			switch operation {
			case "Create":
				_, _, err = NewRepository(db, catalog).Create(context.Background(), validCreate)
			case "Update":
				_, _, err = NewRepository(db, catalog).UpdateConfiguration(context.Background(), backend.UpdateConfigurationInput{})
			default:
				_, _, err = NewRepository(db, catalog).TransitionStatus(context.Background(), backend.TransitionStatusInput{})
			}
			if !errors.Is(err, ErrStorage) {
				t.Fatalf("%s begin error = %v", operation, err)
			}
		})
	}
	value, err := backend.NewProfile(validCreate, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock := newDB(t)
	mock.ExpectQuery(".*").WillReturnError(errors.New("read"))
	if _, err := NewRepository(db, catalog).Get(context.Background(), value.TenantID, value.ProfileID); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get read error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, value))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	input := backend.UpdateConfigurationInput{TenantID: value.TenantID, ProfileID: value.ProfileID, ExpectedVersion: value.Version, DisplayName: value.DisplayName, SchemaVersion: value.SchemaVersion, Bindings: value.Bindings, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "conflict", CorrelationID: "backend-conflict"}}
	if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), input); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("Update conflict = %v", err)
	}
}

func TestBackendRepositoryWriteErrorBranches(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}}})
	if err != nil {
		t.Fatal(err)
	}
	create := backend.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "write-errors", DisplayName: "Write Errors", Status: backend.StatusActive, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}}, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "write", CorrelationID: "backend-write"}}
	value, err := backend.NewProfile(create, catalog)
	if err != nil {
		t.Fatal(err)
	}
	update := backend.UpdateConfigurationInput{TenantID: value.TenantID, ProfileID: value.ProfileID, ExpectedVersion: value.Version, DisplayName: value.DisplayName, SchemaVersion: value.SchemaVersion, Bindings: value.Bindings, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "update", CorrelationID: "backend-update-errors"}}
	transition := backend.TransitionStatusInput{TenantID: value.TenantID, ProfileID: value.ProfileID, ExpectedVersion: value.Version, NextStatus: backend.StatusSuspended, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "transition", CorrelationID: "backend-transition-errors"}}
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock
	}
	db, mock := newDB(t)
	mock.ExpectBegin()
	mock.ExpectExec(".*").WillReturnError(errors.New("insert"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).Create(context.Background(), create); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create insert error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO backend_profile_binding").WillReturnError(errors.New("insert binding"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).Create(context.Background(), create); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create binding delete error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO backend_profile_binding").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM backend_profile_binding").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("outbox"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).Create(context.Background(), create); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create outbox error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, value))
	mock.ExpectExec(".*").WillReturnError(errors.New("update"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update write error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, value))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO backend_profile_binding").WillReturnError(errors.New("insert binding"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update binding error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(testBackendBindingRows(t, value))
	mock.ExpectExec(".*").WillReturnError(errors.New("transition"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
		t.Fatalf("Transition write error = %v", err)
	}
}

func TestBackendRepositoryScanAndBindingErrors(t *testing.T) {
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := backend.NewProfile(backend.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "scan", DisplayName: "Scan", Status: backend.StatusActive, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "safe"}}}}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectExec("INSERT INTO backend_profile_binding").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM backend_profile_binding").WillReturnError(errors.New("delete"))
	if err := replaceBackendBindings(context.Background(), db, *value); !errors.Is(err, ErrStorage) {
		t.Fatalf("binding insert error = %v", err)
	}
	mock.ExpectQuery(".*").WillReturnRows(testBackendRootRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"capability", "provider", "endpoint", "options", "secret_ref"}).AddRow(string(backend.CapabilitySession), "inmemory", "", []byte("not-json"), ""))
	if _, err := loadBackendProfile(context.Background(), db, catalog, value.TenantID, value.ProfileID, false); !errors.Is(err, ErrStorage) {
		t.Fatalf("corrupt binding error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepositoryScannersRejectShortRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("short"))
	if _, err := scanBackendEvent(db.QueryRowContext(context.Background(), "SELECT 1")); err == nil {
		t.Fatal("short event row was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func testBackendRootRows(t *testing.T, profiles ...*backend.Profile) *sqlmock.Rows {
	t.Helper()
	rows := sqlmock.NewRows([]string{
		"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "content_digest", "version", "created_at", "updated_at",
	})
	for _, profile := range profiles {
		rows.AddRow(profile.TenantID, profile.ProfileID, profile.ProfileKey, profile.DisplayName, profile.Description, string(profile.Status), profile.SchemaVersion, profile.ContentDigest, profile.Version, profile.CreatedAt, profile.UpdatedAt)
	}
	return rows
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
