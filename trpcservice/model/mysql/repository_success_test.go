package mysql

import (
	"context"
	"database/sql"
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

func TestModelRepositoryListsProfiles(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: model.StatusActive, SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat"}}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`FROM model_profile WHERE tenant_id = \?`).WithArgs(profile.TenantID).WillReturnRows(testModelProfileRows(t, profile))
	items, next, err := NewRepository(db, catalog).List(context.Background(), profile.TenantID, "primary", string(profile.Status), "", 50)
	if err != nil || len(items) != 1 || items[0].ProfileID != profile.ProfileID || next != "" {
		t.Fatalf("listed profiles = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestModelRepositoryListBoundaries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := NewRepository(nil, nil).List(ctx, "tenant", "", "", "", 50); err == nil {
		t.Fatal("canceled model list was accepted")
	}
	if _, _, err := NewRepository(nil, nil).List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil model list error = %v", err)
	}
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional})
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
		t.Fatal("invalid model cursor was accepted")
	}
	mock.ExpectQuery(`FROM model_profile WHERE tenant_id = \?`).WithArgs("tenant").WillReturnError(errors.New("query down"))
	if _, _, err := repository.List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("model query error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	scanDB, scanMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanDB.Close() })
	scanMock.ExpectQuery(`FROM model_profile WHERE tenant_id = \?`).WithArgs("tenant").WillReturnRows(sqlmock.NewRows([]string{"profile_id", "extra"}).AddRow("profile", "bad"))
	if _, _, err := NewRepository(scanDB, catalog).List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("model scan error = %v", err)
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(3, 1))
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(3, 1))
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
	if _, _, err := repository.UpdateConfiguration(ctx, model.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateConfiguration nil-storage error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, model.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("TransitionStatus nil-storage error = %v", err)
	}
}

func TestModelRepositoryCreatesProfileAndEvent(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "created", DisplayName: "Created", Status: model.StatusActive,
		SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
		Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "create", CorrelationID: "model-create"},
	}
	value, err := model.NewProfile(input, catalog)
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(7, 1))
	mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "profile_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow("created", value.TenantID, value.ProfileID, nil, string(value.Status), nil, value.ContentDigest, "test", "user", "create", "model-create", 0, value.Version, value.CreatedAt))
	mock.ExpectCommit()
	created, event, err := NewRepository(db, catalog).Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if created.ProfileID != value.ProfileID || event.EventType != model.EventCreated {
		t.Fatalf("created profile = %+v, event = %+v", created, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestModelRepositoryDatabaseAndConflictErrors(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	validCreate := model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "error-path", DisplayName: "Error Path", Status: model.StatusActive,
		Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
		Metadata:      model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "error", CorrelationID: "model-error"},
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
	t.Run("begin failures", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin"))
		if _, _, err := NewRepository(db, catalog).Create(context.Background(), validCreate); !errors.Is(err, ErrStorage) {
			t.Fatalf("Create begin error = %v", err)
		}
		db, mock = newDB(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin"))
		if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), model.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
			t.Fatalf("Update begin error = %v", err)
		}
		db, mock = newDB(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin"))
		if _, _, err := NewRepository(db, catalog).TransitionStatus(context.Background(), model.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
			t.Fatalf("Transition begin error = %v", err)
		}
	})
	t.Run("read and optimistic conflict", func(t *testing.T) {
		value, err := model.NewProfile(validCreate, catalog)
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
		mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, value))
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()
		input := model.UpdateConfigurationInput{TenantID: value.TenantID, ProfileID: value.ProfileID, ExpectedVersion: value.Version, DisplayName: value.DisplayName, SchemaVersion: value.SchemaVersion, Configuration: value.Configuration, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "conflict", CorrelationID: "model-conflict"}}
		if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), input); !errors.Is(err, model.ErrConflict) {
			t.Fatalf("Update conflict = %v", err)
		}
	})
}

func TestModelRepositoryWriteErrorBranches(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional, Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}}})
	if err != nil {
		t.Fatal(err)
	}
	create := model.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "write-errors", DisplayName: "Write Errors", Status: model.StatusActive, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}}, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "write", CorrelationID: "model-write"}}
	value, err := model.NewProfile(create, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input := model.UpdateConfigurationInput{TenantID: value.TenantID, ProfileID: value.ProfileID, ExpectedVersion: value.Version, DisplayName: value.DisplayName, SchemaVersion: value.SchemaVersion, Configuration: value.Configuration, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "update", CorrelationID: "model-update-errors"}}
	transition := model.TransitionStatusInput{TenantID: value.TenantID, ProfileID: value.ProfileID, ExpectedVersion: value.Version, NextStatus: model.StatusSuspended, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "transition", CorrelationID: "model-transition-errors"}}
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
	mock.ExpectExec(".*").WillReturnError(errors.New("outbox"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).Create(context.Background(), create); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create outbox error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, value))
	mock.ExpectExec(".*").WillReturnError(errors.New("update"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update write error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, value))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("outbox"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).UpdateConfiguration(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update outbox error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testModelProfileRows(t, value))
	mock.ExpectExec(".*").WillReturnError(errors.New("transition"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db, catalog).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
		t.Fatalf("Transition write error = %v", err)
	}
}

func TestModelRepositoryScanRejectsCorruptRows(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional, Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := model.NewProfile(model.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "scan", DisplayName: "Scan", Status: model.StatusActive, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}}}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	newDB := func(t *testing.T, options, generation []byte) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "provider", "model", "endpoint", "options", "secret_ref", "generation", "content_digest", "version", "created_at", "updated_at"}).AddRow(value.TenantID, value.ProfileID, value.ProfileKey, value.DisplayName, value.Description, string(value.Status), value.SchemaVersion, value.Configuration.Provider, value.Configuration.Model, value.Configuration.Endpoint, options, value.Configuration.SecretRef, generation, value.ContentDigest, value.Version, value.CreatedAt, value.UpdatedAt))
		return db, mock
	}
	for _, test := range []struct {
		name                string
		options, generation []byte
	}{{"options", []byte("not-json"), []byte("{}")}, {"generation", []byte("{}"), []byte("not-json")}} {
		t.Run(test.name, func(t *testing.T) {
			db, mock := newDB(t, test.options, test.generation)
			if _, err := scanModelProfile(catalog, db.QueryRowContext(context.Background(), "SELECT 1")); !errors.Is(err, ErrStorage) {
				t.Fatalf("corrupt %s error = %v", test.name, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestModelRepositoryScannersRejectShortRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("short"))
	if _, err := scanModelEvent(db.QueryRowContext(context.Background(), "SELECT 1")); err == nil {
		t.Fatal("short event row was accepted")
	}
	mock.ExpectQuery("SELECT 2").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("short"))
	if _, err := scanModelProfile(nil, db.QueryRowContext(context.Background(), "SELECT 2")); err == nil {
		t.Fatal("short profile row was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func testModelProfileRows(t *testing.T, profile *model.Profile) *sqlmock.Rows {
	t.Helper()
	options, generation, err := encodeModelJSON(profile.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	return sqlmock.NewRows([]string{
		"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "provider", "model", "endpoint",
		"options", "secret_ref", "generation", "content_digest", "version", "created_at", "updated_at",
	}).AddRow(
		profile.TenantID, profile.ProfileID, profile.ProfileKey, profile.DisplayName, profile.Description, string(profile.Status), profile.SchemaVersion,
		profile.Configuration.Provider, profile.Configuration.Model, profile.Configuration.Endpoint, options, profile.Configuration.SecretRef, generation,
		profile.ContentDigest, profile.Version, profile.CreatedAt, profile.UpdatedAt,
	)
}
