package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestChannelRepositoryGetDecodesStoredBinding(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))

	stored, err := NewRepository(db).Get(context.Background(), binding.TenantID, binding.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.BindingID != binding.BindingID || stored.Protocol.Telegram == nil || stored.Protocol.Telegram.WebhookPath != "/inbound" {
		t.Fatalf("stored channel binding = %+v", stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositoryListsBindings(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT binding_id FROM channel_binding WHERE tenant_id = \? ORDER BY binding_id`).WithArgs(binding.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID))
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))

	items, next, err := NewRepository(db).List(context.Background(), binding.TenantID, "primary", string(binding.Status), "", 50)
	if err != nil || len(items) != 1 || items[0].BindingID != binding.BindingID || next != "" {
		t.Fatalf("listed channel bindings = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositoryListSearchIncludesChannel(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT binding_id FROM channel_binding WHERE tenant_id = \? ORDER BY binding_id`).
		WithArgs(binding.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID))
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))

	items, next, err := NewRepository(db).List(context.Background(), binding.TenantID, "telegram", "", "", 50)
	if err != nil || len(items) != 1 || items[0].BindingID != binding.BindingID || next != "" {
		t.Fatalf("channel search = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositoryListBoundaries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := NewRepository(nil).List(ctx, "tenant", "", "", "", 50); err == nil {
		t.Fatal("canceled channel list was accepted")
	}
	if _, _, err := NewRepository(nil).List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil channel list error = %v", err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository := NewRepository(db)
	if _, _, err := repository.List(context.Background(), "tenant", "", "", "bad", 50); err == nil {
		t.Fatal("invalid channel cursor was accepted")
	}
	mock.ExpectQuery(`SELECT binding_id FROM channel_binding WHERE tenant_id = \? ORDER BY binding_id`).WithArgs("tenant").WillReturnError(errors.New("query down"))
	if _, _, err := repository.List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel query error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	scanDB, scanMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanDB.Close() })
	scanMock.ExpectQuery(`SELECT binding_id FROM channel_binding WHERE tenant_id = \? ORDER BY binding_id`).WithArgs("tenant").WillReturnRows(sqlmock.NewRows([]string{"binding_id", "extra"}).AddRow("binding", "bad"))
	if _, _, err := NewRepository(scanDB).List(context.Background(), "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel scan error = %v", err)
	}
}

func TestChannelRepositoryRejectsInvalidCreationMetadata(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, _, err = NewRepository(db).Create(context.Background(), channels.CreateInput{
		TenantID: binding.TenantID, BindingKey: "invalid-metadata", Channel: binding.Channel, ProviderAccountID: binding.ProviderAccountID,
		PublicRouteKeyDigest: binding.PublicRouteKeyDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Status: binding.Status,
	})
	if !errors.Is(err, channels.ErrInvalid) {
		t.Fatalf("invalid creation metadata error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositoryCandidateLookupAndConsumption(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository := NewRepository(db)
	mock.ExpectQuery(".*").WithArgs(string(binding.Channel), binding.PublicRouteKeyDigest).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "binding_id", "version", "config_digest",
	}).AddRow(binding.TenantID, binding.BindingID, binding.Version, binding.ConfigDigest))

	candidates, err := repository.LookupCandidates(context.Background(), binding.Channel, binding.PublicRouteKeyDigest)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].CandidateToken == "" || candidates[0].BindingVersion != binding.Version {
		t.Fatalf("candidate contexts = %+v", candidates)
	}
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))
	consumed, err := repository.ConsumeCandidate(context.Background(), candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	if consumed.BindingID != binding.BindingID || consumed.ConfigDigest != binding.ConfigDigest {
		t.Fatalf("consumed binding = %+v", consumed)
	}
	if _, err := repository.ConsumeCandidate(context.Background(), candidates[0]); err != channels.ErrCandidateUnavailable {
		t.Fatalf("reused candidate error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositoryUpdatesConfigurationAndReturnsEvent(t *testing.T) {
	binding := newStoredChannelBinding(t)
	routeDigest, err := channels.DigestPublicRouteKey(binding.Channel, "updated-route")
	if err != nil {
		t.Fatal(err)
	}
	input := channels.UpdateConfigurationInput{
		TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, ProviderAccountID: "updated-account",
		PublicRouteKeyDigest: routeDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "update", CorrelationID: "channel-update"},
	}
	updated, _, err := channels.PrepareConfigurationChange(*binding, input, binding.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, &updated))
	expectChannelEvent(mock, binding, channels.EventConfigurationUpdated, binding.Status, updated.Status, binding.ConfigDigest, updated.ConfigDigest, binding.Version, updated.Version, updated.UpdatedAt)
	mock.ExpectCommit()

	stored, event, err := NewRepository(db).UpdateConfiguration(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ProviderAccountID != updated.ProviderAccountID || stored.Version != updated.Version || event.EventType != channels.EventConfigurationUpdated || event.NextVersion != updated.Version {
		t.Fatalf("updated channel binding = %+v, event = %+v", stored, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositorySuspendsBindingAndReturnsEvent(t *testing.T) {
	binding := newStoredChannelBinding(t)
	input := channels.TransitionStatusInput{
		TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "suspend", CorrelationID: "channel-transition"},
	}
	prepared := input
	prepared.NextStatus = channels.StatusSuspended
	updated, _, err := channels.PrepareStatusChange(*binding, prepared, binding.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(4, 1))
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, &updated))
	expectChannelEvent(mock, binding, channels.EventSuspended, binding.Status, updated.Status, binding.ConfigDigest, updated.ConfigDigest, binding.Version, updated.Version, updated.UpdatedAt)
	mock.ExpectCommit()

	stored, event, err := NewRepository(db).Suspend(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != channels.StatusSuspended || stored.Version != updated.Version || event.EventType != channels.EventSuspended || event.NextVersion != updated.Version {
		t.Fatalf("suspended channel binding = %+v, event = %+v", stored, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositoryRequiresStorage(t *testing.T) {
	repository := NewRepository(nil)
	ctx := context.Background()
	if _, _, err := repository.Create(ctx, channels.CreateInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create nil-storage error = %v", err)
	}
	if _, err := repository.Get(ctx, "tenant", "binding"); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get nil-storage error = %v", err)
	}
	if _, _, err := repository.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateConfiguration nil-storage error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("TransitionStatus nil-storage error = %v", err)
	}
	if _, _, err := repository.Activate(ctx, channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Activate nil-storage error = %v", err)
	}
	if _, _, err := repository.Resume(ctx, channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Resume nil-storage error = %v", err)
	}
	if _, _, err := repository.Disable(ctx, channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Disable nil-storage error = %v", err)
	}
	if _, err := repository.LookupCandidates(ctx, channels.ChannelTelegram, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); !errors.Is(err, ErrStorage) {
		t.Fatalf("LookupCandidates nil-storage error = %v", err)
	}
	if _, err := repository.ConsumeCandidate(ctx, channels.CandidateBindingContext{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("ConsumeCandidate nil-storage error = %v", err)
	}
}

func TestChannelRepositoryCreatesBindingAndEvent(t *testing.T) {
	digest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "created-route")
	if err != nil {
		t.Fatal(err)
	}
	input := channels.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", BindingKey: "created", Channel: channels.ChannelTelegram,
		ProviderAccountID: "account", PublicRouteKeyDigest: digest, AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAW", SecretRef: "secret://channel",
		Status: channels.StatusActive, Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{APIBaseURL: "https://api.telegram.org", WebhookPath: "/inbound"}},
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "create", CorrelationID: "channel-create"},
	}
	value, err := channels.NewBinding(input)
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(11, 1))
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, value))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "binding_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow("created", value.TenantID, value.BindingID, nil, string(value.Status), nil, value.ConfigDigest, "test", "user", "create", "channel-create", 0, value.Version, value.CreatedAt))
	mock.ExpectCommit()
	created, event, err := NewRepository(db).Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if created.BindingID != value.BindingID || event.EventType != channels.EventCreated {
		t.Fatalf("created binding = %+v, event = %+v", created, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositoryDatabaseAndCandidateErrors(t *testing.T) {
	binding := newStoredChannelBinding(t)
	validCreate := channels.CreateInput{
		TenantID: binding.TenantID, BindingKey: "error-path", Channel: binding.Channel, ProviderAccountID: binding.ProviderAccountID,
		PublicRouteKeyDigest: binding.PublicRouteKeyDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Status: channels.StatusActive,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "error", CorrelationID: "channel-error"},
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
	db, mock := newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, _, err := NewRepository(db).Create(context.Background(), validCreate); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, _, err := NewRepository(db).UpdateConfiguration(context.Background(), channels.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, _, err := NewRepository(db).TransitionStatus(context.Background(), channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Transition begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectQuery(".*").WillReturnError(errors.New("read"))
	if _, err := NewRepository(db).Get(context.Background(), binding.TenantID, binding.BindingID); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get read error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	input := channels.UpdateConfigurationInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, ProviderAccountID: binding.ProviderAccountID, PublicRouteKeyDigest: binding.PublicRouteKeyDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "conflict", CorrelationID: "channel-conflict"}}
	if _, _, err := NewRepository(db).UpdateConfiguration(context.Background(), input); !errors.Is(err, channels.ErrConflict) {
		t.Fatalf("Update conflict = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "binding_id", "version", "config_digest"}))
	if _, err := NewRepository(db).LookupCandidates(context.Background(), binding.Channel, binding.PublicRouteKeyDigest); !errors.Is(err, ErrStorage) && !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("empty candidate lookup = %v", err)
	}
	if _, err := NewRepository(db).ConsumeCandidate(context.Background(), channels.CandidateBindingContext{}); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("invalid candidate consume = %v", err)
	}
}

func TestChannelRepositoryWriteErrorBranches(t *testing.T) {
	binding := newStoredChannelBinding(t)
	create := channels.CreateInput{TenantID: binding.TenantID, BindingKey: "write-errors", Channel: binding.Channel, ProviderAccountID: binding.ProviderAccountID, PublicRouteKeyDigest: binding.PublicRouteKeyDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Status: channels.StatusActive, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "write", CorrelationID: "channel-write"}}
	update := channels.UpdateConfigurationInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, ProviderAccountID: binding.ProviderAccountID, PublicRouteKeyDigest: binding.PublicRouteKeyDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "update", CorrelationID: "channel-update-errors"}}
	transition := channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, NextStatus: channels.StatusSuspended, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "transition", CorrelationID: "channel-transition-errors"}}
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
	if _, _, err := NewRepository(db).Create(context.Background(), create); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create insert error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("outbox"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db).Create(context.Background(), create); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create outbox error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
	mock.ExpectExec(".*").WillReturnError(errors.New("update"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update write error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("outbox"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db).UpdateConfiguration(context.Background(), update); !errors.Is(err, ErrStorage) {
		t.Fatalf("Update outbox error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(testChannelBindingRows(t, binding))
	mock.ExpectExec(".*").WillReturnError(errors.New("transition"))
	mock.ExpectRollback()
	if _, _, err := NewRepository(db).TransitionStatus(context.Background(), transition); !errors.Is(err, ErrStorage) {
		t.Fatalf("Transition write error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectQuery(".*").WillReturnError(errors.New("candidate query"))
	if _, err := NewRepository(db).LookupCandidates(context.Background(), binding.Channel, binding.PublicRouteKeyDigest); !errors.Is(err, ErrStorage) && !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("candidate query error = %v", err)
	}
}

func TestChannelRepositoryCandidateAndScanGuards(t *testing.T) {
	binding := newStoredChannelBinding(t)
	repository := NewRepository(nil)
	now := time.Now().UTC()
	candidate, err := channels.NewCandidateBindingContextFromInput(channels.CandidateBindingInput{
		Channel: binding.Channel, PublicRouteKeyDigest: binding.PublicRouteKeyDigest, BindingVersion: binding.Version,
		ConfigDigest: binding.ConfigDigest, Purpose: channels.PurposeWebhookVerification, CandidateToken: "token",
		IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	repository.candidates[candidate.CandidateToken] = candidateRecord{tenantID: binding.TenantID, bindingID: binding.BindingID, context: candidate}
	if _, err := repository.ConsumeCandidate(context.Background(), candidate.Clone()); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil storage candidate error = %v", err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository = NewRepository(db)
	repository.candidates[candidate.CandidateToken] = candidateRecord{tenantID: binding.TenantID, bindingID: binding.BindingID, context: candidate}
	mismatch := candidate.Clone()
	mismatch.Purpose = "other"
	if _, err := repository.ConsumeCandidate(context.Background(), mismatch); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("mismatched candidate error = %v", err)
	}
	expired := candidate.Clone()
	expired.CandidateToken = "expired"
	expired.ExpiresAt = now.Add(-time.Minute)
	repository.candidates[expired.CandidateToken] = candidateRecord{tenantID: binding.TenantID, bindingID: binding.BindingID, context: expired}
	if _, err := repository.ConsumeCandidate(context.Background(), expired); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("expired candidate error = %v", err)
	}
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "binding_id", "version", "config_digest"}).AddRow(binding.TenantID, binding.BindingID, binding.Version, binding.ConfigDigest))
	full := NewRepository(db)
	valid, err := channels.NewCandidateBindingContextFromInput(channels.CandidateBindingInput{
		Channel: binding.Channel, PublicRouteKeyDigest: binding.PublicRouteKeyDigest, BindingVersion: binding.Version,
		ConfigDigest: binding.ConfigDigest, Purpose: channels.PurposeWebhookVerification, CandidateToken: "seed",
		IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < mysqlMaxCandidates; i++ {
		full.candidates[fmt.Sprintf("filled-%d", i)] = candidateRecord{context: valid}
	}
	if _, err := full.LookupCandidates(context.Background(), binding.Channel, binding.PublicRouteKeyDigest); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("candidate capacity error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelRepositoryScannerRejectsInvalidRows(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("short"))
	if _, err := scanChannelBinding(db.QueryRowContext(context.Background(), "SELECT 1")); err == nil {
		t.Fatal("short binding row was accepted")
	}
	protocol, err := encodeProtocol(binding.Protocol)
	if err != nil {
		t.Fatal(err)
	}
	addRow := func(schema int, channel string, payload []byte) {
		mock.ExpectQuery("SELECT [234]").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "binding_id", "binding_key", "channel", "provider_account_id", "public_route_key_digest", "app_id", "secret_ref", "protocol_config", "schema_version", "status", "version", "config_digest", "created_at", "updated_at"}).AddRow(binding.TenantID, binding.BindingID, binding.BindingKey, channel, binding.ProviderAccountID, binding.PublicRouteKeyDigest, binding.AppID, binding.SecretRef, payload, schema, string(binding.Status), binding.Version, binding.ConfigDigest, binding.CreatedAt, binding.UpdatedAt))
	}
	addRow(2, string(binding.Channel), protocol)
	if _, err := scanChannelBinding(db.QueryRowContext(context.Background(), "SELECT 2")); !errors.Is(err, ErrStorage) {
		t.Fatalf("unsupported schema error = %v", err)
	}
	addRow(1, string(binding.Channel), []byte("not-json"))
	if _, err := scanChannelBinding(db.QueryRowContext(context.Background(), "SELECT 3")); !errors.Is(err, ErrStorage) {
		t.Fatalf("corrupt protocol error = %v", err)
	}
	addRow(1, "unknown", protocol)
	if _, err := scanChannelBinding(db.QueryRowContext(context.Background(), "SELECT 4")); !errors.Is(err, ErrStorage) {
		t.Fatalf("invalid channel error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func newStoredChannelBinding(t *testing.T) *channels.Binding {
	t.Helper()
	digest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "repository-success")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channels.NewBinding(channels.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", BindingKey: "primary", Channel: channels.ChannelTelegram,
		ProviderAccountID: "account", PublicRouteKeyDigest: digest, AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		SecretRef: "secret://tenant/channel", Status: channels.StatusActive,
		Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{APIBaseURL: "https://api.telegram.org", WebhookPath: "/inbound"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func testChannelBindingRows(t *testing.T, binding *channels.Binding) *sqlmock.Rows {
	t.Helper()
	protocol, err := encodeProtocol(binding.Protocol)
	if err != nil {
		t.Fatal(err)
	}
	return sqlmock.NewRows([]string{
		"tenant_id", "binding_id", "binding_key", "channel", "provider_account_id", "public_route_key_digest", "app_id", "secret_ref",
		"protocol_config", "schema_version", "status", "version", "config_digest", "created_at", "updated_at",
	}).AddRow(
		binding.TenantID, binding.BindingID, binding.BindingKey, string(binding.Channel), binding.ProviderAccountID, binding.PublicRouteKeyDigest,
		binding.AppID, binding.SecretRef, protocol, channels.SchemaVersionV1, string(binding.Status), binding.Version, binding.ConfigDigest,
		binding.CreatedAt, binding.UpdatedAt,
	)
}

func expectChannelEvent(mock sqlmock.Sqlmock, binding *channels.Binding, eventType channels.EventType, previousStatus, currentStatus channels.Status, previousDigest, currentDigest string, previousVersion, nextVersion int64, occurredAt time.Time) {
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "binding_id", "previous_status", "current_status", "previous_digest", "current_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow(string(eventType), binding.TenantID, binding.BindingID, string(previousStatus), string(currentStatus), previousDigest, currentDigest, "test", "user", "workflow", "correlation", previousVersion, nextVersion, occurredAt))
}
