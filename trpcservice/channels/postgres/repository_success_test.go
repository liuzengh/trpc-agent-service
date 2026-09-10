package postgres

import (
	"context"
	"errors"
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

func TestChannelRepositoryListsBindings(t *testing.T) {
	binding := newStoredChannelBinding(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT binding_id FROM public\.channel_binding WHERE tenant_id=\$1`).WithArgs(binding.TenantID).WillReturnRows(sqlmock.NewRows([]string{"binding_id"}).AddRow(binding.BindingID))
	mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(testChannelBindingRows(t, binding))

	items, next, err := NewRepository(db).List(context.Background(), binding.TenantID, "account", string(binding.Status), "", 50)
	if err != nil || len(items) != 1 || items[0].BindingID != binding.BindingID || next != "" {
		t.Fatalf("listed channel bindings = items=%+v next=%q err=%v", items, next, err)
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
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(3)))
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
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(4)))
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
	if _, _, err := repository.List(ctx, "tenant", "", "", "", 50); !errors.Is(err, ErrStorage) {
		t.Fatalf("List nil-storage error = %v", err)
	}
	if _, _, err := repository.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateConfiguration nil-storage error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("TransitionStatus nil-storage error = %v", err)
	}
	if _, err := repository.LookupCandidates(ctx, channels.ChannelTelegram, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); !errors.Is(err, ErrStorage) {
		t.Fatalf("LookupCandidates nil-storage error = %v", err)
	}
	if _, err := repository.ConsumeCandidate(ctx, channels.CandidateBindingContext{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("ConsumeCandidate nil-storage error = %v", err)
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
