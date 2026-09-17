package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type adapterStub struct{ id string }

func (a adapterStub) ID() string              { return a.id }
func (adapterStub) Run(context.Context) error { return nil }
func (adapterStub) PublicRoute(context.Context, channel.CallbackRequest) (channel.PublicRouteHint, error) {
	return channel.PublicRouteHint{}, nil
}
func (adapterStub) Verify(context.Context, channel.CallbackRequest, channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	return channel.VerifiedCallback{}, channel.VerificationReceipt{}, nil
}
func (adapterStub) Decode(context.Context, channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	return nil, nil
}
func (adapterStub) Deliver(context.Context, channel.DeliveryRequest) (channel.DeliveryResult, error) {
	return channel.DeliveryResult{}, nil
}
func (adapterStub) Capabilities() channel.Capabilities { return channel.Capabilities{} }

func TestNewValidatesCatalogDependencies(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()
	if _, err := New(nil, adapterStub{id: "feishu"}); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("nil db err=%v", err)
	}
	if _, err := New(db); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("empty adapters err=%v", err)
	}
	if _, err := New(db, nil); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("nil adapter err=%v", err)
	}
	if _, err := New(db, adapterStub{id: "feishu"}, adapterStub{id: "feishu"}); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("duplicate adapter err=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogListsOnlyDeliverablePublishedDestinations(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()
	catalog, err := New(db, adapterStub{id: "feishu"}, adapterStub{id: "wecom"})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT DISTINCT binding\\.tenant_id,binding\\.channel,binding\\.binding_id,binding\\.external_account_id").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "channel", "binding_id", "external_account_id"}).
			AddRow("tenant-a", "feishu", "binding-1", "account-1").
			AddRow("tenant-b", "wecom", "binding-2", "account-2"))

	destinations, err := catalog.ListDeliveryDestinations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []channel.ReplyDestination{
		{TenantID: "tenant-a", Channel: "feishu", ChannelBindingID: "binding-1", ExternalAccountID: "account-1"},
		{TenantID: "tenant-b", Channel: "wecom", ChannelBindingID: "binding-2", ExternalAccountID: "account-2"},
	}
	if len(destinations) != len(want) {
		t.Fatalf("destinations=%#v", destinations)
	}
	for index := range want {
		if destinations[index] != want[index] {
			t.Fatalf("destination[%d]=%#v want=%#v", index, destinations[index], want[index])
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogRejectsUnsupportedChannelAndBackendFailure(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()
	catalog, err := New(db, adapterStub{id: "feishu"})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT DISTINCT binding\\.tenant_id,binding\\.channel,binding\\.binding_id,binding\\.external_account_id").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "channel", "binding_id", "external_account_id"}).
			AddRow("tenant", "unsupported", "binding", "account"))
	if _, err := catalog.ListDeliveryDestinations(context.Background()); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("unsupported adapter err=%v", err)
	}
	mock.ExpectQuery("SELECT DISTINCT binding\\.tenant_id,binding\\.channel,binding\\.binding_id,binding\\.external_account_id").
		WillReturnError(errors.New("database unavailable"))
	if _, err := catalog.ListDeliveryDestinations(context.Background()); !errors.Is(err, runtime.ErrBackendUnavailable) {
		t.Fatalf("backend err=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogResolvesAdapterAgainstFrozenConfigVersion(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()
	feishu := adapterStub{id: "feishu"}
	catalog, err := New(db, feishu)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT binding\\.channel FROM channel_binding binding").WithArgs("tenant", int64(7), "binding").
		WillReturnRows(sqlmock.NewRows([]string{"channel"}).AddRow("feishu"))
	adapter, err := catalog.ResolveVersionedAdapter(context.Background(), "tenant", "binding", 7)
	if err != nil || adapter.ID() != "feishu" {
		t.Fatalf("adapter=%v err=%v", adapter, err)
	}
	mock.ExpectQuery("SELECT binding\\.channel FROM channel_binding binding").WithArgs("tenant", int64(8), "binding").
		WillReturnRows(sqlmock.NewRows([]string{"channel"}))
	if _, err := catalog.ResolveVersionedAdapter(context.Background(), "tenant", "binding", 8); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("not found err=%v", err)
	}
	mock.ExpectQuery("SELECT binding\\.channel FROM channel_binding binding").WithArgs("tenant", int64(9), "binding").
		WillReturnRows(sqlmock.NewRows([]string{"channel"}).AddRow("wecom"))
	if _, err := catalog.ResolveVersionedAdapter(context.Background(), "tenant", "binding", 9); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("unsupported adapter err=%v", err)
	}
	if _, err := catalog.ResolveVersionedAdapter(context.Background(), "tenant", "binding", 0); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("invalid scope err=%v", err)
	}
	if _, err := catalog.ResolveAdapter(context.Background(), "tenant", "binding"); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("unversioned resolution err=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	return db, mock
}
