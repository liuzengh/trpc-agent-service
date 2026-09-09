package tenant

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLRepositoryWebhookLookupUsesOnlyOpaqueRouteKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &SQLRepository{db: db}

	const routeKey = "route_opaque_key"
	config := []byte(`{"channels":[{"type":"telegram","webhookKey":"stale-json-key","config":{}}]}`)
	now := time.Unix(1000, 0).UTC()
	mock.ExpectQuery(regexp.QuoteMeta("WHERE tc.webhook_key = $1 AND t.status = $2")).
		WithArgs(routeKey, TenantStatusActive).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "status", "config", "config_version", "created_at", "updated_at",
			"channel_type", "channel_index", "webhook_key",
		}).AddRow("tenant-a", "Acme", TenantStatusActive, config, int64(7), now, now, "telegram", 0, routeKey))

	got, err := repo.GetByWebhookToken(context.Background(), routeKey)
	if err != nil {
		t.Fatal(err)
	}
	if got.Channels[0].WebhookKey != routeKey {
		t.Fatalf("resolved webhook key = %q, want authoritative row key", got.Channels[0].WebhookKey)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLRepositoryStatusLookupDoesNotSelectTenantConfiguration(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &SQLRepository{db: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT status FROM tenants WHERE id = $1")).
		WithArgs("tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(TenantStatusSuspended))
	status, err := repo.GetStatus(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if status != TenantStatusSuspended {
		t.Fatalf("status=%q, want suspended", status)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
