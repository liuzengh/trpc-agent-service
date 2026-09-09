package tenant

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMySQLStoreTenantCRUD(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	store := NewMySQLStore(db)
	value := validTenant()

	mock.ExpectExec("INSERT INTO tenant").
		WithArgs(value.ID, value.Name, value.IsActive, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.UpsertTenant(context.Background(), value); err != nil {
		t.Fatalf("UpsertTenant() error = %v", err)
	}

	rows := sqlmock.NewRows([]string{"tenant_id", "name", "is_active", "quota_json", "policy_json"}).
		AddRow(value.ID, value.Name, true,
			`{"daily_token_limit":100000,"rate_per_minute":60}`,
			`{"redact_patterns":["account-[0-9]+"],"audit_level":"full","budget_cny":100}`)
	mock.ExpectQuery("SELECT tenant_id, name, is_active, quota_json, policy_json").
		WithArgs(value.ID).
		WillReturnRows(rows)
	got, err := store.GetTenant(context.Background(), value.ID)
	if err != nil {
		t.Fatalf("GetTenant() error = %v", err)
	}
	if got.ID != value.ID || got.Quota.DailyTokenLimit != value.Quota.DailyTokenLimit {
		t.Fatalf("GetTenant() = %#v, want tenant %q", got, value.ID)
	}

	mock.ExpectExec("UPDATE tenant SET is_active = FALSE").
		WithArgs(value.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.DeactivateTenant(context.Background(), value.ID); err != nil {
		t.Fatalf("DeactivateTenant() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestMySQLStoreAppVersionAndActivation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	store := NewMySQLStore(db)
	value := validApp()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(version\\), 0\\) \\+ 1").
		WithArgs(value.ID).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(1))
	mock.ExpectExec("INSERT INTO agent_app").
		WithArgs(value.ID, 1, value.TenantID, value.AppName,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	version, err := store.UpsertApp(context.Background(), value)
	if err != nil {
		t.Fatalf("UpsertApp() error = %v", err)
	}
	if version != 1 {
		t.Fatalf("UpsertApp() version = %d, want 1", version)
	}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE agent_app SET is_current = FALSE").
		WithArgs(value.ID).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("UPDATE agent_app SET is_current = TRUE").
		WithArgs(value.ID, 2).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.BindAppVersion(context.Background(), value.ID, 2); err != nil {
		t.Fatalf("BindAppVersion() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestMySQLStoreBindingRoundTripAndNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	store := NewMySQLStore(db)
	value := validBinding()

	mock.ExpectExec("INSERT INTO channel_binding").
		WithArgs(value.ID, value.TenantID, value.AppID, value.Channel,
			value.RouteKey, sqlmock.AnyArg(), value.IsActive).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.UpsertBinding(context.Background(), value); err != nil {
		t.Fatalf("UpsertBinding() error = %v", err)
	}

	rows := sqlmock.NewRows([]string{
		"binding_id", "tenant_id", "app_id", "channel", "route_key", "config_json", "is_active",
	}).AddRow(value.ID, value.TenantID, value.AppID, value.Channel, value.RouteKey,
		`{"app_secret":"env:FEISHU_APP_SECRET_TENANT_A"}`, true)
	mock.ExpectQuery("SELECT binding_id, tenant_id, app_id, channel, route_key, config_json, is_active").
		WithArgs(value.Channel, value.RouteKey).
		WillReturnRows(rows)
	got, err := store.GetBindingByRoute(context.Background(), value.Channel, value.RouteKey)
	if err != nil {
		t.Fatalf("GetBindingByRoute() error = %v", err)
	}
	if got.ID != value.ID || got.Config["app_secret"] != value.Config["app_secret"] {
		t.Fatalf("GetBindingByRoute() = %#v, want binding %q", got, value.ID)
	}

	mock.ExpectQuery("SELECT tenant_id, name, is_active, quota_json, policy_json").
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)
	_, err = store.GetTenant(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTenant(missing) error = %v, want ErrNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
