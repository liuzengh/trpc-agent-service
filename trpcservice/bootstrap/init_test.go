package bootstrap

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	testInitTenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testInitAppID    = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestInitConfigValidationAndSecretFreeOutput(t *testing.T) {
	config := InitConfig{
		TenantKey:         "acme",
		TenantDisplayName: "Acme",
		AppKey:            "assistant",
		AppDisplayName:    "Assistant",
		AppDescription:    "Initial assistant",
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (InitConfig{TenantKey: "acme", TenantDisplayName: "Acme", AppKey: "bad key", AppDisplayName: "Assistant"}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid init config error = %v", err)
	}

	var output strings.Builder
	if err := WriteInitResult(&output, InitResult{TenantID: testInitTenantID, AppID: testInitAppID, Created: true}); err != nil {
		t.Fatal(err)
	}
	value := output.String()
	if !strings.Contains(value, "export TRPC_TENANT_ID='"+testInitTenantID+"'") || !strings.Contains(value, "export TRPC_APP_ID='"+testInitAppID+"'") {
		t.Fatalf("init output = %q", value)
	}
	for _, secret := range []string{"postgres://", "TRPC_MODEL_API_KEY", "model-secret", "password"} {
		if strings.Contains(value, secret) {
			t.Fatalf("init output contains %q: %q", secret, value)
		}
	}
	if err := WriteInitResult(&output, InitResult{TenantID: "t_bad'\n", AppID: testInitAppID}); !errors.Is(err, ErrInitialization) {
		t.Fatalf("unsafe init output ID error = %v", err)
	}
}

func TestInitializeCreatesAtomicInitialPair(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	emptyTenants := sqlmock.NewRows([]string{"tenant_id"})
	emptyApps := sqlmock.NewRows([]string{"tenant_id", "app_id"})
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(emptyTenants)
	mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnRows(emptyApps)
	mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SELECT public\\.control_plane_create_agent_app").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT 1").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"present"}).AddRow(1))
	mock.ExpectCommit()

	result, err := Initialize(context.Background(), db, InitConfig{
		TenantKey: "acme", TenantDisplayName: "Acme", AppKey: "assistant", AppDisplayName: "Assistant",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Created || !strings.HasPrefix(result.TenantID, "t_") || !strings.HasPrefix(result.AppID, "app_") {
		t.Fatalf("fresh init result = %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeReturnsStableExistingPair(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(testInitTenantID))
	mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(testInitTenantID, testInitAppID))
	mock.ExpectRollback()

	result, err := Initialize(context.Background(), db, InitConfig{
		TenantKey: "different", TenantDisplayName: "Ignored", AppKey: "different", AppDisplayName: "Ignored",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Created || result.TenantID != testInitTenantID || result.AppID != testInitAppID {
		t.Fatalf("existing init result = %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeRejectsAmbiguousAndIncompleteStates(t *testing.T) {
	tests := []struct {
		name       string
		tenantRows *sqlmock.Rows
		appRows    *sqlmock.Rows
		want       error
	}{
		{
			name:       "multiple tenants",
			tenantRows: sqlmock.NewRows([]string{"tenant_id"}).AddRow(testInitTenantID).AddRow("t_01ARZ3NDEKTSV4RRFFQ69G5FAW"),
			appRows:    sqlmock.NewRows([]string{"tenant_id", "app_id"}),
			want:       ErrInitializationAmbiguous,
		},
		{
			name:       "multiple apps",
			tenantRows: sqlmock.NewRows([]string{"tenant_id"}).AddRow(testInitTenantID),
			appRows:    sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(testInitTenantID, testInitAppID).AddRow(testInitTenantID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"),
			want:       ErrInitializationAmbiguous,
		},
		{
			name:       "tenant without app",
			tenantRows: sqlmock.NewRows([]string{"tenant_id"}).AddRow(testInitTenantID),
			appRows:    sqlmock.NewRows([]string{"tenant_id", "app_id"}),
			want:       ErrInitializationIncomplete,
		},
		{
			name:       "app without tenant",
			tenantRows: sqlmock.NewRows([]string{"tenant_id"}),
			appRows:    sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(testInitTenantID, testInitAppID),
			want:       ErrInitializationAmbiguous,
		},
		{
			name:       "app belongs to another tenant",
			tenantRows: sqlmock.NewRows([]string{"tenant_id"}).AddRow(testInitTenantID),
			appRows:    sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow("t_01ARZ3NDEKTSV4RRFFQ69G5FAW", testInitAppID),
			want:       ErrInitializationAmbiguous,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(test.tenantRows)
			mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnRows(test.appRows)
			mock.ExpectRollback()
			_, err = Initialize(context.Background(), db, InitConfig{
				TenantKey: "acme", TenantDisplayName: "Acme", AppKey: "assistant", AppDisplayName: "Assistant",
			})
			if !errors.Is(err, ErrInitialization) || !errors.Is(err, test.want) {
				t.Fatalf("state error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInitializeMapsDatabaseFailuresAndCancellation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnError(errors.New("database details"))
	mock.ExpectRollback()
	_, err = Initialize(context.Background(), db, InitConfig{
		TenantKey: "acme", TenantDisplayName: "Acme", AppKey: "assistant", AppDisplayName: "Assistant",
	})
	if !errors.Is(err, ErrInitialization) || strings.Contains(err.Error(), "database details") {
		t.Fatalf("database failure = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Initialize(canceled, db, InitConfig{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled initialization = %v", err)
	}
	if _, err := Initialize(context.Background(), nil, InitConfig{}); !errors.Is(err, ErrInitialization) {
		t.Fatalf("nil database initialization = %v", err)
	}
	if _, err := Initialize(nil, db, InitConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context initialization = %v", err)
	}
}

func TestInitializeRollsBackIfTheSecondResourceCannotBeCreated(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))
	mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}))
	mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SELECT public\\.control_plane_create_agent_app").WillReturnError(errors.New("secret database details"))
	mock.ExpectRollback()
	_, err = Initialize(context.Background(), db, InitConfig{
		TenantKey: "acme", TenantDisplayName: "Acme", AppKey: "assistant", AppDisplayName: "Assistant",
	})
	if !errors.Is(err, ErrInitialization) || strings.Contains(err.Error(), "secret database details") {
		t.Fatalf("second resource failure = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeMapsTransactionAndReadFailures(t *testing.T) {
	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
	}{
		{
			name: "begin",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("begin database details"))
			},
		},
		{
			name: "tenant read",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnError(errors.New("tenant database details"))
				mock.ExpectRollback()
			},
		},
		{
			name: "app read",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))
				mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnError(errors.New("app database details"))
				mock.ExpectRollback()
			},
		},
		{
			name: "tenant write",
			setup: func(mock sqlmock.Sqlmock) {
				expectFreshInitialization(mock)
				mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnError(errors.New("tenant write details"))
				mock.ExpectRollback()
			},
		},
		{
			name: "pair verification",
			setup: func(mock sqlmock.Sqlmock) {
				expectFreshInitialization(mock)
				mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("SELECT public\\.control_plane_create_agent_app").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SELECT 1").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"present"}))
				mock.ExpectRollback()
			},
		},
		{
			name: "commit",
			setup: func(mock sqlmock.Sqlmock) {
				expectFreshInitialization(mock)
				mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("SELECT public\\.control_plane_create_agent_app").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SELECT 1").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"present"}).AddRow(1))
				mock.ExpectCommit().WillReturnError(errors.New("commit database details"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			test.setup(mock)
			_, err = Initialize(context.Background(), db, InitConfig{
				TenantKey: "acme", TenantDisplayName: "Acme", AppKey: "assistant", AppDisplayName: "Assistant",
			})
			if !errors.Is(err, ErrInitialization) {
				t.Fatalf("initialization error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInitializationErrorMappingAndOutputEdges(t *testing.T) {
	if got := mapInitializationDBError(context.Background(), nil); got != nil {
		t.Fatalf("nil database error = %v", got)
	}
	if got := mapInitializationDBError(nil, errors.New("database details")); !errors.Is(got, ErrInitialization) || strings.Contains(got.Error(), "database details") {
		t.Fatalf("redacted nil-context error = %v", got)
	}
	if got := mapInitializationDBError(context.Background(), context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("cancellation error = %v", got)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := mapInitializationDBError(canceled, errors.New("database details")); !errors.Is(got, context.Canceled) {
		t.Fatalf("canceled context error = %v", got)
	}
	if got := mapInitializationDBError(context.Background(), context.DeadlineExceeded); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("deadline error = %v", got)
	}
	if got := nullableInitText(" USD "); got != " USD " {
		t.Fatalf("non-empty nullable text = %#v", got)
	}
	if got := nullableInitText(" "); got != nil {
		t.Fatalf("empty nullable text = %#v", got)
	}
	if err := WriteInitResult(errorWriter{err: errors.New("created output failure")}, InitResult{TenantID: testInitTenantID, AppID: testInitAppID, Created: true}); err == nil {
		t.Fatal("created output writer failure was ignored")
	}

	var output strings.Builder
	if err := WriteInitResult(&output, InitResult{TenantID: testInitTenantID, AppID: testInitAppID}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "found the existing") {
		t.Fatalf("existing init output = %q", output.String())
	}
	for _, result := range []InitResult{
		{TenantID: testInitTenantID, AppID: "app_bad"},
		{TenantID: "t_8" + strings.Repeat("0", 25), AppID: testInitAppID},
		{TenantID: "t_801ARZ3NDEKTSV4RRFFQ69G5FAV", AppID: testInitAppID},
		{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", AppID: testInitAppID[:len(testInitAppID)-1] + "!"},
	} {
		if err := WriteInitResult(io.Discard, result); !errors.Is(err, ErrInitialization) {
			t.Fatalf("invalid output result %+v = %v", result, err)
		}
	}
	if err := WriteInitResult(nil, InitResult{TenantID: testInitTenantID, AppID: testInitAppID}); !errors.Is(err, ErrInitialization) {
		t.Fatalf("nil output writer error = %v", err)
	}
	writerErr := errors.New("writer failure")
	if err := WriteInitResult(errorWriter{err: writerErr}, InitResult{TenantID: testInitTenantID, AppID: testInitAppID}); !errors.Is(err, writerErr) {
		t.Fatalf("output writer error = %v", err)
	}

}

func TestInitializeRejectsInvalidConfigurationAndCancellation(t *testing.T) {
	validationDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = validationDB.Close() })
	if _, err := Initialize(context.Background(), validationDB, InitConfig{TenantKey: "bad key", TenantDisplayName: "Acme", AppKey: "assistant", AppDisplayName: "Assistant"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid tenant initialization = %v", err)
	}
	if _, err := Initialize(context.Background(), validationDB, InitConfig{TenantKey: "acme", TenantDisplayName: "Acme", AppKey: "bad key", AppDisplayName: "Assistant"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid app initialization = %v", err)
	}

	tenantRoot, app, err := newInitialEntities(InitConfig{TenantKey: "acme", TenantDisplayName: "Acme", AppKey: "assistant", AppDisplayName: "Assistant"})
	if err != nil {
		t.Fatal(err)
	}
	canceledComplete, cancelComplete := context.WithCancel(context.Background())
	cancelComplete()
	if _, err := completeInitialization(canceledComplete, nil, initialState{
		tenants: []string{tenantRoot.TenantID},
		apps:    []initialApp{{tenantID: tenantRoot.TenantID, appID: app.AppID}},
	}, tenantRoot, app); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled complete initialization = %v", err)
	}
}

func expectFreshInitialization(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))
	mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}))
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }
