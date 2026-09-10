package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

var agentAppListColumns = []string{
	"tenant_id", "app_id", "app_key", "display_name", "description", "status",
	"current_revision", "canary_revision", "version", "created_at", "updated_at",
}

func TestAgentRepositoryListRejectsNilReceiver(t *testing.T) {
	var repository *AppRepository
	ctx := context.Background()
	if _, _, err := repository.List(ctx, "tenant", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("List nil-receiver error = %v", err)
	}
	if _, _, err := repository.ListRevisions(ctx, "tenant", "app", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("ListRevisions nil-receiver error = %v", err)
	}
}

func TestAgentRepositoryListPrefersCanceledContextOverStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repository := NewAppRepository(nil)
	if _, _, err := repository.List(ctx, "tenant", "", "", "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("List canceled-context error = %v", err)
	}
	if _, _, err := repository.ListRevisions(ctx, "tenant", "app", "", "", "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListRevisions canceled-context error = %v", err)
	}
}

func TestAgentRepositoryListAppliesPageBounds(t *testing.T) {
	app := newStoredAgentApp(t)
	for _, tc := range []struct {
		name       string
		count      int
		limit      int
		wantCount  int
		wantCursor string
	}{
		{name: "default limit", count: 51, limit: 0, wantCount: 50, wantCursor: "50"},
		{name: "maximum limit", count: 201, limit: 201, wantCount: 200, wantCursor: "200"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newAgentListMock(t)
			apps := repeatedAgentApps(app, tc.count)
			mock.ExpectQuery(`FROM public\.agent_app WHERE tenant_id = \$1`).
				WithArgs(app.TenantID).
				WillReturnRows(agentAppRows(apps...)).
				RowsWillBeClosed()

			items, cursor, err := NewAppRepository(db).List(context.Background(), app.TenantID, "", "", "", tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != tc.wantCount || cursor != tc.wantCursor {
				t.Fatalf("List() = %d items, cursor %q; want %d items, cursor %q", len(items), cursor, tc.wantCount, tc.wantCursor)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAgentRepositoryListFiltersAndPaginates(t *testing.T) {
	first := newStoredAgentApp(t)
	first.AppKey = "payments-primary"
	first.DisplayName = "Payments Primary"
	first.CreatedAt = time.Date(2026, 9, 4, 9, 0, 0, 0, time.FixedZone("source", 2*60*60))
	first.UpdatedAt = first.CreatedAt
	second := first.Clone()
	second.AppKey = "payments-secondary"
	second.DisplayName = "Payments Secondary"
	nonMatching := first.Clone()
	nonMatching.AppKey = "analytics"
	nonMatching.DisplayName = "Analytics"
	disabled := first.Clone()
	disabled.AppKey = "payments-retired"
	disabled.DisplayName = "Payments Retired"
	disabled.Status = appmodel.StatusDisabled

	db, mock := newAgentListMock(t)
	mock.ExpectQuery(`FROM public\.agent_app WHERE tenant_id = \$1`).
		WithArgs(first.TenantID).
		WillReturnRows(agentAppRows(first, &second, &nonMatching, &disabled)).
		RowsWillBeClosed()

	items, cursor, err := NewAppRepository(db).List(context.Background(), first.TenantID, " PAYMENTS ", string(appmodel.StatusDraft), "1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].AppKey != second.AppKey || cursor != "" {
		t.Fatalf("filtered page = items=%+v cursor=%q", items, cursor)
	}
	if items[0].CreatedAt.Location() != time.UTC || items[0].UpdatedAt.Location() != time.UTC {
		t.Fatalf("listed timestamps must be normalized to UTC: %+v", items[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryListReturnsEmptyPagePastEnd(t *testing.T) {
	app := newStoredAgentApp(t)
	db, mock := newAgentListMock(t)
	mock.ExpectQuery(`FROM public\.agent_app WHERE tenant_id = \$1`).
		WithArgs(app.TenantID).
		WillReturnRows(agentAppRows(app)).
		RowsWillBeClosed()

	items, cursor, err := NewAppRepository(db).List(context.Background(), app.TenantID, "", "", "1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if items == nil || len(items) != 0 || cursor != "" {
		t.Fatalf("empty page = items=%+v cursor=%q", items, cursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryListFailureCases(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		setup func(sqlmock.Sqlmock, *appmodel.App)
		call  func(*AppRepository, *appmodel.App) error
		want  error
	}{
		{
			name: "invalid cursor",
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.List(ctx, app.TenantID, "", "", "not-a-cursor", 1)
				return err
			},
		},
		{
			name: "negative cursor",
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.List(ctx, app.TenantID, "", "", "-1", 1)
				return err
			},
		},
		{
			name: "missing rows",
			setup: func(mock sqlmock.Sqlmock, app *appmodel.App) {
				mock.ExpectQuery(`FROM public\.agent_app WHERE tenant_id = \$1`).WithArgs(app.TenantID).WillReturnError(sql.ErrNoRows)
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.List(ctx, app.TenantID, "", "", "", 1)
				return err
			},
			want: appmodel.ErrNotFound,
		},
		{
			name: "query error",
			setup: func(mock sqlmock.Sqlmock, app *appmodel.App) {
				mock.ExpectQuery(`FROM public\.agent_app WHERE tenant_id = \$1`).WithArgs(app.TenantID).WillReturnError(errors.New("query failed"))
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.List(ctx, app.TenantID, "", "", "", 1)
				return err
			},
			want: ErrStorage,
		},
		{
			name: "row scan error",
			setup: func(mock sqlmock.Sqlmock, app *appmodel.App) {
				mock.ExpectQuery(`FROM public\.agent_app WHERE tenant_id = \$1`).
					WithArgs(app.TenantID).
					WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(app.TenantID)).
					RowsWillBeClosed()
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.List(ctx, app.TenantID, "", "", "", 1)
				return err
			},
			want: ErrStorage,
		},
		{
			name: "row iteration error",
			setup: func(mock sqlmock.Sqlmock, app *appmodel.App) {
				rows := agentAppRows(app).RowError(0, errors.New("iteration failed"))
				mock.ExpectQuery(`FROM public\.agent_app WHERE tenant_id = \$1`).WithArgs(app.TenantID).WillReturnRows(rows).RowsWillBeClosed()
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.List(ctx, app.TenantID, "", "", "", 1)
				return err
			},
			want: ErrStorage,
		},
		{
			name: "invalid stored app",
			setup: func(mock sqlmock.Sqlmock, app *appmodel.App) {
				invalid := app.Clone()
				invalid.Status = appmodel.Status("unknown")
				mock.ExpectQuery(`FROM public\.agent_app WHERE tenant_id = \$1`).WithArgs(app.TenantID).WillReturnRows(agentAppRows(&invalid)).RowsWillBeClosed()
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.List(ctx, app.TenantID, "", "", "", 1)
				return err
			},
			want: ErrStorage,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := newStoredAgentApp(t)
			db, mock := newAgentListMock(t)
			if tc.setup != nil {
				tc.setup(mock, app)
			}

			err := tc.call(NewAppRepository(db), app)
			if tc.want == nil {
				if err == nil {
					t.Fatal("invalid cursor was accepted")
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAgentRepositoryListRevisionsAppliesPageBounds(t *testing.T) {
	app := newStoredAgentApp(t)
	for _, tc := range []struct {
		name       string
		count      int
		limit      int
		wantCount  int
		wantCursor string
	}{
		{name: "default limit", count: 51, limit: 0, wantCount: 50, wantCursor: "50"},
		{name: "maximum limit", count: 201, limit: 201, wantCount: 200, wantCursor: "200"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newAgentListMock(t)
			revisions := agentListRevisions(t, app, tc.count)
			expectAgentRevisionList(t, mock, app, revisions)

			items, cursor, err := NewAppRepository(db).ListRevisions(context.Background(), app.TenantID, app.AppID, "", "", "", tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != tc.wantCount || cursor != tc.wantCursor {
				t.Fatalf("ListRevisions() = %d items, cursor %q; want %d items, cursor %q", len(items), cursor, tc.wantCount, tc.wantCursor)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAgentRepositoryListRevisionsFiltersAndPaginates(t *testing.T) {
	app := newStoredAgentApp(t)
	first := newListRevision(t, app, 1, false, "Payments primary workflow", "")
	second := newListRevision(t, app, 2, false, "", "Payments secondary workflow")
	nonMatching := newListRevision(t, app, 3, false, "Analytics workflow", "")
	published := newListRevision(t, app, 4, true, "Payments published workflow", "")
	revisions := []*appmodel.Revision{first, second, nonMatching, published}

	db, mock := newAgentListMock(t)
	expectAgentRevisionList(t, mock, app, revisions)

	items, cursor, err := NewAppRepository(db).ListRevisions(context.Background(), app.TenantID, app.AppID, " PAYMENTS ", string(appmodel.RevisionStateDraft), "1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Revision != second.Revision || cursor != "" {
		t.Fatalf("filtered page = items=%+v cursor=%q", items, cursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryListRevisionsReturnsEmptyPagePastEnd(t *testing.T) {
	app := newStoredAgentApp(t)
	revision := newStoredAgentRevision(t, app, 1, false)
	db, mock := newAgentListMock(t)
	expectAgentRevisionList(t, mock, app, []*appmodel.Revision{revision})

	items, cursor, err := NewAppRepository(db).ListRevisions(context.Background(), app.TenantID, app.AppID, "", "", "1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if items == nil || len(items) != 0 || cursor != "" {
		t.Fatalf("empty page = items=%+v cursor=%q", items, cursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryListRevisionsFailureCases(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, sqlmock.Sqlmock, *appmodel.App)
		call  func(*AppRepository, *appmodel.App) error
		want  error
	}{
		{
			name: "invalid cursor",
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.ListRevisions(ctx, app.TenantID, app.AppID, "", "", "not-a-cursor", 1)
				return err
			},
		},
		{
			name: "negative cursor",
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.ListRevisions(ctx, app.TenantID, app.AppID, "", "", "-1", 1)
				return err
			},
		},
		{
			name: "missing revision rows",
			setup: func(_ *testing.T, mock sqlmock.Sqlmock, app *appmodel.App) {
				mock.ExpectQuery(`FROM public\.agent_app_revision WHERE tenant_id=\$1 AND app_id=\$2`).WithArgs(app.TenantID, app.AppID).WillReturnError(sql.ErrNoRows)
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.ListRevisions(ctx, app.TenantID, app.AppID, "", "", "", 1)
				return err
			},
			want: appmodel.ErrNotFound,
		},
		{
			name: "revision number query error",
			setup: func(_ *testing.T, mock sqlmock.Sqlmock, app *appmodel.App) {
				mock.ExpectQuery(`FROM public\.agent_app_revision WHERE tenant_id=\$1 AND app_id=\$2`).WithArgs(app.TenantID, app.AppID).WillReturnError(errors.New("query failed"))
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.ListRevisions(ctx, app.TenantID, app.AppID, "", "", "", 1)
				return err
			},
			want: ErrStorage,
		},
		{
			name: "revision number scan error",
			setup: func(_ *testing.T, mock sqlmock.Sqlmock, app *appmodel.App) {
				mock.ExpectQuery(`FROM public\.agent_app_revision WHERE tenant_id=\$1 AND app_id=\$2`).
					WithArgs(app.TenantID, app.AppID).
					WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow("not-a-number")).
					RowsWillBeClosed()
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.ListRevisions(ctx, app.TenantID, app.AppID, "", "", "", 1)
				return err
			},
			want: ErrStorage,
		},
		{
			name: "revision number iteration error",
			setup: func(_ *testing.T, mock sqlmock.Sqlmock, app *appmodel.App) {
				rows := sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).RowError(0, errors.New("iteration failed"))
				mock.ExpectQuery(`FROM public\.agent_app_revision WHERE tenant_id=\$1 AND app_id=\$2`).WithArgs(app.TenantID, app.AppID).WillReturnRows(rows).RowsWillBeClosed()
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.ListRevisions(ctx, app.TenantID, app.AppID, "", "", "", 1)
				return err
			},
			want: ErrStorage,
		},
		{
			name: "revision load error",
			setup: func(_ *testing.T, mock sqlmock.Sqlmock, app *appmodel.App) {
				mock.ExpectQuery(`FROM public\.agent_app_revision WHERE tenant_id=\$1 AND app_id=\$2`).
					WithArgs(app.TenantID, app.AppID).
					WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1))).
					RowsWillBeClosed()
				mock.ExpectQuery(`FROM public\.agent_app_revision`).WithArgs(app.TenantID, app.AppID, int64(1)).WillReturnError(errors.New("revision load failed"))
			},
			call: func(repository *AppRepository, app *appmodel.App) error {
				_, _, err := repository.ListRevisions(ctx, app.TenantID, app.AppID, "", "", "", 1)
				return err
			},
			want: ErrStorage,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := newStoredAgentApp(t)
			db, mock := newAgentListMock(t)
			if tc.setup != nil {
				tc.setup(t, mock, app)
			}

			err := tc.call(NewAppRepository(db), app)
			if tc.want == nil {
				if err == nil {
					t.Fatal("invalid cursor was accepted")
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestScanRevisionNumbers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rows  *sqlmock.Rows
		want  []int64
		wantE bool
	}{
		{name: "values", rows: sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).AddRow(int64(2)), want: []int64{1, 2}},
		{name: "scan error", rows: sqlmock.NewRows([]string{"revision"}).AddRow("not-a-number"), wantE: true},
		{name: "iteration error", rows: sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).RowError(0, errors.New("iteration failed")), wantE: true},
		{name: "close error", rows: sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).CloseError(errors.New("close failed")), wantE: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newAgentListMock(t)
			mock.ExpectQuery("SELECT revision").WillReturnRows(tc.rows).RowsWillBeClosed()
			rows, err := db.QueryContext(context.Background(), "SELECT revision")
			if err != nil {
				t.Fatal(err)
			}

			got, err := scanRevisionNumbers(rows)
			if tc.wantE {
				if err == nil {
					t.Fatal("scanRevisionNumbers() error = nil")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(tc.want) {
					t.Fatalf("scanRevisionNumbers() = %v, want %v", got, tc.want)
				}
				for i := range tc.want {
					if got[i] != tc.want[i] {
						t.Fatalf("scanRevisionNumbers() = %v, want %v", got, tc.want)
					}
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func newAgentListMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func agentAppRows(values ...*appmodel.App) *sqlmock.Rows {
	rows := sqlmock.NewRows(agentAppListColumns)
	for _, value := range values {
		var currentRevision, canaryRevision any
		if value.CurrentRevision != nil {
			currentRevision = *value.CurrentRevision
		}
		if value.CanaryRevision != nil {
			canaryRevision = *value.CanaryRevision
		}
		rows.AddRow(value.TenantID, value.AppID, value.AppKey, value.DisplayName, value.Description, string(value.Status), currentRevision, canaryRevision, value.Version, value.CreatedAt, value.UpdatedAt)
	}
	return rows
}

func repeatedAgentApps(value *appmodel.App, count int) []*appmodel.App {
	values := make([]*appmodel.App, count)
	for i := range values {
		clone := value.Clone()
		values[i] = &clone
	}
	return values
}

func agentListRevisions(t *testing.T, app *appmodel.App, count int) []*appmodel.Revision {
	t.Helper()
	values := make([]*appmodel.Revision, count)
	for i := range values {
		values[i] = newStoredAgentRevision(t, app, int64(i+1), false)
	}
	return values
}

func expectAgentRevisionList(t *testing.T, mock sqlmock.Sqlmock, app *appmodel.App, values []*appmodel.Revision) {
	t.Helper()
	rows := sqlmock.NewRows([]string{"revision"})
	for _, value := range values {
		rows.AddRow(value.Revision)
	}
	mock.ExpectQuery(`FROM public\.agent_app_revision WHERE tenant_id=\$1 AND app_id=\$2`).
		WithArgs(app.TenantID, app.AppID).
		WillReturnRows(rows).
		RowsWillBeClosed()
	for _, value := range values {
		expectAgentRevision(t, mock, value)
	}
}

func newListRevision(t *testing.T, app *appmodel.App, number int64, published bool, description, globalInstruction string) *appmodel.Revision {
	t.Helper()
	value, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID:      app.TenantID,
		AppID:         app.AppID,
		Revision:      number,
		Kind:          appmodel.KindLLM,
		SchemaVersion: appmodel.SchemaVersionV1,
		Configuration: appmodel.DraftConfiguration{
			Description:       description,
			Instruction:       "Answer",
			GlobalInstruction: globalInstruction,
			ModelProfileID:    "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Runtime:           appmodel.DefaultRuntimePolicy(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !published {
		return value
	}
	result, err := value.Publish(value.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return &result
}
