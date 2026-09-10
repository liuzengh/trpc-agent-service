package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

func TestAgentRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewAppRepository(nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, err := r.Create(ctx, appmodel.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant", "app"); return err }},
		{"update metadata", func() error { _, err := r.UpdateMetadata(ctx, appmodel.UpdateMetadataInput{}); return err }},
		{"create draft", func() error { _, err := r.CreateDraft(ctx, appmodel.CreateDraftInput{}); return err }},
		{"update draft", func() error { _, err := r.UpdateDraft(ctx, appmodel.UpdateDraftInput{}); return err }},
		{"get revision", func() error { _, err := r.GetRevision(ctx, "tenant", "app", 1); return err }},
		{"list apps", func() error { _, _, err := r.List(ctx, "tenant", "", "", "", 1); return err }},
		{"list revisions", func() error { _, _, err := r.ListRevisions(ctx, "tenant", "app", "", "", "", 1); return err }},
		{"publish", func() error { _, _, _, err := r.Publish(ctx, appmodel.PublishInput{}); return err }},
		{"set canary", func() error { _, _, err := r.SetCanary(ctx, appmodel.SetCanaryInput{}); return err }},
		{"rollback", func() error { _, _, err := r.Rollback(ctx, appmodel.RollbackInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, appmodel.TransitionStatusInput{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestAgentRepositoryListBoundaryBranches(t *testing.T) {
	ctx := context.Background()
	if _, _, err := NewAppRepository(nil).List(ctx, "tenant", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
	if _, _, err := NewAppRepository(nil).ListRevisions(ctx, "tenant", "app", "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage ListRevisions error = %v", err)
	}
	for _, tc := range []struct {
		name string
		call func(*sql.DB, sqlmock.Sqlmock) error
	}{
		{"app invalid cursor", func(db *sql.DB, _ sqlmock.Sqlmock) error {
			_, _, err := NewAppRepository(db).List(ctx, "tenant", "", "", "bad", 1)
			return err
		}},
		{"revision invalid cursor", func(db *sql.DB, _ sqlmock.Sqlmock) error {
			_, _, err := NewAppRepository(db).ListRevisions(ctx, "tenant", "app", "", "", "bad", 1)
			return err
		}},
		{"app query error", func(db *sql.DB, mock sqlmock.Sqlmock) error {
			mock.ExpectQuery("FROM agent_app WHERE tenant_id").WithArgs("tenant").WillReturnError(errors.New("list query"))
			_, _, err := NewAppRepository(db).List(ctx, "tenant", "", "", "", 1)
			return err
		}},
		{"revision query error", func(db *sql.DB, mock sqlmock.Sqlmock) error {
			mock.ExpectQuery("SELECT revision FROM agent_app_revision").WithArgs("tenant", "app").WillReturnError(errors.New("list query"))
			_, _, err := NewAppRepository(db).ListRevisions(ctx, "tenant", "app", "", "", "", 1)
			return err
		}},
		{"app rows error", func(db *sql.DB, mock sqlmock.Sqlmock) error {
			mock.ExpectQuery("FROM agent_app WHERE tenant_id").WithArgs("tenant").WillReturnRows(sqlmock.NewRows([]string{"app_id"}).AddRow("app").RowError(0, errors.New("rows")))
			_, _, err := NewAppRepository(db).List(ctx, "tenant", "", "", "", 1)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			callErr := tc.call(db, mock)
			if !errors.Is(callErr, ErrStorage) && tc.name != "app invalid cursor" && tc.name != "revision invalid cursor" {
				t.Fatalf("error = %v", callErr)
			}
			if (tc.name == "app invalid cursor" || tc.name == "revision invalid cursor") && callErr == nil {
				t.Fatal("invalid cursor was accepted")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

//nolint:gocyclo // Exercises the complete SQL list contract in one fixture.
func TestAgentRepositoryListCoversFilteringPagingAndScanReturns(t *testing.T) {
	first := newStoredAgentApp(t)
	first.AppKey, first.DisplayName = "primary", "Primary"
	second := first.Clone()
	second.AppID = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	second.AppKey, second.DisplayName = "secondary", "Secondary"
	disabled := first.Clone()
	disabled.AppID = "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	disabled.AppKey, disabled.DisplayName, disabled.Status = "retired", "Retired", appmodel.StatusDisabled
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`FROM agent_app WHERE tenant_id = \? ORDER BY app_id`).WithArgs(first.TenantID).
		WillReturnRows(agentMySQLAppRows(first, &second, &disabled))
	items, next, err := NewAppRepository(db).List(ctx, first.TenantID, "primary", string(appmodel.StatusDraft), "", 1)
	if err != nil || len(items) != 1 || items[0].AppID != first.AppID || next != "" {
		t.Fatalf("filtered app page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`FROM agent_app WHERE tenant_id = \? ORDER BY app_id`).WithArgs(first.TenantID).
		WillReturnRows(agentMySQLAppRows(first, &second))
	items, next, err = NewAppRepository(db).List(ctx, first.TenantID, "", "", "", 201)
	if err != nil || len(items) != 2 || next != "" {
		t.Fatalf("maximum app page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`FROM agent_app WHERE tenant_id = \? ORDER BY app_id`).WithArgs(first.TenantID).
		WillReturnRows(agentMySQLAppRows(first))
	items, next, err = NewAppRepository(db).List(ctx, first.TenantID, "", "", "1", 0)
	if err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("past-end app page = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		rows *sqlmock.Rows
	}{
		{name: "scan", rows: sqlmock.NewRows([]string{"tenant_id"}).AddRow(nil)},
		{name: "iteration", rows: agentMySQLAppRows(first).RowError(0, errors.New("iteration"))},
		{name: "invalid stored app", rows: agentMySQLAppRows(func() *appmodel.App { value := first.Clone(); value.Status = appmodel.Status("unknown"); return &value }())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mock.ExpectQuery(`FROM agent_app WHERE tenant_id = \? ORDER BY app_id`).WithArgs(first.TenantID).WillReturnRows(tc.rows)
			if _, _, err := NewAppRepository(db).List(ctx, first.TenantID, "", "", "", 1); !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}

	firstRevision := newStoredAgentRevision(t, first, 1, false)
	secondRevision := newStoredAgentRevision(t, first, 2, false)
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT revision FROM agent_app_revision WHERE tenant_id = \? AND app_id = \? ORDER BY revision`).WithArgs(first.TenantID, first.AppID).
		WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(firstRevision.Revision).AddRow(secondRevision.Revision))
	expectAgentRevision(t, mock, firstRevision)
	expectAgentRevision(t, mock, secondRevision)
	itemsRevision, nextRevision, err := NewAppRepository(db).ListRevisions(ctx, first.TenantID, first.AppID, "answer", string(appmodel.RevisionStateDraft), "", 1)
	if err != nil || len(itemsRevision) != 1 || itemsRevision[0].Revision != firstRevision.Revision || nextRevision == "" {
		t.Fatalf("filtered revision page = items=%+v next=%q err=%v", itemsRevision, nextRevision, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT revision FROM agent_app_revision WHERE tenant_id = \? AND app_id = \? ORDER BY revision`).WithArgs(first.TenantID, first.AppID).
		WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(firstRevision.Revision).AddRow(secondRevision.Revision))
	expectAgentRevision(t, mock, firstRevision)
	expectAgentRevision(t, mock, secondRevision)
	itemsRevision, nextRevision, err = NewAppRepository(db).ListRevisions(ctx, first.TenantID, first.AppID, "", "", "", 201)
	if err != nil || len(itemsRevision) != 2 || nextRevision != "" {
		t.Fatalf("maximum revision page = items=%+v next=%q err=%v", itemsRevision, nextRevision, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`SELECT revision FROM agent_app_revision WHERE tenant_id = \? AND app_id = \? ORDER BY revision`).WithArgs(first.TenantID, first.AppID).
		WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(firstRevision.Revision))
	expectAgentRevision(t, mock, firstRevision)
	if _, _, err = NewAppRepository(db).ListRevisions(ctx, first.TenantID, first.AppID, "", "", "1", 0); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		rows *sqlmock.Rows
	}{
		{name: "revision scan", rows: sqlmock.NewRows([]string{"revision"}).AddRow(nil)},
		{name: "revision iteration", rows: sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)).RowError(0, errors.New("iteration"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mock.ExpectQuery(`SELECT revision FROM agent_app_revision WHERE tenant_id = \? AND app_id = \? ORDER BY revision`).WithArgs(first.TenantID, first.AppID).WillReturnRows(tc.rows)
			if _, _, err := NewAppRepository(db).ListRevisions(ctx, first.TenantID, first.AppID, "", "", "", 1); !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func agentMySQLAppRows(values ...*appmodel.App) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"tenant_id", "app_id", "app_key", "display_name", "description", "status", "current_revision", "canary_revision", "version", "created_at", "updated_at"})
	for _, value := range values {
		var current, canary any
		if value.CurrentRevision != nil {
			current = *value.CurrentRevision
		}
		if value.CanaryRevision != nil {
			canary = *value.CanaryRevision
		}
		rows.AddRow(value.TenantID, value.AppID, value.AppKey, value.DisplayName, value.Description, string(value.Status), current, canary, value.Version, value.CreatedAt, value.UpdatedAt)
	}
	return rows
}

func TestSameAgentRevisionHandlesNilAndValuePairs(t *testing.T) {
	value := int64(7)
	other := int64(8)
	for _, tc := range []struct {
		name        string
		left, right *int64
		want        bool
	}{
		{"both nil", nil, nil, true},
		{"left nil", nil, &value, false},
		{"right nil", &value, nil, false},
		{"equal", &value, &value, true},
		{"different", &value, &other, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameAgentRevision(tc.left, tc.right); got != tc.want {
				t.Fatalf("sameAgentRevision() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMaxTimeChoosesLatestTimestamp(t *testing.T) {
	first := time.Unix(10, 0).UTC()
	second := time.Unix(20, 0).UTC()
	if got := maxTime(first, second); !got.Equal(second) {
		t.Fatalf("maxTime(first, second) = %v, want second", got)
	}
	if got := maxTime(second, first); !got.Equal(second) {
		t.Fatalf("maxTime(second, first) = %v, want second", got)
	}
}

func TestReplaceRevisionToolsClearsEmptySet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := replaceRevisionTools(context.Background(), db, appmodel.Revision{TenantID: "tenant", AppID: "app", Revision: 1}); err != nil {
		t.Fatalf("replace empty tools = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSetCanaryRejectsInactiveTenantBeforeTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, _, err = NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{
		TenantID: "tenant", AppID: "app", TenantActive: false,
		Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "inactive", CorrelationID: "inactive-tenant"},
	})
	if !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("inactive tenant error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSetCanaryGuardsStateAndCandidateBoundaries(t *testing.T) {
	base := newStoredAgentApp(t)
	currentRevision := int64(1)
	base.Status, base.CurrentRevision, base.Version = appmodel.StatusActive, &currentRevision, 2
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "guard", CorrelationID: "canary-guard"}
	tests := []struct {
		name      string
		app       *appmodel.App
		candidate *int64
		setup     func(sqlmock.Sqlmock, *appmodel.App)
		want      error
	}{
		{"disabled app", func() *appmodel.App { v := base.Clone(); v.Status = appmodel.StatusSuspended; return &v }(), agentInt64(2), func(m sqlmock.Sqlmock, a *appmodel.App) { m.ExpectBegin(); expectAgentApp(m, a); m.ExpectRollback() }, appmodel.ErrInvalid},
		{"unchanged canary", func() *appmodel.App { v := base.Clone(); return &v }(), nil, func(m sqlmock.Sqlmock, a *appmodel.App) { m.ExpectBegin(); expectAgentApp(m, a); m.ExpectRollback() }, appmodel.ErrInvalid},
		{"current revision candidate", func() *appmodel.App { v := base.Clone(); return &v }(), &currentRevision, func(m sqlmock.Sqlmock, a *appmodel.App) { m.ExpectBegin(); expectAgentApp(m, a); m.ExpectRollback() }, appmodel.ErrInvalid},
		{"candidate load error", func() *appmodel.App { v := base.Clone(); return &v }(), agentInt64(2), func(m sqlmock.Sqlmock, a *appmodel.App) {
			m.ExpectBegin()
			expectAgentApp(m, a)
			m.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("candidate"))
			m.ExpectRollback()
		}, ErrStorage},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock, tc.app)
			_, _, err = NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: tc.app.TenantID, AppID: tc.app.AppID, CandidateRevision: tc.candidate, ExpectedAppVersion: tc.app.Version, TenantActive: true, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRollbackRejectsUnpublishedAndUnchangedTargets(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(2)
	app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 3
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "guard", CorrelationID: "rollback-guard"}
	for _, tc := range []struct {
		name           string
		target         *appmodel.Revision
		targetRevision int64
		want           error
	}{
		{"unpublished", newStoredAgentRevision(t, app, 1, false), 1, appmodel.ErrInvalid},
		{"unchanged", newStoredAgentRevision(t, app, 2, true), 2, appmodel.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			expectAgentApp(mock, app)
			expectAgentRevision(t, mock, tc.target)
			mock.ExpectRollback()
			_, _, err = NewAppRepository(db).Rollback(context.Background(), appmodel.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: tc.targetRevision, ExpectedAppVersion: app.Version, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSetCanaryPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(1)
	app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 2
	candidate := newStoredAgentRevision(t, app, 2, true)
	updated := app.Clone()
	updated.CanaryRevision = agentInt64(2)
	updated.Version++
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"}
	common := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		expectAgentRevision(t, m, candidate)
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{"update error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnError(errors.New("update"))
			m.ExpectRollback()
		}, ErrStorage},
		{"update conflict", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectRollback()
		}, appmodel.ErrConflict},
		{"event insert error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnError(errors.New("event"))
			m.ExpectRollback()
		}, ErrStorage},
		{"readback error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}, ErrStorage},
		{"event scan error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
			expectAgentApp(m, &updated)
			m.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("bad"))
			m.ExpectRollback()
		}, ErrStorage},
		{"commit error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
			expectAgentApp(m, &updated)
			expectAgentEvent(m, &updated, appmodel.ChangeCanaryStarted, appmodel.StatusActive, appmodel.StatusActive, nil, updated.CanaryRevision, candidate.ContentDigest, app.Version, updated.Version, updated.UpdatedAt)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}, ErrStorage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock)
			_, _, err = NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: app.TenantID, AppID: app.AppID, CandidateRevision: agentInt64(2), ExpectedAppVersion: app.Version, TenantActive: true, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRollbackPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(2)
	app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 3
	target := newStoredAgentRevision(t, app, 1, true)
	updated := app.Clone()
	updated.CurrentRevision = agentInt64(1)
	updated.CanaryRevision = nil
	updated.Version++
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"}
	common := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		expectAgentRevision(t, m, target)
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{"update error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnError(errors.New("update"))
			m.ExpectRollback()
		}, ErrStorage},
		{"update conflict", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectRollback()
		}, appmodel.ErrConflict},
		{"event insert error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnError(errors.New("event"))
			m.ExpectRollback()
		}, ErrStorage},
		{"readback error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(8, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}, ErrStorage},
		{"event scan error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(8, 1))
			expectAgentApp(m, &updated)
			m.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("bad"))
			m.ExpectRollback()
		}, ErrStorage},
		{"commit error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(8, 1))
			expectAgentApp(m, &updated)
			expectAgentEvent(m, &updated, appmodel.ChangeRolledBack, appmodel.StatusActive, appmodel.StatusActive, app.CurrentRevision, updated.CurrentRevision, target.ContentDigest, app.Version, updated.Version, updated.UpdatedAt)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}, ErrStorage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock)
			_, _, err = NewAppRepository(db).Rollback(context.Background(), appmodel.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: 1, ExpectedAppVersion: app.Version, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransitionStatusPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(1)
	app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 2
	revision := newStoredAgentRevision(t, app, 1, true)
	updated := app.Clone()
	updated.Status = appmodel.StatusSuspended
	updated.Version++
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"}
	common := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		expectAgentRevision(t, m, revision)
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{"update error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnError(errors.New("update"))
			m.ExpectRollback()
		}, ErrStorage},
		{"update conflict", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectRollback()
		}, appmodel.ErrConflict},
		{"event insert error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnError(errors.New("event"))
			m.ExpectRollback()
		}, ErrStorage},
		{"event id error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewErrorResult(errors.New("last id")))
			m.ExpectRollback()
		}, ErrStorage},
		{"readback error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(9, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}, ErrStorage},
		{"event scan error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(9, 1))
			expectAgentApp(m, &updated)
			m.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("bad"))
			m.ExpectRollback()
		}, ErrStorage},
		{"commit error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(9, 1))
			expectAgentApp(m, &updated)
			expectAgentEvent(m, &updated, appmodel.ChangeSuspended, appmodel.StatusActive, appmodel.StatusSuspended, app.CurrentRevision, updated.CurrentRevision, revision.ContentDigest, app.Version, updated.Version, updated.UpdatedAt)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}, ErrStorage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock)
			_, _, err = NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusSuspended, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDraftPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	input := appmodel.CreateDraftInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Configuration: appmodel.DraftConfiguration{Instruction: "draft", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: appmodel.DefaultRuntimePolicy()}}
	draft := newStoredAgentRevision(t, app, 1, false)
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock
	}
	createPrefix := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		m.ExpectQuery("COALESCE\\(MAX\\(revision\\)").WillReturnRows(sqlmock.NewRows([]string{"next_revision"}).AddRow(1))
	}
	for _, tc := range []struct {
		name  string
		setup func(sqlmock.Sqlmock)
	}{
		{"insert", func(m sqlmock.Sqlmock) {
			createPrefix(m)
			m.ExpectExec("INSERT INTO agent_app_revision").WillReturnError(errors.New("insert"))
			m.ExpectRollback()
		}},
		{"replace tools", func(m sqlmock.Sqlmock) {
			createPrefix(m)
			m.ExpectExec("INSERT INTO agent_app_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnError(errors.New("tools"))
			m.ExpectRollback()
		}},
		{"readback", func(m sqlmock.Sqlmock) {
			createPrefix(m)
			m.ExpectExec("INSERT INTO agent_app_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}},
		{"commit", func(m sqlmock.Sqlmock) {
			createPrefix(m)
			m.ExpectExec("INSERT INTO agent_app_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
			expectAgentRevision(t, m, draft)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}},
	} {
		t.Run("create "+tc.name, func(t *testing.T) {
			db, mock := newDB(t)
			tc.setup(mock)
			if _, err := NewAppRepository(db).CreateDraft(context.Background(), input); !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	current := newStoredAgentRevision(t, app, 1, false)
	updateInput := appmodel.UpdateDraftInput{TenantID: app.TenantID, AppID: app.AppID, Revision: 1, ExpectedAppVersion: app.Version, ExpectedDraftVersion: current.DraftVersion, Configuration: current.Configuration()}
	updated := current.Clone()
	updated.DraftVersion++
	updated.UpdatedAt = current.UpdatedAt.Add(time.Second)
	updatePrefix := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		expectAgentRevision(t, m, current)
	}
	for _, tc := range []struct {
		name  string
		setup func(sqlmock.Sqlmock)
	}{
		{"update", func(m sqlmock.Sqlmock) {
			updatePrefix(m)
			m.ExpectExec("UPDATE agent_app_revision SET").WillReturnError(errors.New("update"))
			m.ExpectRollback()
		}},
		{"replace tools", func(m sqlmock.Sqlmock) {
			updatePrefix(m)
			m.ExpectExec("UPDATE agent_app_revision SET").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnError(errors.New("tools"))
			m.ExpectRollback()
		}},
		{"readback", func(m sqlmock.Sqlmock) {
			updatePrefix(m)
			m.ExpectExec("UPDATE agent_app_revision SET").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}},
		{"commit", func(m sqlmock.Sqlmock) {
			updatePrefix(m)
			m.ExpectExec("UPDATE agent_app_revision SET").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
			expectAgentRevision(t, m, &updated)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}},
	} {
		t.Run("update "+tc.name, func(t *testing.T) {
			db, mock := newDB(t)
			tc.setup(mock)
			if _, err := NewAppRepository(db).UpdateDraft(context.Background(), updateInput); !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
