package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

//nolint:gocyclo // Boundary coverage intentionally exercises each storage helper outcome.
func TestAgentMySQLBoundaryHelpers(t *testing.T) {
	if err := validateAgentMetadata(appmodel.ChangeMetadata{ActorType: "a", ActorID: "b", Reason: "c", CorrelationID: "d"}); err != nil {
		t.Fatal(err)
	}
	if err := validateAgentMetadata(appmodel.ChangeMetadata{ActorType: "a", ActorID: "b", Reason: strings.Repeat("x", 1001), CorrelationID: "d"}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("long metadata error = %v", err)
	}
	if agentStatusEventType(appmodel.StatusSuspended) != appmodel.ChangeSuspended || agentStatusEventType(appmodel.StatusActive) != appmodel.ChangeResumed || agentStatusEventType(appmodel.StatusDisabled) != appmodel.ChangeDisabled {
		t.Fatal("status event mapping is incorrect")
	}
	value := int64(4)
	if clone := cloneAgentInt64(&value); clone == nil || clone == &value || *clone != value || cloneAgentInt64(nil) != nil {
		t.Fatal("integer clone is not defensive")
	}
	if got := *agentInt64(value); got != value || nullableInt64(sql.NullInt64{Int64: value, Valid: true}) == nil || nullableInt64(sql.NullInt64{}) != nil {
		t.Fatal("integer helpers are incorrect")
	}
	now := time.Now().UTC()
	if pointer := timePointer(now); pointer == nil || !pointer.Equal(now) || !maxTime(now, now.Add(time.Second)).Equal(now.Add(time.Second)) {
		t.Fatal("time helpers are incorrect")
	}
	if err := mutableAgentApp(&appmodel.App{Status: appmodel.StatusDisabled, Version: 1}, 1); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("disabled app error = %v", err)
	}
	if err := mutableAgentApp(&appmodel.App{Status: appmodel.StatusActive, Version: 2}, 1); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("stale app error = %v", err)
	}
	if err := validatePublishInput(appmodel.PublishInput{TenantActive: false, Metadata: appmodel.ChangeMetadata{ActorType: "a", ActorID: "b", Reason: "c", CorrelationID: "d"}}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("inactive tenant publish error = %v", err)
	}
	if !maxTime(now.Add(time.Second), now).Equal(now.Add(time.Second)) {
		t.Fatal("maxTime left branch is incorrect")
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	if err := assertTenantActive(context.Background(), db, "tenant"); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("suspended"))
	if err := assertTenantActive(context.Background(), db, "tenant"); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("inactive tenant error = %v", err)
	}
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnError(errors.New("query"))
	if err := assertTenantActive(context.Background(), db, "tenant"); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant query error = %v", err)
	}
	mock.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := replaceRevisionTools(context.Background(), db, appmodel.Revision{TenantID: "tenant", AppID: "app", Revision: 1, Tools: []appmodel.ToolAuthorization{{ToolID: "tool", Required: true}}}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
