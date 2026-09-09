package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestPostgresResolverLoadsExactPublishedOrRetiredVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snapshot := VersionSnapshot{
		Agent: tenant.AgentConfig{Name: "support", DefaultModel: "gpt-4o-mini"},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4o-mini"},
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT aa.id, aa.name, av.id, av.version_number, av.config_snapshot")).
		WithArgs("tenant-a", "app-1", "version-1").
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "app_name", "version_id", "version_number", "config_snapshot"}).
			AddRow("app-1", "support", "version-1", int64(9), encoded))
	resolved, err := NewPostgresResolver(db).LoadVersion(context.Background(), "tenant-a", "app-1", "version-1")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.AgentAppName != "support" || resolved.VersionNumber != 9 || resolved.Snapshot.Model.ModelName != "gpt-4o-mini" {
		t.Fatalf("resolved version=%#v", resolved)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresResolverExactVersionFailsClosedWhenUnavailable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT aa.id, aa.name, av.id, av.version_number, av.config_snapshot")).
		WithArgs("tenant-a", "app-1", "version-1").
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "app_name", "version_id", "version_number", "config_snapshot"}))
	_, err = NewPostgresResolver(db).LoadVersion(context.Background(), "tenant-a", "app-1", "version-1")
	if !errors.Is(err, ErrVersionNotAvailable) {
		t.Fatalf("error=%v", err)
	}
}
