package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
)

func TestSkillCatalogPostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	db, err := sql.Open("pgx", os.Getenv("TRPC_POSTGRES_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var database string
	var major int
	if err := db.QueryRow(`SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&database, &major); err != nil || !strings.HasPrefix(database, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("database=%q major=%d err=%v", database, major, err)
	}
	catalog := New(db)
	value := skill.Package{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", SkillID: "catalog-contract", Version: 1, ContentDigest: strings.Repeat("a", 64), RelativePath: "skills/catalog-contract/v1"}
	if _, err := catalog.Stage(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if got, err := catalog.Stage(context.Background(), value); err != nil || got != value {
		t.Fatalf("idempotent staged retry got=%#v err=%v", got, err)
	}
	if _, err := catalog.Resolve(context.Background(), value.TenantID, value.SkillID, value.Version); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("staged resolve err=%v", err)
	}
	if _, err := catalog.Publish(context.Background(), value.TenantID, value.SkillID, value.Version); err != nil {
		t.Fatal(err)
	}
	if got, err := catalog.Resolve(context.Background(), value.TenantID, value.SkillID, value.Version); err != nil || got != value {
		t.Fatalf("got=%#v err=%v", got, err)
	}
	if got, err := catalog.Stage(context.Background(), value); err != nil || got != value {
		t.Fatalf("idempotent published retry got=%#v err=%v", got, err)
	}
}
