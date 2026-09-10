package postgres

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

// TestProfileCredentialInvalidationPostgreSQL16 verifies that an immutable
// Profile revision and its broadcast hint commit atomically in the real schema.
func TestProfileCredentialInvalidationPostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var database string
	var major int
	if err := db.QueryRow(`SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&database, &major); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(database, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", database, major)
	}
	catalog, err := provider.NewCatalog(provider.DeepSeekModelSchema())
	if err != nil {
		t.Fatal(err)
	}
	repository := New(db, catalog)
	value, err := repository.PublishModel(context.Background(), provider.ModelProfileSnapshot{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileID: "credential-model-contract", ProfileKey: "credential-model-contract", DisplayName: "Credential contract", Status: "active", SchemaVersion: 1, Provider: "deepseek", Model: "deepseek-v4-flash-vision-exp", Endpoint: "https://api.deepseek.com", SecretRef: secrets.SecretRef{Ref: "vault://tenant/model", Version: 1}, Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	var payload string
	err = db.QueryRowContext(context.Background(), `SELECT payload_ref FROM public.outbox WHERE tenant_id=$1 AND kind='config-invalidation' AND idempotency_key=$2`, value.TenantID, "provider-profile:model:"+value.TenantID+":"+value.ProfileID+":1:invalidate").Scan(&payload)
	if err != nil || payload != "provider-profile://"+value.TenantID+"/model/"+value.ProfileID+"/1" {
		t.Fatalf("payload=%q err=%v", payload, err)
	}
}
