package bootstrap

import (
	"context"
	sharedpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/infra/postgres"
	"strings"
	"testing"
)

func TestOpenDatabaseRequiresBothConnections(t *testing.T) {
	for _, tc := range []struct {
		config Config
		want   string
	}{
		{Config{DatabaseURL: "postgres://%zz"}, "CONTROL_MIGRATION_DATABASE_URL"},
		{Config{MigrationDatabaseURL: "postgres://%zz"}, "CONTROL_DATABASE_URL"},
	} {
		pool, err := openDatabase(context.Background(), tc.config)
		if pool != nil || err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatal("missing explicit connection was not rejected before opening pools")
		}
	}
}

func TestControlDatabaseOwnershipIsFixed(t *testing.T) {
	for _, tc := range []struct{ name, schema, migrator, runtime string }{
		{"gateway workload", "gateway", "gateway_migrator", "gateway_runtime"},
		{"gateway roles in control", "control", "gateway_migrator", "gateway_runtime"},
		{"control roles in gateway", "gateway", "control_migrator", "control_runtime"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			migration := sharedpostgres.Target{Database: "platform", Schema: tc.schema, Role: tc.migrator, SchemaOwner: true, SchemaCreate: true}
			runtime := sharedpostgres.Target{Database: "platform", Schema: tc.schema, Role: tc.runtime}
			if err := checkControlDatabaseTargets(migration, runtime, "control"); err == nil {
				t.Fatal("Control accepted another workload's database contract")
			}
		})
	}
}
