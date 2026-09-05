package main

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

// ensureProductionRuntimeRole provisions the dedicated NOBYPASSRLS runtime
// role for a fixture schema and returns the runtime DSN. The migration-owner
// connection is only used to create the role and its grants.
func ensureProductionRuntimeRole(t *testing.T, adminURL, schema string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: adminURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatalf("runtime role fixture pool: %v", err)
	}
	roleName := "trpc_rt_" + strings.ReplaceAll(strings.ToLower(schema), "-", "_")
	if len(roleName) > 60 {
		roleName = roleName[:60]
	}
	password := "p201rls" + strings.ToLower(strconv.FormatInt(time.Now().UTC().UnixNano(), 36))
	if err := pgstore.EnsureTenantRuntimeRole(ctx, admin, schema, roleName, password); err != nil {
		admin.Close()
		t.Fatalf("ensure runtime role: %v", err)
	}
	parsed, err := url.Parse(adminURL)
	if err != nil {
		admin.Close()
		t.Fatalf("runtime role fixture url: %v", err)
	}
	parsed.User = url.UserPassword(roleName, password)
	runtimeURL := parsed.String()
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer ccancel()
		_, _ = admin.Exec(cctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName))
		admin.Close()
	})
	return runtimeURL
}
