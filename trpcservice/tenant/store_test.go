package tenant_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// TestPGStoreLoadAll exercises the real schema: the test inserts its own rows
// (unique per run) into tenant / agent_app / channel_binding /
// storage_migration and finds them in the loaded snapshot. Cleanup removes the
// rows again, so the shared database stays untouched.
func TestPGStoreLoadAll(t *testing.T) {
	pool := testenv.PG(t)
	ctx := context.Background()
	uid := fmt.Sprintf("%d", time.Now().UnixNano())

	newUUID := func() string {
		var id string
		if err := pool.QueryRow(ctx, "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	tenantID, appID, bindingID, migID := newUUID(), newUUID(), newUUID(), newUUID()

	// Cleanup runs LIFO: children first, then the tenant row.
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM storage_migration WHERE id=$1", migID) })
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM channel_binding WHERE id=$1", bindingID) })
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM agent_app WHERE id=$1", appID) })
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM tenant WHERE id=$1", tenantID) })

	name := "pgstore-" + uid
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, model_config, guardrail_policy, rate_policy, status)
		 VALUES ($1, $2, $3, $4, $5, 'active')`,
		tenantID, name, `{"model":"test-model"}`, `{"max_tokens_per_day":1000}`, `{"qps":5,"burst":10}`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, $3, 'chat', $4, 3, 'published')`,
		appID, tenantID, "app-"+uid, `{"prompt":"p"}`); err != nil {
		t.Fatal(err)
	}
	// token_ref stays NULL (the nullable column the loader must tolerate);
	// aeskey_ref is set.
	if _, err := pool.Exec(ctx,
		`INSERT INTO channel_binding (id, tenant_id, channel, app_id, webhook_path, token_ref, aeskey_ref, config, status)
		 VALUES ($1, $2, 'mock', $3, $4, NULL, $5, $6, 'active')`,
		bindingID, tenantID, appID, "/mock/pgstore/"+uid, "secret-ref-aes", `{"k":"v"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO storage_migration (id, tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, $2, 'session', 'redis', 'postgres', 'dual_write')`,
		migID, tenantID); err != nil {
		t.Fatal(err)
	}

	d, err := tenant.NewPGStore(pool).LoadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var foundT *tenant.Tenant
	for i := range d.Tenants {
		if d.Tenants[i].ID == tenantID {
			foundT = &d.Tenants[i]
		}
	}
	if foundT == nil {
		t.Fatalf("tenant row not in snapshot (%d tenants loaded)", len(d.Tenants))
	}
	if foundT.Name != name || foundT.Status != tenant.StatusActive {
		t.Fatalf("unexpected tenant row: %+v", foundT)
	}
	if string(foundT.RatePolicy) == "" || !json.Valid(foundT.RatePolicy) {
		t.Fatalf("rate_policy not carried as raw JSON: %s", foundT.RatePolicy)
	}

	var foundApp *tenant.AgentApp
	for i := range d.Apps {
		if d.Apps[i].ID == appID {
			foundApp = &d.Apps[i]
		}
	}
	if foundApp == nil {
		t.Fatalf("agent_app row not in snapshot (%d apps loaded)", len(d.Apps))
	}
	if foundApp.TenantID != tenantID || foundApp.Version != 3 || foundApp.Status != "published" {
		t.Fatalf("unexpected app row: %+v", foundApp)
	}

	var foundBinding *tenant.ChannelBinding
	for i := range d.Bindings {
		if d.Bindings[i].ID == bindingID {
			foundBinding = &d.Bindings[i]
		}
	}
	if foundBinding == nil {
		t.Fatalf("channel_binding row not in snapshot (%d bindings loaded)", len(d.Bindings))
	}
	// A NULL token_ref scans into an empty string, not an error.
	if foundBinding.WebhookPath != "/mock/pgstore/"+uid ||
		foundBinding.TokenRef != "" || foundBinding.AESKeyRef != "secret-ref-aes" {
		t.Fatalf("unexpected binding row: %+v", foundBinding)
	}

	var foundMig *tenant.Migration
	for i := range d.Migrations {
		if d.Migrations[i].ID == migID {
			foundMig = &d.Migrations[i]
		}
	}
	if foundMig == nil {
		t.Fatalf("storage_migration row not in snapshot (%d migrations loaded)", len(d.Migrations))
	}
	if foundMig.TenantID != tenantID || foundMig.Resource != "session" ||
		foundMig.Phase != tenant.PhaseDualWrite {
		t.Fatalf("unexpected migration row: %+v", foundMig)
	}
}
