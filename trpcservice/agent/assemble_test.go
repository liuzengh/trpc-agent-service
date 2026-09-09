package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// fakeApps implements AppProvider from in-memory maps.
type fakeApps struct {
	apps       map[string]tenant.AgentApp
	tenants    map[string]tenant.Tenant
	migrations map[string]tenant.Migration // by tenantID+":"+resource
}

func (f *fakeApps) AppByID(_ context.Context, id string) (tenant.AgentApp, tenant.Tenant, error) {
	app, ok := f.apps[id]
	if !ok {
		return tenant.AgentApp{}, tenant.Tenant{}, fmt.Errorf("%w: %s", tenant.ErrUnknownApp, id)
	}
	return app, f.tenants[app.TenantID], nil
}

// ActiveMigration implements AppProvider.
func (f *fakeApps) ActiveMigration(tenantID, resource string) *tenant.Migration {
	m, ok := f.migrations[tenantID+":"+resource]
	if !ok {
		return nil
	}
	return &m
}

// fakeSecrets implements config.SecretResolver from a map.
type fakeSecrets struct{ values map[string]string }

func (f fakeSecrets) Resolve(_ context.Context, ref string) (string, error) {
	v, ok := f.values[ref]
	if !ok {
		return "", fmt.Errorf("no secret %q", ref)
	}
	return v, nil
}

func testAssembler(apps *fakeApps, secrets fakeSecrets) *Assembler {
	sess := sessioninmemory.NewSessionService()
	return NewAssembler(AssemblerConfig{
		Apps:           apps,
		Secrets:        secrets,
		Registry:       tool.DemoTools(),
		SessionsByType: map[string]session.Service{"redis": sess},
		DefaultSession: "redis",
		Defaults: ModelSpec{
			Name:      "env-model",
			BaseURL:   "https://env.example.com",
			APIKeyRef: "env-key",
		},
		DefaultApp: "a1",
		Timeout:    time.Second,
	})
}

func testAppData() *fakeApps {
	return &fakeApps{
		apps: map[string]tenant.AgentApp{
			"a1": {ID: "a1", TenantID: "t1", Name: "assistant", Version: 1, Status: "published"},
			"a2": {ID: "a2", TenantID: "t2", Name: "helper", Version: 1, Status: "published",
				Config: json.RawMessage(`{"prompt":"你是助手二","model":{"name":"other-model"}}`)},
		},
		tenants: map[string]tenant.Tenant{
			"t1": {ID: "t1", Status: tenant.StatusActive},
			"t2": {ID: "t2", Status: tenant.StatusActive,
				ToolPolicy: json.RawMessage(`{"deny":["delete_user_data"]}`)},
		},
	}
}

func TestAssemblerCachesPerApp(t *testing.T) {
	a := testAssembler(testAppData(), fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()
	ctx := context.Background()

	p1, err := a.processorFor(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	again, err := a.processorFor(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if p1 != again {
		t.Fatal("same app + unchanged config must reuse the cached runner")
	}
	p2, err := a.processorFor(ctx, "a2")
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Fatal("different apps must get different runners")
	}
}

func TestAssemblerRebuildsOnConfigChange(t *testing.T) {
	apps := testAppData()
	a := testAssembler(apps, fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()
	ctx := context.Background()

	before, err := a.processorFor(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	app := apps.apps["a1"]
	app.Config = json.RawMessage(`{"prompt":"新提示词"}`)
	apps.apps["a1"] = app

	after, err := a.processorFor(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("config change must rebuild the runner")
	}
}

func TestAssemblerRebuildsOnTenantPolicyChange(t *testing.T) {
	apps := testAppData()
	a := testAssembler(apps, fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()
	ctx := context.Background()

	before, err := a.processorFor(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	t1 := apps.tenants["t1"]
	t1.ToolPolicy = json.RawMessage(`{"allow":["get_weather"]}`)
	apps.tenants["t1"] = t1

	after, err := a.processorFor(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("tenant tool_policy change must rebuild the runner")
	}
}

func TestAssemblerEchoOnMissingKey(t *testing.T) {
	// No secrets at all: every app degrades to echo, and the failure is not
	// cached so a repaired secret takes effect on the next message.
	a := testAssembler(testAppData(), fakeSecrets{values: map[string]string{}})
	defer func() { _ = a.Close() }()
	ctx := context.Background()

	msg := testMsg("hello")
	msg.AppID = "a1"
	out, err := a.Process(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "echo: hello" {
		t.Fatalf("want echo fallback, got %q", out.Text)
	}
	if len(a.cache) != 0 {
		t.Fatal("key failures must not be cached")
	}
}

func TestAssemblerDefaultAppFallback(t *testing.T) {
	// Routing disabled (PG down at startup): the message carries no app ID
	// and the assembler falls back to the env-configured default app, whose
	// config comes from Apps when resolvable.
	a := testAssembler(testAppData(), fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()

	p, err := a.processorFor(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.cache["a1"]; !ok || p == nil {
		t.Fatalf("default app a1 should be assembled and cached, cache=%v", a.cache)
	}
}

// The tenant-less fallback is deliberate, but it may not be quiet: the app it
// builds runs with no tenant tool policy, no rate limit, no budget and no
// guardrail policy, so an operator has to be able to see that a message took
// that path and why.
func TestAssemblerTenantlessFallbackIsLogged(t *testing.T) {
	stop := testenv.CaptureLogs(t, "warn")

	// A provider that cannot resolve the default app (unknown row, disabled
	// tenant, a snapshot that failed to load) is what sends resolveApp down
	// the fallback.
	a := testAssembler(&fakeApps{
		apps:    map[string]tenant.AgentApp{},
		tenants: map[string]tenant.Tenant{},
	}, fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()

	if _, err := a.processorFor(context.Background(), ""); err != nil {
		t.Fatalf("the env-only fallback must still serve: %v", err)
	}
	logged := stop()
	if !strings.Contains(logged, "default app a1 unresolved") ||
		!strings.Contains(logged, "serving without tenant policies") {
		t.Fatalf("the fallback must name the app and the consequence, got %q", logged)
	}
}

// A missing knowledge filter is not a neutral value: the framework reads an
// empty filter as "no predicate" and searches the shared pgvector table, so an
// app with no resolved tenant would answer from every tenant's documents. The
// empty pair matches nothing instead.
func TestKnowledgeFilterIsNeverAbsent(t *testing.T) {
	scoped := knowledgeFilter("t1", "a1")
	if scoped[MetadataTenantID] != "t1" || scoped[MetadataAppID] != "a1" {
		t.Fatalf("a resolved tenant must scope the search, got %v", scoped)
	}

	unscoped := knowledgeFilter("", "a1")
	if len(unscoped) != 2 {
		t.Fatalf("want both keys even without a tenant, got %v", unscoped)
	}
	if v, ok := unscoped[MetadataTenantID]; !ok || v != "" {
		t.Fatalf("an unresolved tenant must still yield a tenant_id predicate, got %v", unscoped)
	}
}

func TestAssemblerUnknownApp(t *testing.T) {
	a := testAssembler(testAppData(), fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()
	_, err := a.processorFor(context.Background(), "nope")
	if !errors.Is(err, tenant.ErrUnknownApp) {
		t.Fatalf("want ErrUnknownApp, got %v", err)
	}
}

func TestAssemblerBadConfigJSON(t *testing.T) {
	apps := testAppData()
	bad := apps.apps["a1"]
	bad.Config = json.RawMessage(`{"prompt":`)
	apps.apps["a1"] = bad
	a := testAssembler(apps, fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()

	_, err := a.processorFor(context.Background(), "a1")
	if err == nil {
		t.Fatal("invalid app config must fail the assembly")
	}
	var ke *keyError
	if errors.As(err, &ke) {
		t.Fatal("config errors must not degrade into the echo key-fallback")
	}
}

func TestMergeModelPrecedence(t *testing.T) {
	temp := 0.7
	env := ModelSpec{Name: "env", BaseURL: "https://env", APIKeyRef: "env-ref"}
	tenantSpec := ModelSpec{Name: "tenant-model", Temperature: &temp}
	appSpec := ModelSpec{BaseURL: "https://app"}

	got := mergeModel(env, tenantSpec, appSpec)
	if got.Name != "tenant-model" || got.BaseURL != "https://app" ||
		got.APIKeyRef != "env-ref" || got.Temperature == nil || *got.Temperature != 0.7 {
		t.Fatalf("unexpected merge result: %+v", got)
	}
	if env.Name != "env" || env.Temperature != nil {
		t.Fatalf("mergeModel must not mutate its inputs: %+v", env)
	}
}

func TestParseAppConfig(t *testing.T) {
	c, err := parseAppConfig(json.RawMessage(
		`{"prompt":"p","model":{"name":"m"},"tools":{"allow":["get_weather"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Prompt != "p" || c.Model.Name != "m" || len(c.Tools.Allow) != 1 {
		t.Fatalf("unexpected parse: %+v", c)
	}

	// Empty (NULL jsonb) is a zero config, not an error.
	c, err = parseAppConfig(nil)
	if err != nil || c.Prompt != "" {
		t.Fatalf("empty config must parse to zero value: %+v, %v", c, err)
	}

	if _, err = parseAppConfig(json.RawMessage(`{`)); err == nil {
		t.Fatal("invalid JSON must error")
	}
}

func TestParseToolPolicy(t *testing.T) {
	p := parseToolPolicy(json.RawMessage(`{"allow":["a"],"deny":["b"]}`))
	if len(p.Allow) != 1 || len(p.Deny) != 1 {
		t.Fatalf("unexpected policy: %+v", p)
	}
	// Invalid JSON degrades to an empty (allow-all) policy with a warning.
	p = parseToolPolicy(json.RawMessage(`{`))
	if len(p.Allow) != 0 || len(p.Deny) != 0 {
		t.Fatalf("invalid policy must degrade to empty, got %+v", p)
	}
}

func TestSessionServiceRouting(t *testing.T) {
	redisSvc := sessioninmemory.NewSessionService()
	pgSvc := sessioninmemory.NewSessionService()
	a := NewAssembler(AssemblerConfig{
		Registry:       tool.DemoTools(),
		SessionsByType: map[string]session.Service{"redis": redisSvc, "postgres": pgSvc},
		DefaultSession: "redis",
		Secrets:        fakeSecrets{values: map[string]string{"env-key": "sk"}},
		Defaults:       ModelSpec{Name: "m", APIKeyRef: "env-key"},
	})

	// No override → platform default.
	if got := a.sessionServiceFor(tenant.Tenant{ID: "t1"}); got != redisSvc {
		t.Fatal("empty storage_config must use the default backend")
	}
	// Controlled-menu override → postgres.
	pg := tenant.Tenant{ID: "t2", StorageConfig: json.RawMessage(`{"session":{"type":"postgres"}}`)}
	if got := a.sessionServiceFor(pg); got != pgSvc {
		t.Fatal("session.type=postgres must route to the pg backend")
	}
	// Unknown type → default with a warning.
	bogus := tenant.Tenant{ID: "t3", StorageConfig: json.RawMessage(`{"session":{"type":"etcd"}}`)}
	if got := a.sessionServiceFor(bogus); got != redisSvc {
		t.Fatal("unknown backend type must fall back to the default")
	}
}

func TestParseStorageConfig(t *testing.T) {
	if got := parseStorageConfig(nil); got.Session.Type != "" {
		t.Fatalf("empty config must be zero: %+v", got)
	}
	c := parseStorageConfig(json.RawMessage(`{"session":{"type":"postgres","dsn_ref":"t2-pg"}}`))
	if c.Session.Type != "postgres" {
		t.Fatalf("unexpected parse: %+v", c)
	}
	// Invalid JSON degrades to zero (default stack) with a warning.
	if got := parseStorageConfig(json.RawMessage(`{`)); got.Session.Type != "" {
		t.Fatalf("invalid config must degrade to zero: %+v", got)
	}
}

// During a session-backend migration the assembler must dual-write: reads
// follow the phase (old backend until the read switch, new one while
// observing), writes hit both.
func TestSessionServiceFanoutDuringMigration(t *testing.T) {
	redisSvc := sessioninmemory.NewSessionService()
	pgSvc := sessioninmemory.NewSessionService()
	apps := testAppData()
	apps.migrations = map[string]tenant.Migration{
		"t1:session": {ID: "m1", TenantID: "t1", Resource: "session",
			FromBackend: "redis", ToBackend: "postgres", Phase: tenant.PhaseBackfilling},
	}
	a := NewAssembler(AssemblerConfig{
		Apps:           apps,
		Registry:       tool.DemoTools(),
		SessionsByType: map[string]session.Service{"redis": redisSvc, "postgres": pgSvc},
		DefaultSession: "redis",
		Secrets:        fakeSecrets{values: map[string]string{"env-key": "sk"}},
		Defaults:       ModelSpec{Name: "m", APIKeyRef: "env-key"},
	})

	got := a.sessionServiceFor(tenant.Tenant{ID: "t1"})
	fo, ok := got.(*storage.FanoutSessionService)
	if !ok {
		t.Fatalf("active migration must fan out, got %T", got)
	}
	if fo.Primary != redisSvc || fo.Secondary != pgSvc {
		t.Fatal("backfilling: reads stay on the old backend, writes shadow the new")
	}

	// After the read switch the new backend serves reads.
	apps.migrations["t1:session"] = tenant.Migration{ID: "m1", TenantID: "t1", Resource: "session",
		FromBackend: "redis", ToBackend: "postgres", Phase: tenant.PhaseObserving}
	fo, ok = a.sessionServiceFor(tenant.Tenant{ID: "t1"}).(*storage.FanoutSessionService)
	if !ok || fo.Primary != pgSvc || fo.Secondary != redisSvc {
		t.Fatal("observing: reads must be on the new backend")
	}

	// A tenant without a migration keeps its plain backend.
	if got := a.sessionServiceFor(tenant.Tenant{ID: "t2"}); got != redisSvc {
		t.Fatalf("unrelated tenant must not fan out, got %T", got)
	}
}
