package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// failingCloseRunner is a runner.Runner whose Close fails, to exercise the
// assembler's shutdown error propagation.
type failingCloseRunner struct{ err error }

func (f *failingCloseRunner) Run(context.Context, string, string, model.Message, ...tagent.RunOption) (<-chan *event.Event, error) {
	ch := make(chan *event.Event)
	close(ch)
	return ch, nil
}

func (f *failingCloseRunner) Close() error { return f.err }

// SessionServiceFor resolves the session backend of an app for out-of-band
// state writes: tenant routing, migration fanout, and the no-provider error.
func TestSessionServiceFor(t *testing.T) {
	redisSvc := sessioninmemory.NewSessionService()
	pgSvc := sessioninmemory.NewSessionService()
	apps := testAppData()

	a := NewAssembler(AssemblerConfig{
		Apps:           apps,
		Registry:       tool.DemoTools(),
		SessionsByType: map[string]session.Service{"redis": redisSvc, "postgres": pgSvc},
		DefaultSession: "redis",
		Secrets:        fakeSecrets{values: map[string]string{"env-key": "sk"}},
		Defaults:       ModelSpec{Name: "m", APIKeyRef: "env-key"},
	})
	ctx := context.Background()

	// Resolvable app: the tenant's backend wins over the default.
	apps.tenants["t2"] = tenant.Tenant{ID: "t2", Status: tenant.StatusActive,
		StorageConfig: json.RawMessage(`{"session":{"type":"postgres"}}`)}
	svc, err := a.SessionServiceFor(ctx, "a2")
	if err != nil {
		t.Fatal(err)
	}
	if svc != pgSvc {
		t.Fatalf("app a2 (tenant t2) must use the postgres backend, got %T", svc)
	}

	// Tenant without an override: the platform default.
	svc, err = a.SessionServiceFor(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if svc != redisSvc {
		t.Fatalf("app a1 must use the default backend, got %T", svc)
	}

	// Unknown app: the error propagates.
	if _, err = a.SessionServiceFor(ctx, "nope"); !errors.Is(err, tenant.ErrUnknownApp) {
		t.Fatalf("want ErrUnknownApp, got %v", err)
	}

	// No app provider at all (PG down at startup): a distinct error.
	bare := NewAssembler(AssemblerConfig{Registry: tool.DemoTools()})
	if _, err = bare.SessionServiceFor(ctx, "a1"); err == nil || !strings.Contains(err.Error(), "no app provider") {
		t.Fatalf("want no-app-provider error, got %v", err)
	}
}

// Assembler.Process surfaces resolution errors instead of dropping them.
func TestAssemblerProcessErrorPropagates(t *testing.T) {
	a := testAssembler(testAppData(), fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()
	msg := testMsg("hello")
	msg.AppID = "ghost"
	if _, err := a.Process(context.Background(), msg); !errors.Is(err, tenant.ErrUnknownApp) {
		t.Fatalf("want ErrUnknownApp from Process, got %v", err)
	}
}

// With no app provider, a non-empty app ID cannot be resolved.
func TestResolveAppWithoutProvider(t *testing.T) {
	a := NewAssembler(AssemblerConfig{
		Registry:       tool.DemoTools(),
		Secrets:        fakeSecrets{values: map[string]string{"env-key": "sk"}},
		SessionsByType: map[string]session.Service{"redis": sessioninmemory.NewSessionService()},
		DefaultSession: "redis",
		Defaults:       ModelSpec{Name: "m", APIKeyRef: "env-key"},
		DefaultApp:     "env-app",
	})
	_, err := a.processorFor(context.Background(), "some-app")
	if err == nil || !strings.Contains(err.Error(), "no app provider") {
		t.Fatalf("want no-app-provider error, got %v", err)
	}

	// An empty app ID still assembles the env default app.
	p, err := a.processorFor(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.cache["env-app"]; !ok || p == nil {
		t.Fatalf("default app must be assembled and cached, cache=%v", a.cache)
	}
}

// A tenant/app model override pointing outside the platform allowlist is
// rejected at assembly time — defense in depth behind the Admin API gate.
func TestAssembleRejectsDisallowedModelHost(t *testing.T) {
	apps := testAppData()
	app := apps.apps["a1"]
	app.Config = json.RawMessage(`{"model":{"base_url":"http://evil.example.com"}}`)
	apps.apps["a1"] = app

	a := testAssembler(apps, fakeSecrets{values: map[string]string{"env-key": "sk"}})
	defer func() { _ = a.Close() }()
	_, err := a.processorFor(context.Background(), "a1")
	if err == nil || !strings.Contains(err.Error(), "model endpoint rejected") {
		t.Fatalf("want allowlist rejection, got %v", err)
	}
	var ke *keyError
	if errors.As(err, &ke) {
		t.Fatal("host rejection must not degrade into the echo fallback")
	}
}

// A migration whose backend pair is not in the controlled menu falls back to
// the platform default with a warning, not a panic.
func TestSessionServiceForUnavailableMigrationPair(t *testing.T) {
	redisSvc := sessioninmemory.NewSessionService()
	apps := testAppData()
	apps.migrations = map[string]tenant.Migration{
		"t1:session": {ID: "m1", TenantID: "t1", Resource: "session",
			FromBackend: "mysql", ToBackend: "etcd", Phase: tenant.PhaseBackfilling},
	}
	a := NewAssembler(AssemblerConfig{
		Apps:           apps,
		Registry:       tool.DemoTools(),
		SessionsByType: map[string]session.Service{"redis": redisSvc},
		DefaultSession: "redis",
		Secrets:        fakeSecrets{values: map[string]string{"env-key": "sk"}},
		Defaults:       ModelSpec{Name: "m", APIKeyRef: "env-key"},
	})
	if got := a.sessionServiceFor(tenant.Tenant{ID: "t1"}); got != redisSvc {
		t.Fatalf("unavailable migration pair must fall back to the default, got %T", got)
	}
}

// gatedApps releases every concurrent AppByID call at once, so all callers
// pass the read-lock fast path (cache still empty) before any of them can
// take the write lock — deterministically forcing the double-checked build
// race inside processorFor.
type gatedApps struct {
	*fakeApps
	barrier chan struct{}
	want    int
	mu      sync.Mutex
	arrived int
}

func (g *gatedApps) AppByID(ctx context.Context, id string) (tenant.AgentApp, tenant.Tenant, error) {
	g.mu.Lock()
	g.arrived++
	release := g.arrived == g.want
	g.mu.Unlock()
	if release {
		close(g.barrier)
	}
	select {
	case <-g.barrier:
	case <-ctx.Done():
		return tenant.AgentApp{}, tenant.Tenant{}, ctx.Err()
	}
	return g.fakeApps.AppByID(ctx, id)
}

// slowSecrets stalls the winning assembly while it holds the assembler write
// lock, giving the other racers time to park on the same lock past their
// read-lock fast path.
type slowSecrets struct {
	fakeSecrets
	delay time.Duration
}

func (f slowSecrets) Resolve(ctx context.Context, ref string) (string, error) {
	time.Sleep(f.delay)
	return f.fakeSecrets.Resolve(ctx, ref)
}

// Concurrent assembly of the same app must stay single-flight: the loser of
// the build race finds the winner's cache entry in the double-check under the
// write lock and reuses it instead of building a second runner.
func TestAssemblerConcurrentAssemblySingleFlight(t *testing.T) {
	for round := 0; round < 3; round++ {
		gated := &gatedApps{fakeApps: testAppData(), barrier: make(chan struct{}), want: 16}
		a := NewAssembler(AssemblerConfig{
			Apps:           gated,
			Registry:       tool.DemoTools(),
			SessionsByType: map[string]session.Service{"redis": sessioninmemory.NewSessionService()},
			DefaultSession: "redis",
			Secrets: slowSecrets{
				fakeSecrets: fakeSecrets{values: map[string]string{"env-key": "sk"}},
				delay:       150 * time.Millisecond,
			},
			Defaults: ModelSpec{Name: "env-model", BaseURL: "https://env.example.com", APIKeyRef: "env-key"},
		})

		const n = 16
		results := make([]Processor, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], errs[i] = a.processorFor(context.Background(), "a1")
			}(i)
		}
		wg.Wait()
		for i := 0; i < n; i++ {
			if errs[i] != nil {
				t.Fatalf("round %d goroutine %d: %v", round, i, errs[i])
			}
			if results[i] != results[0] {
				t.Fatalf("round %d: concurrent assembly produced distinct processors for one app", round)
			}
		}
		if len(a.cache) != 1 {
			t.Fatalf("round %d: exactly one cache entry expected, got %d", round, len(a.cache))
		}
		if _, ok := results[0].(*RunnerProcessor); !ok {
			t.Fatalf("round %d: want a RunnerProcessor, got %T", round, results[0])
		}
		if len(a.live) != 1 {
			t.Fatalf("round %d: single-flight assembly must build one live runner, got %d", round, len(a.live))
		}
		if err := a.Close(); err != nil {
			t.Fatalf("round %d close: %v", round, err)
		}
	}
}

// Close returns the first runner shutdown error and clears the caches.
func TestAssemblerCloseReturnsFirstError(t *testing.T) {
	a := NewAssembler(AssemblerConfig{Registry: tool.DemoTools()})
	closeErr := errors.New("close failed")
	a.live = []*RunnerProcessor{
		newRunnerProcessor(&failingCloseRunner{err: closeErr}, "m", time.Second, 0),
		newRunnerProcessor(&failingCloseRunner{}, "m", time.Second, 0),
	}
	a.cache["a1"] = cacheEntry{proc: EchoProcessor{}}
	if err := a.Close(); err != closeErr {
		t.Fatalf("want first close error, got %v", err)
	}
	if len(a.live) != 0 || len(a.cache) != 0 {
		t.Fatalf("Close must reset live runners and cache: %v %v", a.live, a.cache)
	}
}
