package execution

// This file covers the deferred-assembly contract added with
// execution.WithDeferredKnowledge / Memory / Artifacts: the worker role wires
// builders instead of stores, so the claim loop starts without touching MinIO
// or Qdrant, and a store outage fails only the assemblies that pin it —
// instead of either taking the loop down at boot or leaving the capability
// permanently unavailable (the two states the wiring used to be stuck
// between).

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/minio"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workspace"
)

// TestDeferredServiceCachesSuccessAndRetriesFailure pins the contract the
// worker role relies on: a failed build is not remembered (the next assembly
// retries, so a brief MinIO or Qdrant outage heals itself with no restart),
// and a successful build runs the builder exactly once.
func TestDeferredServiceCachesSuccessAndRetriesFailure(t *testing.T) {
	calls := 0
	d := newDeferredService(func(context.Context) (int, error) {
		calls++
		if calls == 1 {
			return 0, errors.New("backend down")
		}
		return 42, nil
	})
	if _, err := d.get(context.Background()); err == nil {
		t.Fatal("first get: the builder's failure must surface")
	}
	v, err := d.get(context.Background())
	if err != nil || v != 42 {
		t.Fatalf("second get = (%d, %v), want (42, nil): a failed build must not be cached", v, err)
	}
	if _, err := d.get(context.Background()); err != nil {
		t.Fatalf("third get: %v", err)
	}
	if calls != 2 {
		t.Fatalf("builder calls = %d, want 2 (one failure, one success, then cached)", calls)
	}
}

// TestDeferredServiceConcurrentGetBuildsOnce is the -race companion: racing
// claims must not build two stacks.
func TestDeferredServiceConcurrentGetBuildsOnce(t *testing.T) {
	var calls atomic.Int32
	d := newDeferredService(func(context.Context) (string, error) {
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return "ready", nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v, err := d.get(context.Background()); err != nil || v != "ready" {
				t.Errorf("get = (%q, %v), want (ready, nil)", v, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("builder calls = %d, want 1 under concurrent first assemblies", calls.Load())
	}
}

// deferredHarness is the minimal reliable-mode stripe the assembly tests
// need: one tenant, app, channel binding and an upstream that answers without
// calling any tool.
type deferredHarness struct {
	svc       *Service
	worker    *Worker
	cdp       *controlplane.DB
	scope     controlplane.Scope
	resolver  *secrets.Resolver
	tenantID  string
	appID     int64
	modelID   int64
	backendID int64
	bindingID int64
}

func setupDeferredTest(t *testing.T) *deferredHarness {
	t.Helper()
	dsn := os.Getenv("WORKER_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("WORKER_MYSQL_TEST_DSN not set; skipping real-mysql deferred assembly test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetWorkerSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	upstream := &fakeUpstream{}
	srv := httptest.NewServer(http.HandlerFunc(upstream.handler))
	t.Cleanup(srv.Close)

	cdp := controlplane.NewDB(db)
	ctx := context.Background()
	const tenantID = "acme"
	if err := cdp.CreateTenant(ctx, tenantID, "Acme"); err != nil {
		t.Fatal(err)
	}
	scope := cdp.MustScope(tenantID)

	t.Setenv("WORKER_TEST_API_KEY", "not-a-real-key")
	modelID, err := scope.CreateModelProfile(ctx, "default-model", "fake-model", srv.URL, "env:WORKER_TEST_API_KEY")
	if err != nil {
		t.Fatal(err)
	}
	backendID, err := scope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "default-backend", SessionBackend: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	appID, err := scope.CreateApp(ctx, "assistant", "Assistant")
	if err != nil {
		t.Fatal(err)
	}
	bindingID, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "webchat", PublicID: "main", CredentialRef: "env:X",
	})
	if err != nil {
		t.Fatal(err)
	}
	return &deferredHarness{
		svc: NewService(cdp, DefaultLeaseTTL), cdp: cdp, scope: scope,
		resolver: secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: []string{"WORKER_TEST_API_KEY"}}),
		tenantID: tenantID, appID: appID, modelID: modelID, backendID: backendID, bindingID: bindingID,
	}
}

// bindGoTool registers one platform ("go" kind) binding — the shape a
// revision pins for artifact_save / knowledge_search / memory_*.
func (h *deferredHarness) bindGoTool(t *testing.T, name string) {
	t.Helper()
	if _, err := h.scope.BindTool(context.Background(), controlplane.ToolBinding{
		AppID: h.appID, Name: name, Kind: "go",
		RiskLevel: "low", SideEffect: "write", Idempotent: true,
		Spec:        json.RawMessage(`{}`),
		InputSchema: json.RawMessage(`{"type":"object"}`),
		TimeoutMS:   15000,
	}); err != nil {
		t.Fatalf("bind %s: %v", name, err)
	}
}

func (h *deferredHarness) publishPinning(t *testing.T, name string) {
	t.Helper()
	pinned, _ := json.Marshal(map[string]any{
		"pinned": []map[string]any{{"name": name, "version": 1}},
	})
	if _, err := h.scope.PublishRevision(context.Background(), "assistant", controlplane.RevisionSpec{
		Instruction: "be terse", ModelProfileID: h.modelID, BackendProfileID: h.backendID,
		MaxLLMCalls: 8, MessageTimeoutMS: 30000,
		Tools: json.RawMessage(pinned),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func (h *deferredHarness) useFactory(factory RunnerFactory) {
	h.worker = NewWorker(h.svc, WorkerOptions{WorkerID: "worker-deferred-1", IdleWait: 10 * time.Millisecond}, factory)
}

// runAccepted accepts one message, claims it and drives one assembly. It
// returns RunOne's error so failure cases can assert on it.
func (h *deferredHarness) runAccepted(t *testing.T, msgID string) error {
	t.Helper()
	rev, err := h.scope.CurrentRevision(context.Background(), "assistant")
	if err != nil {
		t.Fatalf("current revision: %v", err)
	}
	if _, err := inbox.NewService(h.cdp).Accept(context.Background(), h.tenantID, inbox.Request{
		AppID: h.appID, ChannelType: "webchat", BindingID: h.bindingID,
		ActorKey: "user-1", RevisionID: rev.ID,
		ModelProfileVersion: 1, BackendProfileVersion: 1,
		PlatformMessageID: msgID, Text: "ping",
	}); err != nil {
		t.Fatalf("accept %s: %v", msgID, err)
	}
	claim, err := h.svc.ClaimNext(context.Background(), h.tenantID, "worker-deferred-1")
	if errors.Is(err, ErrNothingToClaim) {
		t.Fatal("nothing claimable after a successful accept")
	}
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return h.worker.RunOne(context.Background(), claim)
}

func (h *deferredHarness) committedExecutions(t *testing.T) int {
	t.Helper()
	var n int
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM executions WHERE tenant_id = ? AND status = 'committed'", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestDeferredAssemblyServesPinnedArtifactTool is the case the worker role
// exists for: a revision pins artifact_save, the deployment configures no
// eager store, and the deferred builder still assembles the runner — the
// claim commits and the builder ran exactly once.
func TestDeferredAssemblyServesPinnedArtifactTool(t *testing.T) {
	h := setupDeferredTest(t)
	h.bindGoTool(t, "artifact_save")
	h.publishPinning(t, "artifact_save")

	objects, err := minio.New("http://127.0.0.1:1", "test-key", "test-secret", "", "artifacts")
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	var calls atomic.Int32
	h.useFactory(DefaultRunnerFactory(h.cdp, h.resolver,
		WithDeferredArtifacts(func(context.Context) (*artifact.Service, error) {
			calls.Add(1)
			return artifact.New(h.cdp, objects)
		})))

	if err := h.runAccepted(t, "msg-deferred-1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("deferred builder calls = %d, want 1", got)
	}
	if n := h.committedExecutions(t); n != 1 {
		t.Fatalf("committed executions = %d, want 1", n)
	}
}

// TestDeferredAssemblyFailureFailsOnlyThatClaim: a builder error must
// surface as an assembly failure (the message stays at its queue head), not
// as a silently missing tool and not as a committed run.
func TestDeferredAssemblyFailureFailsOnlyThatClaim(t *testing.T) {
	h := setupDeferredTest(t)
	h.bindGoTool(t, "artifact_save")
	h.publishPinning(t, "artifact_save")

	var calls atomic.Int32
	h.useFactory(DefaultRunnerFactory(h.cdp, h.resolver,
		WithDeferredArtifacts(func(context.Context) (*artifact.Service, error) {
			calls.Add(1)
			return nil, errors.New("artifact backend unreachable")
		})))

	err := h.runAccepted(t, "msg-deferred-2")
	if err == nil || !strings.Contains(err.Error(), "build runner") {
		t.Fatalf("run error = %v, want a build-runner failure", err)
	}
	if !strings.Contains(err.Error(), "artifact backend unreachable") {
		t.Fatalf("run error = %v, want the builder's own cause preserved", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("deferred builder calls = %d, want 1 for one assembly", got)
	}
	if n := h.committedExecutions(t); n != 0 {
		t.Fatalf("committed executions = %d, want 0: a failed assembly must not commit", n)
	}
}

// TestPinnedArtifactToolWithoutWiringFailsLoudly keeps the pre-existing
// fail-loudly contract: a deployment that pinned the tool but wired neither
// a store nor a builder still refuses the assembly with a named reason.
func TestPinnedArtifactToolWithoutWiringFailsLoudly(t *testing.T) {
	h := setupDeferredTest(t)
	h.bindGoTool(t, "artifact_save")
	h.publishPinning(t, "artifact_save")
	h.useFactory(DefaultRunnerFactory(h.cdp, h.resolver))

	err := h.runAccepted(t, "msg-deferred-3")
	if err == nil || !strings.Contains(err.Error(), "artifact store disabled") {
		t.Fatalf("run error = %v, want the named disabled-deployment refusal", err)
	}
	if n := h.committedExecutions(t); n != 0 {
		t.Fatalf("committed executions = %d, want 0", n)
	}
}

// writeTenantSkill lays out one SKILL.md in a tenant library; the layout is
// the skill package's contract (<root>/<tenant>/<name>/SKILL.md).
func writeTenantSkill(t *testing.T, root, tenant, name, description string) {
	t.Helper()
	dir := filepath.Join(root, tenant, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: " + name + "\ndescription: " + description + "\n---\n\nbody\n"
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestPinnedSkillMountsTenantLibrary: a revision that pins skill mounts its
// tenant's library at assembly time. The pin needs a binding row (the
// governance record) but the skill tools come from the framework, so nothing
// has to be supplied through extras.
func TestPinnedSkillMountsTenantLibrary(t *testing.T) {
	h := setupDeferredTest(t)
	h.bindGoTool(t, "skill")
	h.publishPinning(t, "skill")

	root := t.TempDir()
	writeTenantSkill(t, root, h.tenantID, "alpha", "a tenant skill")
	h.useFactory(DefaultRunnerFactory(h.cdp, h.resolver, WithSkillStore(root)))

	if err := h.runAccepted(t, "msg-skill-1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := h.committedExecutions(t); n != 1 {
		t.Fatalf("committed executions = %d, want 1", n)
	}
}

// TestPinnedSkillWithoutRootFailsLoudly: a pin in a deployment with no
// skills root is a configuration error and must read as one.
func TestPinnedSkillWithoutRootFailsLoudly(t *testing.T) {
	h := setupDeferredTest(t)
	h.bindGoTool(t, "skill")
	h.publishPinning(t, "skill")
	h.useFactory(DefaultRunnerFactory(h.cdp, h.resolver))

	err := h.runAccepted(t, "msg-skill-2")
	if err == nil || !strings.Contains(err.Error(), "no skills root") {
		t.Fatalf("run error = %v, want the named missing-root refusal", err)
	}
}

// TestPinnedSkillWithoutTenantLibraryFailsLoudly: a configured root that has
// no directory for this tenant is also a fixed error — the pin asked for a
// capability the tenant does not have.
func TestPinnedSkillWithoutTenantLibraryFailsLoudly(t *testing.T) {
	h := setupDeferredTest(t)
	h.bindGoTool(t, "skill")
	h.publishPinning(t, "skill")
	h.useFactory(DefaultRunnerFactory(h.cdp, h.resolver, WithSkillStore(t.TempDir())))

	err := h.runAccepted(t, "msg-skill-3")
	if err == nil || !strings.Contains(err.Error(), "has no skill library") {
		t.Fatalf("run error = %v, want the named missing-library refusal", err)
	}
}

// TestPinnedCodeExecMountsTheSessionWorkspace: a revision that pins code_exec
// assembles an executor bound to this claim's session directory — and the
// directory exists after the run, because assembly created it.
func TestPinnedCodeExecMountsTheSessionWorkspace(t *testing.T) {
	h := setupDeferredTest(t)
	h.bindGoTool(t, "code_exec")
	h.publishPinning(t, "code_exec")

	root := t.TempDir()
	m, err := workspace.NewManager(config.WorkspaceConfig{Root: root, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h.useFactory(DefaultRunnerFactory(h.cdp, h.resolver, WithWorkspaceManager(m)))

	if err := h.runAccepted(t, "msg-code-1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	var sessionPK int64
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT session_pk FROM sessions WHERE tenant_id = ?", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&sessionPK); err != nil {
		t.Fatal(err)
	}
	dir, err := m.Dir(h.tenantID, sessionPK)
	if err != nil {
		t.Fatalf("the session directory must exist after assembly: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("stat %s: %v", dir, err)
	}
}

// TestPinnedCodeExecWithoutWorkspaceFailsLoudly: the pin without the manager
// is a configuration error and must read as one.
func TestPinnedCodeExecWithoutWorkspaceFailsLoudly(t *testing.T) {
	h := setupDeferredTest(t)
	h.bindGoTool(t, "code_exec")
	h.publishPinning(t, "code_exec")
	h.useFactory(DefaultRunnerFactory(h.cdp, h.resolver))

	err := h.runAccepted(t, "msg-code-2")
	if err == nil || !strings.Contains(err.Error(), "no workspace configured") {
		t.Fatalf("run error = %v, want the named missing-workspace refusal", err)
	}
}
