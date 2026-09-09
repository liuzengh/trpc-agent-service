package platform

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/lifecycle"
)

func activeTestPlatform(t *testing.T) *SnapshotControlPlane {
	t.Helper()
	store := NewInMemoryControlPlane()
	if err := store.seedTenant(context.Background(), TenantAssignment{TenantID: "tenant-one", TenantName: "One", Role: RoleOperator}); err != nil {
		t.Fatal(err)
	}
	if created, err := store.createApp(context.Background(), AgentApp{ID: "app-one", TenantID: "tenant-one", Name: "App One"}); err != nil || !created {
		t.Fatal("create app")
	}
	deployment := Deployment{ID: "deploy-one", TenantID: "tenant-one", AgentAppID: "app-one", Status: DeploymentDraft}
	if created, err := store.createDeployment(context.Background(), deployment); err != nil || !created {
		t.Fatal("create deployment")
	}
	version, _, _, err := store.createVersion(context.Background(), deployment, "runtime-version", map[string]any{"model": "fake"})
	if err != nil {
		t.Fatal(err)
	}
	published, _, ok, err := store.transition(context.Background(), deployment, DeploymentPublished, version.ID)
	if err != nil || !ok {
		t.Fatal("publish")
	}
	if _, _, ok, err := store.transition(context.Background(), published, DeploymentActive, ""); err != nil || !ok {
		t.Fatal("activate")
	}
	return store
}

type blockingRunner struct {
	mu      sync.Mutex
	entered chan string
	release chan struct{}
	active  int
	max     int
}

type capturingRunner struct {
	request chan RunnerRequest
	err     error
}

type losingLeaseManager struct {
	lost chan struct{}
}

func (m *losingLeaseManager) Acquire(context.Context, string, string) (SessionExecutionLease, error) {
	return SessionExecutionLease{FencingToken: 42, Lost: m.lost}, nil
}

func (*losingLeaseManager) Close() error { return nil }

type quietStreamingRunner struct {
	started chan struct{}
}

func (r quietStreamingRunner) Run(context.Context, RunnerRequest) (RunnerResponse, error) {
	return RunnerResponse{}, nil
}

func (r quietStreamingRunner) RunEvents(ctx context.Context, _ RunnerRequest) (<-chan RuntimeEvent, error) {
	events := make(chan RuntimeEvent)
	close(r.started)
	go func() {
		defer close(events)
		<-ctx.Done()
	}()
	return events, nil
}

func (quietStreamingRunner) Close() error { return nil }

type notifyingLifecycle struct {
	*lifecycle.Service
	acquired chan struct{}
}

func (life *notifyingLifecycle) Acquire() (func(), bool) {
	release, ok := life.Service.Acquire()
	if ok {
		life.acquired <- struct{}{}
	}
	return release, ok
}

func (r capturingRunner) Run(_ context.Context, request RunnerRequest) (RunnerResponse, error) {
	if r.request != nil {
		r.request <- request
	}
	if r.err != nil {
		return RunnerResponse{}, r.err
	}
	return RunnerResponse{Output: "captured"}, nil
}

func TestGatewayResolvesVersionBeforeStatelessWorkerInvokesRunner(t *testing.T) {
	requests := make(chan RunnerRequest, 1)
	runtime := NewRuntime(activeTestPlatform(t), capturingRunner{request: requests}, nil)
	_, err := runtime.Handle(context.Background(), TenantContext{TenantID: "tenant-one", Role: RoleOperator}, GatewayRequest{AppID: "app-one", SessionID: "session", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if request.DeploymentID != "deploy-one" || request.VersionID != "deploy-one-v1" {
		t.Fatalf("resolved request = %#v", request)
	}
}

func (r *blockingRunner) Run(ctx context.Context, request RunnerRequest) (RunnerResponse, error) {
	r.mu.Lock()
	r.active++
	if r.active > r.max {
		r.max = r.active
	}
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.active--; r.mu.Unlock() }()
	select {
	case r.entered <- request.SessionID:
	case <-ctx.Done():
		return RunnerResponse{}, ctx.Err()
	}
	select {
	case <-r.release:
		return RunnerResponse{Output: "fake:" + request.Input}, nil
	case <-ctx.Done():
		return RunnerResponse{}, ctx.Err()
	}
}

func TestRuntimeSerializesSameSessionAndRunsDifferentSessionsConcurrently(t *testing.T) {
	runner := &blockingRunner{entered: make(chan string, 4), release: make(chan struct{}, 4)}
	runtime := NewRuntime(activeTestPlatform(t), runner, nil)
	tenant := TenantContext{TenantID: "tenant-one", Role: RoleOperator}
	run := func(session string, done chan<- error) {
		_, err := runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: session, Input: session})
		done <- err
	}
	done := make(chan error, 3)
	go run("same", done)
	if got := <-runner.entered; got != "same" {
		t.Fatalf("first = %s", got)
	}
	go run("same", done)
	go run("other", done)
	if got := <-runner.entered; got != "other" {
		t.Fatalf("parallel = %s", got)
	}
	select {
	case got := <-runner.entered:
		t.Fatalf("same Session entered early: %s", got)
	case <-time.After(20 * time.Millisecond):
	}
	runner.release <- struct{}{}
	if got := <-runner.entered; got != "same" {
		t.Fatalf("queued = %s", got)
	}
	runner.release <- struct{}{}
	runner.release <- struct{}{}
	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	runner.mu.Lock()
	max := runner.max
	runner.mu.Unlock()
	if max != 2 {
		t.Fatalf("max concurrency = %d, want 2", max)
	}
}

func TestRuntimeStreamEmitsCancelledWhenLeaseIsLostWithoutWorkerEvent(t *testing.T) {
	started := make(chan struct{})
	lost := make(chan struct{})
	runtime := NewRuntime(activeTestPlatform(t), quietStreamingRunner{started: started}, nil)
	runtime.SetSessionLeaseManager(&losingLeaseManager{lost: lost})

	events, err := runtime.Stream(context.Background(), TenantContext{TenantID: "tenant-one", Role: RoleOperator}, GatewayRequest{
		AppID: "app-one", SessionID: "session", Input: "hello", RequestID: "request-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	leaseEvent := <-events
	if leaseEvent.Type != "session.lease.acquired" || leaseEvent.Data["fencing_token"] != "42" {
		t.Fatalf("lease event = %#v", leaseEvent)
	}
	close(lost)
	select {
	case event, ok := <-events:
		if !ok || event.Type != "run.cancelled" || event.Data["fencing_token"] != "42" {
			t.Fatalf("lease-loss event = %#v, open = %v", event, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("lease loss did not emit a terminal event")
	}
	if _, ok := <-events; ok {
		t.Fatal("stream emitted more than one terminal event")
	}
}

func TestRuntimeDoesNotShareSessionGateAcrossTenants(t *testing.T) {
	store := activeTestPlatform(t)
	if err := store.seedTenant(context.Background(), TenantAssignment{TenantID: "tenant-two", TenantName: "Two", Role: RoleOperator}); err != nil {
		t.Fatal(err)
	}
	if created, err := store.createApp(context.Background(), AgentApp{ID: "app-one", TenantID: "tenant-two", Name: "App One"}); err != nil || !created {
		t.Fatal("create second app")
	}
	deployment := Deployment{ID: "deploy-two", TenantID: "tenant-two", AgentAppID: "app-one", Status: DeploymentDraft}
	if created, err := store.createDeployment(context.Background(), deployment); err != nil || !created {
		t.Fatal("create second deployment")
	}
	version, _, _, err := store.createVersion(context.Background(), deployment, "runtime-version", map[string]any{"model": "fake"})
	if err != nil {
		t.Fatal(err)
	}
	published, _, ok, err := store.transition(context.Background(), deployment, DeploymentPublished, version.ID)
	if err != nil || !ok {
		t.Fatal("publish second")
	}
	if _, _, ok, err := store.transition(context.Background(), published, DeploymentActive, ""); err != nil || !ok {
		t.Fatal("activate second")
	}

	runner := &blockingRunner{entered: make(chan string, 2), release: make(chan struct{}, 2)}
	runtime := NewRuntime(store, runner, nil)
	done := make(chan error, 2)
	for _, tenantID := range []string{"tenant-one", "tenant-two"} {
		go func(id string) {
			_, err := runtime.Handle(context.Background(), TenantContext{TenantID: id, Role: RoleOperator}, GatewayRequest{AppID: "app-one", SessionID: "shared-name", Input: id})
			done <- err
		}(tenantID)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-runner.entered:
		case <-time.After(time.Second):
			t.Fatal("different Tenant was serialized")
		}
	}
	runner.release <- struct{}{}
	runner.release <- struct{}{}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestRuntimeCancelledWaiterNeverExecutes(t *testing.T) {
	runner := &blockingRunner{entered: make(chan string, 3), release: make(chan struct{}, 2)}
	runtime := NewRuntime(activeTestPlatform(t), runner, nil)
	tenant := TenantContext{TenantID: "tenant-one", Role: RoleOperator}
	done := make(chan error, 2)
	go func() {
		_, err := runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "same", Input: "first"})
		done <- err
	}()
	<-runner.entered
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := runtime.Handle(ctx, tenant, GatewayRequest{AppID: "app-one", SessionID: "same", Input: "second"})
		done <- err
	}()
	cancel()
	if err := <-done; err == nil || err.Error() != "request_cancelled" {
		t.Fatalf("cancelled waiter = %v", err)
	}
	runner.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-runner.entered:
		t.Fatalf("cancelled waiter executed: %s", got)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRuntimeShutdownCancelsActiveWorkAndReportsClosing(t *testing.T) {
	runner := &blockingRunner{entered: make(chan string, 1), release: make(chan struct{})}
	life := lifecycle.New()
	runtime := NewRuntime(activeTestPlatform(t), runner, life)
	tenant := TenantContext{TenantID: "tenant-one", Role: RoleOperator}
	done := make(chan error, 1)
	go func() {
		_, err := runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "session", Input: "secret input"})
		done <- err
	}()
	<-runner.entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := life.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("active result = %v", err)
	}
	status := runtime.Status()
	if status[0].Lifecycle != "closing" || status[0].Available || status[1].Lifecycle != "closing" {
		t.Fatalf("status = %#v", status)
	}
	if _, err := runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "new", Input: "x"}); err == nil || err.Error() != "service_closing" {
		t.Fatalf("new work = %v", err)
	}
}

func TestRuntimeShutdownCancelsQueuedSessionWorkBeforeRunner(t *testing.T) {
	runner := &blockingRunner{entered: make(chan string, 2), release: make(chan struct{})}
	life := &notifyingLifecycle{Service: lifecycle.New(), acquired: make(chan struct{}, 2)}
	runtime := NewRuntime(activeTestPlatform(t), runner, life)
	tenant := TenantContext{TenantID: "tenant-one", Role: RoleOperator}
	done := make(chan error, 2)
	go func() {
		_, err := runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "same", Input: "work"})
		done <- err
	}()
	<-life.acquired
	<-runner.entered
	go func() {
		_, err := runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "same", Input: "work"})
		done <- err
	}()
	<-life.acquired
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- life.Shutdown(ctx)
	}()
	for i := 0; i < 2; i++ {
		if err := <-done; err == nil || err.Error() != "request_cancelled" {
			t.Fatalf("work result = %v", err)
		}
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-runner.entered:
		t.Fatal("queued work reached Runner during shutdown")
	default:
	}
}

func TestRuntimeStatusReportsWorkerErrorAndTenantScopedCounters(t *testing.T) {
	runtime := NewRuntime(activeTestPlatform(t), capturingRunner{err: errors.New("runner down")}, nil)
	tenant := TenantContext{TenantID: "tenant-one", Role: RoleOperator}
	_, _ = runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "session", Input: "x"})
	status := runtime.StatusFor(tenant)
	if status[1].Lifecycle != LifecycleError || status[1].Failed != 1 {
		t.Fatalf("tenant status = %#v", status)
	}
	other := runtime.StatusFor(TenantContext{TenantID: "tenant-two", Role: RoleOperator})
	if other[1].Failed != 0 {
		t.Fatalf("cross-tenant counters leaked: %#v", other)
	}
}

func TestUnavailableWorkerReturnsStableError(t *testing.T) {
	runtime := NewRuntime(activeTestPlatform(t), EchoRunner{}, nil)
	runtime.SetWorkerAvailable(false)
	_, err := runtime.Handle(context.Background(), TenantContext{TenantID: "tenant-one", Role: RoleOperator}, GatewayRequest{AppID: "app-one", SessionID: "session", Input: "x"})
	if err == nil || err.Error() != "worker_unavailable" {
		t.Fatalf("error = %v", err)
	}
	if got := runtime.Status()[1].Lifecycle; got != "unavailable" {
		t.Fatalf("worker status = %q", got)
	}
}

func TestTimedOutWorkerIsRetired(t *testing.T) {
	runtime := NewRuntime(activeTestPlatform(t), capturingRunner{err: context.DeadlineExceeded}, nil)
	tenant := TenantContext{TenantID: "tenant-one", Role: RoleOperator}
	_, _ = runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "session", Input: "x"})
	if got := runtime.Status()[1].Lifecycle; got != LifecycleUnavailable {
		t.Fatalf("worker lifecycle = %q", got)
	}
	_, err := runtime.Handle(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "next", Input: "x"})
	if err == nil || err.Error() != "worker_unavailable" {
		t.Fatalf("retired worker accepted work: %v", err)
	}
}
