package platform

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

type fencingRunner struct {
	started chan RunnerRequest
	release <-chan struct{}
}

func (r fencingRunner) Run(ctx context.Context, request RunnerRequest) (RunnerResponse, error) {
	return RunnerResponse{}, nil
}

func (r fencingRunner) RunEvents(ctx context.Context, request RunnerRequest) (<-chan RuntimeEvent, error) {
	events := make(chan RuntimeEvent, 1)
	r.started <- request
	go func() {
		defer close(events)
		select {
		case <-ctx.Done():
		case <-r.release:
			events <- RuntimeEvent{Type: "run.completed", Data: map[string]string{"output": "ok"}}
		}
	}()
	return events, nil
}

func (fencingRunner) Close() error { return nil }

func TestPostgresSessionLeaseSerializesGatewaysAndIncrementsFence(t *testing.T) {
	dsn := os.Getenv("TRPC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TRPC_TEST_POSTGRES_DSN is not configured")
	}
	if err := MigratePostgresControlPlane(dsn); err != nil {
		t.Fatal(err)
	}
	managerA, err := NewPostgresSessionLeaseManager(dsn, "gateway-a")
	if err != nil {
		t.Fatal(err)
	}
	managerB, err := NewPostgresSessionLeaseManager(dsn, "gateway-b")
	if err != nil {
		t.Fatal(err)
	}
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	startedA := make(chan RunnerRequest, 1)
	startedB := make(chan RunnerRequest, 1)
	runtimeA := NewRuntime(activeTestPlatform(t), fencingRunner{started: startedA, release: releaseA}, nil)
	runtimeB := NewRuntime(activeTestPlatform(t), fencingRunner{started: startedB, release: releaseB}, nil)
	runtimeA.SetSessionLeaseManager(managerA)
	runtimeB.SetSessionLeaseManager(managerB)
	defer runtimeA.Close()
	defer runtimeB.Close()
	tenant := TenantContext{TenantID: "tenant-one", UserID: "user-one", Role: RoleOperator}
	sessionID := "lease-" + time.Now().UTC().Format("150405.000000000")
	eventsA, err := runtimeA.Stream(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: sessionID, Input: "one", RequestID: "lease-a"})
	if err != nil {
		t.Fatal(err)
	}
	requestA := <-startedA
	if requestA.FencingToken == 0 {
		t.Fatal("first Gateway did not receive a fencing token")
	}
	type streamResult struct {
		events <-chan RuntimeEvent
		err    error
	}
	resultB := make(chan streamResult, 1)
	go func() {
		events, err := runtimeB.Stream(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: sessionID, Input: "two", RequestID: "lease-b"})
		resultB <- streamResult{events: events, err: err}
	}()
	select {
	case request := <-startedB:
		t.Fatalf("second Gateway started before lease release: %#v", request)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseA)
	for range eventsA {
	}
	result := <-resultB
	if result.err != nil {
		t.Fatal(result.err)
	}
	requestB := <-startedB
	if requestB.FencingToken <= requestA.FencingToken {
		t.Fatalf("fencing tokens = %d then %d", requestA.FencingToken, requestB.FencingToken)
	}
	store, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stale := SessionEvent{TenantID: "tenant-one", SessionID: sessionID, IdempotencyKey: "stale", Type: "run.completed", FencingToken: requestA.FencingToken}
	if err := store.AppendSessionEvent(context.Background(), stale); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale fenced write error = %v", err)
	}
	current := stale
	current.IdempotencyKey = "current"
	current.FencingToken = requestB.FencingToken
	if err := store.AppendSessionEvent(context.Background(), current); err != nil {
		t.Fatalf("current fenced write: %v", err)
	}
	close(releaseB)
	for range result.events {
	}
}
