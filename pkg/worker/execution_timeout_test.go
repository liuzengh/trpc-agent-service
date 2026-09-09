package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type deadlineBlockingRunner struct {
	observed chan error
}

func (r *deadlineBlockingRunner) Run(
	ctx context.Context,
	_ string,
	_ string,
	_ model.Message,
	_ ...agent.RunOption,
) (<-chan *event.Event, error) {
	events := make(chan *event.Event)
	go func() {
		defer close(events)
		<-ctx.Done()
		r.observed <- ctx.Err()
	}()
	return events, nil
}

func (*deadlineBlockingRunner) Close() error { return nil }

// deadlineIgnoringRunner models a faulty third-party Runner that returns a
// stream but neither honors cancellation nor closes that stream. Worker still
// has to give back its HTTP handler, session lease, and tenant slot at the
// configured deadline; stopping the underlying external side effect requires
// a separate process/container boundary.
type deadlineIgnoringRunner struct {
	started chan struct{}
}

func (r *deadlineIgnoringRunner) Run(
	context.Context,
	string,
	string,
	model.Message,
	...agent.RunOption,
) (<-chan *event.Event, error) {
	close(r.started)
	return make(chan *event.Event), nil
}

func (*deadlineIgnoringRunner) Close() error { return nil }

// deadlineBudgetController simulates an admission dependency that honors the
// invocation context but times out before Runner.Run begins.
type deadlineBudgetController struct{}

func (*deadlineBudgetController) CheckBudget(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (*deadlineBudgetController) AcquireSessionSlot(context.Context) (governance.SessionSlot, error) {
	return nil, nil
}

func (*deadlineBudgetController) ReserveTokenBudget(context.Context) (governance.TokenReservation, error) {
	return governance.TokenReservation{}, nil
}

func (*deadlineBudgetController) DispatchTokenBudget(context.Context, governance.TokenReservation) error {
	return nil
}

func (*deadlineBudgetController) SettleTokenBudget(context.Context, governance.TokenReservation, int64) error {
	return nil
}

func (*deadlineBudgetController) ReleaseTokenBudget(context.Context, governance.TokenReservation) error {
	return nil
}

type failingPreflightBudgetController struct {
	err error
}

func (b *failingPreflightBudgetController) CheckBudget(context.Context) error {
	return b.err
}

func (*failingPreflightBudgetController) AcquireSessionSlot(context.Context) (governance.SessionSlot, error) {
	return nil, nil
}

func (*failingPreflightBudgetController) ReserveTokenBudget(context.Context) (governance.TokenReservation, error) {
	return governance.TokenReservation{}, nil
}

func (*failingPreflightBudgetController) DispatchTokenBudget(context.Context, governance.TokenReservation) error {
	return nil
}

func (*failingPreflightBudgetController) SettleTokenBudget(context.Context, governance.TokenReservation, int64) error {
	return nil
}

func (*failingPreflightBudgetController) ReleaseTokenBudget(context.Context, governance.TokenReservation) error {
	return nil
}

type deadlineIgnoringDispatchBudgetController struct{}

func (*deadlineIgnoringDispatchBudgetController) CheckBudget(context.Context) error { return nil }
func (*deadlineIgnoringDispatchBudgetController) AcquireSessionSlot(context.Context) (governance.SessionSlot, error) {
	return nil, nil
}
func (*deadlineIgnoringDispatchBudgetController) ReserveTokenBudget(context.Context) (governance.TokenReservation, error) {
	return governance.TokenReservation{ID: "reservation", Reserved: 100}, nil
}
func (*deadlineIgnoringDispatchBudgetController) DispatchTokenBudget(ctx context.Context, _ governance.TokenReservation) error {
	<-ctx.Done()
	// Deliberately violate the Adapter contract: the Worker must still check
	// the deadline at the Runner side-effect boundary.
	return nil
}
func (*deadlineIgnoringDispatchBudgetController) SettleTokenBudget(context.Context, governance.TokenReservation, int64) error {
	return nil
}
func (*deadlineIgnoringDispatchBudgetController) ReleaseTokenBudget(context.Context, governance.TokenReservation) error {
	return nil
}

type countingRunner struct{ calls int }

func (r *countingRunner) Run(
	context.Context,
	string,
	string,
	model.Message,
	...agent.RunOption,
) (<-chan *event.Event, error) {
	r.calls++
	events := make(chan *event.Event)
	close(events)
	return events, nil
}

func (*countingRunner) Close() error { return nil }

func TestWorkerProcessEnforcesWorkerOwnedExecutionDeadline(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mini.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	blockingRunner := &deadlineBlockingRunner{observed: make(chan error, 1)}
	value := &Worker{
		tenant: &tenant.Tenant{
			ID:         "deadline-tenant",
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		},
		runner:           blockingRunner,
		sessionLocks:     storage.NewSessionLockManager(redisClient),
		appName:          "deadline-tenant/support",
		agentName:        "support",
		executionTimeout: 25 * time.Millisecond,
	}

	started := time.Now()
	_, err = value.Process(context.Background(), &Request{
		TenantID:    "deadline-tenant",
		ChannelType: "telegram",
		UserID:      "alice",
		SessionID:   "session-1",
		Content:     "hello",
	})
	if !errors.Is(err, ErrExecutionTimedOut) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Process error=%v, want execution timeout and context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Process elapsed=%s, deadline cancellation was not observed", elapsed)
	}
	select {
	case observed := <-blockingRunner.observed:
		if !errors.Is(observed, context.DeadlineExceeded) {
			t.Fatalf("Runner context error=%v, want DeadlineExceeded", observed)
		}
	case <-time.After(time.Second):
		t.Fatal("Runner did not observe the Worker execution deadline")
	}
}

func TestNewWorkerRejectsNegativeExecutionTimeout(t *testing.T) {
	_, err := NewWorkerWithOptionsContext(
		context.Background(),
		&tenant.Tenant{ID: "deadline-tenant"},
		nil,
		nil,
		Options{ExecutionTimeout: -time.Second},
	)
	if !errors.Is(err, ErrInvalidExecutionTimeout) {
		t.Fatalf("negative execution timeout error=%v", err)
	}
}

func TestWorkerProcessClassifiesDeadlineBeforeRunner(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mini.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	value := &Worker{
		tenant: &tenant.Tenant{
			ID:         "deadline-tenant",
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		},
		budgetTracker:    &deadlineBudgetController{},
		sessionLocks:     storage.NewSessionLockManager(redisClient),
		appName:          "deadline-tenant/support",
		agentName:        "support",
		executionTimeout: 25 * time.Millisecond,
	}

	_, err = value.Process(context.Background(), &Request{
		TenantID: "deadline-tenant", ChannelType: "telegram", UserID: "alice",
		SessionID: "session-1", Content: "hello",
	})
	if !errors.Is(err, ErrExecutionPreflightTimedOut) || errors.Is(err, ErrExecutionTimedOut) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Process error=%v, want retry-safe preflight timeout and context deadline", err)
	}
}

func TestWorkerProcessMarksTransientPreflightFailureRetryable(t *testing.T) {
	value := &Worker{
		tenant: &tenant.Tenant{
			ID:         "preflight-tenant",
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		},
		budgetTracker:    &failingPreflightBudgetController{err: errors.New("budget backend unavailable")},
		executionTimeout: 5 * time.Second,
		appName:          "preflight-tenant/support",
		agentName:        "support",
	}

	_, err := value.Process(context.Background(), &Request{
		TenantID: "preflight-tenant", ChannelType: "telegram", UserID: "alice",
		SessionID: "session-1", Content: "hello",
	})
	if !errors.Is(err, ErrExecutionPreflight) {
		t.Fatalf("Process error=%v, want ErrExecutionPreflight", err)
	}
	if errors.Is(err, ErrExecutionPreflightPermanent) || errors.Is(err, ErrExecutionTimedOut) {
		t.Fatalf("transient preflight error was classified as permanent/runner failure: %v", err)
	}
}

func TestWorkerProcessMarksDeterministicPreflightRejection(t *testing.T) {
	value := &Worker{
		tenant: &tenant.Tenant{
			ID:         "preflight-policy-tenant",
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
			Governance: tenant.GovernancePolicy{ContentFilters: []tenant.ContentFilter{{
				Name: "blocked", Type: "keyword", Patterns: []string{"secret"}, Action: "block",
			}}},
		},
		appName:   "preflight-policy-tenant/support",
		agentName: "support",
	}

	_, err := value.Process(context.Background(), &Request{
		TenantID: "preflight-policy-tenant", ChannelType: "telegram", UserID: "alice",
		SessionID: "session-1", Content: "secret data",
	})
	if !errors.Is(err, ErrExecutionPreflightPermanent) || !errors.Is(err, ErrExecutionPreflight) {
		t.Fatalf("policy rejection=%v, want both preflight markers", err)
	}
}

func TestWorkerProcessDeadlineBoundsNonCooperativeEventStream(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mini.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	blockingRunner := &deadlineIgnoringRunner{started: make(chan struct{})}
	value := &Worker{
		tenant: &tenant.Tenant{
			ID:         "deadline-tenant",
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		},
		runner:           blockingRunner,
		sessionLocks:     storage.NewSessionLockManager(redisClient),
		appName:          "deadline-tenant/support",
		agentName:        "support",
		executionTimeout: 25 * time.Millisecond,
	}

	result := make(chan error, 1)
	go func() {
		_, err := value.Process(context.Background(), &Request{
			TenantID: "deadline-tenant", ChannelType: "telegram", UserID: "alice",
			SessionID: "session-1", Content: "hello",
		})
		result <- err
	}()
	select {
	case <-blockingRunner.started:
	case <-time.After(time.Second):
		t.Fatal("Runner did not start")
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrExecutionTimedOut) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Process error=%v, want execution timeout and context deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Worker remained blocked on a non-cooperative event stream")
	}

	// Process must release its full-invocation lease before it returns even
	// though the faulty Runner left its producer channel open.
	next, err := value.sessionLocks.AcquireLease(
		context.Background(),
		sessionLeaseKey("deadline-tenant", "deadline-tenant/support", "alice", "session-1"),
		storage.DefaultLockTTL,
	)
	if err != nil {
		t.Fatalf("timed-out invocation retained the session lease: %v", err)
	}
	if err := next.Release(context.Background()); err != nil {
		t.Fatalf("release successor lease: %v", err)
	}
}

func TestWorkerProcessChecksExpiredContextAtRunnerBoundary(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mini.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	runner := &countingRunner{}
	value := &Worker{
		tenant: &tenant.Tenant{
			ID:         "deadline-tenant",
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		},
		runner:           runner,
		budgetTracker:    &deadlineIgnoringDispatchBudgetController{},
		sessionLocks:     storage.NewSessionLockManager(redisClient),
		appName:          "deadline-tenant/support",
		agentName:        "support",
		executionTimeout: 25 * time.Millisecond,
	}

	_, err = value.Process(context.Background(), &Request{
		TenantID: "deadline-tenant", ChannelType: "telegram", UserID: "alice",
		SessionID: "session-1", Content: "hello",
	})
	if !errors.Is(err, ErrExecutionPreflightTimedOut) || errors.Is(err, ErrExecutionTimedOut) {
		t.Fatalf("Process error=%v, want preflight timeout only", err)
	}
	if runner.calls != 0 {
		t.Fatalf("Runner called %d times after preflight deadline", runner.calls)
	}
}

func TestWorkerProcessDoesNotExtendEarlierCallerDeadline(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mini.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	runner := &deadlineBlockingRunner{observed: make(chan error, 1)}
	value := &Worker{
		tenant: &tenant.Tenant{
			ID:         "deadline-tenant",
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		},
		runner:           runner,
		sessionLocks:     storage.NewSessionLockManager(redisClient),
		appName:          "deadline-tenant/support",
		agentName:        "support",
		executionTimeout: 2 * time.Second,
	}
	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = value.Process(parent, &Request{
		TenantID: "deadline-tenant", ChannelType: "telegram", UserID: "alice",
		SessionID: "session-1", Content: "hello",
	})
	if !errors.Is(err, ErrExecutionTimedOut) || time.Since(started) >= time.Second {
		t.Fatalf("Process error=%v elapsed=%s, caller deadline was extended", err, time.Since(started))
	}
	select {
	case observed := <-runner.observed:
		if !errors.Is(observed, context.DeadlineExceeded) {
			t.Fatalf("Runner observed %v", observed)
		}
	case <-time.After(time.Second):
		t.Fatal("Runner did not observe caller deadline")
	}
}
