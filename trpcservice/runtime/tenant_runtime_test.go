package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestTenantRuntimeRegistryValidationAndLifecycle(t *testing.T) {
	if _, err := NewTenantRuntimeRegistry(nil); !errors.Is(err, ErrInvalidTenantRuntime) {
		t.Fatalf("nil materializer error = %v", err)
	}
	var nilRegistry *TenantRuntimeRegistry
	if err := nilRegistry.Ensure(context.Background(), "tenant"); !errors.Is(err, ErrInvalidTenantRuntime) {
		t.Fatalf("nil registry Ensure error = %v", err)
	}
	nilRegistry.InvalidateTenant("tenant")
	if err := nilRegistry.Close(); err != nil {
		t.Fatalf("nil registry Close error = %v", err)
	}

	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for name, ctx := range map[string]context.Context{
		"nil context":                  nil,
		"background with blank tenant": context.Background(),
	} {
		t.Run(name, func(t *testing.T) {
			tenantID := "tenant"
			if name == "background with blank tenant" {
				tenantID = " \t"
			}
			if err := registry.Ensure(ctx, tenantID); !errors.Is(err, ErrInvalidTenantRuntime) {
				t.Fatalf("Ensure(%q) error = %v", tenantID, err)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.Ensure(canceled, "tenant"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Ensure error = %v", err)
	}

	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Ensure(context.Background(), "tenant"); !errors.Is(err, ErrTenantRuntimeClosed) {
		t.Fatalf("Ensure after Close error = %v", err)
	}
	registry.InvalidateTenant("tenant")
}

func TestTenantRuntimeRegistryMaterializesOnceAndInvalidates(t *testing.T) {
	const tenantID = "tenant-a"
	var mu sync.Mutex
	calls := 0
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Ensure(context.Background(), tenantID); err != nil {
		t.Fatal(err)
	}
	if err := registry.Ensure(context.Background(), tenantID); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if calls != 1 {
		t.Fatalf("materializer calls before invalidation = %d, want 1", calls)
	}
	mu.Unlock()
	registry.InvalidateTenant(tenantID)
	if err := registry.Ensure(context.Background(), tenantID); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if calls != 2 {
		t.Fatalf("materializer calls after invalidation = %d, want 2", calls)
	}
	mu.Unlock()
	registry.InvalidateTenant("missing")
	registry.InvalidateTenant(" \t")
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRuntimeRegistryWaitsForConcurrentMaterialization(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { results <- registry.Ensure(context.Background(), "tenant") }()
	<-started
	go func() { results <- registry.Ensure(context.Background(), "tenant") }()
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Ensure error = %v", err)
		}
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRuntimeRegistryWaiterCancellationAndFailure(t *testing.T) {
	t.Run("waiter cancellation", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
			close(started)
			<-release
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		ownerResult := make(chan error, 1)
		go func() { ownerResult <- registry.Ensure(context.Background(), "tenant") }()
		<-started

		waiterBase, cancel := context.WithCancel(context.Background())
		waiterContext := &tenantRuntimeTrackingContext{Context: waiterBase, doneCalled: make(chan struct{})}
		waiterResult := make(chan error, 1)
		go func() { waiterResult <- registry.Ensure(waiterContext, "tenant") }()
		<-waiterContext.doneCalled
		cancel()
		if err := <-waiterResult; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter cancellation error = %v", err)
		}
		close(release)
		if err := <-ownerResult; err != nil {
			t.Fatalf("owner materialization error = %v", err)
		}
		if err := registry.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("waiter receives materialization failure", func(t *testing.T) {
		materializeErr := errors.New("materialization failed")
		started := make(chan struct{})
		release := make(chan struct{})
		registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
			close(started)
			<-release
			return materializeErr
		})
		if err != nil {
			t.Fatal(err)
		}
		ownerResult := make(chan error, 1)
		go func() { ownerResult <- registry.Ensure(context.Background(), "tenant") }()
		<-started

		waiterContext := &tenantRuntimeTrackingContext{Context: context.Background(), doneCalled: make(chan struct{})}
		waiterResult := make(chan error, 1)
		go func() { waiterResult <- registry.Ensure(waiterContext, "tenant") }()
		<-waiterContext.doneCalled
		close(release)
		if err := <-ownerResult; !errors.Is(err, materializeErr) {
			t.Fatalf("owner materialization error = %v", err)
		}
		if err := <-waiterResult; !errors.Is(err, materializeErr) {
			t.Fatalf("waiter materialization error = %v", err)
		}
		if err := registry.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTenantRuntimeRegistryRetriesFailuresAndHandlesInvalidation(t *testing.T) {
	materializeErr := errors.New("materialization failed")
	var mu sync.Mutex
	calls := 0
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		mu.Lock()
		calls++
		current := calls
		mu.Unlock()
		if current == 1 {
			return materializeErr
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Ensure(context.Background(), "tenant"); !errors.Is(err, materializeErr) {
		t.Fatalf("first materialization error = %v", err)
	}
	if err := registry.Ensure(context.Background(), "tenant"); err != nil {
		t.Fatalf("retry materialization error = %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var gateOnce sync.Once
	registry, err = NewTenantRuntimeRegistry(func(context.Context, string) error {
		gateOnce.Do(func() { close(started) })
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- registry.Ensure(context.Background(), "invalidated") }()
	<-started
	registry.InvalidateTenant("invalidated")
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("invalidation during materialization error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRuntimeRegistryCloseDuringMaterialization(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	registry, err := NewTenantRuntimeRegistry(func(context.Context, string) error {
		close(started)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- registry.Ensure(context.Background(), "tenant") }()
	<-started
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrTenantRuntimeClosed) {
		t.Fatalf("Ensure during Close error = %v", err)
	}
}

type tenantRuntimeTrackingContext struct {
	context.Context
	doneCalled chan struct{}
	once       sync.Once
}

func (context *tenantRuntimeTrackingContext) Done() <-chan struct{} {
	context.once.Do(func() { close(context.doneCalled) })
	return context.Context.Done()
}
