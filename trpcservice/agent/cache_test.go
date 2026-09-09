package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
)

type fakeRunner struct {
	closeCount atomic.Int32
}

func (r *fakeRunner) Run(context.Context, string, string, model.Message, ...frameworkagent.RunOption) (<-chan *event.Event, error) {
	ch := make(chan *event.Event)
	close(ch)
	return ch, nil
}

func (r *fakeRunner) Close() error {
	r.closeCount.Add(1)
	return nil
}

func testKey(version string) CacheKey {
	return CacheKey{TenantID: "tenant", AgentAppID: "app", ConfigVersion: version}
}

func TestRunnerCacheSingleflight(t *testing.T) {
	var creates atomic.Int32
	cache, err := NewRunnerCache(DefaultCacheConfig(), func(context.Context, CacheKey) (frameworkrunner.Runner, error) {
		creates.Add(1)
		time.Sleep(10 * time.Millisecond)
		return &fakeRunner{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey("v1")
	var wg sync.WaitGroup
	leases := make([]*Lease, 100)
	for i := range leases {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease, acquireErr := cache.Acquire(context.Background(), key)
			if acquireErr != nil {
				t.Errorf("Acquire() error = %v", acquireErr)
				return
			}
			leases[i] = lease
		}(i)
	}
	wg.Wait()
	for _, lease := range leases {
		lease.Release()
	}
	if got := creates.Load(); got != 1 {
		t.Fatalf("factory called %d times, want 1", got)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerCacheVersionDrainAndCloseOnce(t *testing.T) {
	created := make(chan *fakeRunner, 2)
	cache, err := NewRunnerCache(DefaultCacheConfig(), func(context.Context, CacheKey) (frameworkrunner.Runner, error) {
		r := &fakeRunner{}
		created <- r
		return r, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := cache.Acquire(context.Background(), testKey("v1"))
	if err != nil {
		t.Fatal(err)
	}
	oldRunner := (<-created)
	if _, err := cache.Acquire(context.Background(), testKey("v2")); err != nil {
		t.Fatal(err)
	}
	newRunner := (<-created)
	if oldRunner == newRunner {
		t.Fatal("different config versions must use different runners")
	}
	oldLease.Release()
	if err := cache.Drain(context.Background(), testKey("v1")); err != nil {
		t.Fatal(err)
	}
	if got := oldRunner.closeCount.Load(); got != 1 {
		t.Fatalf("old runner closed %d times, want 1", got)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if got := newRunner.closeCount.Load(); got != 1 {
		t.Fatalf("new runner closed %d times, want 1", got)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerCacheRejectsInvalidConfiguration(t *testing.T) {
	_, err := NewRunnerCache(CacheConfig{}, func(context.Context, CacheKey) (frameworkrunner.Runner, error) {
		return nil, errors.New("unused")
	})
	if err == nil {
		t.Fatal("expected invalid configuration error")
	}
}

func TestRunnerCacheSharesFactoryFailure(t *testing.T) {
	factoryErr := errors.New("factory failed")
	var creates atomic.Int32
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	cache, err := NewRunnerCache(DefaultCacheConfig(), func(context.Context, CacheKey) (frameworkrunner.Runner, error) {
		if creates.Add(1) == 1 {
			close(factoryStarted)
		}
		<-releaseFactory
		return nil, factoryErr
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	const callers = 100
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(callers)
	done.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			if _, acquireErr := cache.Acquire(context.Background(), testKey("failure")); !errors.Is(acquireErr, factoryErr) {
				t.Errorf("Acquire() error = %v, want factory error", acquireErr)
			}
		}()
	}
	ready.Wait()
	close(start)
	<-factoryStarted
	time.Sleep(20 * time.Millisecond)
	close(releaseFactory)
	done.Wait()
	if got := creates.Load(); got != 1 {
		t.Fatalf("factory called %d times, want 1", got)
	}
}

func TestRunnerCacheCountsPendingCreationsAgainstCapacity(t *testing.T) {
	config := DefaultCacheConfig()
	config.MaxEntries = 1
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	cache, err := NewRunnerCache(config, func(context.Context, CacheKey) (frameworkrunner.Runner, error) {
		close(factoryStarted)
		<-releaseFactory
		return &fakeRunner{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	leaseResult := make(chan *Lease, 1)
	errResult := make(chan error, 1)
	go func() {
		lease, acquireErr := cache.Acquire(context.Background(), testKey("v1"))
		leaseResult <- lease
		errResult <- acquireErr
	}()
	<-factoryStarted
	if _, err := cache.Acquire(context.Background(), testKey("v2")); !errors.Is(err, ErrCacheFull) {
		t.Fatalf("Acquire() error = %v, want ErrCacheFull", err)
	}
	close(releaseFactory)
	lease := <-leaseResult
	if err := <-errResult; err != nil {
		t.Fatal(err)
	}
	lease.Release()
}

func TestRunnerCacheEvictsLeastRecentlyUsedIdleEntry(t *testing.T) {
	config := DefaultCacheConfig()
	config.MaxEntries = 2
	created := make(map[CacheKey]*fakeRunner)
	cache, err := NewRunnerCache(config, func(_ context.Context, key CacheKey) (frameworkrunner.Runner, error) {
		runner := &fakeRunner{}
		created[key] = runner
		return runner, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	oldest, err := cache.Acquire(context.Background(), testKey("v1"))
	if err != nil {
		t.Fatal(err)
	}
	oldest.Release()
	time.Sleep(time.Millisecond)
	newer, err := cache.Acquire(context.Background(), testKey("v2"))
	if err != nil {
		t.Fatal(err)
	}
	newer.Release()
	third, err := cache.Acquire(context.Background(), testKey("v3"))
	if err != nil {
		t.Fatalf("Acquire(v3) error = %v", err)
	}
	third.Release()
	if got := created[testKey("v1")].closeCount.Load(); got != 1 {
		t.Fatalf("least recently used runner closed %d times, want 1", got)
	}
	if got := created[testKey("v2")].closeCount.Load(); got != 0 {
		t.Fatalf("newer runner closed %d times, want 0", got)
	}
}

func TestRunnerCacheCreationTimeoutClosesLateRunner(t *testing.T) {
	config := DefaultCacheConfig()
	config.CreateTimeout = 10 * time.Millisecond
	runner := &fakeRunner{}
	cache, err := NewRunnerCache(config, func(ctx context.Context, _ CacheKey) (frameworkrunner.Runner, error) {
		<-ctx.Done()
		return runner, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	if _, err := cache.Acquire(context.Background(), testKey("timeout")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire() error = %v, want deadline exceeded", err)
	}
	if got := runner.closeCount.Load(); got != 1 {
		t.Fatalf("late runner closed %d times, want 1", got)
	}
}

func TestRunnerCacheDrainTimeoutClosesActiveRunnerAndReturnsError(t *testing.T) {
	config := DefaultCacheConfig()
	config.DrainTimeout = 20 * time.Millisecond
	runner := &fakeRunner{}
	cache, err := NewRunnerCache(config, func(context.Context, CacheKey) (frameworkrunner.Runner, error) {
		return runner, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(context.Background(), testKey("drain-timeout"))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	drainDone := make(chan error, 1)
	go func() { drainDone <- cache.Drain(context.Background(), testKey("drain-timeout")) }()
	readyDeadline := time.Now().Add(time.Second)
	for !errors.Is(cache.Ready(testKey("drain-timeout")), ErrRunnerDrain) {
		if time.Now().After(readyDeadline) {
			t.Fatal("cache did not report the draining state")
		}
		time.Sleep(time.Millisecond)
	}
	if err := <-drainDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", err)
	}
	if got := runner.closeCount.Load(); got != 1 {
		t.Fatalf("runner closed %d times, want 1", got)
	}
	freshLease, err := cache.Acquire(context.Background(), testKey("drain-timeout"))
	if err != nil {
		t.Fatalf("expected a fresh runner after drain, got %v", err)
	}
	freshLease.Release()
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(cache.Ready(testKey("drain-timeout")), ErrCacheClosed) {
		t.Fatal("closed cache reported ready")
	}
}

func TestRunnerCachesDoNotShareRunnerInstances(t *testing.T) {
	factory := func(context.Context, CacheKey) (frameworkrunner.Runner, error) { return &fakeRunner{}, nil }
	first, err := NewRunnerCache(DefaultCacheConfig(), factory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRunnerCache(DefaultCacheConfig(), factory)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()
	firstLease, err := first.Acquire(context.Background(), testKey("v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer firstLease.Release()
	secondLease, err := second.Acquire(context.Background(), testKey("v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer secondLease.Release()
	if firstLease.Runner == secondLease.Runner {
		t.Fatal("different caches must not share runner instances")
	}
}
