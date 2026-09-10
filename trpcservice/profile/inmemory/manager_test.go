package inmemory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type fakeBundle struct{}

func (fakeBundle) NewRunner(session.Service) (runner.Runner, error) { return nil, nil }

func TestBundleManagerBuildsOnceAndClosesAfterLastLease(t *testing.T) {
	var builds, closes atomic.Int32
	manager := NewBundleManager(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		builds.Add(1)
		return fakeBundle{}, func(context.Context) error { closes.Add(1); return nil }, nil
	})
	key := profile.ExecutionProfileKey{TenantID: "t", AgentAppID: "a", AgentAppRevision: 1, ContentDigest: "d", ConfigVersion: 1, PolicyVersion: 1}
	var wg sync.WaitGroup
	leases := make([]profile.BundleLease, 20)
	for i := range leases {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease, err := manager.Acquire(context.Background(), key)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			leases[i] = lease
		}(i)
	}
	wg.Wait()
	if builds.Load() != 1 {
		t.Fatalf("builds=%d", builds.Load())
	}
	manager.Retire(key)
	if _, err := manager.Acquire(context.Background(), key); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("retired acquire=%v", err)
	}
	for _, lease := range leases {
		lease.Release()
	}
	if closes.Load() != 1 {
		t.Fatalf("closes=%d", closes.Load())
	}
}

func TestBundleManagerNegativeCachesBuildFailure(t *testing.T) {
	var builds atomic.Int32
	manager := NewBundleManagerWithPolicy(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		builds.Add(1)
		return nil, nil, runtime.ErrBackendUnavailable
	}, BundleManagerPolicy{FailureBackoff: 20 * time.Millisecond, CloseTimeout: time.Second})
	key := profile.ExecutionProfileKey{TenantID: "t", AgentAppID: "a", AgentAppRevision: 1, ContentDigest: "d", ConfigVersion: 1, PolicyVersion: 1}
	for i := 0; i < 2; i++ {
		if _, err := manager.Acquire(context.Background(), key); !errors.Is(err, runtime.ErrBackendUnavailable) {
			t.Fatal(err)
		}
	}
	if builds.Load() != 1 {
		t.Fatalf("builds during backoff=%d", builds.Load())
	}
	time.Sleep(25 * time.Millisecond)
	_, _ = manager.Acquire(context.Background(), key)
	if builds.Load() != 2 {
		t.Fatalf("builds after backoff=%d", builds.Load())
	}
}

func TestBundleManagerCloseWaitsForLeaseAndRejectsAcquire(t *testing.T) {
	var closes atomic.Int32
	manager := NewBundleManager(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		return fakeBundle{}, func(context.Context) error { closes.Add(1); return nil }, nil
	})
	key := profile.ExecutionProfileKey{TenantID: "t", AgentAppID: "a", AgentAppRevision: 1, ContentDigest: "d", ConfigVersion: 1, PolicyVersion: 1}
	lease, err := manager.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- manager.Close(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	if _, err := manager.Acquire(context.Background(), key); !errors.Is(err, runtime.ErrBackendUnavailable) {
		t.Fatalf("acquire while closing=%v", err)
	}
	select {
	case err := <-closed:
		t.Fatalf("close returned before release: %v", err)
	default:
	}
	lease.Release()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if closes.Load() != 1 {
		t.Fatalf("closes=%d", closes.Load())
	}
}

func TestBundleManagerRetireDuringBuildClosesBuiltResource(t *testing.T) {
	started := make(chan struct{})
	continueBuild := make(chan struct{})
	var closes atomic.Int32
	manager := NewBundleManager(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		close(started)
		<-continueBuild
		return fakeBundle{}, func(context.Context) error { closes.Add(1); return nil }, nil
	})
	key := profile.ExecutionProfileKey{TenantID: "t", AgentAppID: "a", AgentAppRevision: 1, ContentDigest: "d", ConfigVersion: 1, PolicyVersion: 1}
	result := make(chan error, 1)
	go func() { _, err := manager.Acquire(context.Background(), key); result <- err }()
	<-started
	manager.Retire(key)
	close(continueBuild)
	if err := <-result; !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("build result=%v", err)
	}
	if _, err := manager.Acquire(context.Background(), key); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("retired reacquire=%v", err)
	}
	if closes.Load() != 1 {
		t.Fatalf("closes=%d", closes.Load())
	}
}

func TestBundleManagerCloseHonorsDeadlineWithActiveLease(t *testing.T) {
	var closes atomic.Int32
	manager := NewBundleManager(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		return fakeBundle{}, func(context.Context) error { closes.Add(1); return nil }, nil
	})
	key := profile.ExecutionProfileKey{TenantID: "t", AgentAppID: "a", AgentAppRevision: 1, ContentDigest: "d", ConfigVersion: 1, PolicyVersion: 1}
	lease, err := manager.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := manager.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close deadline=%v", err)
	}
	lease.Release()
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closes.Load() != 1 {
		t.Fatalf("closes=%d", closes.Load())
	}
}

func TestBundleManagerAcquireHonorsCallerDeadlineForNonCooperativeFactory(t *testing.T) {
	finishBuild := make(chan struct{})
	var closes atomic.Int32
	manager := NewBundleManagerWithPolicy(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		<-finishBuild
		return fakeBundle{}, func(context.Context) error { closes.Add(1); return nil }, nil
	}, BundleManagerPolicy{FailureBackoff: time.Millisecond, CloseTimeout: 20 * time.Millisecond})
	key := profile.ExecutionProfileKey{TenantID: "t", AgentAppID: "a", AgentAppRevision: 1, ContentDigest: "d", ConfigVersion: 1, PolicyVersion: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := manager.Acquire(ctx, key); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire deadline=%v", err)
	}
	close(finishBuild)
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for closes.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if closes.Load() != 1 {
		t.Fatalf("late-built resource closes=%d", closes.Load())
	}
}
