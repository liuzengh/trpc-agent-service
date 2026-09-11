package agent

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

type countCloser struct{ calls atomic.Int32 }

func (c *countCloser) Close() error { c.calls.Add(1); return nil }

func durableCacheTenant(version string) config.TenantConfig {
	return config.TenantConfig{TenantID: "cache", Version: version, Data: config.DataConfig{
		Session: config.BackendConfig{Type: "redis"}, Memory: config.BackendConfig{Type: "disabled"}, Artifact: config.BackendConfig{Type: "object"},
	}}
}

func TestManagerEvictsIdleDurableRevisionAndRebuildsSnapshot(t *testing.T) {
	m := NewManager()
	defer m.Close()
	m.cacheLimit = 2
	counts := map[*Runtime]*countCloser{}
	var versions []string
	m.build = func(_ context.Context, c config.TenantConfig) (*Runtime, error) {
		closer := &countCloser{}
		rt := &Runtime{backendClosers: []io.Closer{closer}}
		counts[rt] = closer
		versions = append(versions, c.Version)
		return rt, nil
	}
	a, releaseA, err := m.Acquire(context.Background(), durableCacheTenant("v1"))
	if err != nil {
		t.Fatal(err)
	}
	b, releaseB, err := m.Acquire(context.Background(), durableCacheTenant("v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Acquire(context.Background(), durableCacheTenant("v3")); !errors.Is(err, ErrRuntimeCapacity) {
		t.Fatalf("active runtime was evicted: %v", err)
	}
	releaseA()
	releaseA() // release is idempotent.
	c, releaseC, err := m.Acquire(context.Background(), durableCacheTenant("v3"))
	if err != nil {
		t.Fatal(err)
	}
	if counts[a].calls.Load() != 1 || counts[b].calls.Load() != 0 || c == a {
		t.Fatal("eviction did not close only idle runtime")
	}
	releaseB()
	rebuilt, releaseRebuilt, err := m.Acquire(context.Background(), durableCacheTenant("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt == a || versions[len(versions)-1] != "v1" {
		t.Fatal("historical snapshot was not rebuilt")
	}
	releaseC()
	releaseRebuilt()
	if len(m.handles) != 2 {
		t.Fatal("cache exceeded limit")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	for _, count := range counts {
		if count.calls.Load() != 1 {
			t.Fatal("runtime was not closed exactly once")
		}
	}
}

func TestManagerPinsLocalStateAtCapacity(t *testing.T) {
	for _, component := range []string{"session", "memory", "artifact", "default_artifact"} {
		t.Run(component, func(t *testing.T) {
			m := NewManager()
			defer m.Close()
			m.cacheLimit = 1
			closeCount := &countCloser{}
			m.build = func(context.Context, config.TenantConfig) (*Runtime, error) {
				return &Runtime{backendClosers: []io.Closer{closeCount}}, nil
			}
			tenant := durableCacheTenant("v1")
			switch component {
			case "session":
				tenant.Data.Session.Type = "inmemory"
			case "memory":
				tenant.Data.Memory.Type = "inmemory"
			case "artifact":
				tenant.Data.Artifact.Type = "inmemory"
			case "default_artifact":
				tenant.Data.Artifact.Type = ""
			}
			rt, release, err := m.Acquire(context.Background(), tenant)
			if err != nil {
				t.Fatal(err)
			}
			release()
			if _, _, err := m.Acquire(context.Background(), durableCacheTenant("v2")); !errors.Is(err, ErrRuntimeCapacity) {
				t.Fatalf("local state discarded: %v", err)
			}
			again, releaseAgain, err := m.Acquire(context.Background(), tenant)
			if err != nil {
				t.Fatal(err)
			}
			releaseAgain()
			if rt != again || closeCount.calls.Load() != 0 {
				t.Fatal("pinned data was closed or replaced")
			}
		})
	}
}

func TestManagerBuildReservationCancellationAndShutdown(t *testing.T) {
	m := NewManager()
	defer m.Close()
	m.cacheLimit = 1
	started, finish := make(chan struct{}), make(chan struct{})
	closeCount := &countCloser{}
	var builds atomic.Int32
	m.build = func(context.Context, config.TenantConfig) (*Runtime, error) {
		builds.Add(1)
		close(started)
		<-finish
		return &Runtime{backendClosers: []io.Closer{closeCount}}, nil
	}
	result := make(chan error, 1)
	go func() {
		_, release, err := m.Acquire(context.Background(), durableCacheTenant("v1"))
		if release != nil {
			release()
		}
		result <- err
	}()
	<-started
	if _, _, err := m.Acquire(context.Background(), durableCacheTenant("v2")); !errors.Is(err, ErrRuntimeCapacity) {
		t.Fatalf("build reservation not counted: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := m.Acquire(ctx, durableCacheTenant("v1")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting acquire ignored cancellation: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	close(finish)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("published runtime after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("build did not finish")
	}
	if builds.Load() != 1 || closeCount.calls.Load() != 1 {
		t.Fatal("duplicate build or leaked shutdown resource")
	}
}

func TestManagerConcurrentAcquireSharesOneRuntime(t *testing.T) {
	m := NewManager()
	defer m.Close()
	var builds atomic.Int32
	m.build = func(context.Context, config.TenantConfig) (*Runtime, error) { builds.Add(1); return &Runtime{}, nil }
	const n = 24
	got := make(chan *Runtime, n)
	releaseAll := make(chan struct{})
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			rt, release, err := m.Acquire(context.Background(), durableCacheTenant("v1"))
			if err != nil {
				got <- nil
				done <- struct{}{}
				return
			}
			got <- rt
			<-releaseAll
			release()
			done <- struct{}{}
		}()
	}
	var first *Runtime
	for i := 0; i < n; i++ {
		rt := <-got
		if rt == nil {
			t.Error("acquire failed")
		}
		if i == 0 {
			first = rt
		} else if first != rt {
			t.Error("same revision has different runtimes")
		}
	}
	close(releaseAll)
	for i := 0; i < n; i++ {
		<-done
	}
	if builds.Load() != 1 {
		t.Fatal("revision built more than once")
	}
}
