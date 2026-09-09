package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeProcessor struct {
	closes   atomic.Int32
	closeErr error
}

func (*fakeProcessor) Process(context.Context, *Request) (*Response, error) { return &Response{}, nil }
func (p *fakeProcessor) Close() error {
	p.closes.Add(1)
	return p.closeErr
}

func testCacheKey(version string) CacheKey {
	return CacheKey{TenantID: "tenant-a", TenantConfigVersion: 7, AgentApp: "assistant", AgentVersionID: version, DeploymentID: "deployment-" + version}
}

func TestCacheReusesExactImmutableKey(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 2, IdleTTL: time.Hour})
	var builds atomic.Int32
	factory := func(context.Context) (Processor, error) {
		builds.Add(1)
		return &fakeProcessor{}, nil
	}
	first, releaseFirst, err := cache.Acquire(context.Background(), testCacheKey("v1"), factory)
	if err != nil {
		t.Fatal(err)
	}
	second, releaseSecond, err := cache.Acquire(context.Background(), testCacheKey("v1"), factory)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || builds.Load() != 1 {
		t.Fatalf("reuse first=%p second=%p builds=%d", first, second, builds.Load())
	}
	releaseFirst()
	releaseFirst()
	releaseSecond()
	if err := cache.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first.(*fakeProcessor).closes.Load() != 1 {
		t.Fatalf("close count = %d", first.(*fakeProcessor).closes.Load())
	}
}

func TestCacheAcceptsNilContextsAsBackground(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	//lint:ignore SA1012 Preserve the cache's defensive fallback for legacy callers.
	processor, release, err := cache.Acquire(nil, testCacheKey("nil-context"), func(ctx context.Context) (Processor, error) {
		if ctx == nil {
			t.Fatal("factory received nil context")
		}
		return &fakeProcessor{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	release()
	//lint:ignore SA1012 Preserve the cache's defensive fallback for legacy callers.
	if err := cache.Close(nil); err != nil {
		t.Fatal(err)
	}
	if processor.(*fakeProcessor).closes.Load() != 1 {
		t.Fatalf("close count = %d", processor.(*fakeProcessor).closes.Load())
	}
}

func TestCacheConcurrentBuildRunsOnce(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 2, IdleTTL: time.Hour})
	var builds atomic.Int32
	start := make(chan struct{})
	factory := func(context.Context) (Processor, error) {
		builds.Add(1)
		<-start
		return &fakeProcessor{}, nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	releases := make(chan func(), 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, err := cache.Acquire(context.Background(), testCacheKey("v1"), factory)
			if err == nil {
				releases <- release
			}
			errs <- err
		}()
	}
	for builds.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	close(releases)
	for release := range releases {
		release()
	}
	if builds.Load() != 1 {
		t.Fatalf("builds = %d", builds.Load())
	}
	if err := cache.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCacheNeverEvictsInUseEntry(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	processor, release, err := cache.Acquire(context.Background(), testCacheKey("v1"), func(context.Context) (Processor, error) { return &fakeProcessor{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cache.Acquire(context.Background(), testCacheKey("v2"), func(context.Context) (Processor, error) { return &fakeProcessor{}, nil }); !errors.Is(err, ErrCacheSaturated) {
		t.Fatalf("saturation error = %v", err)
	}
	if processor.(*fakeProcessor).closes.Load() != 0 {
		t.Fatal("in-use processor was closed")
	}
	release()
	second, releaseSecond, err := cache.Acquire(context.Background(), testCacheKey("v2"), func(context.Context) (Processor, error) { return &fakeProcessor{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if processor.(*fakeProcessor).closes.Load() != 1 {
		t.Fatal("idle processor was not evicted")
	}
	releaseSecond()
	if err := cache.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if second.(*fakeProcessor).closes.Load() != 1 {
		t.Fatal("remaining processor was not closed")
	}
}

func TestCacheCloseWaitsForActiveReference(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	processor, release, err := cache.Acquire(context.Background(), testCacheKey("v1"), func(context.Context) (Processor, error) { return &fakeProcessor{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cache.Close(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("close returned before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if processor.(*fakeProcessor).closes.Load() != 0 {
		t.Fatal("active processor closed early")
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if processor.(*fakeProcessor).closes.Load() != 1 {
		t.Fatalf("close count = %d", processor.(*fakeProcessor).closes.Load())
	}
	if _, _, err := cache.Acquire(context.Background(), testCacheKey("v2"), func(context.Context) (Processor, error) { return &fakeProcessor{}, nil }); !errors.Is(err, ErrCacheClosed) {
		t.Fatalf("post-close acquire error = %v", err)
	}
}

func TestCacheSweepUsesIdleTTL(t *testing.T) {
	now := time.Unix(1000, 0)
	cache := NewCache(CacheOptions{MaxEntries: 2, IdleTTL: time.Minute, Clock: func() time.Time { return now }})
	processor, release, err := cache.Acquire(context.Background(), testCacheKey("v1"), func(context.Context) (Processor, error) { return &fakeProcessor{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	release()
	now = now.Add(time.Minute)
	if err := cache.Sweep(); err != nil {
		t.Fatal(err)
	}
	if processor.(*fakeProcessor).closes.Load() != 1 {
		t.Fatalf("close count = %d", processor.(*fakeProcessor).closes.Load())
	}
	if err := cache.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCacheEvictionCloseErrorDoesNotStrandReplacement(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	retirementErr := errors.New("https://model-user:secret@example.invalid/v1: retirement failed")
	first, release, err := cache.Acquire(context.Background(), testCacheKey("v1"), func(context.Context) (Processor, error) {
		return &fakeProcessor{closeErr: retirementErr}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	release()
	second, releaseSecond, err := cache.Acquire(context.Background(), testCacheKey("v2"), func(context.Context) (Processor, error) {
		return &fakeProcessor{}, nil
	})
	if err != nil {
		t.Fatalf("replacement build failed after retirement error: %v", err)
	}
	if first == second || first.(*fakeProcessor).closes.Load() != 1 {
		t.Fatal("old processor was not retired before replacement")
	}
	releaseSecond()
	if err := cache.Close(context.Background()); !errors.Is(err, ErrProcessorCloseFailed) {
		t.Fatalf("close error = %v, want ErrProcessorCloseFailed", err)
	} else if strings.Contains(err.Error(), "model-user:secret@example.invalid") {
		t.Fatalf("close error leaked provider details: %v", err)
	}
}

func TestCacheCloseWaitsForActiveProcessorClose(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	started := make(chan struct{})
	releaseClose := make(chan struct{})
	processor, release, err := cache.Acquire(context.Background(), testCacheKey("blocking-close"), func(context.Context) (Processor, error) {
		return &blockingProcessor{started: started, release: releaseClose, closeErr: errors.New("postgres://user:secret@example.invalid/db: close failed")}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if processor == nil {
		t.Fatal("Acquire returned nil processor")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- cache.Close(context.Background()) }()
	select {
	case err := <-closeDone:
		t.Fatalf("Cache.Close returned before active release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	releaseDone := make(chan struct{})
	go func() {
		release()
		close(releaseDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("processor close did not start")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Cache.Close returned while processor close was blocked: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseClose)
	<-releaseDone
	if err := <-closeDone; !errors.Is(err, ErrProcessorCloseFailed) {
		t.Fatalf("Cache.Close error=%v, want ErrProcessorCloseFailed", err)
	} else if strings.Contains(err.Error(), "secret@example.invalid") {
		t.Fatalf("Cache.Close leaked provider details: %v", err)
	}
}

type blockingProcessor struct {
	started  chan struct{}
	release  chan struct{}
	closeErr error
}

func (*blockingProcessor) Process(context.Context, *Request) (*Response, error) {
	return &Response{}, nil
}

func (p *blockingProcessor) Close() error {
	close(p.started)
	<-p.release
	return p.closeErr
}

func TestCacheClosesPartialProcessorAfterFactoryError(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	partial := &fakeProcessor{}
	buildErr := errors.New("build failed")
	if _, _, err := cache.Acquire(context.Background(), testCacheKey("v1"), func(context.Context) (Processor, error) {
		return partial, buildErr
	}); !errors.Is(err, buildErr) {
		t.Fatalf("factory error = %v", err)
	}
	if partial.closes.Load() != 1 {
		t.Fatalf("partial close count = %d", partial.closes.Load())
	}
	processor, release, err := cache.Acquire(context.Background(), testCacheKey("v1"), func(context.Context) (Processor, error) {
		return &fakeProcessor{}, nil
	})
	if err != nil || processor == partial {
		t.Fatalf("retry failed after factory error: processor=%p err=%v", processor, err)
	}
	release()
	if err := cache.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCacheRejectsTypedNilProcessor(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	var typedNil *fakeProcessor
	processor, release, err := cache.Acquire(context.Background(), testCacheKey("typed-nil"), func(context.Context) (Processor, error) {
		return typedNil, nil
	})
	if processor != nil || release != nil || err == nil {
		t.Fatalf("typed-nil processor result=%T release=%v err=%v, want nil processor/release and an error", processor, release != nil, err)
	}
	if closeErr := cache.Close(context.Background()); closeErr != nil {
		t.Fatalf("Close after typed-nil build: %v", closeErr)
	}
}

func TestCacheContainsFactoryPanic(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	processor, release, err := cache.Acquire(context.Background(), testCacheKey("factory-panic"), func(context.Context) (Processor, error) {
		panic("provider credential")
	})
	if processor != nil || release != nil || !errors.Is(err, ErrProcessorBuildPanic) {
		t.Fatalf("factory panic result=%T release=%v err=%v, want stable build panic error", processor, release != nil, err)
	}
	if closeErr := cache.Close(context.Background()); closeErr != nil {
		t.Fatalf("Close after factory panic: %v", closeErr)
	}
}
