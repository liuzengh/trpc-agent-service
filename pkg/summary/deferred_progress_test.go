package summary

import (
	"context"
	"errors"
	"testing"
	"time"
)

type targetResolverFunc func(context.Context, Job) (int64, error)

func (f targetResolverFunc) ResolveTarget(ctx context.Context, job Job) (int64, error) {
	return f(ctx, job)
}

func TestProcessorDeferredRequestAfterCompletionResolvesAgain(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(nil)
	sink := NewMemorySinkWithLeaseValidator(nil, store.ValidateLease)
	key := summaryKey()
	if _, err := store.Enqueue(ctx, summaryRequest(key, 998)); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor(store, sink, generatorFunc(func(_ context.Context, job Job) (Candidate, error) {
		return candidateFor(job.Key, job.TargetEventSequence, "summary"), nil
	}), "worker-a", time.Minute)
	if _, err := processor.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, summaryRequest(key, 0)); err != nil {
		t.Fatal(err)
	}
	resolutions := 0
	processor.TargetResolver = targetResolverFunc(func(context.Context, Job) (int64, error) {
		resolutions++
		return 1200, nil
	})
	job, err := processor.RunOnce(ctx)
	if err != nil || job.Status != StatusCompleted || job.CompletedEventSequence != 1200 || resolutions != 1 {
		t.Fatalf("new deferred request did not advance old checkpoint: job=%#v resolutions=%d err=%v", job, resolutions, err)
	}
	if _, err := processor.RunOnce(ctx); !errors.Is(err, ErrNoWork) {
		t.Fatalf("resolved request did not drain: %v", err)
	}
}

func TestProcessorConcurrentDeferredRequestsPreserveBoundAndCoalesceNextPass(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(nil)
	sink := NewMemorySinkWithLeaseValidator(nil, store.ValidateLease)
	key := summaryKey()
	if _, err := store.Enqueue(ctx, summaryRequest(key, 998)); err != nil {
		t.Fatal(err)
	}
	generated := []int64{}
	processor := NewProcessor(store, sink, generatorFunc(func(_ context.Context, job Job) (Candidate, error) {
		generated = append(generated, job.TargetEventSequence)
		if len(generated) == 1 {
			for range 20 {
				if _, err := store.Enqueue(ctx, summaryRequest(key, 0)); err != nil {
					return Candidate{}, err
				}
			}
		}
		return candidateFor(job.Key, job.TargetEventSequence, "summary"), nil
	}), "worker-a", time.Minute)
	resolutions := 0
	processor.TargetResolver = targetResolverFunc(func(context.Context, Job) (int64, error) {
		resolutions++
		return 1200, nil
	})
	first, err := processor.RunOnce(ctx)
	if err != nil || first.Status != StatusPending || first.CompletedEventSequence != 998 {
		t.Fatalf("in-flight deferred requests were lost or changed frozen boundary: %#v err=%v", first, err)
	}
	second, err := processor.RunOnce(ctx)
	if err != nil || second.Status != StatusCompleted || second.CompletedEventSequence != 1200 || resolutions != 1 {
		t.Fatalf("coalesced next pass = %#v resolutions=%d err=%v", second, resolutions, err)
	}
	if len(generated) != 2 || generated[0] != 998 || generated[1] != 1200 {
		t.Fatalf("generated boundaries = %v", generated)
	}
	if _, err := processor.RunOnce(ctx); !errors.Is(err, ErrNoWork) {
		t.Fatalf("coalesced requests did not drain: %v", err)
	}
}

func TestProcessorDeferredEnqueueDuringResolutionGetsAnotherPass(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(nil)
	sink := NewMemorySinkWithLeaseValidator(nil, store.ValidateLease)
	key := summaryKey()
	if _, err := store.Enqueue(ctx, summaryRequest(key, 0)); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor(store, sink, generatorFunc(func(_ context.Context, job Job) (Candidate, error) {
		return candidateFor(job.Key, job.TargetEventSequence, "summary"), nil
	}), "worker-a", time.Minute)
	resolutions := 0
	processor.TargetResolver = targetResolverFunc(func(context.Context, Job) (int64, error) {
		resolutions++
		if resolutions == 1 {
			if _, err := store.Enqueue(ctx, summaryRequest(key, 0)); err != nil {
				return 0, err
			}
			return 1000, nil
		}
		return 1200, nil
	})
	first, err := processor.RunOnce(ctx)
	if err != nil || first.Status != StatusPending || first.CompletedEventSequence != 1000 {
		t.Fatalf("request arriving after read was consumed early: %#v err=%v", first, err)
	}
	second, err := processor.RunOnce(ctx)
	if err != nil || second.Status != StatusCompleted || second.CompletedEventSequence != 1200 || resolutions != 2 {
		t.Fatalf("follow-up resolution = %#v resolutions=%d err=%v", second, resolutions, err)
	}
}
