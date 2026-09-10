package inmemory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestInMemoryErrorAndClockBranches(t *testing.T) {
	assertCandidateTTLCapped(t)
	clock := &testClock{now: time.Now().UTC().Add(time.Hour)}
	repo := NewInMemoryRepository(Options{Clock: clock.Now, CandidateTTL: time.Second})
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "edge-route")
	first := assertActiveBindingCollisionPaths(t, repo, routeDigest)
	candidate := assertCandidateConsumptionBoundaries(t, repo, clock, routeDigest)
	assertRepositoryMissingAndClockBoundaries(t, repo, first)
	assertRepositoryCancellationBoundaries(t, repo, first, candidate, routeDigest)
}

func assertCandidateTTLCapped(t *testing.T) {
	t.Helper()
	if repository := NewInMemoryRepository(Options{CandidateTTL: 10 * time.Minute}); repository.candidateTTL != channels.MaxCandidateLifetime {
		t.Fatalf("candidate TTL was not bounded: %s", repository.candidateTTL)
	}
}

func assertActiveBindingCollisionPaths(t *testing.T, repo *InMemoryRepository, routeDigest string) *channels.Binding {
	t.Helper()
	first := mustCreate(t, repo, bindingInput("t_00000000000000000000000006", "edge-first", "corp-edge-first", routeDigest))
	first, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: first.TenantID, BindingID: first.BindingID, ExpectedVersion: first.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	activeInput := bindingInput(first.TenantID, "edge-active-collision", first.ProviderAccountID, routeDigest)
	activeInput.Status = channels.StatusActive
	if _, _, err := repo.Create(context.Background(), activeInput); !errors.Is(err, channels.ErrDuplicateKey) {
		t.Fatalf("active account collision during create returned %v", err)
	}
	second := mustCreate(t, repo, bindingInput(first.TenantID, "edge-second", "corp-edge-second", routeDigest))
	if _, _, err := repo.UpdateConfiguration(context.Background(), channels.UpdateConfigurationInput{TenantID: second.TenantID, BindingID: second.BindingID, ExpectedVersion: second.Version, ProviderAccountID: second.ProviderAccountID, PublicRouteKeyDigest: routeDigest, AppID: second.AppID, SecretRef: second.SecretRef, Protocol: second.Protocol, Metadata: validMetadata()}); err != nil {
		t.Fatalf("draft configuration update unexpectedly failed: %v", err)
	}
	activeSecond, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: second.TenantID, BindingID: second.BindingID, ExpectedVersion: second.Version + 1, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.UpdateConfiguration(context.Background(), channels.UpdateConfigurationInput{TenantID: activeSecond.TenantID, BindingID: activeSecond.BindingID, ExpectedVersion: activeSecond.Version, ProviderAccountID: first.ProviderAccountID, PublicRouteKeyDigest: routeDigest, AppID: activeSecond.AppID, SecretRef: activeSecond.SecretRef, Protocol: activeSecond.Protocol, Metadata: validMetadata()}); !errors.Is(err, channels.ErrDuplicateKey) {
		t.Fatalf("active account collision during update returned %v", err)
	}
	return first
}

func assertCandidateConsumptionBoundaries(t *testing.T, repo *InMemoryRepository, clock *testClock, routeDigest string) channels.CandidateBindingContext {
	t.Helper()
	candidates, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest)
	if err != nil || len(candidates) != 2 {
		t.Fatalf("expected two edge candidates, got %d: %v", len(candidates), err)
	}
	candidate := candidates[0]
	clock.now = clock.now.Add(2 * time.Second)
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest); err != nil {
		t.Fatalf("expired candidate pruning made active route unavailable: %v", err)
	}
	if _, err := repo.ConsumeCandidate(context.Background(), candidate); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("pruned candidate was accepted: %v", err)
	}
	if _, err := repo.ConsumeCandidate(context.Background(), channels.CandidateBindingContext{}); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("empty candidate was accepted: %v", err)
	}
	invalidCandidate := candidate
	invalidCandidate.Channel = channels.Channel("bad")
	if _, err := repo.ConsumeCandidate(context.Background(), invalidCandidate); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("invalid candidate shape was accepted: %v", err)
	}
	unknownCandidate := candidate
	unknownCandidate.CandidateToken = "unknown-token"
	if _, err := repo.ConsumeCandidate(context.Background(), unknownCandidate); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("unknown candidate token was accepted: %v", err)
	}
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, "bad"); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("invalid candidate lookup returned %v", err)
	}
	return candidate
}

func assertRepositoryMissingAndClockBoundaries(t *testing.T, repo *InMemoryRepository, first *channels.Binding) {
	t.Helper()
	if _, err := repo.Get(context.Background(), first.TenantID, "cb_00000000000000000000000000"); !errors.Is(err, channels.ErrNotFound) {
		t.Fatalf("unknown Binding lookup returned %v", err)
	}
	if _, _, err := repo.UpdateConfiguration(context.Background(), channels.UpdateConfigurationInput{TenantID: first.TenantID, BindingID: "cb_00000000000000000000000000", ExpectedVersion: 1}); !errors.Is(err, channels.ErrNotFound) {
		t.Fatalf("unknown Binding update returned %v", err)
	}
	if _, _, err := repo.TransitionStatus(context.Background(), channels.TransitionStatusInput{TenantID: first.TenantID, BindingID: "cb_00000000000000000000000000", ExpectedVersion: 1}); !errors.Is(err, channels.ErrNotFound) {
		t.Fatalf("unknown Binding transition returned %v", err)
	}
	if cloneBinding(nil) != nil {
		t.Fatal("nil Binding clone was not nil")
	}

	zeroClockRepo := NewInMemoryRepository(Options{Clock: func() time.Time { return time.Time{} }})
	if zeroClockRepo.nowUTC().IsZero() {
		t.Fatal("zero repository clock was not replaced by wall clock")
	}
}

func assertRepositoryCancellationBoundaries(t *testing.T, repo *InMemoryRepository, first *channels.Binding, candidate channels.CandidateBindingContext, routeDigest string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repo.Get(ctx, first.TenantID, first.BindingID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Get returned %v", err)
	}
	if _, _, err := repo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled update returned %v", err)
	}
	if _, _, err := repo.TransitionStatus(ctx, channels.TransitionStatusInput{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled transition returned %v", err)
	}
	if _, err := repo.LookupCandidates(ctx, channels.ChannelWeCom, routeDigest); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup returned %v", err)
	}
	if _, err := repo.ConsumeCandidate(ctx, candidate); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled consume returned %v", err)
	}
}

func TestContextMutexCancellationAndNilContexts(t *testing.T) {
	if err := checkContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	mutex := contextRWMutex{}
	if err := mutex.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- mutex.lock(ctx) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled writer wait returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled writer wait did not return")
	}
	mutex.unlock()
}

func TestAllRepositoryOperationsCancelWhileWaitingForWriter(t *testing.T) {
	repo := NewInMemoryRepository()
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "waiting-route")
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000007", "waiting", "corp-waiting", routeDigest))
	if err := repo.mu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer repo.mu.unlock()
	type result struct{ err error }
	results := make(chan result, 4)
	contexts := make([]context.Context, 4)
	cancels := make([]context.CancelFunc, 4)
	for index := range contexts {
		contexts[index], cancels[index] = context.WithCancel(context.Background())
	}
	go func() {
		_, _, err := repo.Create(contexts[0], bindingInput(binding.TenantID, "waiting-create", "corp-waiting-create", routeDigest))
		results <- result{err}
	}()
	go func() {
		_, err := repo.Get(contexts[1], binding.TenantID, binding.BindingID)
		results <- result{err}
	}()
	go func() {
		_, _, err := repo.UpdateConfiguration(contexts[2], channels.UpdateConfigurationInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, ProviderAccountID: binding.ProviderAccountID, PublicRouteKeyDigest: routeDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Metadata: validMetadata()})
		results <- result{err}
	}()
	go func() {
		_, _, err := repo.TransitionStatus(contexts[3], channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, NextStatus: channels.StatusActive, Metadata: validMetadata()})
		results <- result{err}
	}()
	time.Sleep(25 * time.Millisecond)
	for _, cancel := range cancels {
		cancel()
	}
	for range contexts {
		if err := (<-results).err; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting repository operation returned %v", err)
		}
	}
}

func TestRepositoryReadLocksAllowConcurrentReaders(t *testing.T) {
	repo := NewInMemoryRepository()
	if err := repo.mu.rlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer repo.mu.runlock()
	acquired := make(chan error, 1)
	go func() {
		err := repo.mu.rlock(context.Background())
		if err == nil {
			repo.mu.runlock()
		}
		acquired <- err
	}()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second reader was serialized")
	}
}

func TestContextMutexWakePathsAndPanicGuards(t *testing.T) {
	assertPanics(t, func() { (&contextRWMutex{}).unlock() })
	assertPanics(t, func() { (&contextRWMutex{}).runlock() })

	mutex := contextRWMutex{}
	if err := mutex.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	writerAcquired := make(chan error, 1)
	go func() {
		if err := mutex.lock(context.Background()); err != nil {
			writerAcquired <- err
			return
		}
		mutex.unlock()
		writerAcquired <- nil
	}()
	time.Sleep(25 * time.Millisecond)
	mutex.unlock()
	select {
	case err := <-writerAcquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting writer did not acquire after wake")
	}

	if err := mutex.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	readerAcquired := make(chan error, 1)
	go func() {
		if err := mutex.rlock(context.Background()); err != nil {
			readerAcquired <- err
			return
		}
		mutex.runlock()
		readerAcquired <- nil
	}()
	time.Sleep(25 * time.Millisecond)
	mutex.unlock()
	select {
	case err := <-readerAcquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting reader did not acquire after wake")
	}
}

func assertPanics(t *testing.T, function func()) {
	t.Helper()
	deferred := false
	func() {
		defer func() {
			if recover() != nil {
				deferred = true
			}
		}()
		function()
	}()
	if !deferred {
		t.Fatal("function did not panic")
	}
}
