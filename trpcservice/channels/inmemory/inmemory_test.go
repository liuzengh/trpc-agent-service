package inmemory

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

type testClock struct {
	now time.Time
}

func (c *testClock) Now() time.Time { return c.now }

func TestRepositoryIsTenantScopedAndCandidateLookupIsRedacted(t *testing.T) {
	setup := setupTenantScopedBindings(t)
	assertTenantScopedBindingRules(t, setup.repo, setup.first, setup.second, setup.routeDigest)
	assertCandidateLookupIsOpaqueAndRedacted(t, setup.repo, setup.first, setup.second, setup.routeDigest)
	assertRepositoryReturnsDefensiveBindingCopies(t, setup.repo, setup.first)
}

type tenantScopedSetup struct {
	repo          *InMemoryRepository
	first, second *channels.Binding
	routeDigest   string
}

func setupTenantScopedBindings(t *testing.T) tenantScopedSetup {
	t.Helper()
	clock := &testClock{now: time.Now().UTC().Add(time.Hour)}
	repo := NewInMemoryRepository(Options{Clock: clock.Now, CandidateTTL: time.Minute})
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "shared-route")
	if err != nil {
		t.Fatal(err)
	}
	first := mustCreate(t, repo, bindingInput("t_00000000000000000000000000", "shared", "corp-one", routeDigest))
	second := mustCreate(t, repo, bindingInput("t_00000000000000000000000001", "shared", "corp-two", routeDigest))
	if first.BindingKey != second.BindingKey {
		t.Fatal("different tenants did not accept the same binding key")
	}
	return tenantScopedSetup{repo: repo, first: first, second: second, routeDigest: routeDigest}
}

func assertTenantScopedBindingRules(t *testing.T, repo *InMemoryRepository, first, second *channels.Binding, routeDigest string) {
	t.Helper()
	if _, err := repo.Get(context.Background(), second.TenantID, first.BindingID); !errors.Is(err, channels.ErrNotFound) {
		t.Fatalf("cross-tenant Binding lookup was not isolated: %v", err)
	}
	if _, _, err := repo.Create(context.Background(), bindingInput(first.TenantID, "shared", "corp-three", routeDigest)); !errors.Is(err, channels.ErrDuplicateKey) {
		t.Fatalf("tenant-local binding key collision was not rejected: %v", err)
	}
	if _, _, err := repo.Create(context.Background(), bindingInput(second.TenantID, "other", "corp-one", routeDigest)); err != nil {
		t.Fatal(err)
	}
	activated, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: first.TenantID, BindingID: first.BindingID, ExpectedVersion: first.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	*first = *activated
}

func assertCandidateLookupIsOpaqueAndRedacted(t *testing.T, repo *InMemoryRepository, first, second *channels.Binding, routeDigest string) {
	t.Helper()
	third, _, err := repo.Create(context.Background(), bindingInput(second.TenantID, "other-active", "corp-one", routeDigest))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: third.TenantID, BindingID: third.BindingID, ExpectedVersion: third.Version, Metadata: validMetadata()}); !errors.Is(err, channels.ErrDuplicateKey) {
		t.Fatalf("active provider account collision was not rejected: %v", err)
	}

	second, _, err = repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: second.TenantID, BindingID: second.BindingID, ExpectedVersion: second.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected two tenant-isolated candidates, got %d", len(candidates))
	}
	for _, candidate := range candidates {
		if candidate.CandidateToken == "" || candidate.Purpose != channels.PurposeWebhookVerification {
			t.Fatalf("candidate was not opaque and purpose-bound: %+v", candidate)
		}
		encoded, err := json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		encodedText := string(encoded)
		for _, forbidden := range []string{first.TenantID, second.TenantID, first.AppID, second.AppID, first.SecretRef, second.SecretRef, first.BindingID, second.BindingID} {
			if strings.Contains(encodedText, forbidden) {
				t.Fatalf("candidate leaked %q: %s", forbidden, encodedText)
			}
		}
	}
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, strings.Repeat("0", 63)); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("invalid route digest did not use generic unavailable error: %v", err)
	}
	if _, err := repo.LookupCandidates(context.Background(), channels.Channel("unknown"), routeDigest); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("invalid channel did not use generic unavailable error: %v", err)
	}
}

func assertRepositoryReturnsDefensiveBindingCopies(t *testing.T, repo *InMemoryRepository, first *channels.Binding) {
	t.Helper()

	stored, err := repo.Get(context.Background(), first.TenantID, first.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	stored.Protocol.WeCom.CorpID = "caller mutation"
	again, err := repo.Get(context.Background(), first.TenantID, first.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Protocol.WeCom.CorpID == "caller mutation" {
		t.Fatal("repository returned mutable protocol state")
	}
}

func TestRepositoryCreateUsesInjectedClockForLifecycle(t *testing.T) {
	base := time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC)
	clock := &testClock{now: base}
	repo := NewInMemoryRepository(
		Options{Clock: func() time.Time { return base.Add(-time.Hour) }},
		Options{Clock: clock.Now}, Options{},
	)
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "injected-clock")
	if err != nil {
		t.Fatal(err)
	}
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000006", "injected-clock", "corp-clock", routeDigest))
	if !binding.CreatedAt.Equal(base) || !binding.UpdatedAt.Equal(base) {
		t.Fatalf("create ignored injected clock: created=%s updated=%s", binding.CreatedAt, binding.UpdatedAt)
	}
	activated, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{
		TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata(),
	})
	if err != nil {
		t.Fatalf("create followed by lifecycle mutation failed with injected clock: %v", err)
	}
	if !activated.UpdatedAt.Equal(base) || activated.Status != channels.StatusActive {
		t.Fatalf("unexpected activated binding: %+v", activated)
	}
}

func TestCandidateStorePrunesAndBoundsAbandonedLookups(t *testing.T) {
	base := time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC)
	clock := &testClock{now: base}
	repo := NewInMemoryRepository(
		Options{Clock: clock.Now, CandidateTTL: time.Minute, MaxCandidates: 1},
		Options{CandidateTTL: 2 * time.Second, MaxCandidates: 2}, Options{},
	)
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "candidate-capacity")
	if err != nil {
		t.Fatal(err)
	}
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000008", "candidate-capacity", "corp-candidate-capacity", routeDigest))
	if _, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest); err != nil {
		t.Fatal(err)
	}
	if got := len(repo.candidates); got != 2 {
		t.Fatalf("expected two retained candidates, got %d", got)
	}
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("candidate capacity was not enforced: %v", err)
	}
	if got := len(repo.candidates); got != 2 {
		t.Fatalf("candidate store exceeded configured capacity: got %d", got)
	}
	clock.now = base.Add(3 * time.Second)
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest); err != nil {
		t.Fatal(err)
	}
	if got := len(repo.candidates); got != 1 {
		t.Fatalf("expired candidates were not pruned before minting: got %d", got)
	}
}

func TestRepositoryLifecycleExpectedVersionAndCandidateInvalidation(t *testing.T) {
	clock := &testClock{now: time.Now().UTC().Add(time.Hour)}
	repo := NewInMemoryRepository(Options{Clock: clock.Now, CandidateTTL: 2 * time.Second})
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "lifecycle-route")
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000002", "lifecycle", "bot-one", routeDigest))
	active, candidate := assertBindingActivation(t, repo, binding, routeDigest)
	updated, routeDigest, candidate := assertConfigurationUpdateInvalidatesCandidate(t, repo, binding, active, candidate, routeDigest)
	resumed := assertSuspensionInvalidatesCandidate(t, repo, updated, candidate, routeDigest)
	assertResumeExpiryAndDisable(t, repo, clock, resumed, routeDigest)
}

func assertBindingActivation(t *testing.T, repo *InMemoryRepository, binding *channels.Binding, routeDigest string) (*channels.Binding, channels.CandidateBindingContext) {
	t.Helper()
	active, event, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()})
	if err != nil || event.EventType != channels.EventActivated || active.Status != channels.StatusActive || active.Version != 2 {
		t.Fatalf("activation failed: binding=%+v event=%+v err=%v", active, event, err)
	}
	candidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	return active, candidate
}

func assertConfigurationUpdateInvalidatesCandidate(t *testing.T, repo *InMemoryRepository, binding, active *channels.Binding, candidate channels.CandidateBindingContext, routeDigest string) (*channels.Binding, string, channels.CandidateBindingContext) {
	t.Helper()
	if _, _, err := repo.UpdateConfiguration(context.Background(), channels.UpdateConfigurationInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: 1, ProviderAccountID: active.ProviderAccountID, PublicRouteKeyDigest: routeDigest, AppID: active.AppID, SecretRef: active.SecretRef, Protocol: active.Protocol, Metadata: validMetadata()}); !errors.Is(err, channels.ErrConflict) {
		t.Fatalf("stale configuration update was accepted: %v", err)
	}
	newRouteDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "lifecycle-route-v2")
	updated, event, err := repo.UpdateConfiguration(context.Background(), channels.UpdateConfigurationInput{TenantID: active.TenantID, BindingID: active.BindingID, ExpectedVersion: active.Version, ProviderAccountID: active.ProviderAccountID, PublicRouteKeyDigest: newRouteDigest, AppID: active.AppID, SecretRef: active.SecretRef, Protocol: active.Protocol, Metadata: validMetadata()})
	if err != nil || event.EventType != channels.EventConfigurationUpdated || updated.Version != active.Version+1 || updated.ConfigDigest == active.ConfigDigest {
		t.Fatalf("configuration update failed to replace version/digest: binding=%+v event=%+v err=%v", updated, event, err)
	}
	if _, err := repo.ConsumeCandidate(context.Background(), candidate); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("configuration update did not invalidate an old candidate: %v", err)
	}
	active = updated
	routeDigest = newRouteDigest
	candidate, err = firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	return updated, newRouteDigest, candidate
}

func assertSuspensionInvalidatesCandidate(t *testing.T, repo *InMemoryRepository, active *channels.Binding, candidate channels.CandidateBindingContext, routeDigest string) *channels.Binding {
	t.Helper()
	suspended, event, err := repo.Suspend(context.Background(), channels.TransitionStatusInput{TenantID: active.TenantID, BindingID: active.BindingID, ExpectedVersion: active.Version, Metadata: validMetadata()})
	if err != nil || event.EventType != channels.EventSuspended || suspended.Status != channels.StatusSuspended {
		t.Fatalf("suspension failed: binding=%+v event=%+v err=%v", suspended, event, err)
	}
	if _, err := repo.ConsumeCandidate(context.Background(), candidate); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("candidate survived suspension: %v", err)
	}
	if _, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("suspended binding remained discoverable: %v", err)
	}
	resumed, _, err := repo.Resume(context.Background(), channels.TransitionStatusInput{TenantID: suspended.TenantID, BindingID: suspended.BindingID, ExpectedVersion: suspended.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != channels.StatusActive || resumed.Version != suspended.Version+1 {
		t.Fatalf("resume did not advance lifecycle: %+v", resumed)
	}
	return resumed
}

func assertResumeExpiryAndDisable(t *testing.T, repo *InMemoryRepository, clock *testClock, resumed *channels.Binding, routeDigest string) {
	t.Helper()
	candidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(3 * time.Second)
	if _, err := repo.ConsumeCandidate(context.Background(), candidate); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("expired candidate was accepted: %v", err)
	}

	disabled, event, err := repo.Disable(context.Background(), channels.TransitionStatusInput{TenantID: resumed.TenantID, BindingID: resumed.BindingID, ExpectedVersion: resumed.Version, Metadata: validMetadata()})
	if err != nil || event.EventType != channels.EventDisabled || disabled.Status != channels.StatusDisabled {
		t.Fatalf("disable failed: binding=%+v event=%+v err=%v", disabled, event, err)
	}
	if _, _, err := repo.Resume(context.Background(), channels.TransitionStatusInput{TenantID: disabled.TenantID, BindingID: disabled.BindingID, ExpectedVersion: disabled.Version, Metadata: validMetadata()}); !errors.Is(err, channels.ErrDisabled) {
		t.Fatalf("disabled binding was resumable: %v", err)
	}
}

func TestRepositoryOperationsObserveContextCancellationWhileWaiting(t *testing.T) {
	repo := NewInMemoryRepository()
	if err := repo.mu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer repo.mu.unlock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := repo.Get(ctx, "t_00000000000000000000000000", "cb_00000000000000000000000000")
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting repository operation returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("repository operation did not observe cancellation")
	}
}

func TestCandidateConsumptionIsSingleUse(t *testing.T) {
	repo := NewInMemoryRepository()
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "single-use")
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000003", "single-use", "corp-single", routeDigest))
	binding, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := repo.ConsumeCandidate(context.Background(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	if consumed.BindingID != binding.BindingID || consumed.TenantID != binding.TenantID {
		t.Fatalf("wrong binding consumed: %+v", consumed)
	}
	if _, err := repo.ConsumeCandidate(context.Background(), candidate); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("candidate was replayable: %v", err)
	}
}

func TestConcurrentExpectedVersionUpdatesHaveOneWinner(t *testing.T) {
	clock := &testClock{now: time.Now().UTC().Add(time.Hour)}
	repo := NewInMemoryRepository(Options{Clock: clock.Now})
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "concurrent-route")
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000004", "concurrent", "corp-concurrent", routeDigest))
	input := channels.UpdateConfigurationInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, ProviderAccountID: binding.ProviderAccountID, PublicRouteKeyDigest: routeDigest, AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol, Metadata: validMetadata()}
	results := make(chan error, 2)
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer waitGroup.Done()
			_, _, err := repo.UpdateConfiguration(context.Background(), input)
			results <- err
		}()
	}
	waitGroup.Wait()
	close(results)
	var successes, conflicts int
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, channels.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent update result: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected one optimistic-lock winner, got successes=%d conflicts=%d", successes, conflicts)
	}
}

func firstCandidate(t *testing.T, repo *InMemoryRepository, channel channels.Channel, routeDigest string) (channels.CandidateBindingContext, error) {
	t.Helper()
	candidates, err := repo.LookupCandidates(context.Background(), channel, routeDigest)
	if err != nil {
		return channels.CandidateBindingContext{}, err
	}
	if len(candidates) != 1 {
		t.Fatalf("expected one candidate, got %d", len(candidates))
	}
	return candidates[0], nil
}

func mustCreate(t *testing.T, repo *InMemoryRepository, input channels.CreateInput) *channels.Binding {
	t.Helper()
	binding, _, err := repo.Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func bindingInput(tenantID, bindingKey, providerAccountID, routeDigest string) channels.CreateInput {
	return channels.CreateInput{
		TenantID: tenantID, BindingKey: bindingKey, Channel: channels.ChannelWeCom,
		ProviderAccountID: providerAccountID, PublicRouteKeyDigest: routeDigest,
		AppID: "app_00000000000000000000000000", SecretRef: "secret/" + bindingKey,
		Protocol: channels.ProtocolConfiguration{WeCom: &channels.WeComProtocolConfiguration{CorpID: providerAccountID}},
		Metadata: validMetadata(),
	}
}

func validMetadata() channels.ChangeMetadata {
	return channels.ChangeMetadata{ActorType: "test", ActorID: "test-actor", Reason: "test mutation", CorrelationID: "correlation-1"}
}
