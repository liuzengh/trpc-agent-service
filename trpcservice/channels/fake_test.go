package channels

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

type fakeCandidateConsumer struct {
	candidate       CandidateBindingContext
	binding         *Binding
	current         *Binding
	consumeErr      error
	getErr          error
	cancelOnConsume context.CancelFunc
}

func (c *fakeCandidateConsumer) LookupCandidates(context.Context, Channel, string) ([]CandidateBindingContext, error) {
	return []CandidateBindingContext{c.candidate.Clone()}, nil
}

func (c *fakeCandidateConsumer) ConsumeCandidate(_ context.Context, candidate CandidateBindingContext) (*Binding, error) {
	if c.cancelOnConsume != nil {
		c.cancelOnConsume()
	}
	if c.consumeErr != nil {
		return nil, c.consumeErr
	}
	if c.binding == nil || candidate.CandidateToken != c.candidate.CandidateToken {
		return nil, ErrCandidateUnavailable
	}
	binding := c.binding.Clone()
	return &binding, nil
}

func (c *fakeCandidateConsumer) Get(_ context.Context, tenantID, bindingID string) (*Binding, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	binding := c.binding
	if c.current != nil {
		binding = c.current
	}
	if binding == nil || binding.TenantID != tenantID || binding.BindingID != bindingID {
		return nil, ErrNotFound
	}
	clone := binding.Clone()
	return &clone, nil
}

func newFakeResolverFixture(t *testing.T, now time.Time) (*fakeCandidateConsumer, *Binding, CandidateBindingContext, string, SecretScope) {
	t.Helper()
	routeDigest, err := DigestPublicRouteKey(ChannelWeCom, "fake-channel-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := NewBindingAt(CreateInput{
		TenantID: testTenantID, BindingKey: "fake-channel", Channel: ChannelWeCom,
		ProviderAccountID: "corp-fake", PublicRouteKeyDigest: routeDigest, AppID: testAppID,
		SecretRef: "secret/fake", Protocol: ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{CorpID: "corp-fake"}},
		Status: StatusActive,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := NewCandidateBindingContextFromInput(CandidateBindingInput{
		Channel: binding.Channel, PublicRouteKeyDigest: binding.PublicRouteKeyDigest, BindingVersion: binding.Version,
		ConfigDigest: binding.ConfigDigest, Purpose: PurposeWebhookVerification, CandidateToken: "fake-candidate-token",
		IssuedAt: now, ExpiresAt: now.Add(MaxCandidateLifetime),
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef}
	return &fakeCandidateConsumer{candidate: candidate, binding: binding}, binding, candidate, "fake-secret", scope
}

func newFakeHandle(t *testing.T, resolver *FakeCandidateResolver, candidate CandidateBindingContext) ScopedVerifierHandle {
	t.Helper()
	handle, err := resolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: candidate, Purpose: PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func fakeVerificationRequest(timestamp time.Time, nonce, body string) VerificationRequest {
	digest := sha256.Sum256([]byte(body))
	return VerificationRequest{
		Purpose:       PurposeWebhookVerification,
		Timestamp:     timestamp.UTC(),
		Nonce:         nonce,
		MessageDigest: hex.EncodeToString(digest[:]),
		ReceiveID:     "receive",
	}
}

func TestFakeCandidateResolverOwnsCandidateAndVerificationBoundaries(t *testing.T) {
	setup := newFakeBoundarySetup(t)
	assertFakeResolverSuccess(t, setup)
	assertFakeResolverRejectsCandidates(t, setup)
	assertFakeResolverRejectsConsumerFailures(t, setup)
	assertFakeResolverCancellationAndNil(t, setup)
}

type fakeBoundarySetup struct {
	now       time.Time
	clockNow  *time.Time
	repo      *fakeCandidateConsumer
	binding   *Binding
	candidate CandidateBindingContext
	secret    string
	scope     SecretScope
	secrets   map[SecretScope]string
	resolver  *FakeCandidateResolver
}

func newFakeBoundarySetup(t *testing.T) fakeBoundarySetup {
	t.Helper()
	now := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	repo, binding, candidate, secret, scope := newFakeResolverFixture(t, now)
	clockNow := now
	secrets := map[SecretScope]string{scope: secret}
	resolver := NewFakeCandidateResolver(repo, secrets, FakeResolverOptions{
		Clock: func() time.Time { return clockNow }, MaxClockSkew: time.Minute, MaxHandles: 2,
	})
	return fakeBoundarySetup{now: now, clockNow: &clockNow, repo: repo, binding: binding, candidate: candidate, secret: secret, scope: scope, secrets: secrets, resolver: resolver}
}

func assertFakeResolverSuccess(t *testing.T, setup fakeBoundarySetup) {
	t.Helper()
	if setup.resolver == nil || !setup.resolver.NowUTC().Equal(setup.now) {
		t.Fatalf("resolver clock was not configured: %+v", setup.resolver)
	}
	if got := setup.resolver.HandleCount(); got != 0 {
		t.Fatalf("new resolver has %d handles", got)
	}
	setup.secrets[setup.scope] = "mutated-after-construction"
	if _, err := setup.repo.LookupCandidates(context.Background(), ChannelWeCom, "route"); err != nil {
		t.Fatal(err)
	}
	handle := newFakeHandle(t, setup.resolver, setup.candidate)
	request := fakeVerificationRequest(setup.now, "nonce-success", "message")
	request.RouteHints = UntrustedRouteHints{TenantID: "t_forged", BindingID: "cb_forged", AppID: "app_forged"}
	request.Signature = SignFakeRequest(setup.secret, request)
	verified, err := setup.resolver.Verify(context.Background(), handle, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := verified.Validate(); err != nil || verified.TenantID != setup.binding.TenantID || verified.BindingID != setup.binding.BindingID {
		t.Fatalf("verified binding = %+v, err=%v", verified, err)
	}
	if got := setup.resolver.HandleCount(); got != 0 {
		t.Fatalf("verified handle was not consumed: %d", got)
	}
	if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("reusable handle returned %v", err)
	}
	if SignFakeRequest(setup.secret, request) != SignFakeRequest(setup.secret, func() VerificationRequest {
		copy := request
		copy.RouteHints = UntrustedRouteHints{}
		return copy
	}()) {
		t.Fatal("untrusted route hints changed the fake signature")
	}
}

func assertFakeResolverRejectsCandidates(t *testing.T, setup fakeBoundarySetup) {
	t.Helper()
	invalidPurpose := setup.candidate
	invalidPurpose.Purpose = VerificationPurpose("other-purpose")
	if _, err := setup.resolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: invalidPurpose, Purpose: PurposeWebhookVerification}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("candidate purpose mutation returned %v", err)
	}
	if _, err := setup.resolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: setup.candidate, Purpose: VerificationPurpose("other-purpose")}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("request purpose mutation returned %v", err)
	}
	invalidCandidate := setup.candidate
	invalidCandidate.ConfigDigest = "bad"
	if _, err := setup.resolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: invalidCandidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("invalid candidate returned %v", err)
	}
	expiredCandidate := setup.candidate
	expiredCandidate.ExpiresAt = setup.now
	if _, err := setup.resolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: expiredCandidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("expired candidate returned %v", err)
	}
	missingSecret := NewFakeCandidateResolver(setup.repo, map[SecretScope]string{}, FakeResolverOptions{Clock: func() time.Time { return *setup.clockNow }})
	if _, err := missingSecret.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("missing secret returned %v", err)
	}
	if _, err := NewFakeCandidateResolver(nil, setup.secrets).ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatal("nil consumer was accepted")
	}
	badBinding := setup.binding.Clone()
	badBinding.TenantID = "bad"
	badRepo := &fakeCandidateConsumer{candidate: setup.candidate, binding: &badBinding}
	if _, err := NewFakeCandidateResolver(badRepo, setup.secrets, FakeResolverOptions{Clock: func() time.Time { return *setup.clockNow }}).ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatal("invalid consumed binding was accepted")
	}
}

func assertFakeResolverRejectsConsumerFailures(t *testing.T, setup fakeBoundarySetup) {
	t.Helper()
	setup.repo.consumeErr = errors.New("private consumer failure")
	if _, err := setup.resolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("consumer failure returned %v", err)
	}
	setup.repo.consumeErr = context.Canceled
	if _, err := setup.resolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, context.Canceled) {
		t.Fatalf("consumer cancellation returned %v", err)
	}
	setup.repo.consumeErr = nil
}

func assertFakeResolverCancellationAndNil(t *testing.T, setup fakeBoundarySetup) {
	t.Helper()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := setup.resolver.ResolveCandidate(canceled, CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled resolve returned %v", err)
	}
	afterConsume, cancelAfterConsume := context.WithCancel(context.Background())
	setup.repo.cancelOnConsume = cancelAfterConsume
	if _, err := setup.resolver.ResolveCandidate(afterConsume, CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-consume cancellation returned %v", err)
	}
	setup.repo.cancelOnConsume = nil
	var nilResolver *FakeCandidateResolver
	if _, err := nilResolver.ResolveCandidate(context.Background(), CandidateSecretRequest{}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("nil resolver resolve returned %v", err)
	}
	if _, err := nilResolver.Verify(context.Background(), ScopedVerifierHandle{}, VerificationRequest{}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("nil resolver verify returned %v", err)
	}
	if nilResolver.HandleCount() != 0 {
		t.Fatal("nil resolver reported handles")
	}
}

func TestFakeCandidateResolverRejectsForgeryReplayAndLifecycleEdges(t *testing.T) {
	setup := newFakeLifecycleSetup(t)
	assertFakeResolverRejectsBadRequests(t, setup)
	assertFakeResolverRejectsForgedHandles(t, setup)
	assertFakeResolverRejectsRepositoryFailures(t, setup)
	assertFakeResolverRejectsReplayAndPrunesNonce(t, setup)
	assertFakeResolverEnforcesCapacityAndPrimitiveBoundaries(t, setup)
}

type fakeLifecycleSetup struct {
	now       time.Time
	clockNow  *time.Time
	repo      *fakeCandidateConsumer
	binding   *Binding
	candidate CandidateBindingContext
	secret    string
	scope     SecretScope
	resolver  *FakeCandidateResolver
}

func newFakeLifecycleSetup(t *testing.T) fakeLifecycleSetup {
	t.Helper()
	now := time.Date(2026, 8, 23, 11, 0, 0, 0, time.UTC)
	repo, binding, candidate, secret, scope := newFakeResolverFixture(t, now)
	clockNow := now
	resolver := NewFakeCandidateResolver(repo, map[SecretScope]string{scope: secret}, FakeResolverOptions{
		Clock: func() time.Time { return clockNow }, MaxClockSkew: time.Minute, MaxHandles: 1,
	})
	return fakeLifecycleSetup{now: now, clockNow: &clockNow, repo: repo, binding: binding, candidate: candidate, secret: secret, scope: scope, resolver: resolver}
}

func assertFakeResolverRejectsBadRequests(t *testing.T, setup fakeLifecycleSetup) {
	t.Helper()
	badRequest := func(name string, mutate func(*VerificationRequest)) {
		t.Helper()
		handle := newFakeHandle(t, setup.resolver, setup.candidate)
		request := fakeVerificationRequest(setup.now, "nonce-"+name, name)
		mutate(&request)
		if name != "signature" {
			request.Signature = SignFakeRequest(setup.secret, request)
		}
		if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, ErrVerificationFailed) {
			t.Fatalf("invalid request %s returned %v", name, err)
		}
	}
	badRequest("purpose", func(request *VerificationRequest) { request.Purpose = VerificationPurpose("other") })
	badRequest("nonce", func(request *VerificationRequest) { request.Nonce = "" })
	badRequest("digest", func(request *VerificationRequest) { request.MessageDigest = "not-a-digest" })
	badRequest("signature", func(request *VerificationRequest) { request.Signature = "" })
	badRequest("receive", func(request *VerificationRequest) { request.ReceiveID = "bad\nreceive" })
	badRequest("timestamp", func(request *VerificationRequest) { request.Timestamp = setup.now.In(time.FixedZone("not-utc", 3600)) })
	badRequest("future", func(request *VerificationRequest) { request.Timestamp = setup.now.Add(2 * time.Minute) })
}

func assertFakeResolverRejectsForgedHandles(t *testing.T, setup fakeLifecycleSetup) {
	t.Helper()
	handle := newFakeHandle(t, setup.resolver, setup.candidate)
	forgedPurpose, err := NewScopedVerifierHandle(handle.Token(), VerificationPurpose("other-purpose"), handle.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	request := fakeVerificationRequest(setup.now, "nonce-forged-purpose", "forged-purpose")
	request.Signature = SignFakeRequest(setup.secret, request)
	if _, err := setup.resolver.Verify(context.Background(), forgedPurpose, request); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("forged purpose handle returned %v", err)
	}

	handle = newFakeHandle(t, setup.resolver, setup.candidate)
	forgedExpiry, err := NewScopedVerifierHandle(handle.Token(), handle.Purpose, handle.ExpiresAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	request = fakeVerificationRequest(setup.now, "nonce-forged-expiry", "forged-expiry")
	request.Signature = SignFakeRequest(setup.secret, request)
	if _, err := setup.resolver.Verify(context.Background(), forgedExpiry, request); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("forged expiry handle returned %v", err)
	}

	handle = newFakeHandle(t, setup.resolver, setup.candidate)
	*setup.clockNow = setup.now.Add(6 * time.Minute)
	request = fakeVerificationRequest(*setup.clockNow, "nonce-expired-handle", "expired-handle")
	request.Signature = SignFakeRequest(setup.secret, request)
	if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("expired handle returned %v", err)
	}
	*setup.clockNow = setup.now
}

func assertFakeResolverRejectsRepositoryFailures(t *testing.T, setup fakeLifecycleSetup) {
	t.Helper()
	handle := newFakeHandle(t, setup.resolver, setup.candidate)
	request := fakeVerificationRequest(setup.now, "nonce-bad-signature", "bad-signature")
	request.Signature = SignFakeRequest("wrong-secret", request)
	if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("bad signature returned %v", err)
	}

	handle = newFakeHandle(t, setup.resolver, setup.candidate)
	request = fakeVerificationRequest(setup.now, "nonce-get-error", "get-error")
	request.Signature = SignFakeRequest(setup.secret, request)
	setup.repo.getErr = errors.New("private lookup failure")
	if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("repository lookup failure returned %v", err)
	}
	setup.repo.getErr = context.Canceled
	handle = newFakeHandle(t, setup.resolver, setup.candidate)
	request = fakeVerificationRequest(setup.now, "nonce-get-canceled", "get-canceled")
	request.Signature = SignFakeRequest(setup.secret, request)
	if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("repository cancellation returned %v", err)
	}
	setup.repo.getErr = nil

	changed := setup.binding.Clone()
	changed.Version++
	setup.repo.current = &changed
	handle = newFakeHandle(t, setup.resolver, setup.candidate)
	request = fakeVerificationRequest(setup.now, "nonce-stale-binding", "stale-binding")
	request.Signature = SignFakeRequest(setup.secret, request)
	if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("stale binding was accepted: %v", err)
	}
	setup.repo.current = nil
}

func assertFakeResolverRejectsReplayAndPrunesNonce(t *testing.T, setup fakeLifecycleSetup) {
	t.Helper()
	handle := newFakeHandle(t, setup.resolver, setup.candidate)
	request := fakeVerificationRequest(setup.now, "nonce-replay", "replay")
	request.Signature = SignFakeRequest(setup.secret, request)
	if _, err := setup.resolver.Verify(context.Background(), handle, request); err != nil {
		t.Fatal(err)
	}
	handle = newFakeHandle(t, setup.resolver, setup.candidate)
	if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("replayed nonce returned %v", err)
	}
	*setup.clockNow = setup.now.Add(2 * time.Minute)
	handle = newFakeHandle(t, setup.resolver, setup.candidate)
	request = fakeVerificationRequest(*setup.clockNow, "nonce-after-prune", "after-prune")
	request.Signature = SignFakeRequest(setup.secret, request)
	if _, err := setup.resolver.Verify(context.Background(), handle, request); err != nil {
		t.Fatal(err)
	}
	*setup.clockNow = setup.now
}

func assertFakeResolverEnforcesCapacityAndPrimitiveBoundaries(t *testing.T, setup fakeLifecycleSetup) {
	t.Helper()
	capacityResolver := NewFakeCandidateResolver(setup.repo, map[SecretScope]string{setup.scope: setup.secret}, FakeResolverOptions{Clock: func() time.Time { return *setup.clockNow }, MaxHandles: 1})
	if _, err := capacityResolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); err != nil {
		t.Fatal(err)
	}
	if _, err := capacityResolver.ResolveCandidate(context.Background(), CandidateSecretRequest{Candidate: setup.candidate, Purpose: PurposeWebhookVerification}); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("handle capacity returned %v", err)
	}

	if validFakeText("", 1) || validFakeText("bad\n", 10) || validFakeText("long", 2) || !validFakeText("ok", 2) {
		t.Fatal("fake text validation boundary failed")
	}
	if validLowerHexDigest(strings.Repeat("A", 64)) || validLowerHexDigest(strings.Repeat("g", 64)) || validLowerHexDigest("short") || !validLowerHexDigest(strings.Repeat("a", 64)) {
		t.Fatal("fake digest validation boundary failed")
	}
	if validFakeVerificationRequest(VerificationRequest{Purpose: PurposeWebhookVerification, Timestamp: setup.now, Nonce: "n", MessageDigest: strings.Repeat("a", 64), Signature: "s"}, setup.now, 0) {
		t.Fatal("zero clock skew accepted a verification request")
	}
	var nilContext context.Context
	if checkFakeContext(nilContext) != nil {
		t.Fatal("nil context should be accepted by the offline fake")
	}
	canceledContext, cancelContext := context.WithCancel(context.Background())
	cancelContext()
	if !errors.Is(checkFakeContext(canceledContext), context.Canceled) {
		t.Fatal("canceled context was not detected")
	}
}
