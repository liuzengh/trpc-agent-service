package inmemory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestFakeResolverIsCoveredInsideItsOwningPackage(t *testing.T) {
	setup := setupFakeResolverTest(t)
	candidate := assertFakeResolverRejectsInvalidCandidates(t, setup)
	assertFakeVerifierConsumesHandleAndPreservesScope(t, setup, candidate)
	assertFakeVerifierRejectsForgedHandlesAndBadProofs(t, setup)
	replayRequest := assertFakeVerifierRejectsReplayAndInvalidRequests(t, setup)
	assertFakeVerifierRejectsExpiredAndBoundaryReplays(t, setup, replayRequest)
	assertFakeResolverCancellationAndOptionBoundaries(t, setup, candidate)
}

type fakeResolverTestSetup struct {
	base        time.Time
	clock       *testClock
	repo        *InMemoryRepository
	routeDigest string
	binding     *channels.Binding
	secret      string
	resolver    *FakeCandidateResolver
}

func setupFakeResolverTest(t *testing.T) fakeResolverTestSetup {
	t.Helper()
	base := time.Now().UTC().Add(time.Hour)
	clock := &testClock{now: base}
	repo := NewRepository(Options{Clock: clock.Now, CandidateTTL: 2 * time.Second})
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "fake-package-route")
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000005", "fake-package", "corp-fake", routeDigest))
	binding, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	secret := "package-fake-secret"
	scope := channels.SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef}
	resolver := NewFakeCandidateResolver(repo, map[channels.SecretScope]string{scope: secret}, FakeResolverOptions{Clock: clock.Now, MaxClockSkew: time.Minute})
	return fakeResolverTestSetup{base: base, clock: clock, repo: repo, routeDigest: routeDigest, binding: binding, secret: secret, resolver: resolver}
}

func assertFakeResolverRejectsInvalidCandidates(t *testing.T, setup fakeResolverTestSetup) channels.CandidateBindingContext {
	t.Helper()
	candidate, err := firstCandidate(t, setup.repo, channels.ChannelWeCom, setup.routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	invalidCandidate := candidate
	invalidCandidate.ConfigDigest = "bad"
	if _, err := setup.resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: invalidCandidate, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("invalid candidate shape was accepted: %v", err)
	}
	unknownCandidate := candidate
	unknownCandidate.CandidateToken = "unknown-token"
	if _, err := setup.resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: unknownCandidate, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("unknown candidate token was accepted: %v", err)
	}
	missingSecretResolver := NewFakeCandidateResolver(setup.repo, map[channels.SecretScope]string{}, FakeResolverOptions{Clock: setup.clock.Now})
	missingSecretCandidate, err := firstCandidate(t, setup.repo, channels.ChannelWeCom, setup.routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missingSecretResolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: missingSecretCandidate, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("missing fake secret was accepted: %v", err)
	}
	return candidate
}

func assertFakeVerifierConsumesHandleAndPreservesScope(t *testing.T, setup fakeResolverTestSetup, candidate channels.CandidateBindingContext) {
	t.Helper()
	purposeMismatch := candidate
	purposeMismatch.Purpose = channels.VerificationPurpose("other-purpose")
	if _, err := setup.resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: purposeMismatch, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("candidate purpose mutation was accepted: %v", err)
	}
	handle, err := setup.resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	request := packageFakeRequest(setup.base, "package-nonce", "package message")
	request.Signature = SignFakeRequest(setup.secret, request)
	verified, err := setup.resolver.Verify(context.Background(), handle, request)
	if err != nil {
		t.Fatal(err)
	}
	if verified.TenantID != setup.binding.TenantID || verified.BindingID != setup.binding.BindingID {
		t.Fatalf("fake resolver returned the wrong trusted scope: %+v", verified)
	}
	if _, err := setup.resolver.Verify(context.Background(), handle, request); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("fake verifier handle was reusable: %v", err)
	}
}

func assertFakeVerifierRejectsForgedHandlesAndBadProofs(t *testing.T, setup fakeResolverTestSetup) {
	t.Helper()
	forgedSource := resolvePackageHandle(t, setup.repo, setup.resolver, setup.routeDigest)
	forgedHandle, err := channels.NewScopedVerifierHandle(forgedSource.Token(), channels.VerificationPurpose("other-purpose"), forgedSource.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	request := packageFakeRequest(setup.base, "package-nonce", "package message")
	request.Signature = SignFakeRequest(setup.secret, request)
	if _, err := setup.resolver.Verify(context.Background(), forgedHandle, request); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("forged-purpose fake handle was accepted: %v", err)
	}

	badCandidate, err := firstCandidate(t, setup.repo, channels.ChannelWeCom, setup.routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	badHandle, err := setup.resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: badCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	badRequest := packageFakeRequest(setup.base, "bad-proof", "bad message")
	badRequest.Signature = SignFakeRequest("wrong-secret", badRequest)
	if _, err := setup.resolver.Verify(context.Background(), badHandle, badRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("bad fake proof was accepted: %v", err)
	}
}

func assertFakeVerifierRejectsReplayAndInvalidRequests(t *testing.T, setup fakeResolverTestSetup) channels.VerificationRequest {
	t.Helper()
	successRequest := packageFakeRequest(setup.base, "replayed-nonce", "replayed message")
	successRequest.Signature = SignFakeRequest(setup.secret, successRequest)
	successHandle := resolvePackageHandle(t, setup.repo, setup.resolver, setup.routeDigest)
	if _, err := setup.resolver.Verify(context.Background(), successHandle, successRequest); err != nil {
		t.Fatal(err)
	}
	replayHandle := resolvePackageHandle(t, setup.repo, setup.resolver, setup.routeDigest)
	if _, err := setup.resolver.Verify(context.Background(), replayHandle, successRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nonce replay on a new candidate was accepted: %v", err)
	}

	invalidCases := []channels.VerificationRequest{
		packageFakeRequest(setup.base.Add(10*time.Minute), "future", "future"),
		packageFakeRequest(setup.base, "", "empty nonce"),
		packageFakeRequest(setup.base, "bad-digest", "bad digest"),
		packageFakeRequest(setup.base, "bad-signature", "bad signature"),
		packageFakeRequest(setup.base, "control", "control receive"),
	}
	invalidCases[2].MessageDigest = "not-a-digest"
	invalidCases[3].Signature = ""
	invalidCases[4].ReceiveID = string(unicode.ReplacementChar) + "\n"
	for index, invalidRequest := range invalidCases {
		handle := resolvePackageHandle(t, setup.repo, setup.resolver, setup.routeDigest)
		if index != 0 {
			invalidRequest.Timestamp = setup.base
		}
		invalidRequest.Signature = SignFakeRequest(setup.secret, invalidRequest)
		if index == 3 {
			invalidRequest.Signature = ""
		}
		if _, err := setup.resolver.Verify(context.Background(), handle, invalidRequest); !errors.Is(err, channels.ErrVerificationFailed) {
			t.Fatalf("invalid fake request %d was accepted: %v", index, err)
		}
	}
	return successRequest
}

func assertFakeVerifierRejectsExpiredAndBoundaryReplays(t *testing.T, setup fakeResolverTestSetup, replayRequest channels.VerificationRequest) {
	t.Helper()
	expiringHandle := resolvePackageHandle(t, setup.repo, setup.resolver, setup.routeDigest)
	setup.clock.now = setup.base.Add(3 * time.Second)
	expiringRequest := packageFakeRequest(setup.clock.now, "expired", "expired")
	expiringRequest.Signature = SignFakeRequest(setup.secret, expiringRequest)
	if _, err := setup.resolver.Verify(context.Background(), expiringHandle, expiringRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("expired fake handle was accepted: %v", err)
	}
	expiredCandidateReplayHandle := resolvePackageHandle(t, setup.repo, setup.resolver, setup.routeDigest)
	if _, err := setup.resolver.Verify(context.Background(), expiredCandidateReplayHandle, replayRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nonce replay after candidate expiry was accepted: %v", err)
	}
	setup.clock.now = setup.base.Add(time.Minute)
	boundaryReplayHandle := resolvePackageHandle(t, setup.repo, setup.resolver, setup.routeDigest)
	if _, err := setup.resolver.Verify(context.Background(), boundaryReplayHandle, replayRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nonce replay at the timestamp-window boundary was accepted: %v", err)
	}
}

func assertFakeResolverCancellationAndOptionBoundaries(t *testing.T, setup fakeResolverTestSetup, candidate channels.CandidateBindingContext) {
	t.Helper()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := setup.resolver.ResolveCandidate(canceled, channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fake resolve returned %v", err)
	}
	if _, err := setup.resolver.Verify(canceled, channels.ScopedVerifierHandle{}, channels.VerificationRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fake verify returned %v", err)
	}
	var nilResolver *FakeCandidateResolver
	if _, err := nilResolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nil fake resolver returned %v", err)
	}
	if _, err := nilResolver.Verify(context.Background(), channels.ScopedVerifierHandle{}, channels.VerificationRequest{}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nil fake verifier returned %v", err)
	}

	if !validFakeText("ok", 2) || validFakeText("", 2) || validFakeText("bad\n", 10) || validFakeText("long", 2) {
		t.Fatal("fake text validation boundaries are incorrect")
	}
	if !validLowerHexDigest(strings.Repeat("a", 64)) || validLowerHexDigest(strings.Repeat("A", 64)) || validLowerHexDigest("short") {
		t.Fatal("fake digest validation boundaries are incorrect")
	}
	if (FakeResolverOptions{}).MaxClockSkew != 0 {
		t.Fatal("zero fake options unexpectedly changed")
	}
	zeroClock := NewFakeCandidateResolver(setup.repo, map[channels.SecretScope]string{}, FakeResolverOptions{Clock: func() time.Time { return time.Time{} }})
	if zeroClock.NowUTC().IsZero() {
		t.Fatal("zero fake clock was not replaced by wall clock")
	}
}

func TestFakeResolverPrunesAndBoundsUnverifiedHandles(t *testing.T) {
	base := time.Now().UTC().Add(time.Hour)
	clock := &testClock{now: base}
	repo := NewRepository(Options{Clock: clock.Now, CandidateTTL: 2 * time.Second})
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "handle-capacity")
	binding := mustCreate(t, repo, bindingInput("t_00000000000000000000000007", "handle-capacity", "corp-capacity", routeDigest))
	if _, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()}); err != nil {
		t.Fatal(err)
	}
	secret := "handle-capacity-secret"
	resolver := NewFakeCandidateResolver(repo, map[channels.SecretScope]string{{TenantID: binding.TenantID, SecretRef: binding.SecretRef}: secret}, FakeResolverOptions{Clock: clock.Now, MaxHandles: 2})
	if _, err := resolveHandleForTest(t, repo, resolver, routeDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveHandleForTest(t, repo, resolver, routeDigest); err != nil {
		t.Fatal(err)
	}
	if got := resolver.HandleCount(); got != 2 {
		t.Fatalf("expected two outstanding handles, got %d", got)
	}
	clock.now = base.Add(3 * time.Second)
	if _, err := resolveHandleForTest(t, repo, resolver, routeDigest); err != nil {
		t.Fatal(err)
	}
	if got := resolver.HandleCount(); got != 1 {
		t.Fatalf("expired handles were not pruned before minting: got %d", got)
	}
	if _, err := resolveHandleForTest(t, repo, resolver, routeDigest); err != nil {
		t.Fatal(err)
	}
	if got := resolver.HandleCount(); got != 2 {
		t.Fatalf("expected configured handle capacity, got %d", got)
	}
	candidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("handle capacity was not enforced: %v", err)
	}
	if got := resolver.HandleCount(); got != 2 {
		t.Fatalf("handle store exceeded configured capacity: got %d", got)
	}
}

func resolveHandleForTest(t *testing.T, repo *InMemoryRepository, resolver *FakeCandidateResolver, routeDigest string) (channels.ScopedVerifierHandle, error) {
	t.Helper()
	candidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		return channels.ScopedVerifierHandle{}, err
	}
	return resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
}

func resolvePackageHandle(t *testing.T, repo *InMemoryRepository, resolver *FakeCandidateResolver, routeDigest string) channels.ScopedVerifierHandle {
	t.Helper()
	candidate, err := firstCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func packageFakeRequest(timestamp time.Time, nonce, body string) channels.VerificationRequest {
	digest := sha256.Sum256([]byte(body))
	return channels.VerificationRequest{Purpose: channels.PurposeWebhookVerification, Timestamp: timestamp.UTC(), Nonce: nonce, MessageDigest: hex.EncodeToString(digest[:]), ReceiveID: "receive"}
}
