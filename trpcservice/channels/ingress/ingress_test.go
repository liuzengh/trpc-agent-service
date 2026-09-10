package ingress_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	storememory "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretmemory "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
)

func TestCandidateVerifierPromotesExactlyOnceWithoutPublicTenantLeak(t *testing.T) {
	resolver, _ := fixture(t)
	hint := channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt-1"}
	candidate, err := resolver.ResolveCandidate(context.Background(), hint)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.CandidateToken == "" || candidate.Channel != "feishu" || candidate.Purpose != ingress.PurposeChannelVerify {
		t.Fatalf("candidate=%#v", candidate)
	}
	handle, err := resolver.AcquireVerifier(context.Background(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	callback, receipt, err := handle.Verify(context.Background(), channel.CallbackRequest{Body: []byte("ciphertext"), ReceivedAt: time.Now()},
		func(_ context.Context, request channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
			if string(secret) != "verify-secret" || string(request.Body) != "ciphertext" {
				return channel.VerifiedProtocolPayload{}, errors.New("unexpected verifier input")
			}
			return channel.VerifiedProtocolPayload{Body: []byte("plaintext"), ProtocolIdentityDigest: "protocol-identity"}, nil
		})
	if err != nil || string(callback.Body) != "plaintext" || receipt.ReceiptToken == "" {
		t.Fatalf("callback=%#v receipt=%#v err=%v", callback, receipt, err)
	}
	verified, err := resolver.PromoteVerified(context.Background(), candidate, receipt)
	if err != nil || verified.TenantID != "tenant-a" || verified.AgentAppID != "app" || verified.ChannelBindingID != "binding" {
		t.Fatalf("verified=%#v err=%v", verified, err)
	}
	if _, err := resolver.PromoteVerified(context.Background(), candidate, receipt); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("replayed promotion=%v", err)
	}
	if _, err := resolver.AcquireVerifier(context.Background(), candidate); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("replayed acquire=%v", err)
	}
}

func TestVerifierFailureAndCloseBurnCandidate(t *testing.T) {
	resolver, _ := fixture(t)
	for _, failVerification := range []bool{true, false} {
		candidate, err := resolver.ResolveCandidate(context.Background(), channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt"})
		if err != nil {
			t.Fatal(err)
		}
		handle, err := resolver.AcquireVerifier(context.Background(), candidate)
		if err != nil {
			t.Fatal(err)
		}
		if failVerification {
			_, _, err = handle.Verify(context.Background(), channel.CallbackRequest{}, func(context.Context, channel.CallbackRequest, []byte) (channel.VerifiedProtocolPayload, error) {
				return channel.VerifiedProtocolPayload{}, errors.New("bad signature")
			})
			if err == nil {
				t.Fatal("expected verification failure")
			}
		} else {
			err = handle.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err = resolver.AcquireVerifier(context.Background(), candidate); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("burned candidate was reusable: %v", err)
		}
	}
}

func TestConcurrentAcquireHasSingleWinner(t *testing.T) {
	resolver, _ := fixture(t)
	candidate, err := resolver.ResolveCandidate(context.Background(), channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt"})
	if err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle, acquireErr := resolver.AcquireVerifier(context.Background(), candidate)
			if acquireErr == nil {
				winners.Add(1)
				_ = handle.Close()
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("acquire winners=%d", winners.Load())
	}
}

func TestForgedReceiptDoesNotPromote(t *testing.T) {
	resolver, _ := fixture(t)
	candidate, _ := resolver.ResolveCandidate(context.Background(), channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt"})
	handle, _ := resolver.AcquireVerifier(context.Background(), candidate)
	_, receipt, err := handle.Verify(context.Background(), channel.CallbackRequest{}, func(context.Context, channel.CallbackRequest, []byte) (channel.VerifiedProtocolPayload, error) {
		return channel.VerifiedProtocolPayload{Body: []byte("ok"), ProtocolIdentityDigest: "identity"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	original := receipt
	receipt.ReceiptToken = "forged"
	if _, err = resolver.PromoteVerified(context.Background(), candidate, receipt); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("forged receipt=%v", err)
	}
	receipt = original
	receipt.ProtocolIdentityDigest = "forged-identity"
	if _, err = resolver.PromoteVerified(context.Background(), candidate, receipt); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("forged identity=%v", err)
	}
	if _, err = resolver.PromoteVerified(context.Background(), candidate, original); err != nil {
		t.Fatalf("valid receipt after rejected forgeries=%v", err)
	}
}

func TestCandidateContextMutationAndRouteRotationAreRejected(t *testing.T) {
	resolver, store := fixture(t)
	candidate, err := resolver.ResolveCandidate(context.Background(), channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt"})
	if err != nil {
		t.Fatal(err)
	}
	mutated := candidate
	mutated.RouteKeyDigest = "route-digest-forged"
	if _, err := resolver.AcquireVerifier(context.Background(), mutated); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("mutated candidate=%v", err)
	}
	if _, err := resolver.AcquireVerifier(context.Background(), candidate); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("mutated candidate remained usable=%v", err)
	}

	old, err := resolver.ResolveCandidate(context.Background(), channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "old-attempt"})
	if err != nil {
		t.Fatal(err)
	}
	rotated := ingress.BindingRoute{OpaqueBindingID: "opaque-binding-0001", Channel: "feishu", RouteKeyDigest: "route-digest-0001",
		TenantID: "tenant-a", AgentAppID: "app", ChannelBindingID: "binding", ExternalAccountID: "account", TenantVersion: 4, BindingVersion: 2,
		SecretRef: secrets.SecretRef{Ref: "secret://feishu", Version: 1}, IdentitySecretRef: secrets.SecretRef{Ref: "secret://identity", Version: 1},
		SessionSecretRef: secrets.SecretRef{Ref: "secret://session", Version: 1}, Enabled: true}
	if err := store.PutBindingRoute(context.Background(), rotated); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.AcquireVerifier(context.Background(), old); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("old route candidate acquired after rotation=%v", err)
	}
}

func TestCandidateIssuanceRejectsInvalidTokenOrClock(t *testing.T) {
	resolver, _ := fixture(t)
	resolver.RandomToken = func() (string, error) { return "", nil }
	if _, err := resolver.ResolveCandidate(context.Background(), channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt"}); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("empty token error=%v", err)
	}
	resolver, _ = fixture(t)
	resolver.Now = func() time.Time { return time.Time{} }
	if _, err := resolver.ResolveCandidate(context.Background(), channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt"}); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("zero clock error=%v", err)
	}
}

func TestCandidateExpiryReconcilerBurnsVerifiedCandidates(t *testing.T) {
	resolver, store := fixture(t)
	clock := time.Now().UTC().Truncate(time.Microsecond)
	resolver.Now = func() time.Time { return clock }
	resolver.TTL = time.Minute
	candidate, err := resolver.ResolveCandidate(context.Background(), channel.PublicRouteHint{Channel: "feishu", RouteKeyDigest: "route-digest-0001", IngressAttemptID: "attempt"})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := resolver.AcquireVerifier(context.Background(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	_, receipt, err := handle.Verify(context.Background(), channel.CallbackRequest{}, func(context.Context, channel.CallbackRequest, []byte) (channel.VerifiedProtocolPayload, error) {
		return channel.VerifiedProtocolPayload{ProtocolIdentityDigest: "identity"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Minute)
	sweeper := ingress.CandidateExpiryReconciler{Store: store, Now: func() time.Time { return clock }, BatchSize: 1}
	if count, err := sweeper.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("sweep count=%d err=%v", count, err)
	}
	if _, err := resolver.PromoteVerified(context.Background(), candidate, receipt); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("expired verified candidate promoted: %v", err)
	}
}

func fixture(t *testing.T) (ingress.Resolver, *storememory.Store) {
	t.Helper()
	store := storememory.New()
	secretProvider := secretmemory.New()
	route := ingress.BindingRoute{
		OpaqueBindingID: "opaque-binding-0001", Channel: "feishu", RouteKeyDigest: "route-digest-0001",
		TenantID: "tenant-a", AgentAppID: "app", ChannelBindingID: "binding", ExternalAccountID: "account", TenantVersion: 3, BindingVersion: 1,
		SecretRef: secrets.SecretRef{Ref: "secret://feishu", Version: 1}, IdentitySecretRef: secrets.SecretRef{Ref: "secret://identity", Version: 1},
		SessionSecretRef: secrets.SecretRef{Ref: "secret://session", Version: 1}, Enabled: true,
	}
	if err := store.PutBindingRoute(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	secretProvider.Put(secrets.Scope{TenantID: route.TenantID, Subject: route.ChannelBindingID, Purpose: secrets.PurposeChannelVerify,
		ResourceID: route.ChannelBindingID, ResourceVersion: route.BindingVersion}, route.SecretRef, []byte("verify-secret"))
	var tokenID atomic.Int64
	resolver := ingress.Resolver{Store: store, Secrets: secretProvider, TTL: time.Minute, RandomToken: func() (string, error) {
		return "token-unique-" + time.Unix(0, tokenID.Add(1)).UTC().Format("150405.000000000"), nil
	}}
	return resolver, store
}
