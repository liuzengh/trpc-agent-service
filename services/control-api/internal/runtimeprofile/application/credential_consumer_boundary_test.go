package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestDecodeResolveAttemptRejectsNonHexDigests(t *testing.T) {
	for _, field := range []string{"manifest", "audience"} {
		for _, digest := range []string{"sha256:" + strings.Repeat("g", 64), "sha256:" + strings.Repeat("A", 64)} {
			input := application.ResolveAttemptInput{ExecutionToken: "execution-proof", ManifestID: "manifest", ManifestDigest: "sha256:" + strings.Repeat("a", 64), Uses: []application.CredentialUse{{CredentialID: "crd_" + strings.Repeat("a", 32), Purpose: "api_key", AudienceDigest: "sha256:" + strings.Repeat("b", 64)}}}
			if field == "manifest" {
				input.ManifestDigest = digest
			} else {
				input.Uses[0].AudienceDigest = digest
			}
			raw, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := application.DecodeResolveAttempt(raw); !errors.Is(err, domain.ErrCredentialInput) {
				t.Fatalf("noncanonical %s digest accepted: %v", field, err)
			}
		}
	}
}

type beforeProfileTransaction struct {
	application.CredentialStore
	before func()
}

func (s beforeProfileTransaction) WithinProfile(ctx context.Context, tenant, profile string, fn func(application.CredentialTransaction) error) error {
	s.before()
	return s.CredentialStore.WithinProfile(ctx, tenant, profile, fn)
}

func TestResolveForAttemptRechecksAuthorizationAfterProfileLockWait(t *testing.T) {
	for _, change := range []string{"revoked", "epoch", "allowed-uses"} {
		t.Run(change, func(t *testing.T) {
			h := newCredentialHarness(t)
			h.save(t, modelCredentialCommand(1, "seed", "private-value"))
			publishCredentialHarness(t, h)
			command, verifier := executionForHarness(h, credentialUses(h))
			spy := &credentialCipherObserver{inner: h.cipher}
			store := beforeProfileTransaction{CredentialStore: h.store, before: func() {
				switch change {
				case "revoked":
					verifier.err = application.ErrExecutionUnauthorized
				case "epoch":
					verifier.auth.LeaseEpoch++
				case "allowed-uses":
					verifier.auth.AllowedUses = nil
				}
			}}
			service := application.NewService(application.Dependencies{Store: h.store.base, Credentials: store, Cipher: spy,
				OwnerAccess: h.owners, ExecutionVerifier: verifier,
				TenantAccess: accessStub{members: map[string]bool{"tnt_a/usr_owner": true}},
				NewProfileID: func() (string, error) { return "unused", nil }, NewRevisionID: func() (string, error) { return "unused", nil },
				NewCredentialID: func() (string, error) { return "unused", nil }, Now: func() time.Time { return h.now }})
			batch, err := service.ResolveForAttempt(context.Background(), command)
			if !errors.Is(err, application.ErrExecutionUnauthorized) || len(batch.Credentials) != 0 || spy.decryptCalls != 0 {
				t.Fatalf("stale authorization reached decryption: err=%v decrypts=%d", err, spy.decryptCalls)
			}
		})
	}
}

func TestResolveForAttemptClassifiesDependencyFailuresBeforeAndAfterLock(t *testing.T) {
	for _, afterLock := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-lock", true: "after-lock"}[afterLock], func(t *testing.T) {
			h := newCredentialHarness(t)
			h.save(t, modelCredentialCommand(1, "seed", "private-value"))
			publishCredentialHarness(t, h)
			command, verifier := executionForHarness(h, credentialUses(h))
			spy := &credentialCipherObserver{inner: h.cipher}
			var store application.CredentialStore = h.store
			if afterLock {
				store = beforeProfileTransaction{CredentialStore: h.store, before: func() { verifier.err = errors.New("transport secret canary") }}
			} else {
				verifier.err = errors.New("transport secret canary")
			}
			service := application.NewService(application.Dependencies{Store: h.store.base, Credentials: store, Cipher: spy, OwnerAccess: h.owners, ExecutionVerifier: verifier, TenantAccess: accessStub{members: map[string]bool{"tnt_a/usr_owner": true}}, NewProfileID: func() (string, error) { return "unused", nil }, NewRevisionID: func() (string, error) { return "unused", nil }, NewCredentialID: func() (string, error) { return "unused", nil }, Now: func() time.Time { return h.now }})
			batch, err := service.ResolveForAttempt(context.Background(), command)
			if !errors.Is(err, application.ErrExecutionDependencyUnavailable) || errors.Is(err, application.ErrExecutionUnauthorized) || len(batch.Credentials) != 0 || spy.decryptCalls != 0 || strings.Contains(err.Error(), "canary") {
				t.Fatalf("dependency misclassified: %v", err)
			}
		})
	}
}
