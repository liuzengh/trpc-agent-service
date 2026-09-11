package application_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestProfileCredentialCheckerChecksMetadataWithoutDecrypting(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed", "private-value"))
	publishCredentialHarness(t, h)
	uses := credentialUses(h)
	spy := &credentialCipherObserver{inner: h.cipher}
	service := credentialConsumerService(h, nil, spy)
	// Corrupt but nonempty ciphertext proves this boundary does not authenticate
	// or decrypt secret values while checking configured state and ownership.
	record := h.store.records[uses[0].CredentialID]
	record.Ciphertext = []byte("invalid-ciphertext-for-metadata-only-test")
	h.store.records[record.ID] = record
	command := application.CheckProfileCredentialsCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_member", ProfileRevisionNumber: 1, Uses: uses}
	if err := service.CheckUsable(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if len(spy.decrypted) != 0 || spy.decryptCalls != 0 {
		t.Fatal("deployment checker decrypted credential material")
	}
	record.Status = domain.CredentialCleared
	record.Ciphertext = nil
	h.store.records[record.ID] = record
	if err := service.CheckUsable(context.Background(), command); !errors.Is(err, domain.ErrCredentialUnavailable) {
		t.Fatalf("checker accepted cleared credential: %v", err)
	}
}

func TestProfileCredentialCheckerRejectsForgedAndCrossScopeUses(t *testing.T) {
	for _, mode := range []string{"purpose", "audience", "unknown-id", "duplicate", "tenant", "profile", "no-member"} {
		t.Run(mode, func(t *testing.T) {
			h := newCredentialHarness(t)
			h.save(t, modelCredentialCommand(1, "seed", "private-value"))
			publishCredentialHarness(t, h)
			uses := credentialUses(h)
			command := application.CheckProfileCredentialsCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ProfileRevisionNumber: 1, Uses: uses}
			want := domain.ErrCredentialAssociation
			switch mode {
			case "purpose":
				command.Uses[0].Purpose = "bearer_token"
			case "audience":
				command.Uses[0].AudienceDigest = "sha256:" + strings.Repeat("0", 64)
			case "unknown-id":
				command.Uses[0].CredentialID = "crd_ffffffffffffffffffffffffffffffff"
			case "duplicate":
				command.Uses = append(command.Uses, command.Uses[0])
			case "tenant", "profile":
				record := h.store.records[uses[0].CredentialID]
				if mode == "tenant" {
					record.TenantID = "tnt_b"
				} else {
					record.ProfileID = "rpf_b"
				}
				h.store.records[record.ID] = record
			case "no-member":
				command.ActorUserID = "usr_outsider"
				want = application.ErrTenantForbidden
			}
			if err := h.service.CheckUsable(context.Background(), command); !errors.Is(err, want) {
				t.Fatalf("forged %s: got %v want %v", mode, err, want)
			}
		})
	}
}

func TestResolveForAttemptRequiresExplicitCurrentAuthorization(t *testing.T) {
	for _, mode := range []string{"no-verifier", "revoked-lease", "expired", "zero-epoch", "missing-run", "missing-attempt", "wrong-workload", "wrong-manifest", "wrong-digest", "missing-token", "changed-use", "undeclared-resource"} {
		t.Run(mode, func(t *testing.T) {
			h := twoCredentialHarness(t)
			uses := credentialUses(h)
			command, verifier := executionForHarness(h, uses[:1])
			var port application.ExecutionAuthorizationVerifier = verifier
			switch mode {
			case "no-verifier":
				port = nil
			case "revoked-lease":
				verifier.err = application.ErrExecutionUnauthorized
			case "expired":
				verifier.auth.ExpiresAt = h.now
			case "zero-epoch":
				verifier.auth.LeaseEpoch = 0
			case "missing-run":
				verifier.auth.RunID = ""
			case "missing-attempt":
				verifier.auth.AttemptID = ""
			case "wrong-workload":
				verifier.auth.WorkerID = "other-worker"
			case "wrong-manifest":
				verifier.auth.ManifestID = "other-manifest"
			case "wrong-digest":
				verifier.auth.ManifestDigest = "other-manifest-digest"
			case "missing-token":
				command.Authorization.ExecutionToken = ""
			case "changed-use":
				command.Uses[0].Purpose = "bearer_token"
			case "undeclared-resource":
				command.Uses = uses
			}
			spy := &credentialCipherObserver{inner: h.cipher}
			service := credentialConsumerService(h, port, spy)
			result, err := service.ResolveForAttempt(context.Background(), command)
			if !errors.Is(err, application.ErrExecutionUnauthorized) || !reflect.DeepEqual(result, application.CredentialBatch{}) {
				t.Fatalf("%s authorization accepted or returned batch: %v", mode, err)
			}
			if spy.decryptCalls != 0 {
				t.Fatal("authorization failure reached decryption")
			}
		})
	}
}

func TestResolveForAttemptRechecksCurrentLeaseWithSameToken(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed", "private-value"))
	publishCredentialHarness(t, h)
	command, verifier := executionForHarness(h, credentialUses(h))
	service := credentialConsumerService(h, verifier, h.cipher)
	batch, err := service.ResolveForAttempt(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Clear()
	verifier.err = application.ErrExecutionUnauthorized
	if result, err := service.ResolveForAttempt(context.Background(), command); !errors.Is(err, application.ErrExecutionUnauthorized) || len(result.Credentials) != 0 {
		t.Fatalf("unchanged token bypassed current lease verification: %v", err)
	}
	if verifier.calls != 3 {
		t.Fatal("resolver reused authorization without querying the verifier")
	}
}

func TestResolveForAttemptDecryptsAllOrNothingAndClearsTemporaryValues(t *testing.T) {
	for _, mode := range []string{"second-ciphertext-invalid", "commit-failure", "canceled-after-first-value"} {
		t.Run(mode, func(t *testing.T) {
			h := twoCredentialHarness(t)
			uses := credentialUses(h)
			command, verifier := executionForHarness(h, uses)
			spy := &credentialCipherObserver{inner: h.cipher}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "second-ciphertext-invalid":
				record := h.store.records[uses[1].CredentialID]
				record.Ciphertext[len(record.Ciphertext)-1] ^= 0xff
				h.store.records[record.ID] = record
			case "commit-failure":
				h.store.failAt = "commit"
			case "canceled-after-first-value":
				spy.afterDecrypt = func(count int) {
					if count == 1 {
						cancel()
					}
				}
			}
			service := credentialConsumerService(h, verifier, spy)
			result, err := service.ResolveForAttempt(ctx, command)
			if err == nil || !reflect.DeepEqual(result, application.CredentialBatch{}) {
				t.Fatal("failed batch returned partial material")
			}
			if len(spy.decrypted) == 0 {
				t.Fatal("test did not exercise cleanup after a successful decryption")
			}
			for _, value := range spy.decrypted {
				if !bytes.Equal(value, make([]byte, len(value))) {
					t.Fatal("temporary decrypted slice was not cleared after batch failure")
				}
			}
			if strings.Contains(err.Error(), "private-") {
				t.Fatal("resolver error disclosed supplied value")
			}
		})
	}
}

func TestResolveForAttemptNewBatchObservesRotationWithoutMutatingOldBatch(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed", "old-private-value"))
	revision := publishCredentialHarness(t, h)
	uses := credentialUses(h)
	command, verifier := executionForHarness(h, uses)
	service := credentialConsumerService(h, verifier, h.cipher)
	oldBatch, err := service.ResolveForAttempt(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	defer oldBatch.Clear()
	if len(oldBatch.Credentials) != 1 || string(oldBatch.Credentials[0].Value) != "old-private-value" || oldBatch.Credentials[0].CredentialRevision != 1 {
		t.Fatal("initial attempt batch has wrong value/version")
	}
	rotate := liveModelCommand(t, h, "rotate-for-new-attempt", 1, "new-private-value")
	if _, err := h.service.UpdateUsedProfileCredential(context.Background(), rotate); err != nil {
		t.Fatal(err)
	}
	verifier.auth.AttemptID = "attempt-2"
	verifier.auth.LeaseEpoch = 2
	newBatch, err := service.ResolveForAttempt(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	defer newBatch.Clear()
	if newBatch.AttemptID != "attempt-2" || newBatch.LeaseEpoch != 2 || string(newBatch.Credentials[0].Value) != "new-private-value" || newBatch.Credentials[0].CredentialRevision != 2 {
		t.Fatal("new attempt did not receive current value with trusted attempt identity")
	}
	if string(oldBatch.Credentials[0].Value) != "old-private-value" || !reflect.DeepEqual(revision, h.store.base.revisions[resourceKey("tnt_a", "rpf_a")][0]) {
		t.Fatal("rotation mutated old initialized batch or immutable profile")
	}
	clearCommand := liveModelCommand(t, h, "clear-for-future", 2, "")
	clearCommand.Update.Action = "clear"
	clearCommand.Update.Value = nil
	if _, err := h.service.UpdateUsedProfileCredential(context.Background(), clearCommand); err != nil {
		t.Fatal(err)
	}
	verifier.auth.AttemptID = "attempt-3"
	verifier.auth.LeaseEpoch = 3
	if result, err := service.ResolveForAttempt(context.Background(), command); !errors.Is(err, domain.ErrCredentialUnavailable) || len(result.Credentials) != 0 {
		t.Fatalf("new attempt used cleared credential: %v", err)
	}
	if string(newBatch.Credentials[0].Value) != "new-private-value" {
		t.Fatal("clear altered an already initialized in-memory batch")
	}
	retained := newBatch.Credentials[0].Value
	newBatch.Clear()
	if newBatch.Credentials[0].Value != nil || !bytes.Equal(retained, make([]byte, len(retained))) {
		t.Fatal("batch Clear did not erase and detach plaintext")
	}
}

func TestResolveForAttemptRejectsUsesOutsideSelectedProfileRevision(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed", "private-value"))
	publishCredentialHarness(t, h)
	command, verifier := executionForHarness(h, credentialUses(h))
	fakeUse := application.CredentialUse{CredentialID: "crd_ffffffffffffffffffffffffffffffff", Purpose: "api_key", AudienceDigest: command.Uses[0].AudienceDigest}
	command.Uses = []application.CredentialUse{fakeUse}
	verifier.auth.AllowedUses = []application.CredentialUse{fakeUse}
	service := credentialConsumerService(h, verifier, h.cipher)
	if batch, err := service.ResolveForAttempt(context.Background(), command); !errors.Is(err, domain.ErrCredentialAssociation) || len(batch.Credentials) != 0 {
		t.Fatalf("verifier allowance bypassed selected ProfileRevision association: %v", err)
	}
}

// This verifier exists ONLY in tests. It checks the complete expected workload
// request and returns explicitly seeded trusted state; no production fallback,
// request-to-tenant inference, or locally self-signed authorization is involved.
type explicitExecutionVerifier struct {
	expected application.ExecutionAuthorizationRequest
	auth     application.ExecutionAuthorization
	err      error
	calls    int
}

func (v *explicitExecutionVerifier) VerifyAttempt(_ context.Context, request application.ExecutionAuthorizationRequest) (application.ExecutionAuthorization, error) {
	v.calls++
	if v.err != nil {
		return application.ExecutionAuthorization{}, v.err
	}
	if request != v.expected {
		return application.ExecutionAuthorization{}, errors.New("workload request not authorized by test attempt owner")
	}
	auth := v.auth
	auth.AllowedUses = append([]application.CredentialUse(nil), v.auth.AllowedUses...)
	return auth, nil
}

// Observer delegates every cryptographic operation to the actual adapter. It
// retains returned slice references only to test the consumer's failure cleanup.
type credentialCipherObserver struct {
	inner        application.CredentialCipher
	decryptCalls int
	decrypted    [][]byte
	afterDecrypt func(int)
}

func (s *credentialCipherObserver) Encrypt(ctx context.Context, aad, value []byte) ([]byte, error) {
	return s.inner.Encrypt(ctx, aad, value)
}
func (s *credentialCipherObserver) Decrypt(ctx context.Context, aad, value []byte) ([]byte, error) {
	s.decryptCalls++
	result, err := s.inner.Decrypt(ctx, aad, value)
	if err == nil {
		s.decrypted = append(s.decrypted, result)
		if s.afterDecrypt != nil {
			s.afterDecrypt(s.decryptCalls)
		}
	}
	return result, err
}
func (s *credentialCipherObserver) MAC(purpose string, payload []byte) string {
	return s.inner.MAC(purpose, payload)
}

func credentialConsumerService(h *credentialHarness, verifier application.ExecutionAuthorizationVerifier, cipher application.CredentialCipher) *application.Service {
	return application.NewService(application.Dependencies{
		Store: h.store.base, Credentials: h.store, Cipher: cipher, OwnerAccess: h.owners, ExecutionVerifier: verifier,
		TenantAccess: accessStub{members: map[string]bool{"tnt_a/usr_owner": true, "tnt_a/usr_member": true}},
		NewProfileID: func() (string, error) { return "rpf_unused", nil }, NewRevisionID: func() (string, error) { return "rpr_unused", nil }, NewCredentialID: func() (string, error) { return "crd_ffffffffffffffffffffffffffffffff", nil }, Now: func() time.Time { return h.now },
	})
}
func credentialUses(h *credentialHarness) []application.CredentialUse {
	uses := make([]application.CredentialUse, 0, len(h.store.records))
	for _, record := range h.store.records {
		uses = append(uses, application.CredentialUse{CredentialID: record.ID, Purpose: record.Purpose, AudienceDigest: record.AudienceDigest})
	}
	sort.Slice(uses, func(i, j int) bool { return uses[i].CredentialID < uses[j].CredentialID })
	return uses
}
func executionForHarness(h *credentialHarness, uses []application.CredentialUse) (application.ResolveAttemptCommand, *explicitExecutionVerifier) {
	request := application.ExecutionAuthorizationRequest{WorkloadIdentity: "worker-1", ExecutionToken: "test-only-trusted-run-assertion", ManifestID: "manifest-1", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}
	auth := application.ExecutionAuthorization{TenantID: "tnt_a", ProfileID: "rpf_a", ProfileRevisionNumber: 1, RunID: "run-1", AttemptID: "attempt-1", WorkerID: "worker-1", LeaseEpoch: 1, ExpiresAt: h.now.Add(time.Hour), ManifestID: request.ManifestID, ManifestDigest: request.ManifestDigest, AllowedUses: append([]application.CredentialUse(nil), uses...)}
	return application.ResolveAttemptCommand{Authorization: request, Uses: append([]application.CredentialUse(nil), uses...)}, &explicitExecutionVerifier{expected: request, auth: auth}
}
func twoCredentialHarness(t *testing.T) *credentialHarness {
	t.Helper()
	h := newCredentialHarness(t)
	command := modelCredentialCommand(1, "seed-two", "primary-private-value")
	command.Write.Config.Models["secondary"] = command.Write.Config.Models["primary"]
	second := "secondary-private-value"
	command.Write.Credentials["models"]["secondary"] = map[string]application.CredentialAction{"api_key": {Action: "replace", Value: &second}}
	h.save(t, command)
	publishCredentialHarness(t, h)
	return h
}
