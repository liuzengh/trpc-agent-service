package application

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

func TestPublicCredentialFieldCheckExaminesKeysNotUserText(t *testing.T) {
	allowed := json.RawMessage(`{
		"instruction":"Explain the credential_id, purpose, value, status, and crd_example concepts without resolving any secret.",
		"resources":{"models":{"primary":{"credential_present":true}}}
	}`)
	if publicJSONContainsCredentialID(allowed) {
		t.Fatal("credential field check rejected ordinary user-controlled text")
	}

	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"credential_id":"crd_00000000000000000000000000000001"}`),
		json.RawMessage(`{"nested":{"api_key_credential_id":"crd_00000000000000000000000000000001"}}`),
		json.RawMessage(`{"nested":[{"audience_digest":"sha256:deadbeef"}]}`),
		json.RawMessage(`{"credential":{"purpose":"api_key"}}`),
		json.RawMessage(`{"credential_revision":1}`),
		json.RawMessage(`{"configured":true}`),
		json.RawMessage(`{"status":"active"}`),
		json.RawMessage(`{"ciphertext":"redacted"}`),
		json.RawMessage(`not-json`),
	} {
		if !publicJSONContainsCredentialID(raw) {
			t.Fatalf("credential field check accepted private shape: %s", raw)
		}
	}
}

func TestVerifyPublishedRejectsDigestValidCrossRecordManifestContent(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	if _, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner",
		IdempotencyKey: "publish-integrity", Input: deploymentInput(),
	}); err != nil {
		t.Fatal(err)
	}
	var stored domain.PublishedRevision
	for _, value := range h.store.published {
		stored = value
		break
	}
	if stored.Revision.ID == "" {
		t.Fatal("published fixture is missing")
	}

	tests := []struct {
		name   string
		mutate func(*domain.ManifestContent)
	}{
		{"tenant", func(content *domain.ManifestContent) { content.TenantID = "tenant-other" }},
		{"agent id", func(content *domain.ManifestContent) { content.Sources.Agent.AgentID = "agent-other" }},
		{"agent version number", func(content *domain.ManifestContent) { content.Sources.Agent.VersionNumber++ }},
		{"profile id", func(content *domain.ManifestContent) { content.Sources.Profile.ProfileID = "profile-other" }},
		{"profile revision number", func(content *domain.ManifestContent) { content.Sources.Profile.RevisionNumber++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := stored
			content, err := domain.DecodeManifestContent(value.Manifest.Content)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(&content)
			_, raw, digest, err := domain.CanonicalizeManifest(content)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := domain.ValidateManifestContent(raw, digest); err != nil {
				t.Fatalf("tampered content is not independently valid: %v", err)
			}
			value.Manifest.Content = raw
			value.Manifest.ContentDigest = digest
			value.ManifestDigest = digest
			value.ManifestView = nil
			if _, err := verifyPublished(value); !errors.Is(err, ErrPublicationIntegrity) {
				t.Fatalf("verifyPublished() error = %v, want %v", err, ErrPublicationIntegrity)
			}
		})
	}
}
