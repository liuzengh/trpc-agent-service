package application

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gowebpki/jcs"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

func validIdempotencyKey(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func keyHash(value string) string { return digestBytes([]byte(value)) }

func canonicalJSON(value any) (json.RawMessage, string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, "", fmt.Errorf("marshal canonical value: %w", err)
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		return nil, "", fmt.Errorf("canonicalize value: %w", err)
	}
	return append(json.RawMessage(nil), canonical...), digestBytes(canonical), nil
}

func requestDigest(operation, tenantID, scopeID string, body any) (string, error) {
	_, digest, err := canonicalJSON(struct {
		Operation string `json:"operation"`
		TenantID  string `json:"tenant_id"`
		ScopeID   string `json:"scope_id"`
		Body      any    `json:"body"`
	}{operation, tenantID, scopeID, body})
	return digest, err
}

func verifyReceipt(receipt domain.CommandReceipt, key CommandReceiptKey, request string) error {
	if receipt.TenantID != key.TenantID || receipt.Operation != key.Operation ||
		receipt.ScopeID != key.ScopeID || receipt.KeyHash != key.KeyHash ||
		receipt.RequestDigest == "" || len(receipt.Result) == 0 {
		return ErrPublicationIntegrity
	}
	if receipt.RequestDigest != request {
		return ErrIdempotencyConflict
	}
	return nil
}

func sameDigest(raw json.RawMessage, expected string) bool {
	canonical, err := jcs.Transform(raw)
	return err == nil && digestBytes(canonical) == expected
}

func closedJSONMatches(raw json.RawMessage, decoded any) bool {
	stored, err := jcs.Transform(raw)
	if err != nil {
		return false
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return false
	}
	canonical, err := jcs.Transform(encoded)
	return err == nil && bytes.Equal(stored, canonical)
}

func sameDeploymentMetadata(left, right domain.Deployment) bool {
	if left.ID != right.ID || left.TenantID != right.TenantID ||
		left.Name != right.Name || left.Description != right.Description ||
		left.MetadataRevision != right.MetadataRevision ||
		left.CreatedBy != right.CreatedBy ||
		!left.CreatedAt.Equal(right.CreatedAt) || !left.UpdatedAt.Equal(right.UpdatedAt) {
		return false
	}
	// Publication progresses independently of metadata CAS. A concurrent publish
	// may advance Latest between the initial read and the UPDATE RETURNING.
	return true
}

func verifyPublished(value domain.PublishedRevision) (domain.PublishedRevision, error) {
	r, m := value.Revision, value.Manifest
	if r.ID == "" || r.TenantID == "" || r.DeploymentID == "" || r.RevisionNumber <= 0 ||
		m.ID == "" || m.TenantID != r.TenantID || m.DeploymentID != r.DeploymentID ||
		m.DeploymentRevisionID != r.ID || m.RevisionNumber != r.RevisionNumber ||
		!m.PublishedAt.Equal(r.PublishedAt) || r.Input.Validate() != nil ||
		r.Input.Agent.AgentID == "" || r.Input.Profile.ProfileID == "" {
		return domain.PublishedRevision{}, fmt.Errorf("%w: envelope identity", ErrPublicationIntegrity)
	}
	r.PublishedAt = r.PublishedAt.UTC()
	m.PublishedAt = m.PublishedAt.UTC()
	value.Revision = r
	value.Manifest = m
	canonicalInput, _, err := canonicalJSON(r.Input)
	storedInput, transformErr := jcs.Transform(r.CanonicalInput)
	if err != nil || transformErr != nil || string(canonicalInput) != string(storedInput) || !sameDigest(r.CanonicalInput, r.InputDigest) {
		return domain.PublishedRevision{}, fmt.Errorf("%w: canonical input", ErrPublicationIntegrity)
	}
	content, err := domain.ValidateManifestContent(m.Content, m.ContentDigest)
	if err != nil {
		return domain.PublishedRevision{}, fmt.Errorf("%w: manifest digest", ErrPublicationIntegrity)
	}
	if err := domain.ValidateManifestPublicationBinding(content, r, m); err != nil {
		return domain.PublishedRevision{}, fmt.Errorf("%w: manifest publication binding", ErrPublicationIntegrity)
	}
	view := domain.NewPublicManifestView(content)
	encodedView, _, err := canonicalJSON(view)
	if err != nil {
		return domain.PublishedRevision{}, err
	}
	if len(value.ManifestView) > 0 {
		storedCanonical, err := jcs.Transform(value.ManifestView)
		if err != nil || string(storedCanonical) != string(encodedView) {
			return domain.PublishedRevision{}, fmt.Errorf("%w: manifest view", ErrPublicationIntegrity)
		}
	}
	if value.ManifestID != "" && value.ManifestID != m.ID {
		return domain.PublishedRevision{}, fmt.Errorf("%w: manifest id", ErrPublicationIntegrity)
	}
	if value.ManifestDigest != "" && value.ManifestDigest != m.ContentDigest {
		return domain.PublishedRevision{}, fmt.Errorf("%w: manifest digest reference", ErrPublicationIntegrity)
	}
	value.ManifestID = m.ID
	value.ManifestDigest = m.ContentDigest
	value.ManifestView = encodedView
	return value, nil
}

func publicJSONContainsCredentialID(raw json.RawMessage) bool {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return true
	}
	return containsPrivateCredentialField(value)
}

func containsPrivateCredentialField(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ToLower(key)
			if strings.Contains(normalized, "credential_id") ||
				normalized == "purpose" || normalized == "audience_digest" ||
				normalized == "value" || normalized == "ciphertext" ||
				normalized == "nonce" || normalized == "credential_revision" ||
				normalized == "association_token" || normalized == "configured" ||
				normalized == "status" {
				return true
			}
			if containsPrivateCredentialField(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsPrivateCredentialField(child) {
				return true
			}
		}
	}
	return false
}
