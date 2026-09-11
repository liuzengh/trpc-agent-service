package domain

import (
	"encoding/json"
	"errors"
	"time"
)

const (
	SchemaVersionV1       = "v1"
	EventSchemaVersionV1  = "v1"
	ManifestPublishedType = "RuntimeManifestPublished.v1"
)

var ErrInvalidPublication = errors.New("invalid deployment publication")

// DeploymentInput contains only the two fixed source selections. Environment,
// binding maps, latest selectors and worker options are deliberately absent.
type DeploymentInput struct {
	SchemaVersion string       `json:"schema_version"`
	Agent         AgentInput   `json:"agent"`
	Profile       ProfileInput `json:"profile"`
}

type AgentInput struct {
	AgentID       string `json:"agent_id"`
	VersionNumber int64  `json:"version_number"`
}

type ProfileInput struct {
	ProfileID      string `json:"profile_id"`
	RevisionNumber int64  `json:"revision_number"`
}

func (i DeploymentInput) Validate() error {
	if i.SchemaVersion != SchemaVersionV1 || i.Agent.AgentID == "" ||
		i.Agent.VersionNumber <= 0 || i.Profile.ProfileID == "" ||
		i.Profile.RevisionNumber <= 0 {
		return ErrInvalidPublication
	}
	return nil
}

// DeploymentRevision is one immutable publication fact. CanonicalInput is
// retained for integrity checking; Input is its typed representation.
type DeploymentRevision struct {
	ID                   string          `json:"id"`
	TenantID             string          `json:"tenant_id"`
	DeploymentID         string          `json:"deployment_id"`
	RevisionNumber       int64           `json:"revision_number"`
	SchemaVersion        string          `json:"schema_version"`
	Input                DeploymentInput `json:"input"`
	CanonicalInput       json.RawMessage `json:"-"`
	InputDigest          string          `json:"input_digest"`
	AgentVersionID       string          `json:"agent_version_id"`
	AgentSchemaVersion   string          `json:"agent_schema_version"`
	AgentSpecDigest      string          `json:"agent_spec_digest"`
	ProfileRevisionID    string          `json:"profile_revision_id"`
	ProfileSchemaVersion string          `json:"profile_schema_version"`
	ProfileSpecDigest    string          `json:"profile_spec_digest"`
	PublishedBy          string          `json:"published_by"`
	PublishedAt          time.Time       `json:"published_at"`
}

// RuntimeManifest is the immutable envelope around canonical manifest content.
type RuntimeManifest struct {
	ID                     string          `json:"manifest_id"`
	TenantID               string          `json:"tenant_id"`
	DeploymentID           string          `json:"deployment_id"`
	DeploymentRevisionID   string          `json:"deployment_revision_id"`
	RevisionNumber         int64           `json:"revision_number"`
	SchemaVersion          string          `json:"-"`
	CompilerVersion        string          `json:"-"`
	RuntimeContractVersion string          `json:"-"`
	Content                json.RawMessage `json:"content"`
	ContentDigest          string          `json:"content_digest"`
	PublishedAt            time.Time       `json:"published_at"`
}

// PublishedRevision is the full trusted read model. ManifestView is derived
// once from the internal manifest and contains no credential identifiers.
type PublishedRevision struct {
	Revision       DeploymentRevision `json:"revision"`
	Manifest       RuntimeManifest    `json:"-"`
	ManifestID     string             `json:"manifest_id"`
	ManifestDigest string             `json:"manifest_digest"`
	ManifestView   json.RawMessage    `json:"manifest_view"`
}

type DeploymentRevisionSummary struct {
	ID                    string    `json:"id"`
	TenantID              string    `json:"tenant_id"`
	DeploymentID          string    `json:"deployment_id"`
	RevisionNumber        int64     `json:"revision_number"`
	SchemaVersion         string    `json:"schema_version"`
	AgentID               string    `json:"agent_id"`
	AgentVersionNumber    int64     `json:"agent_version_number"`
	ProfileID             string    `json:"profile_id"`
	ProfileRevisionNumber int64     `json:"profile_revision_number"`
	InputDigest           string    `json:"input_digest"`
	ManifestID            string    `json:"manifest_id"`
	ManifestDigest        string    `json:"manifest_digest"`
	PublishedBy           string    `json:"published_by"`
	PublishedAt           time.Time `json:"published_at"`
}

type CommandReceipt struct {
	TenantID             string          `json:"tenant_id"`
	Operation            string          `json:"operation"`
	ScopeID              string          `json:"scope_id"`
	KeyHash              string          `json:"key_hash"`
	RequestDigest        string          `json:"request_digest"`
	DeploymentID         string          `json:"deployment_id"`
	DeploymentRevisionID string          `json:"deployment_revision_id,omitempty"`
	RuntimeManifestID    string          `json:"runtime_manifest_id,omitempty"`
	Result               json.RawMessage `json:"result"`
	CreatedBy            string          `json:"created_by"`
	CreatedAt            time.Time       `json:"created_at"`
}

type OutboxEvent struct {
	ID                string          `json:"event_id"`
	TenantID          string          `json:"tenant_id"`
	AggregateType     string          `json:"aggregate_type"`
	AggregateID       string          `json:"aggregate_id"`
	AggregateRevision int64           `json:"aggregate_revision"`
	EventType         string          `json:"event_type"`
	SchemaVersion     string          `json:"schema_version"`
	Payload           json.RawMessage `json:"payload"`
	PayloadDigest     string          `json:"payload_digest"`
	CreatedAt         time.Time       `json:"created_at"`
}

// RuntimeManifestPublishedEvent is the complete, immutable Control Outbox
// payload. It intentionally contains the internal manifest (credential IDs and
// audiences) needed by a trusted runtime projection, but never secret values.
type RuntimeManifestPublishedEvent struct {
	SchemaVersion        string          `json:"schema_version"`
	EventType            string          `json:"event_type"`
	EventID              string          `json:"event_id"`
	TenantID             string          `json:"tenant_id"`
	DeploymentID         string          `json:"deployment_id"`
	DeploymentRevisionID string          `json:"deployment_revision_id"`
	RevisionNumber       int64           `json:"revision_number"`
	OccurredAt           time.Time       `json:"occurred_at"`
	Manifest             RuntimeManifest `json:"manifest"`
}

// ValidateManifestPublicationBinding proves that an independently stored
// Manifest Content belongs to the same immutable source publications and wire
// contract as its DeploymentRevision and RuntimeManifest envelopes.
func ValidateManifestPublicationBinding(
	content ManifestContent,
	revision DeploymentRevision,
	manifest RuntimeManifest,
) error {
	if content.SchemaVersion != revision.SchemaVersion ||
		content.SchemaVersion != manifest.SchemaVersion ||
		content.CompilerVersion != manifest.CompilerVersion ||
		content.RuntimeContractVersion != manifest.RuntimeContractVersion ||
		content.TenantID != revision.TenantID ||
		content.Sources.Agent.AgentID != revision.Input.Agent.AgentID ||
		content.Sources.Agent.VersionNumber != revision.Input.Agent.VersionNumber ||
		content.Sources.Agent.VersionID != revision.AgentVersionID ||
		content.Sources.Agent.SchemaVersion != revision.AgentSchemaVersion ||
		content.Sources.Agent.Digest != revision.AgentSpecDigest ||
		content.Sources.Profile.ProfileID != revision.Input.Profile.ProfileID ||
		content.Sources.Profile.RevisionNumber != revision.Input.Profile.RevisionNumber ||
		content.Sources.Profile.RevisionID != revision.ProfileRevisionID ||
		content.Sources.Profile.SchemaVersion != revision.ProfileSchemaVersion ||
		content.Sources.Profile.Digest != revision.ProfileSpecDigest {
		return ErrInvalidPublication
	}
	return nil
}
