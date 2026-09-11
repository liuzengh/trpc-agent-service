package postgresadapter

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

type deploymentRecord struct {
	ID                   string    `json:"id"`
	TenantID             string    `json:"tenant_id"`
	Name                 string    `json:"name"`
	Description          string    `json:"description"`
	MetadataRevision     int64     `json:"metadata_revision"`
	LatestRevisionNumber *int64    `json:"latest_revision_number"`
	CreatedBy            string    `json:"created_by"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func (record deploymentRecord) domain() domain.Deployment {
	return domain.Deployment{
		ID: record.ID, TenantID: record.TenantID, Name: record.Name,
		Description: record.Description, MetadataRevision: record.MetadataRevision,
		LatestRevisionNumber: record.LatestRevisionNumber, CreatedBy: record.CreatedBy,
		CreatedAt: record.CreatedAt.UTC(), UpdatedAt: record.UpdatedAt.UTC(),
	}
}

type revisionSummaryRecord struct {
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

func (record revisionSummaryRecord) domain() domain.DeploymentRevisionSummary {
	return domain.DeploymentRevisionSummary{
		ID: record.ID, TenantID: record.TenantID, DeploymentID: record.DeploymentID,
		RevisionNumber: record.RevisionNumber, SchemaVersion: record.SchemaVersion,
		AgentID: record.AgentID, AgentVersionNumber: record.AgentVersionNumber,
		ProfileID: record.ProfileID, ProfileRevisionNumber: record.ProfileRevisionNumber,
		InputDigest: record.InputDigest, ManifestID: record.ManifestID,
		ManifestDigest: record.ManifestDigest, PublishedBy: record.PublishedBy,
		PublishedAt: record.PublishedAt.UTC(),
	}
}

func scanDeployment(row interface{ Scan(...any) error }) (domain.Deployment, error) {
	var value domain.Deployment
	err := row.Scan(
		&value.ID, &value.TenantID, &value.Name, &value.Description,
		&value.MetadataRevision, &value.LatestRevisionNumber, &value.CreatedBy,
		&value.CreatedAt, &value.UpdatedAt,
	)
	if err == nil {
		value.CreatedAt = value.CreatedAt.UTC()
		value.UpdatedAt = value.UpdatedAt.UTC()
	}
	return value, err
}

func scanReceipt(row interface{ Scan(...any) error }) (domain.CommandReceipt, error) {
	var value domain.CommandReceipt
	err := row.Scan(
		&value.TenantID, &value.Operation, &value.ScopeID, &value.KeyHash,
		&value.RequestDigest, &value.DeploymentID, &value.DeploymentRevisionID,
		&value.RuntimeManifestID, &value.Result, &value.CreatedBy, &value.CreatedAt,
	)
	if err == nil {
		value.CreatedAt = value.CreatedAt.UTC()
	}
	return value, err
}

func scanPublishedRevision(row interface{ Scan(...any) error }) (domain.PublishedRevision, error) {
	var value domain.PublishedRevision
	var inputJSON json.RawMessage
	var agentID, profileID string
	var agentVersionNumber, profileRevisionNumber int64
	err := row.Scan(
		&value.Revision.ID, &value.Revision.TenantID,
		&value.Revision.DeploymentID, &value.Revision.RevisionNumber,
		&value.Revision.SchemaVersion, &inputJSON, &value.Revision.InputDigest,
		&agentID, &value.Revision.AgentVersionID, &agentVersionNumber,
		&value.Revision.AgentSchemaVersion, &value.Revision.AgentSpecDigest,
		&profileID, &value.Revision.ProfileRevisionID, &profileRevisionNumber,
		&value.Revision.ProfileSchemaVersion, &value.Revision.ProfileSpecDigest,
		&value.Revision.PublishedBy, &value.Revision.PublishedAt,
		&value.Manifest.ID, &value.Manifest.SchemaVersion,
		&value.Manifest.CompilerVersion, &value.Manifest.RuntimeContractVersion,
		&value.Manifest.Content, &value.Manifest.ContentDigest,
		&value.Manifest.PublishedAt,
	)
	if err != nil {
		return domain.PublishedRevision{}, err
	}
	// LEFT JOIN preserves a revision whose required one-to-one Manifest is
	// missing. That is stored publication corruption, not a missing revision.
	if value.Manifest.ID == "" {
		return domain.PublishedRevision{}, application.ErrPublicationIntegrity
	}
	value.Revision.PublishedAt = value.Revision.PublishedAt.UTC()
	value.Manifest.PublishedAt = value.Manifest.PublishedAt.UTC()
	if err := json.Unmarshal(inputJSON, &value.Revision.Input); err != nil {
		return domain.PublishedRevision{}, fmt.Errorf("decode deployment revision input: %w", err)
	}
	if value.Revision.Input.SchemaVersion != value.Revision.SchemaVersion ||
		value.Revision.Input.Agent.AgentID != agentID ||
		value.Revision.Input.Agent.VersionNumber != agentVersionNumber ||
		value.Revision.Input.Profile.ProfileID != profileID ||
		value.Revision.Input.Profile.RevisionNumber != profileRevisionNumber {
		return domain.PublishedRevision{}, application.ErrPublicationIntegrity
	}
	value.Revision.CanonicalInput = append(json.RawMessage(nil), inputJSON...)
	value.Manifest.TenantID = value.Revision.TenantID
	value.Manifest.DeploymentID = value.Revision.DeploymentID
	value.Manifest.DeploymentRevisionID = value.Revision.ID
	value.Manifest.RevisionNumber = value.Revision.RevisionNumber
	value.ManifestID = value.Manifest.ID
	value.ManifestDigest = value.Manifest.ContentDigest
	return value, nil
}
