package postgresadapter

import (
	"encoding/json"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

type agentRecord struct {
	ID                  string    `json:"id"`
	TenantID            string    `json:"tenant_id"`
	Name                string    `json:"name"`
	Description         string    `json:"description"`
	LatestVersionNumber *int64    `json:"latest_version_number"`
	CreatedBy           string    `json:"created_by"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func (record agentRecord) domain() domain.Agent {
	return domain.Agent{
		ID: record.ID, TenantID: record.TenantID, Name: record.Name,
		Description: record.Description, LatestVersionNumber: record.LatestVersionNumber,
		CreatedBy: record.CreatedBy, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	}
}

type versionRecord struct {
	ID                  string          `json:"id"`
	TenantID            string          `json:"tenant_id"`
	AgentID             string          `json:"agent_id"`
	VersionNumber       int64           `json:"version_number"`
	SourceDraftRevision int64           `json:"source_draft_revision"`
	SchemaVersion       string          `json:"schema_version"`
	Spec                json.RawMessage `json:"spec"`
	SpecDigest          string          `json:"spec_digest"`
	PublishedBy         string          `json:"published_by"`
	PublishedAt         time.Time       `json:"published_at"`
}

func (record versionRecord) domain() domain.AgentVersion {
	return domain.AgentVersion{
		ID: record.ID, TenantID: record.TenantID, AgentID: record.AgentID,
		VersionNumber: record.VersionNumber, SourceDraftRevision: record.SourceDraftRevision,
		SchemaVersion: record.SchemaVersion, Spec: append(json.RawMessage(nil), record.Spec...),
		SpecDigest: record.SpecDigest, PublishedBy: record.PublishedBy,
		PublishedAt: record.PublishedAt,
	}
}

func scanVersion(row interface{ Scan(...any) error }) (domain.AgentVersion, error) {
	var version domain.AgentVersion
	err := row.Scan(
		&version.ID, &version.TenantID, &version.AgentID, &version.VersionNumber,
		&version.SourceDraftRevision, &version.SchemaVersion, &version.Spec,
		&version.SpecDigest, &version.PublishedBy, &version.PublishedAt,
	)
	return version, err
}
