package postgresadapter

import (
	"encoding/json"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

type profileRecord struct {
	ID                   string    `json:"id"`
	TenantID             string    `json:"tenant_id"`
	Name                 string    `json:"name"`
	Description          string    `json:"description"`
	LatestRevisionNumber *int64    `json:"latest_revision_number"`
	CreatedBy            string    `json:"created_by"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func (record profileRecord) domain() domain.RuntimeProfile {
	return domain.RuntimeProfile{
		ID: record.ID, TenantID: record.TenantID, Name: record.Name,
		Description: record.Description, LatestRevisionNumber: record.LatestRevisionNumber,
		CreatedBy: record.CreatedBy, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	}
}

type revisionRecord struct {
	ID                  string          `json:"id"`
	TenantID            string          `json:"tenant_id"`
	ProfileID           string          `json:"profile_id"`
	RevisionNumber      int64           `json:"revision_number"`
	SourceDraftRevision int64           `json:"source_draft_revision"`
	SchemaVersion       string          `json:"schema_version"`
	Spec                json.RawMessage `json:"spec"`
	SpecDigest          string          `json:"spec_digest"`
	PublishedBy         string          `json:"published_by"`
	PublishedAt         time.Time       `json:"published_at"`
}

func (record revisionRecord) domain() domain.ProfileRevision {
	return domain.ProfileRevision{
		ID: record.ID, TenantID: record.TenantID, ProfileID: record.ProfileID,
		RevisionNumber:      record.RevisionNumber,
		SourceDraftRevision: record.SourceDraftRevision,
		SchemaVersion:       record.SchemaVersion,
		Spec:                append(json.RawMessage(nil), record.Spec...),
		SpecDigest:          record.SpecDigest, PublishedBy: record.PublishedBy,
		PublishedAt: record.PublishedAt,
	}
}

type revisionSummaryRecord struct {
	ID                  string    `json:"id"`
	TenantID            string    `json:"tenant_id"`
	ProfileID           string    `json:"profile_id"`
	RevisionNumber      int64     `json:"revision_number"`
	SourceDraftRevision int64     `json:"source_draft_revision"`
	SchemaVersion       string    `json:"schema_version"`
	SpecDigest          string    `json:"spec_digest"`
	PublishedBy         string    `json:"published_by"`
	PublishedAt         time.Time `json:"published_at"`
}

func (record revisionSummaryRecord) domain() domain.ProfileRevisionSummary {
	return domain.ProfileRevisionSummary{
		ID: record.ID, TenantID: record.TenantID, ProfileID: record.ProfileID,
		RevisionNumber: record.RevisionNumber, SourceDraftRevision: record.SourceDraftRevision,
		SchemaVersion: record.SchemaVersion, SpecDigest: record.SpecDigest,
		PublishedBy: record.PublishedBy, PublishedAt: record.PublishedAt,
	}
}

func scanRevision(row interface{ Scan(...any) error }) (domain.ProfileRevision, error) {
	var revision domain.ProfileRevision
	err := row.Scan(
		&revision.ID, &revision.TenantID, &revision.ProfileID,
		&revision.RevisionNumber, &revision.SourceDraftRevision,
		&revision.SchemaVersion, &revision.Spec, &revision.SpecDigest,
		&revision.PublishedBy, &revision.PublishedAt,
	)
	return revision, err
}
