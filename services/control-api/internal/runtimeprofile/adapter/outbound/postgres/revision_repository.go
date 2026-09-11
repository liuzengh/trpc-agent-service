package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const revisionColumns = `
	id, tenant_id, profile_id, revision_number, source_draft_revision,
			 schema_version, spec_jsonb, spec_digest, published_by, published_at
`

func (s *Store) FindRevisionBySourceDraft(
	ctx context.Context,
	tenantID, profileID string,
	sourceDraftRevision int64,
) (domain.ProfileRevision, bool, error) {
	const query = `
		SELECT ` + revisionColumns + `
		FROM runtime_profile_revisions
		WHERE tenant_id = $1 AND profile_id = $2 AND source_draft_revision = $3
	`
	revision, err := scanRevision(s.db.QueryRow(
		ctx, query, tenantID, profileID, sourceDraftRevision,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProfileRevision{}, false, nil
	}
	if err != nil {
		return domain.ProfileRevision{}, false, fmt.Errorf("query revision by source draft: %w", err)
	}
	return revision, true, nil
}

func (s *Store) PublishProfileRevision(
	ctx context.Context,
	candidate domain.ProfileRevision,
	expectedRevision int64,
) (domain.ProfileRevision, bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return domain.ProfileRevision{}, false, fmt.Errorf("begin runtime profile publication: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Match WithinProfile's lock order: owning Profile, then mutable Draft.
	// A joint d,p lock can deadlock with a credential/configuration save.
	const profileLock = `
		SELECT latest_revision_number FROM runtime_profiles
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE
	`
	var latestRevision *int64
	err = tx.QueryRow(ctx, profileLock, candidate.TenantID, candidate.ProfileID).Scan(&latestRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProfileRevision{}, false, application.ErrRuntimeProfileNotFound
	}
	if err != nil {
		return domain.ProfileRevision{}, false, fmt.Errorf("lock runtime profile publication owner: %w", err)
	}
	const draftLock = `
		SELECT spec_revision FROM runtime_profile_drafts
		WHERE tenant_id = $1 AND profile_id = $2 FOR UPDATE
	`
	var currentRevision int64
	err = tx.QueryRow(ctx, draftLock, candidate.TenantID, candidate.ProfileID).Scan(&currentRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProfileRevision{}, false, application.ErrRuntimeProfileNotFound
	}
	if err != nil {
		return domain.ProfileRevision{}, false, fmt.Errorf("lock runtime profile publication source: %w", err)
	}

	// Re-check the immutable idempotency key while holding both aggregate rows.
	// It deliberately precedes the mutable Draft Revision comparison.
	const existingQuery = `
		SELECT ` + revisionColumns + `
		FROM runtime_profile_revisions
		WHERE tenant_id = $1 AND profile_id = $2 AND source_draft_revision = $3
	`
	existing, err := scanRevision(tx.QueryRow(
		ctx, existingQuery, candidate.TenantID, candidate.ProfileID,
		expectedRevision,
	))
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return domain.ProfileRevision{}, false, fmt.Errorf("commit idempotent runtime profile publication: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.ProfileRevision{}, false, fmt.Errorf("query existing runtime profile publication: %w", err)
	}

	if currentRevision != expectedRevision || candidate.SourceDraftRevision != expectedRevision {
		return domain.ProfileRevision{}, false, application.ErrDraftRevisionConflict
	}

	candidate.RevisionNumber = 1
	if latestRevision != nil {
		candidate.RevisionNumber = *latestRevision + 1
	}
	const insertStatement = `
		INSERT INTO runtime_profile_revisions (
			id, tenant_id, profile_id, revision_number, source_draft_revision,
			schema_version, spec_jsonb, spec_digest, published_by, published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10)
		RETURNING ` + revisionColumns
	// Return the database representation on both first publication and retry:
	// PostgreSQL normalizes JSONB and persists timestamps at microsecond precision.
	persisted, err := scanRevision(tx.QueryRow(
		ctx, insertStatement, candidate.ID, candidate.TenantID, candidate.ProfileID,
		candidate.RevisionNumber, candidate.SourceDraftRevision,
		candidate.SchemaVersion, []byte(candidate.Spec), candidate.SpecDigest,
		candidate.PublishedBy, candidate.PublishedAt,
	))
	if err != nil {
		return domain.ProfileRevision{}, false, fmt.Errorf("insert runtime profile revision: %w", err)
	}
	const updateProfile = `
		UPDATE runtime_profiles
		SET latest_revision_number = $3, updated_at = $4
		WHERE tenant_id = $1 AND id = $2
	`
	tag, err := tx.Exec(
		ctx, updateProfile, persisted.TenantID, persisted.ProfileID,
		persisted.RevisionNumber, persisted.PublishedAt,
	)
	if err != nil {
		return domain.ProfileRevision{}, false, fmt.Errorf("update latest runtime profile revision: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ProfileRevision{}, false, fmt.Errorf(
			"update latest runtime profile revision: affected %d rows", tag.RowsAffected(),
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ProfileRevision{}, false, fmt.Errorf("commit runtime profile publication: %w", err)
	}
	return persisted, true, nil
}

func (s *Store) GetProfileRevision(
	ctx context.Context, tenantID, profileID string, revisionNumber int64,
) (domain.ProfileRevision, error) {
	const query = `
		SELECT ` + revisionColumns + `
		FROM runtime_profile_revisions
		WHERE tenant_id = $1 AND profile_id = $2 AND revision_number = $3
	`
	revision, err := scanRevision(s.db.QueryRow(
		ctx, query, tenantID, profileID, revisionNumber,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProfileRevision{}, application.ErrProfileRevisionNotFound
	}
	if err != nil {
		return domain.ProfileRevision{}, fmt.Errorf("query runtime profile revision: %w", err)
	}
	return revision, nil
}

func (s *Store) ListProfileRevisionSummaries(
	ctx context.Context,
	tenantID, profileID string,
	page application.Page,
) (application.ProfileRevisionSummaryPage, error) {
	const query = `
		SELECT
			EXISTS (
				SELECT 1 FROM runtime_profiles WHERE tenant_id = $1 AND id = $2
			),
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'id', p.id,
						'tenant_id', p.tenant_id,
						'profile_id', p.profile_id,
						'revision_number', p.revision_number,
						'source_draft_revision', p.source_draft_revision,
						'schema_version', p.schema_version,
						'spec_digest', p.spec_digest,
						'published_by', p.published_by,
						'published_at', p.published_at
					) ORDER BY p.revision_number DESC
				)
				FROM (
					SELECT
						id, tenant_id, profile_id, revision_number,
						source_draft_revision, schema_version, spec_digest,
						published_by, published_at
					FROM runtime_profile_revisions
					WHERE tenant_id = $1 AND profile_id = $2
					ORDER BY revision_number DESC
					LIMIT $3 OFFSET $4
				) AS p
			), '[]'::jsonb),
			(SELECT count(*)::int FROM runtime_profile_revisions
			 WHERE tenant_id = $1 AND profile_id = $2)
	`
	var exists bool
	var encoded []byte
	var total int
	if err := s.db.QueryRow(ctx, query, tenantID, profileID, page.Limit, page.Offset).Scan(
		&exists, &encoded, &total,
	); err != nil {
		return application.ProfileRevisionSummaryPage{}, fmt.Errorf("query runtime profile revision summary page: %w", err)
	}
	if !exists {
		return application.ProfileRevisionSummaryPage{}, application.ErrRuntimeProfileNotFound
	}
	var records []revisionSummaryRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.ProfileRevisionSummaryPage{}, fmt.Errorf("decode runtime profile revision summary page: %w", err)
	}
	revisions := make([]domain.ProfileRevisionSummary, 0, len(records))
	for _, record := range records {
		revisions = append(revisions, record.domain())
	}
	return application.ProfileRevisionSummaryPage{Revisions: revisions, Total: total}, nil
}

var _ application.Store = (*Store)(nil)
