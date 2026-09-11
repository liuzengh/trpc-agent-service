package postgresadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gowebpki/jcs"
	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

const receiptColumns = `
	tenant_id, operation, scope_id, key_hash, request_digest, deployment_id,
	COALESCE(deployment_revision_id, ''), COALESCE(runtime_manifest_id, ''),
	result_jsonb, created_by, created_at
`

func (s *Store) FindCommandReceipt(
	ctx context.Context, key application.CommandReceiptKey,
) (domain.CommandReceipt, bool, error) {
	value, found, err := findCommandReceipt(ctx, s.db, key)
	if err != nil {
		return domain.CommandReceipt{}, false, fmt.Errorf("query deployment command receipt: %w", err)
	}
	return value, found, nil
}

func findCommandReceipt(
	ctx context.Context, q queryer, key application.CommandReceiptKey,
) (domain.CommandReceipt, bool, error) {
	const query = `
		SELECT ` + receiptColumns + `
		FROM deployment_command_receipts
		WHERE tenant_id = $1 AND operation = $2 AND scope_id = $3 AND key_hash = $4
	`
	value, err := scanReceipt(q.QueryRow(
		ctx, query, key.TenantID, key.Operation, key.ScopeID, key.KeyHash,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.CommandReceipt{}, false, nil
	}
	if err != nil {
		return domain.CommandReceipt{}, false, err
	}
	return value, true, nil
}

func (s *Store) CreateDeployment(
	ctx context.Context, commit application.CreateCommit,
) (application.CreateCommitResult, error) {
	if err := validateCreateCommit(commit); err != nil {
		return application.CreateCommitResult{}, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return application.CreateCommitResult{}, fmt.Errorf("begin deployment creation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	key := receiptKey(commit.Receipt)
	existing, found, err := findCommandReceipt(ctx, tx, key)
	if err != nil {
		return application.CreateCommitResult{}, fmt.Errorf("recheck create receipt: %w", err)
	}
	if found {
		if err := matchReceiptRequest(existing, commit.Receipt.RequestDigest); err != nil {
			return application.CreateCommitResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return application.CreateCommitResult{}, fmt.Errorf("commit create receipt replay: %w", err)
		}
		return application.CreateCommitResult{Receipt: existing}, nil
	}

	const insertDeployment = `
		INSERT INTO deployments (
			tenant_id, id, name, description, metadata_revision,
			latest_revision_number, created_by, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	tag, err := tx.Exec(
		ctx, insertDeployment, commit.Deployment.TenantID, commit.Deployment.ID,
		commit.Deployment.Name, commit.Deployment.Description,
		commit.Deployment.MetadataRevision, commit.Deployment.LatestRevisionNumber,
		commit.Deployment.CreatedBy, commit.Deployment.CreatedAt, commit.Deployment.UpdatedAt,
	)
	if err != nil {
		return application.CreateCommitResult{}, fmt.Errorf("insert deployment: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return application.CreateCommitResult{}, fmt.Errorf("insert deployment: affected %d rows", tag.RowsAffected())
	}
	persisted, err := insertCommandReceipt(ctx, tx, commit.Receipt)
	if err != nil {
		if isConstraint(err, "deployment_command_receipts_pkey") {
			// The unique receipt key arbitrates concurrent creates. The losing
			// transaction must end before a new READ COMMITTED statement can see
			// the winner and return its original immutable response.
			_ = tx.Rollback(context.Background())
			winner, found, queryErr := s.FindCommandReceipt(ctx, key)
			if queryErr != nil {
				return application.CreateCommitResult{}, fmt.Errorf("read concurrent create winner: %w", queryErr)
			}
			if !found {
				return application.CreateCommitResult{}, fmt.Errorf("read concurrent create winner: receipt missing after %w", err)
			}
			if err := matchReceiptRequest(winner, commit.Receipt.RequestDigest); err != nil {
				return application.CreateCommitResult{}, err
			}
			return application.CreateCommitResult{Receipt: winner}, nil
		}
		return application.CreateCommitResult{}, fmt.Errorf("insert create receipt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return application.CreateCommitResult{}, fmt.Errorf("commit deployment creation: %w", err)
	}
	return application.CreateCommitResult{Receipt: persisted, Created: true}, nil
}

func (s *Store) CommitPublication(
	ctx context.Context, commit application.PublicationCommit,
) (application.PublicationCommitResult, error) {
	if err := validatePublicationCommit(commit); err != nil {
		return application.PublicationCommitResult{}, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return application.PublicationCommitResult{}, fmt.Errorf("begin deployment publication: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// READ COMMITTED plus this tenant-scoped owner lock serializes publication.
	// The receipt lookup deliberately happens after the lock so a waiter gets a
	// fresh statement snapshot containing the winner's committed receipt.
	const lockDeployment = `
		SELECT latest_revision_number
		FROM deployments
		WHERE tenant_id = $1 AND id = $2
		FOR UPDATE
	`
	var latest *int64
	err = tx.QueryRow(
		ctx, lockDeployment, commit.Revision.TenantID, commit.Revision.DeploymentID,
	).Scan(&latest)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.PublicationCommitResult{}, application.ErrDeploymentNotFound
	}
	if err != nil {
		return application.PublicationCommitResult{}, fmt.Errorf("lock deployment publication owner: %w", err)
	}

	key := receiptKey(commit.Receipt)
	existing, found, err := findCommandReceipt(ctx, tx, key)
	if err != nil {
		return application.PublicationCommitResult{}, fmt.Errorf("recheck publish receipt after lock: %w", err)
	}
	if found {
		if err := matchReceiptRequest(existing, commit.Receipt.RequestDigest); err != nil {
			return application.PublicationCommitResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return application.PublicationCommitResult{}, fmt.Errorf("commit publish receipt replay: %w", err)
		}
		return application.PublicationCommitResult{Receipt: existing}, nil
	}
	if !sameRevisionPointer(latest, commit.ExpectedLatestRevisionNumber) {
		return application.PublicationCommitResult{}, application.ErrLatestRevisionConflict
	}
	next := int64(1)
	if latest != nil {
		next = *latest + 1
	}
	if commit.Revision.RevisionNumber != next || commit.Manifest.RevisionNumber != next ||
		commit.OutboxEvent.AggregateRevision != next {
		return application.PublicationCommitResult{}, application.ErrLatestRevisionConflict
	}

	if err := insertDeploymentRevision(ctx, tx, commit.Revision); err != nil {
		return application.PublicationCommitResult{}, err
	}
	if err := insertRuntimeManifest(ctx, tx, commit.Manifest); err != nil {
		return application.PublicationCommitResult{}, err
	}
	if err := insertOutboxEvent(ctx, tx, commit.OutboxEvent); err != nil {
		return application.PublicationCommitResult{}, err
	}
	persisted, err := insertCommandReceipt(ctx, tx, commit.Receipt)
	if err != nil {
		return application.PublicationCommitResult{}, fmt.Errorf("insert publish receipt: %w", err)
	}
	const advanceLatest = `
		UPDATE deployments
		SET latest_revision_number = $3, updated_at = $4
		WHERE tenant_id = $1 AND id = $2
		  AND latest_revision_number IS NOT DISTINCT FROM $5
	`
	tag, err := tx.Exec(
		ctx, advanceLatest, commit.Revision.TenantID, commit.Revision.DeploymentID,
		commit.Revision.RevisionNumber, commit.Revision.PublishedAt,
		commit.ExpectedLatestRevisionNumber,
	)
	if err != nil {
		return application.PublicationCommitResult{}, fmt.Errorf("advance latest deployment revision: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return application.PublicationCommitResult{}, application.ErrLatestRevisionConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return application.PublicationCommitResult{}, fmt.Errorf("commit deployment publication: %w", err)
	}
	return application.PublicationCommitResult{Receipt: persisted, Created: true}, nil
}

func insertDeploymentRevision(
	ctx context.Context, q queryer, revision domain.DeploymentRevision,
) error {
	// Agent/Profile owner Query ports have already verified the immutable source
	// identities, schemas and digests before this transaction. The Store checks
	// these values against the compiled Manifest in validatePublicationCommit,
	// then persists them without reading another module's tables (ARC-206).
	// Tenant-scoped foreign keys retain source existence/isolation guarantees.
	const statement = `
		INSERT INTO deployment_revisions (
			tenant_id, id, deployment_id, revision_number, schema_version,
			input_jsonb, input_digest,
			agent_id, agent_version_id, agent_version_number,
			agent_schema_version, agent_spec_digest,
			profile_id, profile_revision_id, profile_revision_number,
			profile_schema_version, profile_spec_digest,
			published_by, published_at
		)
		VALUES (
			$1, $2, $3, $4, $5, $6::jsonb, $7,
			$8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19
		)
		RETURNING id
	`
	var id string
	err := q.QueryRow(
		ctx, statement, revision.TenantID, revision.ID, revision.DeploymentID,
		revision.RevisionNumber, revision.SchemaVersion, []byte(revision.CanonicalInput),
		revision.InputDigest, revision.Input.Agent.AgentID, revision.AgentVersionID,
		revision.Input.Agent.VersionNumber, revision.AgentSchemaVersion,
		revision.AgentSpecDigest, revision.Input.Profile.ProfileID,
		revision.ProfileRevisionID, revision.Input.Profile.RevisionNumber,
		revision.ProfileSchemaVersion, revision.ProfileSpecDigest,
		revision.PublishedBy, revision.PublishedAt,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrPublicationIntegrity
	}
	if err != nil {
		return fmt.Errorf("insert deployment revision: %w", err)
	}
	if id != revision.ID {
		return application.ErrPublicationIntegrity
	}
	return nil
}

func insertRuntimeManifest(
	ctx context.Context, q queryer, manifest domain.RuntimeManifest,
) error {
	const statement = `
		INSERT INTO runtime_manifests (
			tenant_id, id, deployment_revision_id, schema_version,
			compiler_version, runtime_contract_version, content_jsonb,
			content_digest, published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
	`
	tag, err := q.Exec(
		ctx, statement, manifest.TenantID, manifest.ID,
		manifest.DeploymentRevisionID, manifest.SchemaVersion,
		manifest.CompilerVersion, manifest.RuntimeContractVersion,
		[]byte(manifest.Content), manifest.ContentDigest, manifest.PublishedAt,
	)
	if err != nil {
		return fmt.Errorf("insert runtime manifest: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert runtime manifest: affected %d rows", tag.RowsAffected())
	}
	return nil
}

func insertOutboxEvent(ctx context.Context, q queryer, event domain.OutboxEvent) error {
	const statement = `
		INSERT INTO control_outbox (
			tenant_id, id, aggregate_type, aggregate_id, aggregate_revision,
			event_type, schema_version, payload_jsonb, payload_digest,
			status, attempt_count, available_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9,
		          'PENDING', 0, $10, $10, $10)
	`
	tag, err := q.Exec(
		ctx, statement, event.TenantID, event.ID, event.AggregateType,
		event.AggregateID, event.AggregateRevision, event.EventType,
		event.SchemaVersion, []byte(event.Payload), event.PayloadDigest,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert control outbox event: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert control outbox event: affected %d rows", tag.RowsAffected())
	}
	return nil
}

func insertCommandReceipt(
	ctx context.Context, q queryer, receipt domain.CommandReceipt,
) (domain.CommandReceipt, error) {
	const statement = `
		INSERT INTO deployment_command_receipts (
			tenant_id, operation, scope_id, key_hash, request_digest,
			deployment_id, deployment_revision_id, runtime_manifest_id,
			result_jsonb, created_by, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), NULLIF($8, ''),
		          $9::jsonb, $10, $11)
		RETURNING ` + receiptColumns
	return scanReceipt(q.QueryRow(
		ctx, statement, receipt.TenantID, receipt.Operation, receipt.ScopeID,
		receipt.KeyHash, receipt.RequestDigest, receipt.DeploymentID,
		receipt.DeploymentRevisionID, receipt.RuntimeManifestID,
		[]byte(receipt.Result), receipt.CreatedBy, receipt.CreatedAt,
	))
}

func validateCreateCommit(commit application.CreateCommit) error {
	d, receipt := commit.Deployment, commit.Receipt
	if d.Validate() != nil || d.MetadataRevision != 1 || d.LatestRevisionNumber != nil ||
		receipt.TenantID != d.TenantID || receipt.Operation != "create" ||
		receipt.ScopeID != d.TenantID || receipt.DeploymentID != d.ID ||
		receipt.DeploymentRevisionID != "" || receipt.RuntimeManifestID != "" ||
		receipt.CreatedBy != d.CreatedBy || receipt.KeyHash == "" ||
		receipt.RequestDigest == "" || !createReceiptMatches(receipt.Result, d) ||
		!receipt.CreatedAt.Equal(d.CreatedAt) {
		return application.ErrInvalidDeployment
	}
	return nil
}

func validatePublicationCommit(commit application.PublicationCommit) error {
	r, manifest, event, receipt := commit.Revision, commit.Manifest, commit.OutboxEvent, commit.Receipt
	canonicalInput, inputDigest, inputOK := canonicalDocument(r.CanonicalInput)
	if r.ID == "" || r.TenantID == "" || r.DeploymentID == "" || r.RevisionNumber <= 0 ||
		r.SchemaVersion != r.Input.SchemaVersion || r.Input.Validate() != nil ||
		!inputOK || !bytes.Equal(r.CanonicalInput, canonicalInput) || r.InputDigest != inputDigest ||
		!canonicalInputMatches(r.CanonicalInput, r.Input) ||
		r.AgentVersionID == "" || r.AgentSchemaVersion == "" || r.AgentSpecDigest == "" ||
		r.ProfileRevisionID == "" || r.ProfileSchemaVersion == "" || r.ProfileSpecDigest == "" ||
		r.PublishedBy == "" || r.PublishedAt.IsZero() {
		return application.ErrPublicationIntegrity
	}
	content, err := domain.VerifyManifestContent(manifest.Content, manifest.ContentDigest)
	if manifest.ID == "" || manifest.TenantID != r.TenantID ||
		manifest.DeploymentID != r.DeploymentID || manifest.DeploymentRevisionID != r.ID ||
		manifest.RevisionNumber != r.RevisionNumber || manifest.SchemaVersion == "" ||
		manifest.CompilerVersion == "" || manifest.RuntimeContractVersion == "" || err != nil ||
		domain.ValidateManifestPublicationBinding(content, r, manifest) != nil ||
		!manifest.PublishedAt.Equal(r.PublishedAt) {
		return application.ErrPublicationIntegrity
	}
	canonicalEvent, eventDigest, eventOK := canonicalDocument(event.Payload)
	if event.ID == "" || event.TenantID != r.TenantID || event.AggregateType == "" ||
		event.AggregateID != r.DeploymentID || event.AggregateRevision != r.RevisionNumber ||
		event.EventType != domain.ManifestPublishedType ||
		event.SchemaVersion != domain.EventSchemaVersionV1 ||
		!eventOK || !bytes.Equal(event.Payload, canonicalEvent) || event.PayloadDigest != eventDigest ||
		!eventPayloadMatches(event.Payload, event, manifest) ||
		!event.CreatedAt.Equal(r.PublishedAt) {
		return application.ErrPublicationIntegrity
	}
	if receipt.TenantID != r.TenantID || receipt.Operation != "publish" ||
		receipt.ScopeID != r.DeploymentID || receipt.DeploymentID != r.DeploymentID ||
		receipt.DeploymentRevisionID != r.ID || receipt.RuntimeManifestID != manifest.ID ||
		receipt.CreatedBy != r.PublishedBy || receipt.KeyHash == "" ||
		receipt.RequestDigest == "" || !publicationReceiptMatches(receipt.Result, r, manifest, content) ||
		!receipt.CreatedAt.Equal(r.PublishedAt) {
		return application.ErrPublicationIntegrity
	}
	return nil
}

func canonicalDocument(raw json.RawMessage) (json.RawMessage, string, bool) {
	if !json.Valid(raw) {
		return nil, "", false
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, "", false
	}
	sum := sha256.Sum256(canonical)
	return canonical, fmt.Sprintf("sha256:%x", sum), true
}

type createReceiptResult struct {
	Deployment domain.Deployment `json:"deployment"`
}

func createReceiptMatches(raw json.RawMessage, deployment domain.Deployment) bool {
	if !validJSONObject(raw) {
		return false
	}
	var result createReceiptResult
	if json.Unmarshal(raw, &result) != nil || !sameDeployment(result.Deployment, deployment) {
		return false
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return false
	}
	canonicalRaw, _, rawOK := canonicalDocument(raw)
	canonicalEncoded, _, encodedOK := canonicalDocument(encoded)
	return rawOK && encodedOK && bytes.Equal(canonicalRaw, canonicalEncoded)
}

func sameDeployment(actual, expected domain.Deployment) bool {
	if actual.ID != expected.ID || actual.TenantID != expected.TenantID ||
		actual.Name != expected.Name || actual.Description != expected.Description ||
		actual.MetadataRevision != expected.MetadataRevision ||
		actual.CreatedBy != expected.CreatedBy ||
		!actual.CreatedAt.Equal(expected.CreatedAt) ||
		!actual.UpdatedAt.Equal(expected.UpdatedAt) {
		return false
	}
	return sameRevisionPointer(actual.LatestRevisionNumber, expected.LatestRevisionNumber)
}

type publicationReceiptResult struct {
	Published  domain.PublishedRevision `json:"published"`
	Validation domain.ValidationReport  `json:"validation"`
}

func publicationReceiptMatches(
	raw json.RawMessage,
	revision domain.DeploymentRevision,
	manifest domain.RuntimeManifest,
	content domain.ManifestContent,
) bool {
	if !validJSONObject(raw) {
		return false
	}
	var result publicationReceiptResult
	if json.Unmarshal(raw, &result) != nil || !result.Validation.Valid {
		return false
	}
	// Re-encoding the closed wire model and comparing canonical forms rejects
	// unknown receipt fields at every regular struct layer.
	encoded, err := json.Marshal(result)
	if err != nil {
		return false
	}
	canonicalRaw, _, rawOK := canonicalDocument(raw)
	canonicalEncoded, _, encodedOK := canonicalDocument(encoded)
	if !rawOK || !encodedOK || !bytes.Equal(canonicalRaw, canonicalEncoded) {
		return false
	}
	if !sameReceiptRevision(result.Published.Revision, revision) ||
		result.Published.ManifestID != manifest.ID ||
		result.Published.ManifestDigest != manifest.ContentDigest {
		return false
	}
	wantView, err := json.Marshal(domain.NewPublicManifestView(content))
	if err != nil {
		return false
	}
	actualView, _, actualOK := canonicalDocument(result.Published.ManifestView)
	expectedView, _, expectedOK := canonicalDocument(wantView)
	return actualOK && expectedOK && bytes.Equal(actualView, expectedView)
}

func sameReceiptRevision(actual, expected domain.DeploymentRevision) bool {
	return actual.ID == expected.ID && actual.TenantID == expected.TenantID &&
		actual.DeploymentID == expected.DeploymentID &&
		actual.RevisionNumber == expected.RevisionNumber &&
		actual.SchemaVersion == expected.SchemaVersion && actual.Input == expected.Input &&
		actual.InputDigest == expected.InputDigest &&
		actual.AgentVersionID == expected.AgentVersionID &&
		actual.AgentSchemaVersion == expected.AgentSchemaVersion &&
		actual.AgentSpecDigest == expected.AgentSpecDigest &&
		actual.ProfileRevisionID == expected.ProfileRevisionID &&
		actual.ProfileSchemaVersion == expected.ProfileSchemaVersion &&
		actual.ProfileSpecDigest == expected.ProfileSpecDigest &&
		actual.PublishedBy == expected.PublishedBy &&
		actual.PublishedAt.Equal(expected.PublishedAt)
}

func validJSONObject(raw json.RawMessage) bool {
	if !json.Valid(raw) {
		return false
	}
	var value map[string]json.RawMessage
	return json.Unmarshal(raw, &value) == nil && value != nil
}

func canonicalInputMatches(raw json.RawMessage, expected domain.DeploymentInput) bool {
	if !validJSONObject(raw) {
		return false
	}
	var actual domain.DeploymentInput
	if json.Unmarshal(raw, &actual) != nil || actual != expected {
		return false
	}
	encoded, err := json.Marshal(actual)
	if err != nil {
		return false
	}
	canonicalRaw, _, rawOK := canonicalDocument(raw)
	canonicalEncoded, _, encodedOK := canonicalDocument(encoded)
	return rawOK && encodedOK && bytes.Equal(canonicalRaw, canonicalEncoded)
}

func eventPayloadMatches(
	raw json.RawMessage, expected domain.OutboxEvent, manifest domain.RuntimeManifest,
) bool {
	if !validJSONObject(raw) {
		return false
	}
	var actual domain.RuntimeManifestPublishedEvent
	if json.Unmarshal(raw, &actual) != nil {
		return false
	}
	reencoded, err := json.Marshal(actual)
	if err != nil {
		return false
	}
	canonicalRaw, _, rawOK := canonicalDocument(raw)
	canonicalEncoded, _, encodedOK := canonicalDocument(reencoded)
	if !rawOK || !encodedOK || !bytes.Equal(canonicalRaw, canonicalEncoded) {
		return false
	}
	return actual.SchemaVersion == expected.SchemaVersion &&
		actual.EventType == expected.EventType && actual.EventID == expected.ID &&
		actual.TenantID == expected.TenantID && actual.DeploymentID == expected.AggregateID &&
		actual.DeploymentRevisionID == manifest.DeploymentRevisionID &&
		actual.RevisionNumber == expected.AggregateRevision &&
		actual.OccurredAt.Equal(expected.CreatedAt) &&
		actual.Manifest.ID == manifest.ID && actual.Manifest.TenantID == manifest.TenantID &&
		actual.Manifest.DeploymentID == manifest.DeploymentID &&
		actual.Manifest.DeploymentRevisionID == manifest.DeploymentRevisionID &&
		actual.Manifest.RevisionNumber == manifest.RevisionNumber &&
		actual.Manifest.ContentDigest == manifest.ContentDigest &&
		string(actual.Manifest.Content) == string(manifest.Content) &&
		actual.Manifest.PublishedAt.Equal(manifest.PublishedAt)
}

func receiptKey(receipt domain.CommandReceipt) application.CommandReceiptKey {
	return application.CommandReceiptKey{
		TenantID: receipt.TenantID, Operation: receipt.Operation,
		ScopeID: receipt.ScopeID, KeyHash: receipt.KeyHash,
	}
}

func matchReceiptRequest(receipt domain.CommandReceipt, expected string) error {
	if receipt.RequestDigest != expected {
		return application.ErrIdempotencyConflict
	}
	return nil
}

func sameRevisionPointer(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
