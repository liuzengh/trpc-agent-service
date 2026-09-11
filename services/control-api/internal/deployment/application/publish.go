package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/gowebpki/jcs"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

type publishReceiptResult struct {
	Published  domain.PublishedRevision `json:"published"`
	Validation domain.ValidationReport  `json:"validation"`
}

func (s *Service) PublishDeploymentRevision(ctx context.Context, command PublishDeploymentCommand) (PublishDeploymentResult, error) {
	if err := s.authorizeOwner(ctx, command.TenantID, command.ActorUserID); err != nil {
		return PublishDeploymentResult{}, err
	}
	if !validIdempotencyKey(command.IdempotencyKey) || command.Input.Validate() != nil ||
		(command.ExpectedLatestRevisionNumber != nil && *command.ExpectedLatestRevisionNumber <= 0) {
		return PublishDeploymentResult{}, ErrInvalidDeploymentInput
	}
	digest, err := requestDigest("publish", command.TenantID, command.DeploymentID, struct {
		ExpectedLatestRevisionNumber *int64                 `json:"expected_latest_revision_number"`
		Input                        domain.DeploymentInput `json:"input"`
	}{command.ExpectedLatestRevisionNumber, command.Input})
	if err != nil {
		return PublishDeploymentResult{}, err
	}
	key := CommandReceiptKey{
		TenantID: command.TenantID, Operation: "publish", ScopeID: command.DeploymentID,
		KeyHash: keyHash(command.IdempotencyKey),
	}
	if receipt, found, err := s.deps.Publications.FindCommandReceipt(ctx, key); err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("find publish receipt: %w", err)
	} else if found {
		return s.replayPublish(ctx, receipt, key, digest)
	}
	compiled, err := s.compileAndCheck(ctx, command.TenantID, command.DeploymentID, command.ActorUserID, command.Input)
	if err != nil {
		return PublishDeploymentResult{}, err
	}
	if !compiled.Report.Valid {
		return PublishDeploymentResult{Validation: compiled.Report}, ErrDeploymentRevisionInvalid
	}
	number := int64(1)
	if command.ExpectedLatestRevisionNumber != nil {
		if *command.ExpectedLatestRevisionNumber == math.MaxInt64 {
			return PublishDeploymentResult{}, ErrLatestRevisionConflict
		}
		number = *command.ExpectedLatestRevisionNumber + 1
	}
	revisionID, err := s.deps.NewRevisionID()
	if err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("generate deployment revision id: %w", err)
	}
	manifestID, err := s.deps.NewManifestID()
	if err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("generate runtime manifest id: %w", err)
	}
	eventID, err := s.deps.NewEventID()
	if err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("generate deployment event id: %w", err)
	}
	publishedAt := s.deps.Now().UTC().Truncate(time.Microsecond)
	canonicalInput, inputDigest, err := canonicalJSON(command.Input)
	if err != nil {
		return PublishDeploymentResult{}, err
	}
	revision := domain.DeploymentRevision{
		ID: revisionID, TenantID: command.TenantID, DeploymentID: command.DeploymentID,
		RevisionNumber: number, SchemaVersion: domain.SchemaVersionV1,
		Input: command.Input, CanonicalInput: canonicalInput, InputDigest: inputDigest,
		AgentVersionID: compiled.Agent.ID, AgentSchemaVersion: compiled.Agent.SchemaVersion,
		AgentSpecDigest:   compiled.Agent.SpecDigest,
		ProfileRevisionID: compiled.Profile.ID, ProfileSchemaVersion: compiled.Profile.SchemaVersion,
		ProfileSpecDigest: compiled.Profile.SpecDigest,
		PublishedBy:       command.ActorUserID, PublishedAt: publishedAt,
	}
	manifest := domain.RuntimeManifest{
		ID: manifestID, TenantID: command.TenantID, DeploymentID: command.DeploymentID,
		DeploymentRevisionID: revisionID, RevisionNumber: number,
		SchemaVersion:          compiled.Compiled.Content.SchemaVersion,
		CompilerVersion:        compiled.Compiled.Content.CompilerVersion,
		RuntimeContractVersion: compiled.Compiled.Content.RuntimeContractVersion,
		Content:                append(json.RawMessage(nil), compiled.Compiled.CanonicalContent...),
		ContentDigest:          compiled.Compiled.ContentDigest, PublishedAt: publishedAt,
	}
	view := domain.NewPublicManifestView(compiled.Compiled.Content)
	viewJSON, _, err := canonicalJSON(view)
	if err != nil || publicJSONContainsCredentialID(viewJSON) {
		return PublishDeploymentResult{}, ErrPublicationIntegrity
	}
	published := domain.PublishedRevision{
		Revision: revision, Manifest: manifest, ManifestID: manifest.ID,
		ManifestDigest: manifest.ContentDigest, ManifestView: viewJSON,
	}
	eventPayload, eventDigest, err := canonicalJSON(domain.RuntimeManifestPublishedEvent{
		SchemaVersion: domain.EventSchemaVersionV1, EventType: domain.ManifestPublishedType,
		EventID: eventID, TenantID: command.TenantID, DeploymentID: command.DeploymentID,
		DeploymentRevisionID: revisionID, RevisionNumber: number,
		OccurredAt: publishedAt, Manifest: manifest,
	})
	if err != nil {
		return PublishDeploymentResult{}, err
	}
	if len(eventPayload) > s.deps.MaxEventBytes {
		report := compiled.Report.WithDiagnostics(domain.Diagnostic{
			Code: domain.DiagnosticManifestTooLarge, Severity: domain.SeverityError,
			Source: domain.DiagnosticSourcePlatform, Path: "/event",
			Message: "publication event exceeds the platform limit",
		})
		return PublishDeploymentResult{Validation: report}, ErrDeploymentRevisionInvalid
	}
	receiptResult := publishReceiptResult{Published: published, Validation: compiled.Report}
	receiptJSON, err := json.Marshal(receiptResult)
	if err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("encode publish receipt: %w", err)
	}
	receipt := domain.CommandReceipt{
		TenantID: command.TenantID, Operation: "publish", ScopeID: command.DeploymentID,
		KeyHash: key.KeyHash, RequestDigest: digest, DeploymentID: command.DeploymentID,
		DeploymentRevisionID: revisionID, RuntimeManifestID: manifestID,
		Result: receiptJSON, CreatedBy: command.ActorUserID, CreatedAt: publishedAt,
	}
	commit := PublicationCommit{
		ExpectedLatestRevisionNumber: command.ExpectedLatestRevisionNumber,
		Revision:                     revision, Manifest: manifest,
		OutboxEvent: domain.OutboxEvent{
			ID: eventID, TenantID: command.TenantID, AggregateType: "deployment",
			AggregateID: command.DeploymentID, AggregateRevision: number,
			EventType: domain.ManifestPublishedType, SchemaVersion: domain.EventSchemaVersionV1,
			Payload: eventPayload, PayloadDigest: eventDigest, CreatedAt: publishedAt,
		},
		Receipt: receipt,
	}
	committed, err := s.deps.Publications.CommitPublication(ctx, commit)
	if err != nil {
		if errors.Is(err, ErrLatestRevisionConflict) || errors.Is(err, ErrIdempotencyConflict) ||
			errors.Is(err, ErrDeploymentNotFound) {
			return PublishDeploymentResult{}, err
		}
		return PublishDeploymentResult{}, fmt.Errorf("commit deployment publication: %w", err)
	}
	result, err := s.replayPublish(ctx, committed.Receipt, key, digest)
	if err != nil {
		return PublishDeploymentResult{}, err
	}
	result.Created = committed.Created
	return result, nil
}

func (s *Service) replayPublish(ctx context.Context, receipt domain.CommandReceipt, key CommandReceiptKey, digest string) (PublishDeploymentResult, error) {
	if err := verifyReceipt(receipt, key, digest); err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("verify publish receipt: %w", err)
	}
	var stored publishReceiptResult
	if err := json.Unmarshal(receipt.Result, &stored); err != nil || !stored.Validation.Valid ||
		stored.Published.Revision.ID != receipt.DeploymentRevisionID ||
		stored.Published.ManifestID != receipt.RuntimeManifestID ||
		stored.Published.Revision.DeploymentID != receipt.DeploymentID ||
		stored.Published.Revision.TenantID != receipt.TenantID ||
		publicJSONContainsCredentialID(stored.Published.ManifestView) {
		return PublishDeploymentResult{}, fmt.Errorf("decode publish receipt result: %w", ErrPublicationIntegrity)
	}
	actual, err := s.deps.Queries.GetPublishedRevision(ctx, receipt.TenantID, receipt.DeploymentID, stored.Published.Revision.RevisionNumber)
	if err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("load receipt publication: %w", ErrPublicationIntegrity)
	}
	actual, err = verifyPublished(actual)
	if err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("verify receipt publication: %w", err)
	}
	if actual.Revision.ID != receipt.DeploymentRevisionID ||
		actual.Manifest.ID != receipt.RuntimeManifestID || actual.ManifestID != stored.Published.ManifestID ||
		actual.ManifestDigest != stored.Published.ManifestDigest {
		return PublishDeploymentResult{}, fmt.Errorf("verify receipt publication: %w", ErrPublicationIntegrity)
	}
	storedCanonical, err := jcs.Transform(receipt.Result)
	if err != nil {
		return PublishDeploymentResult{}, fmt.Errorf("canonicalize receipt result: %w", ErrPublicationIntegrity)
	}
	expectedJSON, err := json.Marshal(publishReceiptResult{Published: actual, Validation: stored.Validation})
	if err != nil {
		return PublishDeploymentResult{}, err
	}
	expectedCanonical, err := jcs.Transform(expectedJSON)
	if err != nil || string(storedCanonical) != string(expectedCanonical) {
		return PublishDeploymentResult{}, fmt.Errorf("compare receipt result: %w", ErrPublicationIntegrity)
	}
	stored.Published = actual
	return PublishDeploymentResult{Published: stored.Published, Validation: stored.Validation}, nil
}
