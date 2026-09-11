package application

import (
	"context"
	"errors"

	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

var (
	ErrBackendMigrationUnavailable = errors.New("backend migration unavailable")
	ErrBackendMigrationBusy        = errors.New("backend migration source is busy")
	ErrBackendMigrationInvalid     = errors.New("backend migration is invalid")
	ErrBackendMigrationFailed      = errors.New("backend migration failed")
)

func (s *Service) MigrateAndPublish(ctx context.Context, command MigrateAndPublishCommand) (MigrateAndPublishResult, error) {
	if err := s.authorizeOwner(ctx, command.TenantID, command.ActorUserID); err != nil {
		return MigrateAndPublishResult{}, err
	}
	if s.deps.BackendMigrations == nil || s.deps.StorageCredentials == nil || command.SourceRevisionNumber < 1 || !validIdempotencyKey(command.IdempotencyKey) || command.Input.Validate() != nil {
		return MigrateAndPublishResult{}, ErrBackendMigrationInvalid
	}
	source, err := s.deps.Queries.GetPublishedRevision(ctx, command.TenantID, command.DeploymentID, command.SourceRevisionNumber)
	if err != nil {
		return MigrateAndPublishResult{}, err
	}
	sourceContent, err := domain.ValidateManifestContent(source.Manifest.Content, source.Manifest.ContentDigest)
	if err != nil || source.Revision.TenantID != command.TenantID || source.Revision.DeploymentID != command.DeploymentID || source.Revision.RevisionNumber != command.SourceRevisionNumber || source.Revision.Input.Agent.AgentID != command.Input.Agent.AgentID {
		return MigrateAndPublishResult{}, ErrBackendMigrationInvalid
	}
	target, err := s.compileAndCheck(ctx, command.TenantID, command.DeploymentID, command.ActorUserID, command.Input)
	if err != nil {
		return MigrateAndPublishResult{}, err
	}
	if !target.Report.Valid {
		return MigrateAndPublishResult{Publication: PublishDeploymentResult{Validation: target.Report}}, ErrDeploymentRevisionInvalid
	}
	sourceStorage, ok := memoryStorage(sourceContent)
	if !ok {
		return MigrateAndPublishResult{}, ErrBackendMigrationInvalid
	}
	targetStorage, ok := memoryStorage(target.Compiled.Content)
	if !ok || sourceStorage.Backend.Matches(*targetStorage.Backend) {
		return MigrateAndPublishResult{}, ErrBackendMigrationInvalid
	}
	sourceBatch, err := s.resolveStorage(ctx, command, source.Revision.Input.Profile.ProfileID, source.Revision.Input.Profile.RevisionNumber, sourceStorage.Credential)
	if err != nil {
		return MigrateAndPublishResult{}, err
	}
	defer sourceBatch.Clear()
	targetBatch, err := s.resolveStorage(ctx, command, command.Input.Profile.ProfileID, command.Input.Profile.RevisionNumber, targetStorage.Credential)
	if err != nil {
		return MigrateAndPublishResult{}, err
	}
	defer targetBatch.Clear()
	migrated, err := s.deps.BackendMigrations.Execute(ctx, executionv1.BackendMigrationRequest{
		TenantID: command.TenantID, SourceDeploymentRevision: source.Revision.ID,
		AgentID: command.Input.Agent.AgentID,
		Source:  executionv1.BackendMigrationTarget{Backend: sourceStorage.Backend.Clone(), Password: string(sourceBatch.Credentials[0].Value)},
		Target:  executionv1.BackendMigrationTarget{Backend: targetStorage.Backend.Clone(), Password: string(targetBatch.Credentials[0].Value)},
	})
	if err != nil {
		return MigrateAndPublishResult{}, err
	}
	published, err := s.PublishDeploymentRevision(ctx, PublishDeploymentCommand{
		TenantID: command.TenantID, DeploymentID: command.DeploymentID, ActorUserID: command.ActorUserID,
		IdempotencyKey: command.IdempotencyKey, ExpectedLatestRevisionNumber: command.ExpectedLatestRevisionNumber, Input: command.Input,
	})
	return MigrateAndPublishResult{MemoryScopesCopied: migrated.MemoryScopesCopied, Publication: published}, err
}

func memoryStorage(content domain.ManifestContent) (domain.ManifestStorageResource, bool) {
	name, ok := content.StorageRoles[domain.StorageRoleMemory]
	resource, found := content.Resources.Storage[name]
	return resource, ok && found && resource.Backend != nil && resource.Backend.ValidateForRole("memory") == nil && resource.Credential.Purpose == domain.CredentialPurposeDSNPassword
}

func (s *Service) resolveStorage(ctx context.Context, command MigrateAndPublishCommand, profileID string, revision int64, use domain.CredentialUse) (profileapp.CredentialBatch, error) {
	batch, err := s.deps.StorageCredentials.ResolveStorageForOwner(ctx, profileapp.CheckProfileCredentialsCommand{
		TenantID: command.TenantID, ProfileID: profileID, ActorUserID: command.ActorUserID, ProfileRevisionNumber: revision,
		Uses: []profileapp.CredentialUse{{CredentialID: use.CredentialID, Purpose: use.Purpose, AudienceDigest: use.AudienceDigest}},
	})
	if err != nil || len(batch.Credentials) != 1 {
		batch.Clear()
		if errors.Is(err, profileapp.ErrTenantForbidden) {
			return profileapp.CredentialBatch{}, ErrTenantForbidden
		}
		if errors.Is(err, profileapp.ErrRuntimeProfileNotFound) || errors.Is(err, profileapp.ErrProfileRevisionNotFound) {
			return profileapp.CredentialBatch{}, ErrProfileRevisionNotFound
		}
		if errors.Is(err, profiledomain.ErrCredentialUnavailable) || errors.Is(err, profiledomain.ErrCredentialAssociation) || errors.Is(err, profiledomain.ErrCredentialNotFound) || errors.Is(err, profiledomain.ErrCredentialInput) {
			return profileapp.CredentialBatch{}, ErrCredentialDependencyUnavailable
		}
		if err != nil {
			return profileapp.CredentialBatch{}, ErrCredentialDependencyUnavailable
		}
		return profileapp.CredentialBatch{}, ErrCredentialDependencyUnavailable
	}
	return batch, nil
}
