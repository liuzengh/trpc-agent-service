package admin

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const knowledgeMigrationDomain = knowledgedriver.Domain

type KnowledgeMigrationCreateInput struct {
	MigrationID         string `json:"migration_id"`
	TargetConfigVersion int64  `json:"target_config_version"`
}

type KnowledgeMigrationSwitchInput struct {
	ExpectedTenantVersion    int64  `json:"expected_tenant_version"`
	ExpectedMigrationVersion int64  `json:"expected_migration_version"`
	SwitchID                 string `json:"switch_id"`
}

type KnowledgeMigrationObserveInput struct {
	ExpectedTenantVersion    int64     `json:"expected_tenant_version"`
	ExpectedMigrationVersion int64     `json:"expected_migration_version"`
	ObserveUntil             time.Time `json:"observe_until"`
}

type KnowledgeMigrationControlInput struct {
	ExpectedMigrationVersion int64 `json:"expected_migration_version"`
}

type KnowledgeMigrationStatus struct {
	Migration migration.Migration         `json:"migration"`
	Drain     knowledgedriver.DrainStatus `json:"drain"`
}

func (s Service) CreateKnowledgeMigration(ctx context.Context, principal Principal, pathTenant string, in KnowledgeMigrationCreateInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return migration.Migration{}, err
	}
	if s.Configs == nil || s.Migrations == nil || in.MigrationID == "" || in.TargetConfigVersion < 1 {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	if err := metadata.Validate(); err != nil {
		return migration.Migration{}, err
	}
	source, err := s.Configs.GetCurrent(ctx, pathTenant)
	if err != nil {
		return migration.Migration{}, err
	}
	target, err := s.Configs.Get(ctx, pathTenant, in.TargetConfigVersion)
	if err != nil {
		return migration.Migration{}, err
	}
	if source.TenantID != pathTenant || target.TenantID != pathTenant || source.ConfigVersion < 1 ||
		target.ConfigVersion <= source.ConfigVersion || target.State != config.StatePublished {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	sourceBinding, ok := knowledgeMigrationBinding(source.Payload)
	if !ok {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	targetBinding, ok := knowledgeMigrationBinding(target.Payload)
	if !ok || (sourceBinding.BackendProfileID == targetBinding.BackendProfileID && sourceBinding.BackendVersion == targetBinding.BackendVersion) {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.Migrations.Create(ctx, migration.CreateRequest{TenantID: pathTenant, MigrationID: in.MigrationID,
		Domain: knowledgeMigrationDomain, Epoch: target.ConfigVersion,
		Source:    migration.Binding{ConfigVersion: source.ConfigVersion, BackendProfileID: sourceBinding.BackendProfileID, BackendVersion: sourceBinding.BackendVersion},
		Target:    migration.Binding{ConfigVersion: target.ConfigVersion, BackendProfileID: targetBinding.BackendProfileID, BackendVersion: targetBinding.BackendVersion},
		CreatedAt: time.Now().UTC(), Audit: migration.CreateAudit{ActorID: metadata.ActorID, ReasonCode: metadata.ReasonCode,
			CorrelationID: metadata.CorrelationID, TraceID: metadata.TraceID}})
}

func (s Service) GetKnowledgeMigration(ctx context.Context, principal Principal, pathTenant, migrationID string) (migration.Migration, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return migration.Migration{}, err
	}
	if s.Migrations == nil || migrationID == "" {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	value, err := s.Migrations.Get(ctx, pathTenant, migrationID)
	if err != nil {
		return migration.Migration{}, err
	}
	if value.Domain != knowledgeMigrationDomain {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return value, nil
}

func (s Service) ListKnowledgeMigrations(ctx context.Context, principal Principal, pathTenant string) ([]migration.Migration, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return nil, err
	}
	if s.Migrations == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return s.Migrations.List(ctx, pathTenant, knowledgeMigrationDomain)
}

func (s Service) GetKnowledgeMigrationStatus(ctx context.Context, principal Principal, pathTenant, migrationID string) (KnowledgeMigrationStatus, error) {
	value, err := s.GetKnowledgeMigration(ctx, principal, pathTenant, migrationID)
	if err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	if s.KnowledgeMigrationPublisher == nil {
		return KnowledgeMigrationStatus{}, runtime.ErrCapabilityUnsupported
	}
	drain, err := s.KnowledgeMigrationPublisher.DrainStatus(ctx, pathTenant, migrationID)
	if err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	return KnowledgeMigrationStatus{Migration: value, Drain: drain}, nil
}

func (s Service) PauseKnowledgeMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in KnowledgeMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlKnowledgeMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Pause)
}

func (s Service) ResumeKnowledgeMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in KnowledgeMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlKnowledgeMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Resume)
}

func (s Service) AbortKnowledgeMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in KnowledgeMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlKnowledgeMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Abort)
}

func (s Service) controlKnowledgeMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in KnowledgeMigrationControlInput, metadata tenant.ChangeMetadata, apply func(context.Context, migration.ControlRequest) (migration.Migration, error)) (migration.Migration, error) {
	if s.Migrations == nil || in.ExpectedMigrationVersion < 1 {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	if _, err := s.GetKnowledgeMigration(ctx, principal, pathTenant, migrationID); err != nil {
		return migration.Migration{}, err
	}
	if err := metadata.Validate(); err != nil {
		return migration.Migration{}, err
	}
	return apply(ctx, migration.ControlRequest{TenantID: pathTenant, MigrationID: migrationID, ExpectedVersion: in.ExpectedMigrationVersion,
		At: time.Now().UTC(), Metadata: migration.ControlMetadata{ActorID: metadata.ActorID, ReasonCode: metadata.ReasonCode,
			CorrelationID: metadata.CorrelationID, TraceID: metadata.TraceID}})
}

func (s Service) CutoverKnowledgeMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in KnowledgeMigrationSwitchInput, metadata tenant.ChangeMetadata) (knowledgedriver.SwitchResult, error) {
	current, err := s.requireKnowledgeSwitch(ctx, principal, pathTenant, migrationID, in.ExpectedMigrationVersion, metadata)
	if err != nil || current.Verification.SourceDigest == "" {
		if err == nil {
			err = runtime.ErrInvariantViolation
		}
		return knowledgedriver.SwitchResult{}, err
	}
	return s.KnowledgeMigrationPublisher.Cutover(ctx, knowledgedriver.CutoverRequest{TenantID: pathTenant, MigrationID: migrationID,
		ExpectedTenantVersion: in.ExpectedTenantVersion, ExpectedVersion: in.ExpectedMigrationVersion, Verification: current.Verification,
		At: time.Now().UTC(), Metadata: knowledgeSwitchMetadata(in.SwitchID, metadata)})
}

func (s Service) BeginKnowledgeMigrationObserve(ctx context.Context, principal Principal, pathTenant, migrationID string, in KnowledgeMigrationObserveInput) (knowledgedriver.SwitchResult, error) {
	if _, err := s.requireKnowledgeSwitch(ctx, principal, pathTenant, migrationID, in.ExpectedMigrationVersion, tenant.ChangeMetadata{}); err != nil {
		return knowledgedriver.SwitchResult{}, err
	}
	return s.KnowledgeMigrationPublisher.BeginObserve(ctx, knowledgedriver.ObserveRequest{TenantID: pathTenant, MigrationID: migrationID,
		ExpectedTenantVersion: in.ExpectedTenantVersion, ExpectedVersion: in.ExpectedMigrationVersion, At: time.Now().UTC(), ObserveUntil: in.ObserveUntil.UTC()})
}

func (s Service) RollbackKnowledgeMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in KnowledgeMigrationSwitchInput, metadata tenant.ChangeMetadata) (knowledgedriver.SwitchResult, error) {
	current, err := s.requireKnowledgeSwitch(ctx, principal, pathTenant, migrationID, in.ExpectedMigrationVersion, metadata)
	if err != nil || current.Verification.TargetWatermark == "" {
		if err == nil {
			err = runtime.ErrInvariantViolation
		}
		return knowledgedriver.SwitchResult{}, err
	}
	return s.KnowledgeMigrationPublisher.Rollback(ctx, knowledgedriver.RollbackRequest{TenantID: pathTenant, MigrationID: migrationID,
		ExpectedTenantVersion: in.ExpectedTenantVersion, ExpectedVersion: in.ExpectedMigrationVersion, RollbackSyncWatermark: current.Verification.TargetWatermark,
		At: time.Now().UTC(), Metadata: knowledgeSwitchMetadata(in.SwitchID, metadata)})
}

func (s Service) CleanupKnowledgeMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in KnowledgeMigrationSwitchInput) (knowledgedriver.SwitchResult, error) {
	current, err := s.requireKnowledgeSwitch(ctx, principal, pathTenant, migrationID, in.ExpectedMigrationVersion, tenant.ChangeMetadata{})
	if err != nil || current.Verification.TargetWatermark == "" {
		if err == nil {
			err = runtime.ErrInvariantViolation
		}
		return knowledgedriver.SwitchResult{}, err
	}
	return s.KnowledgeMigrationPublisher.Cleanup(ctx, knowledgedriver.CleanupRequest{TenantID: pathTenant, MigrationID: migrationID,
		ExpectedTenantVersion: in.ExpectedTenantVersion, ExpectedVersion: in.ExpectedMigrationVersion, RollbackSyncWatermark: current.Verification.TargetWatermark, At: time.Now().UTC()})
}

func (s Service) requireKnowledgeSwitch(ctx context.Context, principal Principal, pathTenant, migrationID string, expectedVersion int64, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	current, err := s.GetKnowledgeMigration(ctx, principal, pathTenant, migrationID)
	if err != nil {
		return migration.Migration{}, err
	}
	if s.KnowledgeMigrationPublisher == nil || expectedVersion < 1 || current.Version != expectedVersion {
		return migration.Migration{}, runtime.ErrVersionConflict
	}
	if metadata != (tenant.ChangeMetadata{}) {
		if err := metadata.Validate(); err != nil {
			return migration.Migration{}, err
		}
	}
	return current, nil
}

func knowledgeMigrationBinding(value config.ConfigV1) (config.BackendBinding, bool) {
	var binding config.BackendBinding
	for _, candidate := range value.BackendBindings {
		if candidate.Domain != knowledgeMigrationDomain {
			continue
		}
		if binding.Domain != "" || candidate.BackendProfileID == "" || candidate.BackendVersion < 1 || !hasCapability(candidate.Required, "tenant_filter") {
			return config.BackendBinding{}, false
		}
		binding = candidate
	}
	return binding, binding.Domain != ""
}

func knowledgeSwitchMetadata(switchID string, metadata tenant.ChangeMetadata) knowledgedriver.SwitchMetadata {
	return knowledgedriver.SwitchMetadata{SwitchID: switchID, ActorID: metadata.ActorID, ReasonCode: metadata.ReasonCode, CorrelationID: metadata.CorrelationID, TraceID: metadata.TraceID}
}
