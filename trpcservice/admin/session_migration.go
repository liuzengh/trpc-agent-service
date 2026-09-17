package admin

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const sessionMigrationDomain = "session"

// SessionMigrationCreateInput deliberately derives the source from the active
// ConfigSnapshot. The only operator choice is the immutable candidate config
// and a stable idempotency identity for this migration. Configuration
// snapshots are immutable once published; a candidate is distinguished from
// the active source by the tenant's active-config pointer, not by a mutable
// snapshot state.
type SessionMigrationCreateInput struct {
	MigrationID         string `json:"migration_id"`
	TargetConfigVersion int64  `json:"target_config_version"`
}

// SessionMigrationSwitchInput contains only CAS and idempotency coordinates.
// Verification and rollback watermark are loaded from migration authority,
// never accepted from a browser request.
type SessionMigrationSwitchInput struct {
	ExpectedTenantVersion    int64  `json:"expected_tenant_version"`
	ExpectedMigrationVersion int64  `json:"expected_migration_version"`
	SwitchID                 string `json:"switch_id"`
}

type SessionMigrationObserveInput struct {
	ExpectedTenantVersion    int64     `json:"expected_tenant_version"`
	ExpectedMigrationVersion int64     `json:"expected_migration_version"`
	ObserveUntil             time.Time `json:"observe_until"`
}

// SessionMigrationControlInput is intentionally migration-CAS-only: these
// controls never repoint tenant configuration, unlike cutover and rollback.
type SessionMigrationControlInput struct {
	ExpectedMigrationVersion int64 `json:"expected_migration_version"`
}

type SessionMigrationStatus struct {
	Migration migration.Migration          `json:"migration"`
	Drain     migration.SessionDrainStatus `json:"drain"`
}

func (s Service) CreateSessionMigration(ctx context.Context, principal Principal, pathTenant string, in SessionMigrationCreateInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
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
	sourceBinding, ok := sessionBinding(source.Payload)
	if !ok {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	targetBinding, ok := sessionBinding(target.Payload)
	if !ok || (sourceBinding.BackendProfileID == targetBinding.BackendProfileID && sourceBinding.BackendVersion == targetBinding.BackendVersion) {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.Migrations.Create(ctx, migration.CreateRequest{TenantID: pathTenant, MigrationID: in.MigrationID,
		Domain: sessionMigrationDomain, Epoch: target.ConfigVersion,
		Source:    migration.Binding{ConfigVersion: source.ConfigVersion, BackendProfileID: sourceBinding.BackendProfileID, BackendVersion: sourceBinding.BackendVersion},
		Target:    migration.Binding{ConfigVersion: target.ConfigVersion, BackendProfileID: targetBinding.BackendProfileID, BackendVersion: targetBinding.BackendVersion},
		CreatedAt: time.Now().UTC(), Audit: migration.CreateAudit{ActorID: metadata.ActorID, ReasonCode: metadata.ReasonCode,
			CorrelationID: metadata.CorrelationID, TraceID: metadata.TraceID}})
}

func (s Service) GetSessionMigration(ctx context.Context, principal Principal, pathTenant, migrationID string) (migration.Migration, error) {
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
	if value.Domain != sessionMigrationDomain {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return value, nil
}

func (s Service) ListSessionMigrations(ctx context.Context, principal Principal, pathTenant string) ([]migration.Migration, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return nil, err
	}
	if s.Migrations == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return s.Migrations.List(ctx, pathTenant, sessionMigrationDomain)
}

func (s Service) GetSessionMigrationStatus(ctx context.Context, principal Principal, pathTenant, migrationID string) (SessionMigrationStatus, error) {
	value, err := s.GetSessionMigration(ctx, principal, pathTenant, migrationID)
	if err != nil {
		return SessionMigrationStatus{}, err
	}
	if s.SessionMigrationPublisher == nil {
		return SessionMigrationStatus{}, runtime.ErrCapabilityUnsupported
	}
	drain, err := s.SessionMigrationPublisher.DrainStatus(ctx, pathTenant, migrationID)
	if err != nil {
		return SessionMigrationStatus{}, err
	}
	return SessionMigrationStatus{Migration: value, Drain: drain}, nil
}

func (s Service) PauseSessionMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in SessionMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlSessionMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Pause)
}

func (s Service) ResumeSessionMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in SessionMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlSessionMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Resume)
}

func (s Service) AbortSessionMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in SessionMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlSessionMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Abort)
}

func (s Service) controlSessionMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in SessionMigrationControlInput, metadata tenant.ChangeMetadata, apply func(context.Context, migration.ControlRequest) (migration.Migration, error)) (migration.Migration, error) {
	if in.ExpectedMigrationVersion < 1 {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	if _, err := s.GetSessionMigration(ctx, principal, pathTenant, migrationID); err != nil {
		return migration.Migration{}, err
	}
	if err := metadata.Validate(); err != nil {
		return migration.Migration{}, err
	}
	return apply(ctx, migration.ControlRequest{TenantID: pathTenant, MigrationID: migrationID, ExpectedVersion: in.ExpectedMigrationVersion,
		At: time.Now().UTC(), Metadata: migration.ControlMetadata{ActorID: metadata.ActorID, ReasonCode: metadata.ReasonCode,
			CorrelationID: metadata.CorrelationID, TraceID: metadata.TraceID}})
}

func (s Service) CutoverSessionMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in SessionMigrationSwitchInput, metadata tenant.ChangeMetadata) (migration.SessionSwitchResult, error) {
	current, err := s.requireSessionSwitch(ctx, principal, pathTenant, migrationID, in.ExpectedMigrationVersion, metadata)
	if err != nil {
		return migration.SessionSwitchResult{}, err
	}
	if current.Verification.SourceDigest == "" {
		return migration.SessionSwitchResult{}, runtime.ErrInvariantViolation
	}
	return s.SessionMigrationPublisher.Cutover(ctx, migration.SessionCutoverRequest{TenantID: pathTenant, MigrationID: migrationID,
		ExpectedTenantVersion: in.ExpectedTenantVersion, ExpectedVersion: in.ExpectedMigrationVersion,
		Verification: current.Verification, At: time.Now().UTC(), Metadata: switchMetadata(in.SwitchID, metadata)})
}

func (s Service) BeginSessionMigrationObserve(ctx context.Context, principal Principal, pathTenant, migrationID string, in SessionMigrationObserveInput) (migration.SessionSwitchResult, error) {
	if _, err := s.requireSessionSwitch(ctx, principal, pathTenant, migrationID, in.ExpectedMigrationVersion, tenant.ChangeMetadata{}); err != nil {
		return migration.SessionSwitchResult{}, err
	}
	return s.SessionMigrationPublisher.BeginObserve(ctx, migration.SessionObserveRequest{TenantID: pathTenant, MigrationID: migrationID,
		ExpectedTenantVersion: in.ExpectedTenantVersion, ExpectedVersion: in.ExpectedMigrationVersion, At: time.Now().UTC(), ObserveUntil: in.ObserveUntil.UTC()})
}

func (s Service) RollbackSessionMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in SessionMigrationSwitchInput, metadata tenant.ChangeMetadata) (migration.SessionSwitchResult, error) {
	current, err := s.requireSessionSwitch(ctx, principal, pathTenant, migrationID, in.ExpectedMigrationVersion, metadata)
	if err != nil {
		return migration.SessionSwitchResult{}, err
	}
	if current.Verification.TargetWatermark == "" {
		return migration.SessionSwitchResult{}, runtime.ErrInvariantViolation
	}
	return s.SessionMigrationPublisher.Rollback(ctx, migration.SessionRollbackRequest{TenantID: pathTenant, MigrationID: migrationID,
		ExpectedTenantVersion: in.ExpectedTenantVersion, ExpectedVersion: in.ExpectedMigrationVersion,
		RollbackSyncWatermark: current.Verification.TargetWatermark, At: time.Now().UTC(), Metadata: switchMetadata(in.SwitchID, metadata)})
}

func (s Service) CleanupSessionMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in SessionMigrationSwitchInput) (migration.SessionSwitchResult, error) {
	current, err := s.requireSessionSwitch(ctx, principal, pathTenant, migrationID, in.ExpectedMigrationVersion, tenant.ChangeMetadata{})
	if err != nil {
		return migration.SessionSwitchResult{}, err
	}
	if current.Verification.TargetWatermark == "" {
		return migration.SessionSwitchResult{}, runtime.ErrInvariantViolation
	}
	return s.SessionMigrationPublisher.Cleanup(ctx, migration.SessionCleanupRequest{TenantID: pathTenant, MigrationID: migrationID,
		ExpectedTenantVersion: in.ExpectedTenantVersion, ExpectedVersion: in.ExpectedMigrationVersion,
		RollbackSyncWatermark: current.Verification.TargetWatermark, At: time.Now().UTC()})
}

func (s Service) requireSessionSwitch(ctx context.Context, principal Principal, pathTenant, migrationID string, expectedMigrationVersion int64, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	current, err := s.GetSessionMigration(ctx, principal, pathTenant, migrationID)
	if err != nil {
		return migration.Migration{}, err
	}
	if s.SessionMigrationPublisher == nil || expectedMigrationVersion < 1 || current.Version != expectedMigrationVersion {
		return migration.Migration{}, runtime.ErrVersionConflict
	}
	if metadata != (tenant.ChangeMetadata{}) {
		if err := metadata.Validate(); err != nil {
			return migration.Migration{}, err
		}
	}
	return current, nil
}

func sessionBinding(value config.ConfigV1) (config.BackendBinding, bool) {
	var binding config.BackendBinding
	for _, candidate := range value.BackendBindings {
		if candidate.Domain != sessionMigrationDomain {
			continue
		}
		if binding.Domain != "" || candidate.BackendProfileID == "" || candidate.BackendVersion < 1 || !hasCapability(candidate.Required, "atomic_turn_commit") {
			return config.BackendBinding{}, false
		}
		binding = candidate
	}
	return binding, binding.Domain != ""
}

func hasCapability(values []string, capability string) bool {
	for _, value := range values {
		if value == capability {
			return true
		}
	}
	return false
}

func switchMetadata(switchID string, metadata tenant.ChangeMetadata) migration.SessionSwitchMetadata {
	return migration.SessionSwitchMetadata{SwitchID: switchID, ActorID: metadata.ActorID, ReasonCode: metadata.ReasonCode,
		CorrelationID: metadata.CorrelationID, TraceID: metadata.TraceID}
}
