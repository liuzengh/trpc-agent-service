package admin

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type MemoryMigrationCreateInput struct {
	MigrationID         string `json:"migration_id"`
	TargetConfigVersion int64  `json:"target_config_version"`
}
type MemoryMigrationControlInput struct {
	ExpectedMigrationVersion int64 `json:"expected_migration_version"`
}

func (s Service) CreateMemoryMigration(ctx context.Context, principal Principal, pathTenant string, in MemoryMigrationCreateInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
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
	if source.TenantID != pathTenant || target.TenantID != pathTenant || target.ConfigVersion <= source.ConfigVersion || target.State != config.StatePublished {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	sourceBinding, ok := memoryMigrationBinding(source.Payload)
	if !ok {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	targetBinding, ok := memoryMigrationBinding(target.Payload)
	if !ok || (sourceBinding.BackendProfileID == targetBinding.BackendProfileID && sourceBinding.BackendVersion == targetBinding.BackendVersion) {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.Migrations.Create(ctx, migration.CreateRequest{TenantID: pathTenant, MigrationID: in.MigrationID, Domain: memorydriver.Domain,
		Epoch: target.ConfigVersion, Source: migration.Binding{ConfigVersion: source.ConfigVersion, BackendProfileID: sourceBinding.BackendProfileID, BackendVersion: sourceBinding.BackendVersion},
		Target:    migration.Binding{ConfigVersion: target.ConfigVersion, BackendProfileID: targetBinding.BackendProfileID, BackendVersion: targetBinding.BackendVersion},
		CreatedAt: time.Now().UTC(), Audit: migration.CreateAudit{ActorID: metadata.ActorID, ReasonCode: metadata.ReasonCode, CorrelationID: metadata.CorrelationID, TraceID: metadata.TraceID}})
}

func (s Service) GetMemoryMigration(ctx context.Context, principal Principal, pathTenant, migrationID string) (migration.Migration, error) {
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
	if value.Domain != memorydriver.Domain {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return value, nil
}
func (s Service) ListMemoryMigrations(ctx context.Context, principal Principal, pathTenant string) ([]migration.Migration, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return nil, err
	}
	if s.Migrations == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return s.Migrations.List(ctx, pathTenant, memorydriver.Domain)
}
func (s Service) PauseMemoryMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in MemoryMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlMemoryMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Pause)
}
func (s Service) ResumeMemoryMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in MemoryMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlMemoryMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Resume)
}
func (s Service) AbortMemoryMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in MemoryMigrationControlInput, metadata tenant.ChangeMetadata) (migration.Migration, error) {
	if s.Migrations == nil {
		return migration.Migration{}, runtime.ErrCapabilityUnsupported
	}
	return s.controlMemoryMigration(ctx, principal, pathTenant, migrationID, in, metadata, s.Migrations.Abort)
}
func (s Service) controlMemoryMigration(ctx context.Context, principal Principal, pathTenant, migrationID string, in MemoryMigrationControlInput, metadata tenant.ChangeMetadata, apply func(context.Context, migration.ControlRequest) (migration.Migration, error)) (migration.Migration, error) {
	if s.Migrations == nil || in.ExpectedMigrationVersion < 1 {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	if _, err := s.GetMemoryMigration(ctx, principal, pathTenant, migrationID); err != nil {
		return migration.Migration{}, err
	}
	if err := metadata.Validate(); err != nil {
		return migration.Migration{}, err
	}
	return apply(ctx, migration.ControlRequest{TenantID: pathTenant, MigrationID: migrationID, ExpectedVersion: in.ExpectedMigrationVersion, At: time.Now().UTC(), Metadata: migration.ControlMetadata{ActorID: metadata.ActorID, ReasonCode: metadata.ReasonCode, CorrelationID: metadata.CorrelationID, TraceID: metadata.TraceID}})
}

func memoryMigrationBinding(value config.ConfigV1) (config.BackendBinding, bool) {
	var binding config.BackendBinding
	for _, candidate := range value.BackendBindings {
		if candidate.Domain != memorydriver.Domain {
			continue
		}
		if binding.Domain != "" || candidate.BackendProfileID == "" || candidate.BackendVersion < 1 || !hasCapability(candidate.Required, "strong_ryw") {
			return config.BackendBinding{}, false
		}
		binding = candidate
	}
	return binding, binding.Domain != ""
}
