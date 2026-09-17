// Package admin exposes the authenticated minimum control-plane operations.
package admin

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrUnauthenticated = errors.New("admin principal is not authenticated")
	ErrForbidden       = errors.New("admin principal is not authorized for tenant")
)

type Principal struct {
	Authenticated     bool
	TenantID          string
	SubjectID         string
	CanManage         bool
	CanManageReleases bool
}

type Service struct {
	Configs                     config.Repository
	Migrations                  migration.Repository
	SessionMigrationPublisher   migration.SessionCutoverPublisher
	KnowledgeMigrationPublisher knowledgedriver.CutoverPublisher
}

func authorize(principal Principal, pathTenant string) error {
	if !principal.Authenticated {
		return ErrUnauthenticated
	}
	if !principal.CanManage || principal.TenantID == "" || principal.TenantID != pathTenant {
		return ErrForbidden
	}
	return nil
}

func (s Service) Validate(ctx context.Context, principal Principal, pathTenant string, payload config.ConfigV1) error {
	if err := authorize(principal, pathTenant); err != nil {
		return err
	}
	return s.Configs.Validate(ctx, config.ValidateInput{TenantID: pathTenant, Payload: payload})
}

func (s Service) Publish(ctx context.Context, principal Principal, pathTenant string, expectedVersion int64, payload config.ConfigV1, metadata tenant.ChangeMetadata) (config.PublishResult, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.PublishResult{}, err
	}
	return s.Configs.Publish(ctx, config.PublishInput{TenantID: pathTenant, ExpectedTenantVersion: expectedVersion, Payload: payload, Metadata: metadata})
}

func (s Service) Current(ctx context.Context, principal Principal, pathTenant string) (config.Snapshot, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.Snapshot{}, err
	}
	return s.Configs.GetCurrent(ctx, pathTenant)
}

func (s Service) Get(ctx context.Context, principal Principal, pathTenant string, version int64) (config.Snapshot, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.Snapshot{}, err
	}
	return s.Configs.Get(ctx, pathTenant, version)
}

func (s Service) Rollback(ctx context.Context, principal Principal, pathTenant string, expectedVersion, targetVersion int64, metadata tenant.ChangeMetadata) (config.PublishResult, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.PublishResult{}, err
	}
	return s.Configs.Rollback(ctx, config.RollbackInput{TenantID: pathTenant, ExpectedTenantVersion: expectedVersion, TargetVersion: targetVersion, Metadata: metadata})
}

func (s Service) Stage(ctx context.Context, principal Principal, pathTenant string, expectedVersion int64, payload config.ConfigV1, metadata tenant.ChangeMetadata) (config.Snapshot, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.Snapshot{}, err
	}
	return s.Configs.Stage(ctx, config.StageInput{TenantID: pathTenant, ExpectedTenantVersion: expectedVersion, Payload: payload, Metadata: metadata})
}
func (s Service) CreateRelease(ctx context.Context, principal Principal, in config.ReleaseCreateInput) (config.Release, error) {
	if err := authorizeReleases(principal); err != nil {
		return config.Release{}, err
	}
	return s.Configs.CreateRelease(ctx, in)
}
func (s Service) GetRelease(ctx context.Context, principal Principal, releaseID string) (config.Release, error) {
	if err := authorizeReleases(principal); err != nil {
		return config.Release{}, err
	}
	return s.Configs.GetRelease(ctx, releaseID)
}
func (s Service) UpdateRelease(ctx context.Context, principal Principal, in config.ReleaseUpdateInput) (config.Release, error) {
	if err := authorizeReleases(principal); err != nil {
		return config.Release{}, err
	}
	return s.Configs.UpdateRelease(ctx, in)
}
func (s Service) RollbackRelease(ctx context.Context, principal Principal, in config.ReleaseRollbackInput) (config.Release, error) {
	if err := authorizeReleases(principal); err != nil {
		return config.Release{}, err
	}
	return s.Configs.RollbackRelease(ctx, in)
}

func authorizeReleases(principal Principal) error {
	if !principal.Authenticated {
		return ErrUnauthenticated
	}
	if !principal.CanManageReleases || principal.SubjectID == "" {
		return ErrForbidden
	}
	return nil
}
