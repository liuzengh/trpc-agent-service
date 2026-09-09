package migrationruntime

import (
	"context"
	"mime"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
)

type liveArtifactService struct {
	runtime           *DataPlaneRuntime
	tenantID, profile string
	cache             *dataPlaneCache[dataPlaneArtifactService]
}

func (s *liveArtifactService) raw(ctx context.Context, profile string, fn func(dataPlaneArtifactService) error) error {
	service, release, err := s.cache.borrow(ctx, profile)
	if err != nil {
		return err
	}
	defer release()
	return fn(service)
}
func (s *liveArtifactService) read(ctx context.Context, fn func(context.Context, dataPlaneArtifactService) error) error {
	return s.runtime.coordinator.WithRead(ctx, s.tenantID, datamigration.DomainArtifact, s.profile, func(ctx context.Context, profile string) error {
		return s.raw(ctx, profile, func(service dataPlaneArtifactService) error { return fn(ctx, service) })
	})
}
func (s *liveArtifactService) SaveArtifact(ctx context.Context, info artifact.SessionInfo, filename string, value *artifact.Artifact) (saved int, err error) {
	if value == nil {
		return 0, artifactplane.ErrInvalidArtifact
	}
	mimeType, _, err := mime.ParseMediaType(value.MimeType)
	if err != nil {
		return 0, artifactplane.ErrInvalidArtifact
	}
	copyValue := *value
	copyValue.Data = append([]byte(nil), value.Data...)
	preparedVersion := -1
	keysFn := func(ctx context.Context, profile string) (keys []string, err error) {
		err = s.raw(ctx, profile, func(service dataPlaneArtifactService) error {
			version, err := service.NextMigrationVersion(ctx, info, filename)
			if err != nil {
				return err
			}
			preparedVersion = version
			key, err := artifactEntryKey(artifactplane.MigrationEntry{AppName: info.AppName, UserID: info.UserID, SessionID: info.SessionID, Filename: filename, Version: version, MIMEType: mimeType})
			if err != nil {
				return err
			}
			keys = []string{key}
			return nil
		})
		return
	}
	err = s.runtime.coordinator.WithWriteKeys(ctx, s.tenantID, datamigration.DomainArtifact, s.profile, keysFn, func(ctx context.Context, profile string) error {
		return s.raw(ctx, profile, func(service dataPlaneArtifactService) error {
			var err error
			saved, err = service.SaveArtifact(ctx, info, filename, &copyValue)
			if err != nil {
				return err
			}
			if preparedVersion >= 0 && saved != preparedVersion {
				return datamigration.ErrMigrationConflict
			}
			return nil
		})
	})
	return saved, err
}
func (s *liveArtifactService) DeleteArtifact(ctx context.Context, info artifact.SessionInfo, filename string) error {
	keysFn := func(ctx context.Context, profile string) (keys []string, err error) {
		err = s.raw(ctx, profile, func(service dataPlaneArtifactService) error {
			entries, err := service.MigrationFileEntries(ctx, info, filename)
			if err != nil {
				return err
			}
			if len(entries) > maxDataPlaneMutationKeys {
				return datamigration.ErrMigrationCapability
			}
			for _, entry := range entries {
				key, err := artifactEntryKey(entry)
				if err != nil {
					return err
				}
				keys = append(keys, key)
			}
			return nil
		})
		return
	}
	return s.runtime.coordinator.WithWriteKeys(ctx, s.tenantID, datamigration.DomainArtifact, s.profile, keysFn, func(ctx context.Context, profile string) error {
		return s.raw(ctx, profile, func(service dataPlaneArtifactService) error { return service.DeleteArtifact(ctx, info, filename) })
	})
}
func (s *liveArtifactService) LoadArtifact(ctx context.Context, info artifact.SessionInfo, filename string, version *int) (value *artifact.Artifact, err error) {
	err = s.read(ctx, func(ctx context.Context, service dataPlaneArtifactService) error {
		var err error
		value, err = service.LoadArtifact(ctx, info, filename, version)
		return err
	})
	return
}
func (s *liveArtifactService) ListArtifactKeys(ctx context.Context, info artifact.SessionInfo) (keys []string, err error) {
	err = s.read(ctx, func(ctx context.Context, service dataPlaneArtifactService) error {
		var err error
		keys, err = service.ListArtifactKeys(ctx, info)
		return err
	})
	return
}
func (s *liveArtifactService) ListVersions(ctx context.Context, info artifact.SessionInfo, filename string) (versions []int, err error) {
	err = s.read(ctx, func(ctx context.Context, service dataPlaneArtifactService) error {
		var err error
		versions, err = service.ListVersions(ctx, info, filename)
		return err
	})
	return
}

var _ artifact.Service = (*liveArtifactService)(nil)

func (s *liveArtifactService) Close() error { s.runtime.unregister(s); return s.cache.Close() }
