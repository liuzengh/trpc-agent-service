package backend

import (
	"context"
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

// ScopedArtifact prevents callers from selecting another tenant/application
// through SessionInfo.AppName. The platform-derived namespace always wins.
type ScopedArtifact struct {
	inner   artifact.Service
	appName string
}

func NewScopedArtifact(inner artifact.Service, appName string) (*ScopedArtifact, error) {
	if inner == nil || appName == "" {
		return nil, errors.New("artifact service and application scope are required")
	}
	return &ScopedArtifact{inner: inner, appName: appName}, nil
}

func (s *ScopedArtifact) scope(info artifact.SessionInfo) artifact.SessionInfo {
	info.AppName = s.appName
	return info
}

func (s *ScopedArtifact) SaveArtifact(ctx context.Context, info artifact.SessionInfo, filename string, value *artifact.Artifact) (int, error) {
	return s.inner.SaveArtifact(ctx, s.scope(info), filename, value)
}

func (s *ScopedArtifact) LoadArtifact(ctx context.Context, info artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	return s.inner.LoadArtifact(ctx, s.scope(info), filename, version)
}

func (s *ScopedArtifact) ListArtifactKeys(ctx context.Context, info artifact.SessionInfo) ([]string, error) {
	return s.inner.ListArtifactKeys(ctx, s.scope(info))
}

func (s *ScopedArtifact) DeleteArtifact(ctx context.Context, info artifact.SessionInfo, filename string) error {
	return s.inner.DeleteArtifact(ctx, s.scope(info), filename)
}

func (s *ScopedArtifact) ListVersions(ctx context.Context, info artifact.SessionInfo, filename string) ([]int, error) {
	return s.inner.ListVersions(ctx, s.scope(info), filename)
}
