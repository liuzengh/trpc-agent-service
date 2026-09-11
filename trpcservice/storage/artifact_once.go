package storage

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

// SaveArtifactOnce makes the named object immutable. Read/check/save uses the
// same local and distributed lock as normal version allocation, so retrying an
// attachment import does not create another version after acknowledgement loss.
func (r *ArtifactRouter) SaveArtifactOnce(ctx context.Context, info artifact.SessionInfo, name string, value *artifact.Artifact) (int, error) {
	if value == nil {
		return 0, errors.New("artifact required")
	}
	service, err := r.serviceFor(ctx, info.AppName)
	if err != nil {
		return 0, err
	}
	lock := r.lockFor(info, name)
	lock.Lock()
	defer lock.Unlock()
	version := 0
	operation := func() error {
		keys, err := service.ListArtifactKeys(ctx, info)
		if err != nil {
			return err
		}
		if slices.Contains(keys, name) {
			old, err := service.LoadArtifact(ctx, info, name, nil)
			if err != nil {
				return err
			}
			if old == nil || old.MimeType != value.MimeType || !bytes.Equal(old.Data, value.Data) {
				return errors.New("immutable artifact content conflict")
			}
			versions, err := service.ListVersions(ctx, info, name)
			if err != nil {
				return err
			}
			for _, v := range versions {
				version = max(version, v)
			}
			return nil
		}
		version, err = service.SaveArtifact(ctx, info, name, value)
		return err
	}
	if r.lockDB != nil {
		err = r.withDistributedLock(ctx, r.artifactLockKey(info, name), operation)
	} else {
		err = operation()
	}
	return version, err
}
