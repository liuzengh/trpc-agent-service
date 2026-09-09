package storage

import (
	"bytes"
	"context"
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// MirroredSession reads from Primary and synchronously mirrors every mutation.
// A target failure is returned so cutover can never silently lose a write.
type MirroredSession struct {
	Primary session.Service
	Target  session.Service
}

var _ session.Service = (*MirroredSession)(nil)

func (service *MirroredSession) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	created, err := service.Primary.CreateSession(ctx, key, state, options...)
	if err != nil {
		return nil, err
	}
	if _, err := service.Target.CreateSession(ctx, key, state, options...); err != nil {
		return nil, errors.Join(ErrMirrorWrite, err)
	}
	return created, nil
}
func (service *MirroredSession) GetSession(ctx context.Context, key session.Key, options ...session.Option) (*session.Session, error) {
	return service.Primary.GetSession(ctx, key, options...)
}
func (service *MirroredSession) ListSessions(ctx context.Context, key session.UserKey, options ...session.Option) ([]*session.Session, error) {
	return service.Primary.ListSessions(ctx, key, options...)
}
func (service *MirroredSession) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) error {
	if err := service.Primary.DeleteSession(ctx, key, options...); err != nil {
		return err
	}
	if err := service.Target.DeleteSession(ctx, key, options...); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	if err := service.Primary.UpdateAppState(ctx, appName, state); err != nil {
		return err
	}
	if err := service.Target.UpdateAppState(ctx, appName, state); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) DeleteAppState(ctx context.Context, appName, key string) error {
	if err := service.Primary.DeleteAppState(ctx, appName, key); err != nil {
		return err
	}
	if err := service.Target.DeleteAppState(ctx, appName, key); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	return service.Primary.ListAppStates(ctx, appName)
}
func (service *MirroredSession) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	if err := service.Primary.UpdateUserState(ctx, key, state); err != nil {
		return err
	}
	if err := service.Target.UpdateUserState(ctx, key, state); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	return service.Primary.ListUserStates(ctx, key)
}
func (service *MirroredSession) DeleteUserState(ctx context.Context, key session.UserKey, name string) error {
	if err := service.Primary.DeleteUserState(ctx, key, name); err != nil {
		return err
	}
	if err := service.Target.DeleteUserState(ctx, key, name); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	if err := service.Primary.UpdateSessionState(ctx, key, state); err != nil {
		return err
	}
	if err := service.Target.UpdateSessionState(ctx, key, state); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) AppendEvent(ctx context.Context, current *session.Session, item *event.Event, options ...session.Option) error {
	if err := service.Primary.AppendEvent(ctx, current, item, options...); err != nil {
		return err
	}
	key := session.Key{AppName: current.AppName, UserID: current.UserID, SessionID: current.ID}
	shadow, err := service.Target.GetSession(ctx, key)
	if err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	if shadow == nil {
		shadow, err = service.Target.CreateSession(ctx, key, nil)
		if err != nil {
			return errors.Join(ErrMirrorWrite, err)
		}
	}
	if err := service.Target.AppendEvent(ctx, shadow, item, options...); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) CreateSessionSummary(ctx context.Context, current *session.Session, filter string, force bool) error {
	if err := service.Primary.CreateSessionSummary(ctx, current, filter, force); err != nil {
		return err
	}
	key := session.Key{AppName: current.AppName, UserID: current.UserID, SessionID: current.ID}
	shadow, err := service.Target.GetSession(ctx, key)
	if err != nil || shadow == nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	if err := service.Target.CreateSessionSummary(ctx, shadow, filter, force); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) EnqueueSummaryJob(ctx context.Context, current *session.Session, filter string, force bool) error {
	if err := service.Primary.EnqueueSummaryJob(ctx, current, filter, force); err != nil {
		return err
	}
	key := session.Key{AppName: current.AppName, UserID: current.UserID, SessionID: current.ID}
	shadow, err := service.Target.GetSession(ctx, key)
	if err != nil || shadow == nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	if err := service.Target.EnqueueSummaryJob(ctx, shadow, filter, force); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredSession) GetSessionSummaryText(ctx context.Context, current *session.Session, options ...session.SummaryOption) (string, bool) {
	return service.Primary.GetSessionSummaryText(ctx, current, options...)
}
func (service *MirroredSession) Close() error {
	return errors.Join(service.Primary.Close(), service.Target.Close())
}

// MirroredMemory reads from Primary and mirrors all state changes to Target.
type MirroredMemory struct{ Primary, Target memory.Service }

var _ memory.Service = (*MirroredMemory)(nil)

func (service *MirroredMemory) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	return service.Primary.ReadMemories(ctx, key, limit)
}
func (service *MirroredMemory) SearchMemories(ctx context.Context, key memory.UserKey, query string, options ...memory.SearchOption) ([]*memory.Entry, error) {
	return service.Primary.SearchMemories(ctx, key, query, options...)
}
func (service *MirroredMemory) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, options ...memory.AddOption) error {
	if err := service.Primary.AddMemory(ctx, key, value, topics, options...); err != nil {
		return err
	}
	if err := service.Target.AddMemory(ctx, key, value, topics, options...); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredMemory) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, options ...memory.UpdateOption) error {
	if err := service.Primary.UpdateMemory(ctx, key, value, topics, options...); err != nil {
		return err
	}
	if err := service.Target.UpdateMemory(ctx, key, value, topics, options...); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredMemory) DeleteMemory(ctx context.Context, key memory.Key) error {
	if err := service.Primary.DeleteMemory(ctx, key); err != nil {
		return err
	}
	if err := service.Target.DeleteMemory(ctx, key); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredMemory) ClearMemories(ctx context.Context, key memory.UserKey) error {
	if err := service.Primary.ClearMemories(ctx, key); err != nil {
		return err
	}
	if err := service.Target.ClearMemories(ctx, key); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredMemory) Tools() []tool.Tool { return service.Primary.Tools() }
func (service *MirroredMemory) EnqueueAutoMemoryJob(ctx context.Context, current *session.Session) error {
	if err := service.Primary.EnqueueAutoMemoryJob(ctx, current); err != nil {
		return err
	}
	if err := service.Target.EnqueueAutoMemoryJob(ctx, current); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredMemory) Close() error {
	return errors.Join(service.Primary.Close(), service.Target.Close())
}

// MirroredArtifact keeps revision numbers identical on both backends.
type MirroredArtifact struct{ Primary, Target artifact.Service }

var _ artifact.Service = (*MirroredArtifact)(nil)

func (service *MirroredArtifact) SaveArtifact(ctx context.Context, info artifact.SessionInfo, name string, value *artifact.Artifact) (int, error) {
	revision, err := service.Primary.SaveArtifact(ctx, info, name, value)
	if err != nil {
		return 0, err
	}
	// A previous attempt may have committed the primary revision but failed or
	// lost its response while writing the target. Repair every missing target
	// revision in order so an Inbox retry cannot permanently desynchronise the
	// two append-only stores.
	versions, err := service.Target.ListVersions(ctx, info, name)
	if err != nil {
		return 0, errors.Join(ErrMirrorWrite, err)
	}
	present := make(map[int]bool, len(versions))
	for _, current := range versions {
		present[current] = true
	}
	for current := 0; current <= revision; current++ {
		primary, loadErr := service.Primary.LoadArtifact(ctx, info, name, &current)
		if loadErr != nil || primary == nil {
			return 0, errors.Join(ErrMirrorWrite, errors.New("primary artifact revision is unavailable"))
		}
		if present[current] {
			shadow, loadErr := service.Target.LoadArtifact(ctx, info, name, &current)
			if loadErr != nil || !sameArtifact(primary, shadow) {
				return 0, errors.Join(ErrMirrorWrite, errors.New("artifact revision content mismatch"))
			}
			continue
		}
		shadowRevision, saveErr := service.Target.SaveArtifact(ctx, info, name, primary)
		if saveErr != nil {
			return 0, errors.Join(ErrMirrorWrite, saveErr)
		}
		if shadowRevision != current {
			return 0, errors.Join(ErrMirrorWrite, errors.New("artifact revision mismatch"))
		}
	}
	return revision, nil
}

func sameArtifact(left, right *artifact.Artifact) bool {
	return left != nil && right != nil && bytes.Equal(left.Data, right.Data) && left.MimeType == right.MimeType && left.URL == right.URL && left.Name == right.Name
}
func (service *MirroredArtifact) LoadArtifact(ctx context.Context, info artifact.SessionInfo, name string, version *int) (*artifact.Artifact, error) {
	return service.Primary.LoadArtifact(ctx, info, name, version)
}
func (service *MirroredArtifact) ListArtifactKeys(ctx context.Context, info artifact.SessionInfo) ([]string, error) {
	return service.Primary.ListArtifactKeys(ctx, info)
}
func (service *MirroredArtifact) DeleteArtifact(ctx context.Context, info artifact.SessionInfo, name string) error {
	if err := service.Primary.DeleteArtifact(ctx, info, name); err != nil {
		return err
	}
	if err := service.Target.DeleteArtifact(ctx, info, name); err != nil {
		return errors.Join(ErrMirrorWrite, err)
	}
	return nil
}
func (service *MirroredArtifact) ListVersions(ctx context.Context, info artifact.SessionInfo, name string) ([]int, error) {
	return service.Primary.ListVersions(ctx, info, name)
}
func (service *MirroredArtifact) Close() error {
	var primaryErr, targetErr error
	if closer, ok := service.Primary.(interface{ Close() error }); ok {
		primaryErr = closer.Close()
	}
	if closer, ok := service.Target.(interface{ Close() error }); ok {
		targetErr = closer.Close()
	}
	return errors.Join(primaryErr, targetErr)
}

// ErrMirrorWrite marks a migration target failure without leaking its endpoint.
var ErrMirrorWrite = errors.New("storage: migration target write failed")
