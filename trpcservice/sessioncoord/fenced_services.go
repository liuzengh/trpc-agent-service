package sessioncoord

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// FencedMemoryService protects Memory mutations with the session fence held by
// the Worker. Reads remain available to reconstruct a run after failover.
type FencedMemoryService struct {
	delegate  memory.Service
	validator FenceValidator
}

func NewFencedMemoryService(delegate memory.Service, validator FenceValidator) (*FencedMemoryService, error) {
	if delegate == nil || validator == nil {
		return nil, errors.New("sessioncoord: memory service and validator are required")
	}
	return &FencedMemoryService{delegate: delegate, validator: validator}, nil
}

func (service *FencedMemoryService) leaseFor(ctx context.Context, appName, userID string) (Lease, error) {
	lease, ok := ctx.Value(leaseContextKey{}).(Lease)
	if !ok {
		return Lease{}, errors.New("sessioncoord: lease missing from context")
	}
	wantApp, err := tenant.CanonicalAppName(lease.Key.TenantID, lease.Key.AppID)
	if err != nil || appName != wantApp || userID != lease.Key.UserID {
		return Lease{}, fmt.Errorf("sessioncoord: memory write scope does not match lease")
	}
	return lease, nil
}

func (service *FencedMemoryService) mutate(ctx context.Context, appName, userID string, operation func(context.Context) error) error {
	lease, err := service.leaseFor(ctx, appName, userID)
	if err != nil {
		return err
	}
	return service.validator.WithFence(ctx, lease.Key, lease.Token, operation)
}

func (service *FencedMemoryService) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	return service.delegate.ReadMemories(ctx, key, limit)
}
func (service *FencedMemoryService) SearchMemories(ctx context.Context, key memory.UserKey, query string, opts ...memory.SearchOption) ([]*memory.Entry, error) {
	return service.delegate.SearchMemories(ctx, key, query, opts...)
}
func (service *FencedMemoryService) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, opts ...memory.AddOption) error {
	return service.mutate(ctx, key.AppName, key.UserID, func(c context.Context) error { return service.delegate.AddMemory(c, key, value, topics, opts...) })
}
func (service *FencedMemoryService) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, opts ...memory.UpdateOption) error {
	return service.mutate(ctx, key.AppName, key.UserID, func(c context.Context) error { return service.delegate.UpdateMemory(c, key, value, topics, opts...) })
}
func (service *FencedMemoryService) DeleteMemory(ctx context.Context, key memory.Key) error {
	return service.mutate(ctx, key.AppName, key.UserID, func(c context.Context) error { return service.delegate.DeleteMemory(c, key) })
}
func (service *FencedMemoryService) ClearMemories(ctx context.Context, key memory.UserKey) error {
	return service.mutate(ctx, key.AppName, key.UserID, func(c context.Context) error { return service.delegate.ClearMemories(c, key) })
}
func (service *FencedMemoryService) Tools() []tool.Tool { return service.delegate.Tools() }
func (service *FencedMemoryService) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	if sess == nil {
		return errors.New("sessioncoord: nil session")
	}
	return service.mutate(ctx, sess.AppName, sess.UserID, func(c context.Context) error { return service.delegate.EnqueueAutoMemoryJob(c, sess) })
}
func (service *FencedMemoryService) Close() error { return service.delegate.Close() }

// FencedArtifactService protects artifact saves/deletes with the session fence.
type FencedArtifactService struct {
	delegate  artifact.Service
	validator FenceValidator
}

func NewFencedArtifactService(delegate artifact.Service, validator FenceValidator) (*FencedArtifactService, error) {
	if delegate == nil || validator == nil {
		return nil, errors.New("sessioncoord: artifact service and validator are required")
	}
	return &FencedArtifactService{delegate: delegate, validator: validator}, nil
}
func (service *FencedArtifactService) scope(ctx context.Context, info artifact.SessionInfo) (Lease, error) {
	lease, ok := ctx.Value(leaseContextKey{}).(Lease)
	if !ok {
		return Lease{}, errors.New("sessioncoord: lease missing from context")
	}
	wantApp, err := tenant.CanonicalAppName(lease.Key.TenantID, lease.Key.AppID)
	if err != nil || info.AppName != wantApp || info.UserID != lease.Key.UserID || info.SessionID != lease.Key.SessionID {
		return Lease{}, errors.New("sessioncoord: artifact write scope does not match lease")
	}
	return lease, nil
}
func (service *FencedArtifactService) SaveArtifact(ctx context.Context, info artifact.SessionInfo, name string, value *artifact.Artifact) (revision int, err error) {
	lease, err := service.scope(ctx, info)
	if err != nil {
		return 0, err
	}
	err = service.validator.WithFence(ctx, lease.Key, lease.Token, func(c context.Context) error {
		revision, err = service.delegate.SaveArtifact(c, info, name, value)
		return err
	})
	return revision, err
}
func (service *FencedArtifactService) LoadArtifact(ctx context.Context, info artifact.SessionInfo, name string, version *int) (*artifact.Artifact, error) {
	return service.delegate.LoadArtifact(ctx, info, name, version)
}
func (service *FencedArtifactService) ListArtifactKeys(ctx context.Context, info artifact.SessionInfo) ([]string, error) {
	return service.delegate.ListArtifactKeys(ctx, info)
}
func (service *FencedArtifactService) DeleteArtifact(ctx context.Context, info artifact.SessionInfo, name string) error {
	lease, err := service.scope(ctx, info)
	if err != nil {
		return err
	}
	return service.validator.WithFence(ctx, lease.Key, lease.Token, func(c context.Context) error { return service.delegate.DeleteArtifact(c, info, name) })
}
func (service *FencedArtifactService) ListVersions(ctx context.Context, info artifact.SessionInfo, name string) ([]int, error) {
	return service.delegate.ListVersions(ctx, info, name)
}

var _ memory.Service = (*FencedMemoryService)(nil)
var _ artifact.Service = (*FencedArtifactService)(nil)
