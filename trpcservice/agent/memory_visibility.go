package agent

import (
	"context"
	"errors"

	"github.com/cyl6/trpc-agent-service/trpcservice/memoryvisibility"
	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// visibleMemoryService adds an explicit, verifiable read-after-write fence to
// a framework memory backend. Without a minimum watermark the framework keeps
// its normal eventual-read behaviour; with one, a stale node returns a
// degraded/timeout error instead of claiming visibility it cannot prove.
type visibleMemoryService struct {
	inner   memory.Service
	store   memoryvisibility.Store
	tenant  string
	app     string
	backend string
}

func newVisibleMemoryService(inner memory.Service, store memoryvisibility.Store, tenant, app string, backend ...string) memory.Service {
	if inner == nil {
		return inner
	}
	kind := "unknown"
	if len(backend) > 0 {
		kind = backend[0]
	}
	return &visibleMemoryService{inner: inner, store: store, tenant: tenant, app: app, backend: kind}
}

func (s *visibleMemoryService) scope(userKey memory.UserKey) memoryvisibility.Scope {
	return memoryvisibility.Scope{TenantID: s.tenant, AppName: s.app, PrincipalID: userKey.UserID}
}

func (s *visibleMemoryService) wait(ctx context.Context, userKey memory.UserKey) error {
	if s.store == nil {
		return nil
	}
	minimum, ok := memoryvisibility.Minimum(ctx)
	if !ok {
		return nil
	}
	_, err := s.store.WaitUntilVisible(ctx, s.scope(userKey), minimum)
	return err
}

func (s *visibleMemoryService) ReadMemories(ctx context.Context, userKey memory.UserKey, limit int) (entries []*memory.Entry, err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.read", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	if err := s.wait(ctx, userKey); err != nil {
		return nil, err
	}
	return s.inner.ReadMemories(ctx, userKey, limit)
}

func (s *visibleMemoryService) SearchMemories(ctx context.Context, userKey memory.UserKey, query string, opts ...memory.SearchOption) (entries []*memory.Entry, err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.search", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	if err := s.wait(ctx, userKey); err != nil {
		return nil, err
	}
	return s.inner.SearchMemories(ctx, userKey, query, opts...)
}

func (s *visibleMemoryService) AddMemory(ctx context.Context, userKey memory.UserKey, value string, topics []string, opts ...memory.AddOption) (err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.add", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	if err := s.inner.AddMemory(ctx, userKey, value, topics, opts...); err != nil {
		return err
	}
	if s.store == nil {
		return nil
	}
	_, err = s.store.WriteWatermark(ctx, s.scope(userKey))
	return err
}

func (s *visibleMemoryService) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, opts ...memory.UpdateOption) (err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.update", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	if err := s.inner.UpdateMemory(ctx, key, value, topics, opts...); err != nil {
		return err
	}
	if s.store == nil {
		return nil
	}
	_, err = s.store.WriteWatermark(ctx, memoryvisibility.Scope{TenantID: s.tenant, AppName: s.app, PrincipalID: key.UserID})
	return err
}

func (s *visibleMemoryService) DeleteMemory(ctx context.Context, key memory.Key) (err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.delete", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	if err := s.inner.DeleteMemory(ctx, key); err != nil {
		return err
	}
	if s.store == nil {
		return nil
	}
	_, err = s.store.WriteWatermark(ctx, memoryvisibility.Scope{TenantID: s.tenant, AppName: s.app, PrincipalID: key.UserID})
	return err
}

func (s *visibleMemoryService) ClearMemories(ctx context.Context, userKey memory.UserKey) (err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.clear", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	if err := s.inner.ClearMemories(ctx, userKey); err != nil {
		return err
	}
	if s.store == nil {
		return nil
	}
	_, err = s.store.WriteWatermark(ctx, s.scope(userKey))
	return err
}

func (s *visibleMemoryService) Tools() []tool.Tool { return s.inner.Tools() }

func (s *visibleMemoryService) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) (err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.enqueue", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	if err := s.inner.EnqueueAutoMemoryJob(ctx, sess); err != nil {
		return err
	}
	if sess == nil {
		return errors.New("memory visibility: session is nil")
	}
	if s.store == nil {
		return nil
	}
	_, err = s.store.WriteWatermark(ctx, memoryvisibility.Scope{
		TenantID: s.tenant, AppName: s.app, PrincipalID: sess.UserID,
	})
	return err
}

func (s *visibleMemoryService) Close() error { return s.inner.Close() }

var _ memory.Service = (*visibleMemoryService)(nil)
