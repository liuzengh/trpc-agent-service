package agent

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRuntimeResolverUsesDefaultAndPinnedRevisionCaches(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 2)
	var buildCount atomic.Int32
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		buildCount.Add(1)
		return buildTestRuntime(revision), nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	defaultRuntime, err := resolver.Resolve(context.Background(), scope, "assistant", "")
	require.NoError(t, err)
	defer defaultRuntime.Release()
	require.Equal(t, "revision-2", defaultRuntime.Revision.ID)

	defaultAgain, err := resolver.Resolve(context.Background(), scope, "assistant", "")
	require.NoError(t, err)
	defer defaultAgain.Release()
	require.Same(t, defaultRuntime.Runtime, defaultAgain.Runtime)

	// Every caller of one cached revision shares the single Runtime-owned
	// adapter, so no caller needs an adapter cache of its own.
	firstHandler, err := defaultRuntime.Runtime.OpenAIHandler()
	require.NoError(t, err)
	repeatedHandler, err := defaultAgain.Runtime.OpenAIHandler()
	require.NoError(t, err)
	require.Same(t, firstHandler, repeatedHandler)

	pinnedRuntime, err := resolver.Resolve(
		context.Background(), scope, "assistant", "revision-1",
	)
	require.NoError(t, err)
	defer pinnedRuntime.Release()
	require.Equal(t, "revision-1", pinnedRuntime.Revision.ID)
	require.NotSame(t, defaultRuntime.Runtime, pinnedRuntime.Runtime)
	pinnedHandler, err := pinnedRuntime.Runtime.OpenAIHandler()
	require.NoError(t, err)
	require.NotSame(t, firstHandler, pinnedHandler)
	require.Equal(t, int32(2), buildCount.Load())
	require.Equal(t, 2, resolver.CacheSize())
}

func TestRuntimeResolverSingleflightBuild(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	var buildCount atomic.Int32
	buildStarted := make(chan struct{})
	releaseBuild := make(chan struct{})
	var startedOnce sync.Once
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		buildCount.Add(1)
		startedOnce.Do(func() { close(buildStarted) })
		<-releaseBuild
		return buildTestRuntime(revision), nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	const callers = 24
	results := make(chan ResolvedRuntime, callers)
	errorsChannel := make(chan error, callers)
	var waitGroup sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			resolved, resolveErr := resolver.Resolve(context.Background(), scope, "assistant", "")
			if resolveErr != nil {
				errorsChannel <- resolveErr
				return
			}
			results <- resolved
		}()
	}
	<-buildStarted
	close(releaseBuild)
	waitGroup.Wait()
	close(results)
	close(errorsChannel)

	for resolveErr := range errorsChannel {
		require.NoError(t, resolveErr)
	}
	var first *Runtime
	for resolved := range results {
		resolved.Release()
		runtime := resolved.Runtime
		if first == nil {
			first = runtime
			continue
		}
		require.Same(t, first, runtime)
	}
	require.Equal(t, int32(1), buildCount.Load())
	require.Equal(t, 1, resolver.CacheSize())
}

func TestRuntimeResolverDoesNotShareAcrossTenants(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	scopeA := seedResolverTenant(t, repository, "tenant-a", "assistant", 1)
	scopeB := seedResolverTenant(t, repository, "tenant-b", "assistant", 1)
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		return buildTestRuntime(revision), nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	runtimeA, err := resolver.Resolve(context.Background(), scopeA, "assistant", "")
	require.NoError(t, err)
	defer runtimeA.Release()
	runtimeB, err := resolver.Resolve(context.Background(), scopeB, "assistant", "")
	require.NoError(t, err)
	defer runtimeB.Release()
	require.NotSame(t, runtimeA.Runtime, runtimeB.Runtime)
	require.Equal(t, "tenant-a", runtimeA.Runtime.TenantID)
	require.Equal(t, "tenant-b", runtimeB.Runtime.TenantID)
	require.Equal(t, 2, resolver.CacheSize())
}

func TestRuntimeResolverDoesNotCacheBuildFailure(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	var buildCount atomic.Int32
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		if buildCount.Add(1) == 1 {
			return nil, errors.New("temporary build failure")
		}
		return buildTestRuntime(revision), nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	_, err = resolver.Resolve(context.Background(), scope, "assistant", "")
	require.ErrorContains(t, err, "temporary build failure")
	resolved, err := resolver.Resolve(context.Background(), scope, "assistant", "")
	require.NoError(t, err)
	resolved.Release()
	require.NotNil(t, resolved.Runtime)
	require.Equal(t, int32(2), buildCount.Load())
}

func TestRuntimeResolverRejectsIncompleteRuntime(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		return &Runtime{
			TenantID:   revision.TenantID,
			AgentAppID: revision.AgentAppID,
			RevisionID: revision.ID,
		}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	_, err = resolver.Resolve(context.Background(), scope, "assistant", "")
	require.ErrorContains(t, err, "runtime execution unit is incomplete")
	require.Zero(t, resolver.CacheSize())
}

func TestRuntimeResolverClosesIdentityMismatchedRuntime(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	recorder := &closeRecorder{}
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		mismatched := recordingRuntime(recorder, nil, nil, nil)
		mismatched.TenantID = "tenant-b"
		mismatched.AgentAppID = revision.AgentAppID
		mismatched.RevisionID = revision.ID
		return mismatched, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	_, err = resolver.Resolve(context.Background(), scope, "assistant", "")
	require.ErrorContains(t, err, "does not match revision")
	require.Zero(t, resolver.CacheSize())
	require.Equal(t, []string{"adapter", "runner", "session"}, recorder.order())
}

func TestRuntimeResolverCloseReleasesCachedRuntimeResources(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	recorder := &closeRecorder{}
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		runtime := recordingRuntime(recorder, nil, nil, nil)
		runtime.TenantID = revision.TenantID
		runtime.AgentAppID = revision.AgentAppID
		runtime.RevisionID = revision.ID
		return runtime, nil
	})
	require.NoError(t, err)

	resolved, err := resolver.Resolve(context.Background(), scope, "assistant", "")
	require.NoError(t, err)
	require.Empty(t, recorder.order())
	resolved.Release()

	require.NoError(t, resolver.Close())
	require.Equal(t, []string{"adapter", "runner", "session"}, recorder.order())
	require.Zero(t, resolver.CacheSize())

	require.NoError(t, resolver.Close())
	require.Equal(t, []string{"adapter", "runner", "session"}, recorder.order())
}

func TestRuntimeResolverClose(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		return buildTestRuntime(revision), nil
	})
	require.NoError(t, err)
	resolved, err := resolver.Resolve(context.Background(), scope, "assistant", "")
	require.NoError(t, err)
	resolved.Release()
	require.NoError(t, resolver.Close())
	require.NoError(t, resolver.Close())
	require.Zero(t, resolver.CacheSize())

	_, err = resolver.Resolve(context.Background(), scope, "assistant", "")
	require.ErrorIs(t, err, ErrResolverClosed)
}

func TestRuntimeResolverRequestCancellationDoesNotCancelSharedBuild(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	buildStarted := make(chan context.Context, 1)
	releaseBuild := make(chan struct{})
	resolver, err := NewRuntimeResolver(repository, func(
		ctx context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		buildStarted <- ctx
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-releaseBuild:
			return buildTestRuntime(revision), nil
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, resolveErr := resolver.Resolve(firstContext, scope, "assistant", "")
		firstResult <- resolveErr
	}()
	buildContext := <-buildStarted
	secondResult := make(chan ResolvedRuntime, 1)
	secondError := make(chan error, 1)
	go func() {
		resolved, resolveErr := resolver.Resolve(context.Background(), scope, "assistant", "")
		if resolveErr != nil {
			secondError <- resolveErr
			return
		}
		secondResult <- resolved
	}()

	cancelFirst()
	require.ErrorIs(t, <-firstResult, context.Canceled)
	select {
	case <-buildContext.Done():
		t.Fatalf("request cancellation reached shared build: %v", buildContext.Err())
	default:
	}
	close(releaseBuild)

	select {
	case resolveErr := <-secondError:
		require.NoError(t, resolveErr)
	case resolved := <-secondResult:
		resolved.Release()
	}
}

func TestRuntimeResolverCloseWaitsForLease(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		return buildTestRuntime(revision), nil
	})
	require.NoError(t, err)
	resolved, err := resolver.Resolve(context.Background(), scope, "assistant", "")
	require.NoError(t, err)

	closeResult := make(chan error, 1)
	go func() { closeResult <- resolver.Close() }()
	require.Eventually(t, resolver.isClosed, time.Second, time.Millisecond)
	select {
	case <-closeResult:
		t.Fatal("resolver closed a leased runtime")
	default:
	}

	resolved.Release()
	require.NoError(t, <-closeResult)
}

func TestRuntimeResolverRejectsSourceIdentityMismatch(t *testing.T) {
	var buildCount atomic.Int32
	source := revisionSourceFunc(func(
		_ context.Context,
		_ tenant.TenantContext,
		_ string,
		_ string,
	) (tenant.AgentRevision, error) {
		return tenant.AgentRevision{
			ID: "revision-1", TenantID: "tenant-b", AgentAppID: "assistant",
		}, nil
	})
	resolver, err := NewRuntimeResolver(source, func(
		_ context.Context,
		_ tenant.AgentRevision,
	) (*Runtime, error) {
		buildCount.Add(1)
		return &Runtime{}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	_, err = resolver.Resolve(
		context.Background(), tenant.TenantContext{TenantID: "tenant-a"}, "assistant", "",
	)
	require.ErrorIs(t, err, tenant.ErrTenantScope)
	require.Zero(t, buildCount.Load())
}

func resolverRepository(
	t *testing.T,
	tenantID string,
	appID string,
	revisionCount int,
) (*tenant.MemoryRepository, tenant.TenantContext) {
	t.Helper()
	repository := tenant.NewMemoryRepository()
	scope := seedResolverTenant(t, repository, tenantID, appID, revisionCount)
	return repository, scope
}

func seedResolverTenant(
	t *testing.T,
	repository *tenant.MemoryRepository,
	tenantID string,
	appID string,
	revisionCount int,
) tenant.TenantContext {
	t.Helper()
	ctx := context.Background()
	scope := tenant.TenantContext{TenantID: tenantID}
	_, err := repository.CreateTenant(ctx, tenant.Tenant{
		ID: tenantID, Slug: tenantID, Name: "Resolver Test Tenant",
	})
	require.NoError(t, err)
	_, err = repository.CreateAgentApp(ctx, scope, tenant.AgentApp{
		ID: appID, TenantID: tenantID, Name: "Resolver Test App",
	})
	require.NoError(t, err)
	for number := 1; number <= revisionCount; number++ {
		revisionID := "revision-" + strconv.Itoa(number)
		_, err = repository.CreateRevision(ctx, scope, tenant.AgentRevision{
			ID:         revisionID,
			TenantID:   tenantID,
			AgentAppID: appID,
			RevisionNo: uint64(number),
			CreatedBy:  "test-admin",
			Config: tenant.RevisionConfig{
				AgentName: "test-agent",
				Model: tenant.ModelConfig{
					Provider: "deterministic",
					Name:     "echo-test",
				},
			},
		})
		require.NoError(t, err)
		_, _, err = repository.PublishRevision(ctx, scope, appID, revisionID)
		require.NoError(t, err)
	}
	return scope
}

// buildTestRuntime builds a complete Runtime that owns its Session service, so
// the resolver is the only owner of every resource it caches.
//
// The authorizer entitles nothing, deliberately. These revisions name no secret
// and no policy, which is exactly the set that needs no entitlement — so the
// strictest possible authorizer is also the honest one, and any of them that
// later grows a capability ref fails here rather than quietly acquiring it.
func buildTestRuntime(revision tenant.AgentRevision) *Runtime {
	// newOwnedRuntime takes the session service on the way in, so a failure has
	// already closed it and closing it again here would be a double close.
	runtime, err := newOwnedRuntime(
		revision, sessioninmemory.NewSessionService(), nil, security.DenyCapabilities())
	if err != nil {
		panic(err)
	}
	return runtime
}

type revisionSourceFunc func(
	context.Context,
	tenant.TenantContext,
	string,
	string,
) (tenant.AgentRevision, error)

func (f revisionSourceFunc) ResolveRevision(
	ctx context.Context,
	scope tenant.TenantContext,
	appID string,
	pinnedRevisionID string,
) (tenant.AgentRevision, error) {
	return f(ctx, scope, appID, pinnedRevisionID)
}
