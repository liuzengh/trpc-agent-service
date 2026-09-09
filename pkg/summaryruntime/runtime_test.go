package summaryruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmem "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type runtimeTenantReader struct{ value *tenant.Tenant }

func (r runtimeTenantReader) GetTenant(context.Context, string) (*tenant.Tenant, error) {
	return r.value, nil
}

type runtimeVersionReader struct{ value *controlplane.ResolvedVersion }

func (r runtimeVersionReader) LoadVersion(context.Context, string, string, string) (*controlplane.ResolvedVersion, error) {
	return r.value, nil
}

type runtimeServiceAcquirer struct {
	sessions session.Service
	releases int
}

func (a *runtimeServiceAcquirer) AcquireServices(context.Context, *tenant.Tenant) (session.Service, memory.Service, func(), error) {
	return a.sessions, memoryinmem.NewMemoryService(), func() { a.releases++ }, nil
}

type captureSummaryModel struct{ request *model.Request }

func (m *captureSummaryModel) Info() model.Info {
	return model.Info{Name: "summary-test", ContextWindow: 8192}
}

func (m *captureSummaryModel) GenerateContent(_ context.Context, request *model.Request) (<-chan *model.Response, error) {
	m.request = request
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{ID: "summary-response", Done: true, Usage: &model.Usage{TotalTokens: 12}, Choices: []model.Choice{{
		Message: model.Message{Role: model.RoleAssistant, Content: "durable bounded summary"},
	}}}
	close(ch)
	return ch, nil
}

func TestRuntimeGeneratesFromPinnedVersionAndExactSessionPrefix(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mini.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	tenantValue := &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Storage: tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"},
	}
	physicalApp, err := storage.TenantScopedAppName(tenantValue, "support")
	if err != nil {
		t.Fatal(err)
	}
	stored := session.NewSession(physicalApp, "owner-1", "session-1")
	stored.SetState(storage.SessionIncarnationStateKey, []byte("00000000-0000-4000-8000-000000000001"))
	baseTime := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	stored.Events = []event.Event{
		{ID: "event-1", Timestamp: baseTime, Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Content: "hello"}}}}},
		{ID: "event-2", Timestamp: baseTime.Add(time.Second), Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Content: "world"}}}}},
		{ID: "event-late", Timestamp: baseTime.Add(2 * time.Second), Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Content: "late"}}}}},
	}
	services := &runtimeServiceAcquirer{sessions: summarySessionService{value: stored}}
	version := &controlplane.ResolvedVersion{
		TenantID: "tenant-a", AgentAppID: "app-1", AgentAppName: "support",
		VersionID: "version-1", VersionNumber: 1,
		Snapshot: controlplane.VersionSnapshot{
			Agent: tenant.AgentConfig{Name: "support", DefaultModel: "gpt-4o-mini"},
			Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4o-mini", Temperature: 0.2},
		},
	}
	capture := &captureSummaryModel{}
	runtime, err := New(RuntimeOptions{
		Tenants: runtimeTenantReader{value: tenantValue}, Versions: runtimeVersionReader{value: version},
		Services: services, Redis: redisClient, MaxSummaryWords: 128, MaxOutputTokens: 256,
		Checkpoints: summarycoord.NewMemorySink(nil), MinEvents: 1,
		ModelBuilder: func(context.Context, tenant.ModelConfig, *tenant.Tenant, tenant.SecretResolver) (model.Model, error) {
			return capture, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	job := summarycoord.Job{
		ID: 1, Key: summarycoord.Key{TenantID: "tenant-a", AgentAppID: "app-1", SessionOwnerID: "owner-1", SessionID: "session-1", SessionIncarnationID: "00000000-0000-4000-8000-000000000001"},
		AgentVersionID: "version-1", TargetEventSequence: 2, Status: summarycoord.StatusProcessing,
		LeaseOwner: "worker-1", LeaseVersion: 1, LeaseUntil: time.Now().Add(time.Minute), Attempts: 1, MaxAttempts: 8,
	}
	candidate, err := runtime.Generate(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.EventSequence != 2 || candidate.Content != "durable bounded summary" || services.releases != 1 {
		t.Fatalf("candidate=%#v releases=%d", candidate, services.releases)
	}
	if capture.request == nil || capture.request.GenerationConfig.MaxTokens == nil || *capture.request.GenerationConfig.MaxTokens != 256 ||
		capture.request.GenerationConfig.Temperature == nil || *capture.request.GenerationConfig.Temperature != 0.2 {
		t.Fatalf("summary generation config=%#v", capture.request)
	}
}

func TestRuntimeResolvesDeferredTargetUnderSharedSessionLease(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mini.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	tenantValue := &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusActive,
		Storage: tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"}}
	physicalApp, _ := storage.TenantScopedAppName(tenantValue, "support")
	stored := session.NewSession(physicalApp, "owner-1", "session-1")
	stored.SetState(storage.SessionIncarnationStateKey, []byte("00000000-0000-4000-8000-000000000001"))
	stored.Events = []event.Event{{ID: "one"}, {ID: "two"}, {ID: "three"}}
	runtime, err := New(RuntimeOptions{
		Tenants: runtimeTenantReader{value: tenantValue},
		Versions: runtimeVersionReader{value: &controlplane.ResolvedVersion{
			TenantID: "tenant-a", AgentAppID: "app-1", AgentAppName: "support", VersionID: "version-1",
			VersionNumber: 1,
			Snapshot:      controlplane.VersionSnapshot{Agent: tenant.AgentConfig{Name: "support", DefaultModel: "gpt"}, Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt"}},
		}},
		Services: &runtimeServiceAcquirer{sessions: summarySessionService{value: stored}}, Redis: redisClient,
		Checkpoints: summarycoord.NewMemorySink(nil), MinEvents: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := runtime.ResolveTarget(context.Background(), summarycoord.Job{
		ID: 1, Key: summarycoord.Key{TenantID: "tenant-a", AgentAppID: "app-1", SessionOwnerID: "owner-1", SessionID: "session-1", SessionIncarnationID: "00000000-0000-4000-8000-000000000001"},
		AgentVersionID: "version-1", Status: summarycoord.StatusProcessing,
		LeaseOwner: "worker-1", LeaseVersion: 1, LeaseUntil: time.Now().Add(time.Minute), Attempts: 1, MaxAttempts: 8,
	})
	if err != nil || sequence != 3 {
		t.Fatalf("sequence=%d err=%v", sequence, err)
	}
}

func TestRuntimeSkipsProviderCallBelowEventThreshold(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mini.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	tenantValue := &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusActive,
		Storage: tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"}}
	physicalApp, _ := storage.TenantScopedAppName(tenantValue, "support")
	stored := session.NewSession(physicalApp, "owner-1", "session-1")
	stored.SetState(storage.SessionIncarnationStateKey, []byte("00000000-0000-4000-8000-000000000001"))
	stored.Events = []event.Event{
		{ID: "one", Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Content: "one"}}}}},
		{ID: "two", Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Content: "two"}}}}},
	}
	capture := &captureSummaryModel{}
	runtime, err := New(RuntimeOptions{
		Tenants: runtimeTenantReader{value: tenantValue}, Versions: runtimeVersionReader{value: &controlplane.ResolvedVersion{
			TenantID: "tenant-a", AgentAppID: "app-1", AgentAppName: "support", VersionID: "version-1", VersionNumber: 1,
			Snapshot: controlplane.VersionSnapshot{Agent: tenant.AgentConfig{Name: "support", DefaultModel: "gpt"}, Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt"}},
		}}, Services: &runtimeServiceAcquirer{sessions: summarySessionService{value: stored}}, Redis: redisClient,
		Checkpoints: summarycoord.NewMemorySink(nil), MinEvents: 5,
		ModelBuilder: func(context.Context, tenant.ModelConfig, *tenant.Tenant, tenant.SecretResolver) (model.Model, error) {
			return capture, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.Generate(context.Background(), summarycoord.Job{
		ID: 1, Key: summarycoord.Key{TenantID: "tenant-a", AgentAppID: "app-1", SessionOwnerID: "owner-1", SessionID: "session-1", SessionIncarnationID: "00000000-0000-4000-8000-000000000001"},
		AgentVersionID: "version-1", TargetEventSequence: 2, Status: summarycoord.StatusProcessing,
		LeaseOwner: "worker-1", LeaseVersion: 1, LeaseUntil: time.Now().Add(time.Minute), Attempts: 1, MaxAttempts: 8,
	})
	if !errors.Is(err, summarycoord.ErrSummaryNotDue) || capture.request != nil {
		t.Fatalf("error=%v model request=%#v", err, capture.request)
	}
}

type summarySessionService struct {
	session.Service
	value *session.Session
}

func (s summarySessionService) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	return s.value.Clone(), nil
}
