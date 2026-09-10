package postgres_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	gateway "github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/inmemory"
)

type configStub struct{ calls int }

func (s *configStub) Validate(context.Context, config.ValidateInput) error { return nil }
func (s *configStub) Publish(context.Context, config.PublishInput) (config.PublishResult, error) {
	return config.PublishResult{}, nil
}
func (s *configStub) Get(context.Context, string, int64) (config.Snapshot, error) {
	return config.Snapshot{}, nil
}
func (s *configStub) GetCurrent(context.Context, string) (config.Snapshot, error) {
	return config.Snapshot{}, nil
}
func (s *configStub) Rollback(context.Context, config.RollbackInput) (config.PublishResult, error) {
	return config.PublishResult{}, nil
}
func (s *configStub) Stage(context.Context, config.StageInput) (config.Snapshot, error) {
	return config.Snapshot{}, nil
}
func (s *configStub) CreateRelease(context.Context, config.ReleaseCreateInput) (config.Release, error) {
	return config.Release{}, nil
}
func (s *configStub) GetRelease(context.Context, string) (config.Release, error) {
	return config.Release{}, nil
}
func (s *configStub) UpdateRelease(context.Context, config.ReleaseUpdateInput) (config.Release, error) {
	return config.Release{}, nil
}
func (s *configStub) RollbackRelease(context.Context, config.ReleaseRollbackInput) (config.Release, error) {
	return config.Release{}, nil
}
func (s *configStub) SelectEffective(context.Context, string, int64) (config.Snapshot, error) {
	return config.Snapshot{}, nil
}
func (s *configStub) ResolveExecutionBinding(context.Context, tenant.Context) (tenant.ExecutionBinding, error) {
	s.calls++
	return tenant.ExecutionBinding{AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1}, nil
}
func (s *configStub) ResolveExecutionBindingAt(ctx context.Context, tc tenant.Context, _ int64) (tenant.ExecutionBinding, error) {
	return s.ResolveExecutionBinding(ctx, tc)
}

func TestHTTPRouteResolverUsesSignedPrincipalScope(t *testing.T) {
	tenants := tenantmemory.New()
	created, err := tenants.Create(context.Background(), tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "key-a", DisplayName: "A"}, ChangeMetadata: tenant.ChangeMetadata{ActorType: "test", ActorID: "test", ReasonCode: "test", CorrelationID: "corr", TraceID: "trace"}})
	if err != nil {
		t.Fatal(err)
	}
	configs := &configStub{}
	resolver := gatewaypostgres.HTTPRouteResolver{Tenants: tenants, Configs: configs}
	request := httptest.NewRequest("POST", "http://gateway/v1/agent-runs", nil)
	principal := gateway.Principal{Authenticated: true, TenantID: created.TenantID, TenantVersion: created.Version,
		SubjectID: "principal", UserID: "user", AgentAppID: "app", SessionID: "session", CanRun: true}
	route, err := resolver.ResolveRunRoute(request, principal, "app", "")
	if err != nil || route.Tenant.TenantID != "tenant-a" || route.Tenant.AgentAppID != "app" || route.SessionID != "session" || configs.calls != 1 {
		t.Fatalf("route=%#v calls=%d err=%v", route, configs.calls, err)
	}
	if _, err := resolver.ResolveRunRoute(request, principal, "other-app", ""); err != gateway.ErrForbidden {
		t.Fatalf("app mismatch err=%v", err)
	}
	if _, err := resolver.ResolveRunRoute(request, principal, "app", "spoofed"); err != gateway.ErrForbidden {
		t.Fatalf("session mismatch err=%v", err)
	}
	principal.TenantVersion++
	if _, err := resolver.ResolveRunRoute(request, principal, "app", ""); err != runtime.ErrTenantScope {
		t.Fatalf("tenant version mismatch err=%v", err)
	}
}
