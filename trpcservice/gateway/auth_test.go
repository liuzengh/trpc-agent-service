package gateway_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/inmemory"
)

func TestHMACPrincipalResolverBindsTenantVersionAndCanonicalRoute(t *testing.T) {
	ctx := context.Background()
	tenants := tenantmemory.New()
	created, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "key-a", DisplayName: "A"}, ChangeMetadata: tenant.ChangeMetadata{ActorType: "test", ActorID: "test", ReasonCode: "test", CorrelationID: "corr", TraceID: "trace"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	key := []byte("01234567890123456789012345678901")
	claims := gateway.PrincipalClaims{Version: 1, TenantID: created.TenantID, TenantVersion: created.Version, SubjectID: "principal", UserID: "user", AgentAppID: "app", SessionID: "session", CanRead: true, CanRun: true, CanCancel: true, IssuedAt: now.Add(-time.Minute).Unix(), ExpiresAt: now.Add(time.Hour).Unix(), TokenID: "token-1"}
	token, err := gateway.SignPrincipalToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := gateway.NewHMACPrincipalResolver(key, tenants, gateway.HMACPrincipalOptions{Clock: func() time.Time { return now }, ClockSkew: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "http://gateway/v1/agent-runs/req-1", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	principal, err := resolver.Resolve(request)
	if err != nil || !principal.Authenticated || principal.TenantID != "tenant-a" || principal.UserID != "user" || principal.SessionID != "session" || principal.RedactionProgram == nil {
		t.Fatalf("principal=%#v err=%v", principal, err)
	}
	request.Header.Set("X-Tenant-ID", "tenant-b")
	principal, err = resolver.Resolve(request)
	if err != nil || principal.TenantID != "tenant-a" {
		t.Fatalf("untrusted header changed principal=%#v err=%v", principal, err)
	}
	request.URL.Path = "/v1/chat/completions"
	request.Header.Set("Idempotency-Key", "openai-key")
	invocation, err := resolver.ResolveProtocolInvocation(request)
	if err != nil || invocation.Protocol != "openai" || invocation.Tenant.AgentAppID != "app" || invocation.IdempotencyKey != "openai-key" {
		t.Fatalf("invocation=%#v err=%v", invocation, err)
	}
	claims.Protocol = "trpc-agent"
	protocolToken, err := gateway.SignPrincipalToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+protocolToken)
	if _, err := resolver.ResolveProtocolInvocation(request); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("protocol mismatch err=%v", err)
	}
	request.URL.Path = "/trpc-agent/v1/apps/app/runs"
	request.Header.Set("Idempotency-Key", "trpc-key")
	invocation, err = resolver.ResolveProtocolInvocation(request)
	if err != nil || invocation.Protocol != "trpc-agent" || invocation.Tenant.AgentAppID != "app" {
		t.Fatalf("tRPC-Agent invocation=%#v err=%v", invocation, err)
	}
	request.URL.Path = "/trpc-agent/v1/apps/other/runs"
	if _, err := resolver.ResolveProtocolInvocation(request); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-app tRPC-Agent path err=%v", err)
	}
	request.URL.Path = "/trpc-agent/v1/apps/app/runs/extra"
	if _, err := resolver.ResolveProtocolInvocation(request); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("non-canonical tRPC-Agent path err=%v", err)
	}
	claims.Protocol = "a2a"
	a2aToken, err := gateway.SignPrincipalToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+a2aToken)
	request.URL.Path = "/a2a/v1/apps/app/"
	a2aInvocation, err := resolver.ResolveProtocolInvocation(request)
	if err != nil || a2aInvocation.Protocol != "a2a" || !a2aInvocation.CanRun || !a2aInvocation.CanRead || !a2aInvocation.CanCancel {
		t.Fatalf("A2A invocation=%#v err=%v", a2aInvocation, err)
	}
	request.URL.Path = "/a2a/v1/apps/other/"
	if _, err := resolver.ResolveProtocolInvocation(request); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-app A2A path err=%v", err)
	}
	request.URL.Path = "/a2a/v1/apps/app/.well-known/agent-card.json"
	request.Header.Del("Idempotency-Key")
	a2aInvocation, err = resolver.ResolveProtocolInvocation(request)
	if err != nil || a2aInvocation.Protocol != "a2a" || a2aInvocation.IdempotencyKey != "a2a-agent-card" {
		t.Fatalf("A2A Agent Card invocation=%#v err=%v", a2aInvocation, err)
	}
	request.URL.Path = "/v1/chat/completions"
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Del("Idempotency-Key")
	if _, err := resolver.ResolveProtocolInvocation(request); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("missing idempotency err=%v", err)
	}
}

func TestHMACPrincipalResolverRejectsStaleExpiredAndTamperedTokens(t *testing.T) {
	ctx := context.Background()
	tenants := tenantmemory.New()
	created, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "key-a", DisplayName: "A"}, ChangeMetadata: tenant.ChangeMetadata{ActorType: "test", ActorID: "test", ReasonCode: "test", CorrelationID: "corr", TraceID: "trace"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	key := []byte("01234567890123456789012345678901")
	claims := gateway.PrincipalClaims{Version: 1, TenantID: created.TenantID, TenantVersion: created.Version + 1, SubjectID: "principal", UserID: "user", AgentAppID: "app", SessionID: "session", IssuedAt: now.Add(-time.Minute).Unix(), ExpiresAt: now.Add(time.Hour).Unix(), TokenID: "token-1"}
	token, err := gateway.SignPrincipalToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := gateway.NewHMACPrincipalResolver(key, tenants, gateway.HMACPrincipalOptions{Clock: func() time.Time { return now }, ClockSkew: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "http://gateway/v1/agent-runs/req-1", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	if _, err := resolver.Resolve(request); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("stale tenant version err=%v", err)
	}
	claims.TenantVersion = created.Version
	claims.ExpiresAt = now.Add(-time.Minute).Unix()
	claims.IssuedAt = now.Add(-2 * time.Minute).Unix()
	token, err = gateway.SignPrincipalToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if _, err := resolver.Resolve(request); !errors.Is(err, gateway.ErrUnauthenticated) {
		t.Fatalf("expired token err=%v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token+"x")
	if _, err := resolver.Resolve(request); !errors.Is(err, gateway.ErrUnauthenticated) {
		t.Fatalf("tampered token err=%v", err)
	}
	if err := resolver.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(request); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("closed resolver err=%v", err)
	}
}

func TestHMACPrincipalResolverSuspendedTenantKeepsControlButRejectsProtocolRun(t *testing.T) {
	ctx := context.Background()
	tenants := tenantmemory.New()
	created, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "key-a", DisplayName: "A"},
		ChangeMetadata: tenant.ChangeMetadata{ActorType: "test", ActorID: "test", ReasonCode: "test", CorrelationID: "corr", TraceID: "trace"}})
	if err != nil {
		t.Fatal(err)
	}
	suspended, err := tenants.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: created.TenantID, ExpectedVersion: created.Version,
		NextStatus: tenant.StatusSuspended, ChangeMetadata: tenant.ChangeMetadata{ActorType: "test", ActorID: "test", ReasonCode: "suspend", CorrelationID: "corr-2", TraceID: "trace-2"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	key := []byte("01234567890123456789012345678901")
	claims := gateway.PrincipalClaims{Version: 1, TenantID: suspended.Tenant.TenantID, TenantVersion: suspended.Tenant.Version, SubjectID: "principal",
		UserID: "user", AgentAppID: "app", SessionID: "session", CanRead: true, CanRun: true, CanCancel: true,
		IssuedAt: now.Add(-time.Minute).Unix(), ExpiresAt: now.Add(time.Hour).Unix(), TokenID: "token-suspended"}
	token, err := gateway.SignPrincipalToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := gateway.NewHMACPrincipalResolver(key, tenants, gateway.HMACPrincipalOptions{Clock: func() time.Time { return now }, ClockSkew: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "http://gateway/a2a/v1/apps/app/", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Idempotency-Key", "control-key")
	invocation, err := resolver.ResolveProtocolInvocation(request)
	if err != nil || invocation.CanRun || !invocation.CanRead || !invocation.CanCancel {
		t.Fatalf("suspended A2A invocation=%#v err=%v", invocation, err)
	}
	request.URL.Path = "/v1/chat/completions"
	if _, err := resolver.ResolveProtocolInvocation(request); !errors.Is(err, gateway.ErrForbidden) {
		t.Fatalf("suspended OpenAI run err=%v", err)
	}
}
