package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// listableStateStore adds the Console session listing seam to the memory store.
type listableStateStore struct {
	*storage.MemoryStateStore
}

func (s *listableStateStore) ListSessions(_ context.Context, _ string, _ int) ([]storage.Session, error) {
	return nil, nil
}

// listableDedupStore adds the claim lister to the memory dedup store.
type listableDedupStore struct {
	*storage.MemoryExecutionDedupStore
}

func (s *listableDedupStore) ListClaims(_ context.Context, _, _ string, _ int) ([]storage.Claim, error) {
	return nil, nil
}

type testProducer struct{}

func (p *testProducer) Publish(_ context.Context, _ messaging.Envelope) error { return nil }

type testLoginProvider struct{}

func (testLoginProvider) Descriptor() identity.ProviderDescriptor {
	return identity.ProviderDescriptor{ProviderID: "test-login", Type: identity.ProviderOIDC, DisplayName: "测试 SSO"}
}

func (testLoginProvider) Begin(request identity.AuthRequest) (string, error) {
	return "https://sso.example.test/authorize?state=" + request.State, nil
}

func (testLoginProvider) Exchange(context.Context, identity.AuthExchange) (identity.Identity, error) {
	return identity.Identity{}, errors.New("test provider exchange is not used by route-table tests")
}

// buildTestKafkaHandler composes the production route table with in-memory
// identity/auth stores, mirroring buildApplication's wiring.
func buildTestKafkaHandler(t *testing.T) (http.Handler, *identity.MemorySessionStore) {
	t.Helper()
	repository := tenant.NewMemoryRepository()
	if _, err := repository.Publish(context.Background(), config.TenantConfig{
		TenantID: "example", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "example-support-bot"}},
	}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	users := identity.NewMemoryIdentityStore()
	sessions := identity.NewMemorySessionStore()
	authHandler, err := web.NewAuthHandler(web.AuthDependencies{
		Providers:   map[string]identity.IdentityProvider{"test-login": testLoginProvider{}},
		Sessions:    sessions,
		Users:       users,
		Audits:      users,
		SessionTTL:  time.Hour,
		StateSecret: "test-state-secret",
	})
	if err != nil {
		t.Fatalf("construct auth handler: %v", err)
	}
	modelCatalog, err := config.NewModelCatalog(nil)
	if err != nil {
		t.Fatalf("construct model catalog: %v", err)
	}
	applicationValidator, err := config.NewPlatformPolicyValidator(modelCatalog, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("construct application validator: %v", err)
	}
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatalf("construct execution manifest codec: %v", err)
	}
	handler, err := NewKafkaHTTPHandler(KafkaHTTPDependencies{
		Configurations:     repository,
		Producer:           &testProducer{},
		ExecutionManifests: manifests,
		StateStore:         &listableStateStore{MemoryStateStore: storage.NewMemoryStateStore()},
		ExecutionDedup:     &listableDedupStore{MemoryExecutionDedupStore: storage.NewMemoryExecutionDedupStore()},
		RetryTracker:       storage.NewMemoryRetryTracker(),
		ConsoleSystem:      web.SystemInfo{Version: "test"},
		ConsoleProbes:      map[string]web.DependencyProbe{},
		ConsoleFS: fstest.MapFS{
			"index.html": &fstest.MapFile{Data: []byte("<!doctype html><title>console-root</title>")},
		},
		AuthHandler:          authHandler,
		Sessions:             sessions,
		Audits:               users,
		ApplicationValidator: applicationValidator,
	})
	if err != nil {
		t.Fatalf("NewKafkaHTTPHandler() error = %v", err)
	}
	return handler, sessions
}

func TestKafkaHTTPDoesNotExposeLegacyWebhookIngress(t *testing.T) {
	handler, _ := buildTestKafkaHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/webhooks/telegram/example-support-bot", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("legacy binding webhook status = %d, want 404", recorder.Code)
	}
	unscoped := httptest.NewRecorder()
	handler.ServeHTTP(unscoped, httptest.NewRequest(http.MethodPost, "/webhooks/telegram", nil))
	if unscoped.Code != http.StatusNotFound {
		t.Fatalf("unscoped webhook status = %d, want 404", unscoped.Code)
	}
}

func TestKafkaHTTPProtectedRoutesRequireSession(t *testing.T) {
	handler, _ := buildTestKafkaHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/v1/apps without session status = %d, want 401", recorder.Code)
	}
}

func TestKafkaHTTPAdminRouteIsRemoved(t *testing.T) {
	handler, _ := buildTestKafkaHandler(t)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET /admin/ status = %d, want 404", recorder.Code)
	}
}

func TestKafkaHTTPWriteRequiresCSRF(t *testing.T) {
	handler, sessions := buildTestKafkaHandler(t)
	sessionID, err := sessions.Create(context.Background(), identity.SessionUser{PlatformUserID: "system-admin", IsSystemAdmin: true}, time.Hour)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/apps",
		strings.NewReader(`{"tenant_id":"new","app_code":"bot","channels":[]}`))
	request.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("POST without CSRF status = %d, want 403 (body %s)", recorder.Code, recorder.Body.String())
	}
}

func TestKafkaHTTPAuthedConsoleReachable(t *testing.T) {
	handler, sessions := buildTestKafkaHandler(t)
	sessionID, err := sessions.Create(context.Background(), identity.SessionUser{
		PlatformUserID: "system-admin",
		IsSystemAdmin:  true,
		Tenants: []identity.TenantRole{{
			TenantID: "example", Role: identity.RoleAdmin, Status: "active",
		}},
	}, time.Hour)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	const csrf = "test-csrf-token"
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps", nil)
	request.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	request.AddCookie(&http.Cookie{Name: "dsh_csrf", Value: csrf})
	request.Header.Set("X-CSRF-Token", csrf)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/apps with session status = %d, want 200 (body %s)", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "example") {
		t.Fatalf("applications response missing seeded tenant: %s", recorder.Body.String())
	}
}

func TestKafkaHTTPPublicRoutesStayOpen(t *testing.T) {
	handler, _ := buildTestKafkaHandler(t)
	for path, want := range map[string]int{
		"/healthz":                               http.StatusNoContent,
		"/readyz":                                http.StatusNoContent,
		"/api/v1/auth/providers":                 http.StatusOK,
		"/api/v1/auth/login?provider=test-login": http.StatusOK,
		"/console/":                              http.StatusOK,
		"/api/v1/auth/callback?code=x&state=bad": http.StatusFound,
	} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != want {
			t.Fatalf("GET %s status = %d, want %d", path, recorder.Code, want)
		}
	}
}

func TestWorkerHTTPHandlerExposesOnlyHealthEndpoints(t *testing.T) {
	t.Parallel()

	handler := NewWorkerHTTPHandler(nil, map[string]web.DependencyProbe{
		"postgres": func(context.Context) error { return nil },
	})
	for path, want := range map[string]int{
		"/healthz":           http.StatusNoContent,
		"/readyz":            http.StatusNoContent,
		"/api/v1/system":     http.StatusNotFound,
		"/api/v1/apps":       http.StatusNotFound,
		"/console/":          http.StatusNotFound,
		"/webhooks/feishu/x": http.StatusNotFound,
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != want {
			t.Fatalf("GET %s status = %d, want %d", path, recorder.Code, want)
		}
	}
}

func TestWorkerHTTPReadyFailsClosedOnDependencyFailure(t *testing.T) {
	t.Parallel()

	handler := NewWorkerHTTPHandler(nil, map[string]web.DependencyProbe{
		"kafka": func(context.Context) error { return errors.New("broker unavailable") },
	})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}
