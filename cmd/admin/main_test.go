//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/modelcatalog"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/platformtool"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

type legacyReplayStore struct{ reliable.Store }

type conflictTenantService struct{}

type countingTenantService struct {
	getCalls atomic.Int32
	tenant   *tenant.Tenant
	getErr   error
}

func (s *countingTenantService) CreateTenant(context.Context, string, tenant.TenantConfig) (*tenant.Tenant, error) {
	return nil, nil
}
func (s *countingTenantService) GetTenant(context.Context, string) (*tenant.Tenant, error) {
	s.getCalls.Add(1)
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.tenant != nil {
		return s.tenant, nil
	}
	return sampleTenant(), nil
}
func (s *countingTenantService) GetTenantByWebhookToken(context.Context, string) (*tenant.Tenant, error) {
	return nil, nil
}
func (s *countingTenantService) UpdateTenant(context.Context, *tenant.Tenant) error { return nil }
func (s *countingTenantService) DeleteTenant(context.Context, string) error         { return nil }
func (s *countingTenantService) ListTenants(context.Context) ([]*tenant.Tenant, error) {
	return nil, nil
}
func (s *countingTenantService) Close() error { return nil }

func (conflictTenantService) CreateTenant(context.Context, string, tenant.TenantConfig) (*tenant.Tenant, error) {
	return nil, nil
}
func (conflictTenantService) GetTenant(context.Context, string) (*tenant.Tenant, error) {
	return sampleTenant(), nil
}
func (conflictTenantService) GetTenantByWebhookToken(context.Context, string) (*tenant.Tenant, error) {
	return nil, nil
}
func (conflictTenantService) UpdateTenant(context.Context, *tenant.Tenant) error {
	return tenant.ErrTenantConflict
}
func (conflictTenantService) DeleteTenant(context.Context, string) error { return nil }
func (conflictTenantService) ListTenants(context.Context) ([]*tenant.Tenant, error) {
	return nil, nil
}
func (conflictTenantService) Close() error { return nil }

func sampleTenant() *tenant.Tenant {
	return &tenant.Tenant{
		ID:   "t1",
		Name: "Acme",
		Models: []tenant.ModelConfig{
			{Provider: "openai", ModelName: "gpt", APIKey: "sk-super-secret"},
		},
		Channels: []tenant.ChannelBinding{
			{Type: "wework", Token: "tok-123", Secret: "corp-secret", AppID: "app"},
		},
		Storage: tenant.StorageConfig{
			SessionBackend: "postgres", SessionConfig: map[string]string{"dsn": "postgres://session"},
			MemoryBackend: "postgres", MemoryConfig: map[string]string{"dsn": "postgres://memory"},
		},
	}
}

func TestValidateVersionSnapshotPinsOperatorModelLimits(t *testing.T) {
	tenantConfig := sampleTenant()
	tenantConfig.Models = []tenant.ModelConfig{{
		Provider: "openai", ModelName: "gpt-4", APIKey: "test-only-key", MaxTokens: 1_000,
	}}
	tenantConfig.Budget = tenant.BudgetConfig{
		MaxTokensPerDay: 20_000, MaxTokensPerRequest: 16_384,
	}
	tenantConfig.ToolPolicy = tenant.ToolPolicy{Mode: "whitelist"}
	service := &countingTenantService{tenant: tenantConfig}
	snapshot := controlplane.VersionSnapshot{
		Agent: tenant.AgentConfig{Name: "support", Type: "llm", DefaultModel: "gpt-4", MaxLLMCalls: 2},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4", MaxTokens: 1_000},
	}

	if err := validateVersionSnapshot(
		context.Background(), service, platformtool.NewBuiltinCatalog(), worker.NewRuntimeAgentRegistry(), "t1", &snapshot,
	); err != nil {
		t.Fatal(err)
	}
	if snapshot.ModelCatalogRevision != modelcatalog.Revision || snapshot.ModelContextWindow != 8_192 {
		t.Fatalf("snapshot model binding=%q/%d", snapshot.ModelCatalogRevision, snapshot.ModelContextWindow)
	}
	if want := worker.NewRuntimeAgentRegistry().Fingerprint(); snapshot.RuntimeCapabilityFingerprint != want {
		t.Fatalf("snapshot runtime fingerprint=%q, want %q", snapshot.RuntimeCapabilityFingerprint, want)
	}
}

func TestGetTenantTreatsWrappedNotFoundAs404(t *testing.T) {
	service := &countingTenantService{getErr: fmt.Errorf("repository lookup: %w", tenant.ErrTenantNotFound)}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/missing", nil)
	req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{
		ID: "operator", Role: adminauth.RolePlatformAdmin,
	}))
	response := httptest.NewRecorder()

	getTenant(response, req, service, "missing")

	if response.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want %d; body=%s", response.Code, http.StatusNotFound, response.Body.String())
	}
}

func TestValidateVersionSnapshotWithCatalogRejectsUnknownProductionModel(t *testing.T) {
	tenantConfig := sampleTenant()
	tenantConfig.Models = []tenant.ModelConfig{{
		Provider: "openai", ModelName: "unreviewed-model", APIKey: "test-only-key", MaxTokens: 256,
	}}
	service := &countingTenantService{tenant: tenantConfig}
	snapshot := controlplane.VersionSnapshot{
		Agent: tenant.AgentConfig{Name: "support", Type: tenant.AgentTypeLLM, DefaultModel: "unreviewed-model", MaxLLMCalls: 1},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "unreviewed-model", MaxTokens: 256},
	}
	if err := validateVersionSnapshotWithCatalog(
		context.Background(), service, platformtool.NewBuiltinCatalog(), worker.NewRuntimeAgentRegistry(), "t1", &snapshot, true,
	); err == nil {
		t.Fatal("production validation accepted a model outside the operator catalog")
	}
}

func TestValidateVersionSnapshotRejectsCredentialReferenceSubstitution(t *testing.T) {
	tenantConfig := sampleTenant()
	tenantConfig.Models = []tenant.ModelConfig{{
		Provider: "openai", ModelName: "gpt-4", APIKeyRef: "env://TRPC_SECRET_TENANT_A",
		MaxTokens: 1_000,
	}}
	service := &countingTenantService{tenant: tenantConfig}
	snapshot := controlplane.VersionSnapshot{
		Agent: tenant.AgentConfig{Name: "support", Type: tenant.AgentTypeLLM, DefaultModel: "gpt-4", MaxLLMCalls: 1},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4", APIKeyRef: "env://TRPC_SECRET_TENANT_B", MaxTokens: 256},
	}
	if err := validateVersionSnapshot(context.Background(), service, platformtool.NewBuiltinCatalog(), worker.NewRuntimeAgentRegistry(), "t1", &snapshot); err == nil {
		t.Fatal("version accepted a substituted credential reference")
	}
}

func TestValidateVersionSnapshotTreatsKnowledgeSearchAsScopedRuntimeCapability(t *testing.T) {
	tenantConfig := sampleTenant()
	tenantConfig.Models = []tenant.ModelConfig{{
		Provider: "openai", ModelName: "gpt-4", APIKey: "test-only-key", MaxTokens: 1_000,
	}}
	tenantConfig.ToolPolicy = tenant.ToolPolicy{Mode: "whitelist", Allowed: []string{"knowledge_search"}}
	tenantConfig.Storage.KnowledgeBackend = "qdrant"
	tenantConfig.Storage.KnowledgeProfile = "knowledge-primary"
	service := &countingTenantService{tenant: tenantConfig}
	snapshot := controlplane.VersionSnapshot{
		Agent: tenant.AgentConfig{
			Name: "support", Type: tenant.AgentTypeLLM, DefaultModel: "gpt-4", MaxLLMCalls: 2,
			Tools: []string{"knowledge_search"},
		},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4", MaxTokens: 1_000},
	}
	if err := validateVersionSnapshot(
		context.Background(), service, platformtool.NewBuiltinCatalog(), worker.NewRuntimeAgentRegistry(), "t1", &snapshot,
	); err != nil {
		t.Fatalf("managed knowledge capability rejected: %v", err)
	}

	tenantConfig.Storage.KnowledgeBackend = ""
	tenantConfig.Storage.KnowledgeProfile = ""
	if err := validateVersionSnapshot(
		context.Background(), service, platformtool.NewBuiltinCatalog(), worker.NewRuntimeAgentRegistry(), "t1", &snapshot,
	); err == nil {
		t.Fatal("knowledge_search was published without a configured Knowledge backend")
	}
}

func TestValidateVersionSnapshotRejectsInvalidRuntimeCapabilityFingerprint(t *testing.T) {
	base := controlplane.VersionSnapshot{
		Agent: tenant.AgentConfig{Name: "support", Type: tenant.AgentTypeLLM, DefaultModel: "gpt"},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt"},
	}
	for name, fingerprint := range map[string]string{
		"short":   "abcd",
		"non-hex": strings.Repeat("z", 64),
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := base
			snapshot.RuntimeCapabilityFingerprint = fingerprint
			if err := snapshot.Validate(); err == nil {
				t.Fatalf("fingerprint %q was accepted", fingerprint)
			}
		})
	}
}

func TestValidateVersionSnapshotRejectsMalformedRuntimeBeforeTenantRead(t *testing.T) {
	service := &countingTenantService{tenant: sampleTenant()}
	snapshot := controlplane.VersionSnapshot{
		Agent: tenant.AgentConfig{Name: "support", Type: tenant.AgentTypeGraph, DefaultModel: "gpt", MaxLLMCalls: 1},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt"},
	}
	err := validateVersionSnapshot(
		context.Background(), service, platformtool.NewBuiltinCatalog(), worker.NewRuntimeAgentRegistry(), "t1", &snapshot,
	)
	if err == nil || !strings.Contains(err.Error(), "requires a topology") {
		t.Fatalf("graph snapshot error=%v, want topology validation failure", err)
	}
	if calls := service.getCalls.Load(); calls != 0 {
		t.Fatalf("uninstalled runtime loaded tenant %d times", calls)
	}
}

func TestValidateVersionSnapshotRejectsLegacyCustomRuntimeWithoutCapability(t *testing.T) {
	service := &countingTenantService{tenant: sampleTenant()}
	registry := worker.NewRuntimeAgentRegistry()
	if err := registry.Register(tenant.AgentTypeGraph, worker.RuntimeAgentFactoryFunc(func(context.Context, worker.RuntimeAgentBuildSpec, worker.RuntimeAgentDependencies) (agent.Agent, error) {
		return nil, nil
	})); err != nil {
		t.Fatal(err)
	}
	snapshot := controlplane.VersionSnapshot{
		Agent: tenant.AgentConfig{
			Name: "support", Type: tenant.AgentTypeGraph, DefaultModel: "gpt", MaxLLMCalls: 1,
			Runtime: &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "answer"}}, Entry: "answer"},
		},
		Model: tenant.ModelConfig{Provider: "openai", ModelName: "gpt"},
	}
	err := validateVersionSnapshot(context.Background(), service, platformtool.NewBuiltinCatalog(), registry, "t1", &snapshot)
	if !errors.Is(err, worker.ErrRuntimeCapabilityRequired) {
		t.Fatalf("legacy custom runtime error=%v, want ErrRuntimeCapabilityRequired", err)
	}
	if calls := service.getCalls.Load(); calls != 0 {
		t.Fatalf("legacy custom runtime loaded tenant %d times", calls)
	}
}

func TestRedactTenant_MasksSecretsAndPreservesOriginal(t *testing.T) {
	orig := sampleTenant()
	orig.Agents = []tenant.AgentConfig{{Name: "support"}}
	orig.Agents[0].Runtime = &tenant.AgentRuntimeConfig{
		Nodes: []tenant.AgentRuntimeNode{{Name: "answer", Tools: []string{"current_time"}}},
		Entry: "answer",
	}
	orig.Storage.SessionConfig = map[string]string{"dsn": "postgres://user:secret@db/app", "pool": "10"}
	orig.Channels[0].Config = map[string]string{"encoding_aes_key": "legacy-secret", "corp_id": "corp"}
	red := redactTenant(orig)

	if red.Models[0].APIKey != "***REDACTED***" {
		t.Errorf("APIKey not masked: %q", red.Models[0].APIKey)
	}
	if red.Channels[0].Token != "***REDACTED***" || red.Channels[0].Secret != "***REDACTED***" {
		t.Errorf("channel creds not masked: %+v", red.Channels[0])
	}
	// Non-secret fields survive.
	if red.Name != "Acme" || red.Channels[0].AppID != "app" || red.Models[0].ModelName != "gpt" {
		t.Errorf("non-secret fields altered: %+v", red)
	}
	// Original must be untouched (no aliasing of underlying slice arrays).
	if orig.Models[0].APIKey != "sk-super-secret" || orig.Channels[0].Secret != "corp-secret" {
		t.Errorf("redactTenant mutated the original tenant: %+v", orig)
	}
	if red.Storage.SessionConfig["dsn"] != redactedValue || red.Storage.SessionConfig["pool"] != "10" ||
		red.Channels[0].Config["encoding_aes_key"] != redactedValue {
		t.Fatalf("nested credentials not redacted: %#v %#v", red.Storage, red.Channels[0].Config)
	}
	if orig.Storage.SessionConfig["dsn"] == redactedValue || orig.Channels[0].Config["encoding_aes_key"] == redactedValue {
		t.Fatal("nested redaction mutated source tenant")
	}
	red.Agents[0].Runtime.Nodes[0].Name = "mutated"
	red.Agents[0].Runtime.Nodes[0].Tools[0] = "mutated"
	if orig.Agents[0].Runtime.Nodes[0].Name != "answer" || orig.Agents[0].Runtime.Nodes[0].Tools[0] != "current_time" {
		t.Fatal("redacted response aliases the original runtime topology")
	}
}

func TestRedactTenant_MasksUnknownSecretLikeChannelConfig(t *testing.T) {
	tn := sampleTenant()
	tn.Channels[0].Config = map[string]string{
		"api_key":      "sk-channel-secret",
		"access-token": "bearer-secret",
		"display_name": "support",
	}
	red := redactTenant(tn)
	if red.Channels[0].Config["api_key"] != redactedValue ||
		red.Channels[0].Config["access-token"] != redactedValue {
		t.Fatalf("secret-like channel config leaked: %#v", red.Channels[0].Config)
	}
	if red.Channels[0].Config["display_name"] != "support" {
		t.Fatalf("public channel config was unexpectedly masked: %#v", red.Channels[0].Config)
	}
}

func TestRedactTenant_EmptySecretStaysEmpty(t *testing.T) {
	tn := &tenant.Tenant{Channels: []tenant.ChannelBinding{{Type: "telegram"}}}
	red := redactTenant(tn)
	if red.Channels[0].Token != "" || red.Channels[0].Secret != "" {
		t.Errorf("empty secrets must stay empty, got %+v", red.Channels[0])
	}
}

func TestRedactTenant_SanitizesWebhookURL(t *testing.T) {
	tn := &tenant.Tenant{Channels: []tenant.ChannelBinding{
		{WebhookURL: "https://user:password@example.test/callback?token=route-secret&x=1#fragment"},
		{WebhookURL: "/callback?token=route-secret"},
		{WebhookURL: "https://example.test/callback"},
		{WebhookURL: "://invalid"},
	}}
	red := redactTenant(tn)
	if got, want := red.Channels[0].WebhookURL, "https://example.test/callback"; got != want {
		t.Fatalf("sanitized absolute webhook URL=%q, want %q", got, want)
	}
	if got, want := red.Channels[1].WebhookURL, "/callback"; got != want {
		t.Fatalf("sanitized relative webhook URL=%q, want %q", got, want)
	}
	if got := red.Channels[2].WebhookURL; got != "https://example.test/callback" {
		t.Fatalf("safe webhook URL changed: %q", got)
	}
	if got := red.Channels[3].WebhookURL; got != redactedValue {
		t.Fatalf("invalid webhook URL leaked: %q", got)
	}
	if tn.Channels[0].WebhookURL == red.Channels[0].WebhookURL {
		t.Fatal("redaction unexpectedly mutated the source URL")
	}
}

func TestNoStoreAdminResponsesSetsNoStore(t *testing.T) {
	handler := noStoreAdminResponses(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil))
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", got)
	}
}

func TestRequireAdminActorUsesAuthenticatedPrincipal(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants", nil)
	req.Header.Set("X-Admin-Actor", "forged@example.com")
	req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{
		ID: "real-operator", Role: adminauth.RolePlatformAdmin,
	}))
	recorder := httptest.NewRecorder()
	actor, ok := requireAdminActor(recorder, req)
	if !ok || actor != "real-operator" {
		t.Fatalf("actor = %q, ok = %v", actor, ok)
	}
}

func TestPreserveMaskedSecrets(t *testing.T) {
	current := sampleTenant()
	current.Storage.SessionProfile = "shared-postgres"
	current.Storage.MemoryProfile = "shared-redis"
	current.Channels[0].AccountID = "account-1"
	current.Channels[0].WebhookKey = "route-1"
	current.Channels[0].EncodingAESKey = "encoding-key"
	update := &tenant.Tenant{
		Models: []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt", APIKey: redactedValue}},
		Channels: []tenant.ChannelBinding{{
			Type: "wework", AccountID: "account-1", Token: redactedValue,
			Secret: "", EncodingAESKey: redactedValue,
		}},
	}
	preserveMaskedSecrets(current, update)
	if update.Models[0].APIKey != "sk-super-secret" {
		t.Fatalf("model secret was not preserved: %q", update.Models[0].APIKey)
	}
	binding := update.Channels[0]
	if binding.Token != "tok-123" || binding.Secret != "corp-secret" ||
		binding.EncodingAESKey != "encoding-key" || binding.WebhookKey != "route-1" {
		t.Fatalf("channel credentials were not preserved: %#v", binding)
	}
	if update.Storage.SessionProfile != "shared-postgres" || update.Storage.MemoryProfile != "shared-redis" {
		t.Fatalf("storage profile references were not preserved: %#v", update.Storage)
	}
}

func TestPreserveMaskedSecretsKeepsReferenceBackedCredentials(t *testing.T) {
	current := sampleTenant()
	current.Models[0].APIKey = ""
	current.Models[0].APIKeyRef = "env://TRPC_SECRET_OPENAI"
	current.Channels[0].Token = ""
	current.Channels[0].TokenRef = "env://TRPC_SECRET_WECOM_TOKEN"
	current.Channels[0].Secret = ""
	current.Channels[0].SecretRef = "env://TRPC_SECRET_WECOM_SECRET"
	current.Channels[0].EncodingAESKey = ""
	current.Channels[0].EncodingAESKeyRef = "env://TRPC_SECRET_WECOM_AES"
	current.Channels[0].AccountID = "account-ref"
	update := &tenant.Tenant{
		Models: []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt", APIKey: redactedValue}},
		Channels: []tenant.ChannelBinding{{
			Type: "wework", AccountID: "account-ref",
			Token: redactedValue, Secret: "", EncodingAESKey: redactedValue,
		}},
	}
	preserveMaskedSecrets(current, update)
	if update.Models[0].APIKeyRef != current.Models[0].APIKeyRef {
		t.Fatalf("model API key reference = %q, want %q", update.Models[0].APIKeyRef, current.Models[0].APIKeyRef)
	}
	binding := update.Channels[0]
	if binding.TokenRef != current.Channels[0].TokenRef ||
		binding.SecretRef != current.Channels[0].SecretRef ||
		binding.EncodingAESKeyRef != current.Channels[0].EncodingAESKeyRef {
		t.Fatalf("channel secret references were not preserved: %#v", binding)
	}
}

func TestUpdateTenant_ConfigVersionConflictReturns409(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/api/v1/tenants/t1", strings.NewReader(`{
		"name":"Acme",
		"status":"active",
		"configVersion":7,
		"models":[{"provider":"openai","modelName":"gpt","apiKey":"***REDACTED***"}],
		"channels":[{"type":"wework","appId":"app","token":"***REDACTED***","secret":"***REDACTED***"}]
	}`))
	req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{
		ID: "operator@example.com", Role: adminauth.RolePlatformAdmin,
	}))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	updateTenant(recorder, req, conflictTenantService{}, "t1")

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
}

func TestTenantMutationRequiresAdminActor(t *testing.T) {
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tenants/t1", nil)
	recorder := httptest.NewRecorder()
	deleteTenant(recorder, req, conflictTenantService{}, "t1")
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestCrossTenantRequestDeniedBeforeRepositoryRead(t *testing.T) {
	const scopedToken = "tenant-a-admin-token-that-is-long-enough"
	authenticator, err := adminauth.NewAuthenticator(strings.Repeat("b", 32), `[
		{"id":"tenant-a-admin","token":"`+scopedToken+`","role":"tenant_admin","tenantIds":["tenant-a"]}
	]`)
	if err != nil {
		t.Fatal(err)
	}
	service := &countingTenantService{}
	handler := authenticator.Middleware(adminauth.RequireMethods(
		map[string]adminauth.Permission{http.MethodGet: adminauth.PermissionTenantRead},
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			getTenant(writer, request, service, "tenant-b")
		}),
	))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/tenant-b", nil)
	request.Header.Set("Authorization", "Bearer "+scopedToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if calls := service.getCalls.Load(); calls != 0 {
		t.Fatalf("repository read before tenant authorization: %d calls", calls)
	}
}

func TestTenantIDFromPathRejectsNestedAndEscapedPaths(t *testing.T) {
	for _, path := range []string{
		"/api/v1/tenants/tenant-a/child",
		"/api/v1/tenants/tenant%2Fa",
		"/api/v1/tenants/",
	} {
		if id, ok := tenantIDFromPath(path); ok {
			t.Fatalf("path %q accepted as %q", path, id)
		}
	}
	if id, ok := tenantIDFromPath("/api/v1/tenants/tenant-a"); !ok || id != "tenant-a" {
		t.Fatalf("valid path id = %q, ok = %v", id, ok)
	}
}

func TestVersionPublishPathRejectsNestedAndEmptyIDs(t *testing.T) {
	for _, path := range []string{
		"/api/v1/agent-versions//publish",
		"/api/v1/agent-versions/version-a/nested/publish",
		"/api/v1/agent-versions/version%2Fa/publish",
	} {
		if id, ok := versionIDFromPublishPath(path); ok {
			t.Fatalf("path %q accepted as %q", path, id)
		}
	}
	if id, ok := versionIDFromPublishPath("/api/v1/agent-versions/version-a/publish"); !ok || id != "version-a" {
		t.Fatalf("valid version path id = %q, ok = %v", id, ok)
	}
}

func TestApprovalListRequiresTenantScopeAndReturnsOnlySafeMetadata(t *testing.T) {
	store := governance.NewMemoryApprovalStore()
	request := governance.ApprovalRequest{
		TenantID: "tenant-a", UserID: "alice", SessionOwnerID: "owner-a", SessionID: "session-a",
		ToolName: "delete_file", ArgsHash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", InvocationID: "inbox:1",
	}
	if _, err := store.CreateChallenge(context.Background(), request, time.Minute); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	registerControlPlaneRoutes(mux, nil, nil, store)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tool-approvals?tenantId=tenant-a", nil)
	req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{ID: "operator", Role: adminauth.RolePlatformAdmin}))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "challengeId") {
		t.Fatalf("approval list status=%d body=%s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("approval list Cache-Control=%q, want no-store", got)
	}
	if strings.Contains(response.Body.String(), "token") || strings.Contains(response.Body.String(), "raw") {
		t.Fatalf("approval list exposed secret-like fields: %s", response.Body.String())
	}
}

func TestSafeApprovalChallengesFiltersAdapterTenantMismatch(t *testing.T) {
	challenges := []governance.ApprovalChallenge{
		{ChallengeID: "a", Request: governance.ApprovalRequest{TenantID: "tenant-a"}},
		{ChallengeID: "b", Request: governance.ApprovalRequest{TenantID: "tenant-b"}},
	}
	items := safeApprovalChallengesForTenant(challenges, "tenant-a")
	if len(items) != 1 || items[0]["challengeId"] != "a" {
		t.Fatalf("filtered approval list=%#v", items)
	}
}

func TestApprovalGrantRouteRequiresGrantPermissionWhenEmbeddedWithoutOuterMiddleware(t *testing.T) {
	store := governance.NewMemoryApprovalStore()
	request := governance.ApprovalRequest{
		TenantID: "tenant-a", UserID: "alice", SessionOwnerID: "owner-a", SessionID: "session-a",
		ToolName: "delete_file", ArgsHash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", InvocationID: "inbox:2",
	}
	challenge, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	registerControlPlaneRoutes(mux, nil, nil, store)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tool-approvals/"+challenge.ChallengeID+"/grant?tenantId=tenant-a", nil)
	req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{ID: "auditor", Role: adminauth.RoleAuditor}))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != http.StatusForbidden {
		t.Fatalf("approval grant status=%d body=%s, want forbidden", response.Code, response.Body.String())
	}
	if _, err := store.Grant(context.Background(), challenge.ChallengeID, "operator"); err != nil {
		t.Fatalf("permission check consumed or altered pending challenge: %v", err)
	}
}

func TestApprovalGrantRouteKeepsRawTokenServerSide(t *testing.T) {
	store := governance.NewMemoryApprovalStore()
	approvalRequest := governance.ApprovalRequest{
		TenantID: "tenant-a", UserID: "alice", SessionOwnerID: "owner-a", SessionID: "session-a",
		ToolName: "delete_file", ArgsHash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", InvocationID: "inbox:3",
	}
	challenge, err := store.CreateChallenge(context.Background(), approvalRequest, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	registerControlPlaneRoutes(mux, nil, nil, store)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tool-approvals/"+challenge.ChallengeID+"/grant?tenantId=tenant-a", nil)
	req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{
		ID: "operator", Role: adminauth.RolePlatformAdmin,
	}))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("approval grant status=%d body=%s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("approval grant Cache-Control=%q, want no-store", got)
	}
	if strings.Contains(strings.ToLower(response.Body.String()), "token") {
		t.Fatalf("approval grant exposed token material: %s", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), challenge.ChallengeID) {
		t.Fatalf("approval grant omitted challenge ID: %s", response.Body.String())
	}
	if err := store.ConsumeGranted(context.Background(), approvalRequest); err != nil {
		t.Fatalf("durable queue-style approval consumption failed: %v", err)
	}
}

func TestReconciliationRouteRequiresPlatformPermissionAndRecorder(t *testing.T) {
	mux := http.NewServeMux()
	registerControlPlaneRoutes(mux, nil, nil, nil)
	requestBody := `{"tenantId":"tenant-a","executionId":40,"reason":"verified"}`

	for _, test := range []struct {
		name string
		role adminauth.Role
		want int
	}{
		{name: "tenant admin denied", role: adminauth.RoleTenantAdmin, want: http.StatusForbidden},
		{name: "platform admin sees unavailable dependency", role: adminauth.RolePlatformAdmin, want: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/execution-reconciliations", strings.NewReader(requestBody))
			req.Header.Set("Content-Type", "application/json")
			req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{
				ID: "operator", Role: test.role,
			}))
			handler := adminauth.RequireMethods(map[string]adminauth.Permission{
				http.MethodPost: adminauth.PermissionExecutionReconcile,
			}, mux)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, test.want, response.Body.String())
			}
		})
	}
}

func TestOutboxReplayRouteFailsClosedWithoutTenantScopedCapability(t *testing.T) {
	mux := http.NewServeMux()
	legacy := legacyReplayStore{Store: reliable.NewMemoryStore()}
	registerControlPlaneRoutes(mux, nil, nil, nil, legacy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/outbox-replays", strings.NewReader(`{"tenantId":"tenant-a","outboxId":1,"reason":"reconcile"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{ID: "operator", Role: adminauth.RolePlatformAdmin}))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unscoped replay route status=%d body=%s, want 503", response.Code, response.Body.String())
	}
}

func TestOutboxReplayRouteFailsClosedForTypedNilStore(t *testing.T) {
	mux := http.NewServeMux()
	var typedNil *reliable.MemoryStore
	registerControlPlaneRoutes(mux, nil, nil, nil, typedNil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/outbox-replays", strings.NewReader(`{"tenantId":"tenant-a","outboxId":1,"reason":"reconcile"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(adminauth.ContextWithPrincipal(req.Context(), adminauth.Principal{ID: "operator", Role: adminauth.RolePlatformAdmin}))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("typed-nil replay route status=%d body=%s, want 503", response.Code, response.Body.String())
	}
}

func TestWriteReconciliationResultMapsTypedErrors(t *testing.T) {
	for _, test := range []struct {
		err  error
		want int
	}{
		{err: controlplane.ErrExecutionRecordMissing, want: http.StatusNotFound},
		{err: controlplane.ErrReconciliationConflict, want: http.StatusConflict},
		{err: controlplane.ErrReconciliationNotAllowed, want: http.StatusConflict},
	} {
		response := httptest.NewRecorder()
		writeReconciliationResult(response, test.err)
		if response.Code != test.want {
			t.Fatalf("error %v status = %d, want %d", test.err, response.Code, test.want)
		}
	}
}

func TestWriteReconciliationResultDoesNotExposePrefixedInternalErrors(t *testing.T) {
	response := httptest.NewRecorder()
	writeReconciliationResult(response, errors.New("reconcile execution: pq: password authentication failed for host db.internal"))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if strings.Contains(response.Body.String(), "db.internal") || strings.Contains(response.Body.String(), "password") {
		t.Fatalf("internal reconciliation error leaked: %s", response.Body.String())
	}
}

func TestWriteControlResultMapsStableErrorClasses(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid request", err: controlplane.ErrInvalidControlPlaneRequest, want: http.StatusBadRequest},
		{name: "missing resource", err: controlplane.ErrControlPlaneNotFound, want: http.StatusNotFound},
		{name: "state conflict", err: controlplane.ErrControlPlaneConflict, want: http.StatusConflict},
		{name: "tenant inactive", err: controlplane.ErrTenantInactive, want: http.StatusConflict},
		{name: "database unavailable", err: errors.New("pq: connection refused"), want: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeControlResult(response, nil, test.err, http.StatusCreated)
			if response.Code != test.want {
				t.Fatalf("status=%d, want %d; body=%s", response.Code, test.want, response.Body.String())
			}
		})
	}
}

func TestTenantUpdateRejectsUnmatchedRedactionPlaceholder(t *testing.T) {
	value := sampleTenant()
	value.Models = append(value.Models, tenant.ModelConfig{Provider: "openai", ModelName: "new", APIKey: redactedValue})
	if !tenantContainsRedacted(value) {
		t.Fatal("unmatched redaction placeholder was not detected")
	}
}
