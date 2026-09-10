package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/node"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	agentknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type recordingProducer struct {
	published []messaging.Envelope
	err       error
}

type staticNodeLister []node.Record

func (s staticNodeLister) List(context.Context) ([]node.Record, error) {
	return append([]node.Record(nil), s...), nil
}

func (p *recordingProducer) Publish(_ context.Context, envelope messaging.Envelope) error {
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, envelope)
	return nil
}

type fakeClaimLister struct {
	claims  []storage.Claim
	lastApp string
}

func (l *fakeClaimLister) ListClaims(_ context.Context, _ string, appCode string, _ int) ([]storage.Claim, error) {
	l.lastApp = appCode
	return l.claims, nil
}

type fakeToolExecutionLister struct {
	records []platformtool.ExecutionRecord
}

func (l fakeToolExecutionLister) ListExecutions(_ context.Context, _, _ string) ([]platformtool.ExecutionRecord, error) {
	return append([]platformtool.ExecutionRecord(nil), l.records...), nil
}

type fakeSessionLister struct {
	sessions []storage.Session
}

func (l *fakeSessionLister) ListSessions(_ context.Context, _ string, _ int) ([]storage.Session, error) {
	return l.sessions, nil
}

type summarySessionService struct {
	agentsession.Service
	summary   string
	enqueued  int
	filterKey string
	force     bool
}

type staticAgentSessionProvider struct{ service agentsession.Service }

func (p staticAgentSessionProvider) Session(context.Context, config.TenantConfig) (agentsession.Service, error) {
	return p.service, nil
}

type staticAgentMemoryProvider struct{ reader agentmemory.Reader }

func (p staticAgentMemoryProvider) MemoryReader(context.Context, config.TenantConfig) (agentmemory.Reader, error) {
	return p.reader, nil
}

func (p staticAgentMemoryProvider) MemoryService(context.Context, config.TenantConfig) (agentmemory.Service, error) {
	service, ok := p.reader.(agentmemory.Service)
	if !ok {
		return nil, errors.New("test Memory reader is not writable")
	}
	return service, nil
}

func (s *summarySessionService) GetSessionSummaryText(context.Context, *agentsession.Session, ...agentsession.SummaryOption) (string, bool) {
	return s.summary, strings.TrimSpace(s.summary) != ""
}

func (s *summarySessionService) EnqueueSummaryJob(_ context.Context, _ *agentsession.Session, filterKey string, force bool) error {
	s.enqueued++
	s.filterKey = filterKey
	s.force = force
	return nil
}

type memoryKnowledgeStore struct {
	knowledge map[string]storage.KnowledgeDocument
	content   map[string]string
}

func newMemoryKnowledgeStore() *memoryKnowledgeStore {
	return &memoryKnowledgeStore{
		knowledge: make(map[string]storage.KnowledgeDocument),
		content:   make(map[string]string),
	}
}

func (s *memoryKnowledgeStore) seed(document storage.KnowledgeDocument, content string) {
	key := document.TenantID + "/" + document.AppCode + "/" + document.DocumentID
	s.knowledge[key] = document
	s.content[key] = content
}

func (s *memoryKnowledgeStore) ListKnowledgeDocuments(_ context.Context, tenantID, appCode string) ([]storage.KnowledgeDocument, error) {
	result := make([]storage.KnowledgeDocument, 0)
	for _, doc := range s.knowledge {
		if doc.TenantID == tenantID && doc.AppCode == appCode {
			result = append(result, doc)
		}
	}
	return result, nil
}

func (s *memoryKnowledgeStore) DeleteKnowledgeDocument(_ context.Context, application config.TenantConfig, documentID string) error {
	key := application.TenantID + "/" + application.AppCode + "/" + documentID
	delete(s.knowledge, key)
	delete(s.content, key)
	return nil
}

func (s *memoryKnowledgeStore) SearchKnowledge(_ context.Context, application config.TenantConfig, request *agentknowledge.SearchRequest) (*agentknowledge.SearchResult, error) {
	result := &agentknowledge.SearchResult{Documents: make([]*agentknowledge.Result, 0)}
	limit := request.MaxResults
	if limit <= 0 {
		limit = 10
	}
	prefix := application.TenantID + "/" + application.AppCode + "/"
	for key, content := range s.content {
		if !strings.HasPrefix(key, prefix) || !strings.Contains(content, request.Query) {
			continue
		}
		projection := s.knowledge[key]
		hit := &agentknowledge.Result{
			Document: &document.Document{
				ID: projection.DocumentID + ":0", Content: content,
				Metadata: map[string]any{"parent_document_id": projection.DocumentID, "trpc_agent_go_chunk_index": 0},
			},
			Score: 0.8,
		}
		result.Documents = append(result.Documents, hit)
		if result.Document == nil {
			result.Document, result.Score, result.Text = hit.Document, hit.Score, hit.Document.Content
		}
		if len(result.Documents) == limit {
			break
		}
	}
	return result, nil
}

type staticArtifactProvider struct{ service agentartifact.Service }

func (p staticArtifactProvider) ArtifactService(context.Context, config.TenantConfig) (agentartifact.Service, error) {
	return p.service, nil
}

type recordingKnowledgeIngestQueue struct {
	request   storage.KnowledgeIngestRequest
	cancelled string
}

func (q *recordingKnowledgeIngestQueue) EnqueueKnowledgeIngest(_ context.Context, request storage.KnowledgeIngestRequest) (string, error) {
	q.request = request
	return "job-1", nil
}
func (q *recordingKnowledgeIngestQueue) CancelKnowledgeIngest(_ context.Context, _, _, documentID string) error {
	q.cancelled = documentID
	return nil
}
func (*recordingKnowledgeIngestQueue) ClaimKnowledgeIngest(context.Context, string, time.Duration) (storage.KnowledgeIngestJob, bool, error) {
	return storage.KnowledgeIngestJob{}, false, nil
}
func (*recordingKnowledgeIngestQueue) CompleteKnowledgeIngest(context.Context, storage.KnowledgeIngestCompletion) error {
	return nil
}
func (*recordingKnowledgeIngestQueue) FailKnowledgeIngest(context.Context, string, string, string, int) (bool, error) {
	return false, nil
}

// testConsole bundles the session-protected console handler with the memory
// session store so tests can sign in deterministically.
type testConsole struct {
	handler   http.Handler
	store     *identity.MemorySessionStore
	state     *storage.MemoryStateStore
	artifacts agentartifact.Service
	knowledge *memoryKnowledgeStore
}

func testConsoleHandler(t *testing.T, repositories ...func(*ConsoleDependencies)) *testConsole {
	t.Helper()
	configurations := tenant.NewMemoryRepository()
	tenantConfig := config.TenantConfig{
		TenantID: "example", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "example-support-bot"}},
		Model:    config.ModelConfig{ProviderID: "primary", Name: "support"},
		Tools:    config.ToolPolicy{},
		Storage: config.StoragePolicy{
			Session: config.BackendProfileRef{ProfileID: "platform-postgres"}, Memory: config.BackendProfileRef{ProfileID: "platform-postgres"},
			Knowledge: config.BackendProfileRef{ProfileID: "platform-pgvector"}, Artifact: config.BackendProfileRef{ProfileID: "platform-postgres"},
		},
		Governance: config.GovernancePolicy{MaxToolCalls: 4},
	}
	if _, err := configurations.Publish(context.Background(), tenantConfig); err != nil {
		t.Fatalf("publish tenant configuration: %v", err)
	}
	stateStore := storage.NewMemoryStateStore()
	artifactService := artifactinmemory.NewService()
	knowledgeStore := newMemoryKnowledgeStore()
	identities := identity.NewMemoryIdentityStore()
	if err := identities.UpsertTenant(context.Background(), "example", "TrailForge"); err != nil {
		t.Fatalf("seed tenant identity: %v", err)
	}
	if err := identities.ReplaceTenantModelGrants(context.Background(), "example", []identity.TenantModelGrant{
		{ProviderID: "primary", ModelName: "support"},
		{ProviderID: "primary", ModelName: "support-v2"},
	}); err != nil {
		t.Fatalf("seed tenant model policy: %v", err)
	}
	backendProfiles := storage.NewMemoryBackendProfileStore()
	if err := backendProfiles.ReplaceTenantBackendProfiles(context.Background(), "example", []string{"platform-postgres", "platform-pgvector"}); err != nil {
		t.Fatalf("seed tenant backend policy: %v", err)
	}
	dependencies := ConsoleDependencies{
		Configurations:  configurations,
		Identities:      identities,
		Producer:        &recordingProducer{},
		Sessions:        &fakeSessionLister{},
		SessionManager:  stateStore,
		Claims:          &fakeClaimLister{},
		State:           stateStore,
		Attempts:        storage.NewMemoryRetryTracker(),
		WebIdempotency:  storage.NewMemoryIdempotencyStore(),
		KnowledgeIngest: &recordingKnowledgeIngestQueue{},
		BackendProfiles: backendProfiles,
		System: SystemInfo{
			Version:               "test",
			ModelProviders:        []ModelProviderInfo{{ID: "primary", Type: "openai", Models: []ModelInfo{{Name: "support"}, {Name: "support-v2"}}}},
			ChannelCredentialRefs: []string{"env:BOT_CONFIG"},
			ToolCredentialRefs:    []string{"env:TOOL_TOKEN"},
		},
		Probes:           map[string]DependencyProbe{"postgres": func(context.Context) error { return nil }},
		Knowledge:        knowledgeStore,
		AgentMemory:      staticAgentMemoryProvider{reader: memoryinmemory.NewMemoryService()},
		ArtifactServices: staticArtifactProvider{service: artifactService},
		ToolCatalog: []ToolInfo{
			{Name: "query_order", Description: "查询订单"},
			{Name: "refund_order", Description: "发起退款"},
		},
	}
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatalf("construct execution manifest codec: %v", err)
	}
	dependencies.ExecutionManifests = manifests
	modelCatalog, err := config.NewModelCatalog([]config.ModelProviderConfig{{ID: "primary", BaseURL: "https://models.example/v1", APIKeyRef: "env:MODEL_API_KEY", Models: []config.ModelPricingConfig{
		{Name: "support", Capabilities: &config.ModelCapabilities{ThinkingToggle: true, ReasoningEfforts: []string{"medium"}}},
		{Name: "support-v2"},
	}}})
	if err != nil {
		t.Fatalf("construct test model catalog: %v", err)
	}
	validator, err := config.NewPlatformPolicyValidator(
		modelCatalog,
		[]string{"env:MODEL_API_KEY", "env:TOOL_TOKEN"},
		nil,
		[]string{"env:TOOL_TOKEN"},
		nil,
		[]string{"postgres", "s3", "cos"},
	)
	if err != nil {
		t.Fatalf("construct test policy validator: %v", err)
	}
	dependencies.ApplicationValidator = validator
	for _, mutate := range repositories {
		mutate(&dependencies)
	}
	handler, err := NewConsoleHandler(dependencies)
	if err != nil {
		t.Fatalf("construct console handler: %v", err)
	}
	store := identity.NewMemorySessionStore()
	// Mirror the composition root: session authentication plus CSRF for writes.
	return &testConsole{
		handler:   identity.CSRFMiddleware(identity.SessionMiddleware(store, nil, nil, handler)),
		store:     store,
		state:     stateStore,
		artifacts: artifactService,
		knowledge: knowledgeStore,
	}
}

func TestConsoleListsTenantConfigurationCatalog(t *testing.T) {
	handler := testConsoleHandler(t)
	admin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	recorder := handler.requestAs(t, admin, http.MethodGet, "/api/v1/catalog?tenant=example", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("catalog status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var response TenantCatalog
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode tenant catalog: %v", err)
	}
	if len(response.Tools) != 2 || response.Tools[0].Name != "query_order" || response.Tools[1].Name != "refund_order" {
		t.Fatalf("tools = %#v, want injected tenant-selectable catalog", response.Tools)
	}
	if len(response.ModelProviders) != 1 || response.ModelProviders[0].ID != "primary" || len(response.ChannelCredentialRefs) != 1 || len(response.ToolCredentialRefs) != 1 {
		t.Fatalf("catalog = %#v, want managed model and credential catalogs", response)
	}
	member := identity.SessionUser{PlatformUserID: "member", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember}}}
	denied := handler.requestAs(t, member, http.MethodGet, "/api/v1/catalog?tenant=example", "")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("member catalog status = %d, want 403", denied.Code)
	}
}

func TestConsoleTenantModelPolicyFiltersCatalogAndGuardsApplicationPublication(t *testing.T) {
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		if err := store.ReplaceTenantModelGrants(context.Background(), "example", []identity.TenantModelGrant{
			{ProviderID: "primary", ModelName: "support"},
		}); err != nil {
			t.Fatal(err)
		}
	})
	tenantAdmin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}

	catalog := handler.requestAs(t, tenantAdmin, http.MethodGet, "/api/v1/catalog?tenant=example", "")
	if catalog.Code != http.StatusOK {
		t.Fatalf("catalog status = %d: %s", catalog.Code, catalog.Body.String())
	}
	var filtered TenantCatalog
	if err := json.Unmarshal(catalog.Body.Bytes(), &filtered); err != nil {
		t.Fatalf("decode filtered catalog: %v", err)
	}
	if len(filtered.ModelProviders) != 1 || len(filtered.ModelProviders[0].Models) != 1 || filtered.ModelProviders[0].Models[0].Name != "support" {
		t.Fatalf("tenant model catalog = %#v, want only explicitly authorized model", filtered.ModelProviders)
	}

	unauthorized := handler.requestAs(t, tenantAdmin, http.MethodPut, "/api/v1/apps/example/support", `{
		"status":"disabled",
		"model":{"provider_id":"primary","name":"support-v2"},
		"channels":[]
	}`)
	if unauthorized.Code != http.StatusBadRequest || !strings.Contains(unauthorized.Body.String(), "not authorized for tenant") {
		t.Fatalf("unauthorized model publication = %d: %s", unauthorized.Code, unauthorized.Body.String())
	}

	unauthorizedFailover := handler.requestAs(t, tenantAdmin, http.MethodPut, "/api/v1/apps/example/support", `{
		"status":"active",
		"model":{"provider_id":"primary","name":"support","failover_candidates":[{"provider_id":"primary","name":"support-v2"}]},
		"channels":[]
	}`)
	if unauthorizedFailover.Code != http.StatusBadRequest || !strings.Contains(unauthorizedFailover.Body.String(), "failover_candidates") {
		t.Fatalf("unauthorized failover publication = %d: %s", unauthorizedFailover.Code, unauthorizedFailover.Body.String())
	}

	systemAdmin := identity.SessionUser{PlatformUserID: "root", IsSystemAdmin: true}
	updated := handler.requestAs(t, systemAdmin, http.MethodPut, "/api/v1/tenant-model-policy?tenant=example", `{
		"tenant_id":"example",
		"models":[{"provider_id":"primary","name":"support"},{"provider_id":"primary","name":"support-v2"}]
	}`)
	if updated.Code != http.StatusOK {
		t.Fatalf("system model policy update = %d: %s", updated.Code, updated.Body.String())
	}
	deniedPolicyWrite := handler.requestAs(t, tenantAdmin, http.MethodPut, "/api/v1/tenant-model-policy?tenant=example", `{"models":[]}`)
	if deniedPolicyWrite.Code != http.StatusForbidden {
		t.Fatalf("tenant admin model policy write = %d, want 403", deniedPolicyWrite.Code)
	}

	catalog = handler.requestAs(t, tenantAdmin, http.MethodGet, "/api/v1/catalog?tenant=example", "")
	if catalog.Code != http.StatusOK {
		t.Fatalf("catalog after policy update = %d: %s", catalog.Code, catalog.Body.String())
	}
	if err := json.Unmarshal(catalog.Body.Bytes(), &filtered); err != nil {
		t.Fatalf("decode updated filtered catalog: %v", err)
	}
	if len(filtered.ModelProviders) != 1 || len(filtered.ModelProviders[0].Models) != 2 {
		t.Fatalf("updated tenant model catalog = %#v, want two authorized models", filtered.ModelProviders)
	}
}

func TestConsoleListsEmptyToolCatalogAsArray(t *testing.T) {
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.ToolCatalog = nil
	})
	recorder := handler.request(t, http.MethodGet, "/api/v1/catalog?tenant=example", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("catalog status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var response TenantCatalog
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	if response.Tools == nil || len(response.Tools) != 0 {
		t.Fatalf("empty tool catalog = %#v, want non-nil empty array", response.Tools)
	}
}

func TestConsoleExplicitSessionViewsSeparateMemberAndManager(t *testing.T) {
	sessions := &fakeSessionLister{sessions: []storage.Session{
		{TenantID: "example", AppCode: "support", SessionKey: "owned", SubjectID: "member-1", OwnerPlatformUserID: "member-1", Status: "active", Conversations: []storage.SessionConversation{{Channel: "web", Scope: "direct"}, {Channel: "wecom", Scope: "direct"}}},
		{TenantID: "example", AppCode: "support", SessionKey: "other", SubjectID: "member-2", OwnerPlatformUserID: "member-2", Status: "active", Conversations: []storage.SessionConversation{{Channel: "telegram", Scope: "direct"}}},
		{TenantID: "example", AppCode: "support", SessionKey: "anonymous", SubjectID: "external:telegram:tg:42", Status: "active", Conversations: []storage.SessionConversation{{Channel: "telegram", Scope: "direct", ExternalUserID: "42"}}},
		{TenantID: "example", AppCode: "support", SessionKey: "group", SubjectID: "group:feishu:fs:g1", Status: "active", Conversations: []storage.SessionConversation{{Channel: "feishu", Scope: "group", ConversationID: "g1"}}},
	}}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) { dependencies.Sessions = sessions })
	member := identity.SessionUser{PlatformUserID: "member-1", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember}}}
	mine := handler.requestAs(t, member, http.MethodGet, "/api/v1/sessions/mine?tenant=example&channel=wecom", "")
	if mine.Code != http.StatusOK || !strings.Contains(mine.Body.String(), "owned") || strings.Contains(mine.Body.String(), "other") || strings.Contains(mine.Body.String(), "group") {
		t.Fatalf("mine response = %d %s", mine.Code, mine.Body.String())
	}
	denied := handler.requestAs(t, member, http.MethodGet, "/api/v1/sessions/tenant?tenant=example", "")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("member tenant sessions status = %d, want 403", denied.Code)
	}
	admin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	all := handler.requestAs(t, admin, http.MethodGet, "/api/v1/sessions/tenant?tenant=example", "")
	if all.Code != http.StatusOK || !strings.Contains(all.Body.String(), "owned") || !strings.Contains(all.Body.String(), "anonymous") || !strings.Contains(all.Body.String(), "group") {
		t.Fatalf("tenant response = %d %s", all.Code, all.Body.String())
	}
}

func TestConsoleTenantSummaryUsesCanonicalMembershipAndDisplayName(t *testing.T) {
	var platformUserID string
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		user := resolveTestWeComUser(t, store, "corp-trailforge", "ming")
		platformUserID = user.PlatformUserID
		if err := store.GrantMembership(context.Background(), "example", platformUserID, identity.RoleAdmin); err != nil {
			t.Fatal(err)
		}
	})
	admin := identity.SessionUser{PlatformUserID: platformUserID, Tenants: []identity.TenantRole{{TenantID: "example", DisplayName: "TrailForge", Role: identity.RoleAdmin}}}
	list := handler.requestAs(t, admin, http.MethodGet, "/api/v1/tenants", "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "TrailForge") || strings.Contains(list.Body.String(), "corp-trailforge") {
		t.Fatalf("tenant summary = %d %s", list.Code, list.Body.String())
	}
}

// request signs in a fresh session and performs one request with the CSRF
// cookie + header set (write operations require both).
func (tc *testConsole) request(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return tc.requestAs(t, identity.SessionUser{PlatformUserID: "console-admin", IsSystemAdmin: true, Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}, method, path, body)
}

// requestAs signs in as an arbitrary session user for authorization contract tests.
func (tc *testConsole) requestAs(t *testing.T, user identity.SessionUser, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	sessionID, err := tc.store.Create(context.Background(), user, time.Hour)
	if err != nil {
		t.Fatalf("create test session: %v", err)
	}
	const csrf = "test-csrf-token"
	request.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	request.AddCookie(&http.Cookie{Name: "csrf_token", Value: csrf})
	request.Header.Set("X-CSRF-Token", csrf)
	recorder := httptest.NewRecorder()
	tc.handler.ServeHTTP(recorder, request)
	return recorder
}

func TestNewConsoleHandlerRequiresDependencies(t *testing.T) {
	if _, err := NewConsoleHandler(ConsoleDependencies{}); err == nil {
		t.Fatal("expected error for empty dependencies")
	}
}

func TestConsoleRejectsMissingSession(t *testing.T) {
	handler := testConsoleHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps", nil)
	recorder := httptest.NewRecorder()
	handler.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without session, got %d", recorder.Code)
	}
}

func TestConsoleListApplications(t *testing.T) {
	handler := testConsoleHandler(t)
	recorder := handler.request(t, http.MethodGet, "/api/v1/apps", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Applications []tenant.Snapshot `json:"applications"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode applications: %v", err)
	}
	if len(response.Applications) != 1 {
		t.Fatalf("expected 1 application, got %d", len(response.Applications))
	}
	if response.Applications[0].Config.TenantID != "example" {
		t.Fatalf("unexpected application: %+v", response.Applications[0])
	}
}

func TestConsoleArchivesSessionWithoutInventingSummary(t *testing.T) {
	handler := testConsoleHandler(t)
	const sessionKey = "example/support/web/conversation-1"
	if _, err := handler.state.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: sessionKey,
		MessageID: "message-1", Channel: "web", BindingID: "web-console", TraceID: "trace-1",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply",
		SubjectID: "user-1",
	}); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	recorder := handler.request(t, http.MethodPost, "/api/v1/sessions/archive",
		`{"tenant_id":"example","session_key":"example/support/web/conversation-1"}`)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("archive status = %d, want 204: %s", recorder.Code, recorder.Body.String())
	}
	session, err := handler.state.GetSession(context.Background(), "example", sessionKey)
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if session.Status != "archived" || session.ArchivedAt == nil {
		t.Fatalf("archived session = %+v", session)
	}
}

func TestConsoleMemberCanArchiveOwnWebSession(t *testing.T) {
	handler := testConsoleHandler(t)
	const sessionKey = "example/support/web/member-session"
	resolved, err := handler.state.ResolveSession(context.Background(), storage.SessionRoute{
		TenantID: "example", AppCode: "support", Channel: "web", BindingID: "web-console",
		ConversationID: "member-session", SubjectID: "member", OwnerPlatformUserID: "member", Scope: "direct",
	}, sessionKey)
	if err != nil || resolved != sessionKey {
		t.Fatalf("resolve member session = %q, %v; want %q", resolved, err, sessionKey)
	}
	if _, err := handler.state.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: sessionKey,
		MessageID: "member-message", Channel: "web", BindingID: "web-console", TraceID: "member-trace",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply", SubjectID: "member",
	}); err != nil {
		t.Fatalf("seed member session: %v", err)
	}
	member := identity.SessionUser{PlatformUserID: "member", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember}}}
	recorder := handler.requestAs(t, member, http.MethodPost, "/api/v1/sessions/archive", `{"tenant_id":"example","session_key":"example/support/web/member-session"}`)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("member archive status = %d, want 204: %s", recorder.Code, recorder.Body.String())
	}
}

func TestConsoleSessionsReadsSummaryFromFrameworkSessionService(t *testing.T) {
	framework := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = framework.Close() })
	const sessionKey = "example/support/web/conversation-summary"
	if _, err := framework.CreateSession(context.Background(), agentsession.Key{
		AppName: "example/support", UserID: "user-summary", SessionID: sessionKey,
	}, nil); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	summaries := &summarySessionService{Service: framework, summary: "框架生成的摘要"}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Sessions = &fakeSessionLister{sessions: []storage.Session{{
			TenantID: "example", AppCode: "support", SessionKey: sessionKey,
			SubjectID: "user-summary", OwnerPlatformUserID: "console-admin", Status: "active", UpdatedAt: time.Now(),
		}}}
		dependencies.AgentSessions = staticAgentSessionProvider{service: summaries}
	})
	recorder := handler.request(t, http.MethodGet, "/api/v1/sessions/mine?tenant=example", "")
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "框架生成的摘要") {
		t.Fatalf("sessions response = %d: %s", recorder.Code, recorder.Body.String())
	}
	tenantView := handler.request(t, http.MethodGet, "/api/v1/sessions/tenant?tenant=example", "")
	if tenantView.Code != http.StatusOK || strings.Contains(tenantView.Body.String(), "框架生成的摘要") || strings.Contains(tenantView.Body.String(), `"summary":`) || strings.Contains(tenantView.Body.String(), `"preview":`) {
		t.Fatalf("tenant session metadata leaked content = %d: %s", tenantView.Code, tenantView.Body.String())
	}
}

func TestConsoleSessionsDefaultsToTenRecentEntries(t *testing.T) {
	sessions := make([]storage.Session, 0, 12)
	for index := 0; index < 12; index++ {
		sessions = append(sessions, storage.Session{
			TenantID: "example", AppCode: "support",
			SessionKey: fmt.Sprintf("example/support/session/session-%02d", index),
			SubjectID:  fmt.Sprintf("telegram:user-%02d", index),
			Status:     "active", UpdatedAt: time.Now().Add(-time.Duration(index) * time.Minute),
		})
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Sessions = &fakeSessionLister{sessions: sessions}
	})

	assertCount := func(path string, want int) {
		t.Helper()
		recorder := handler.request(t, http.MethodGet, path, "")
		if recorder.Code != http.StatusOK {
			t.Fatalf("sessions response = %d: %s", recorder.Code, recorder.Body.String())
		}
		var payload struct {
			Sessions []json.RawMessage `json:"sessions"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode sessions response: %v", err)
		}
		if len(payload.Sessions) != want {
			t.Fatalf("sessions count = %d, want %d", len(payload.Sessions), want)
		}
	}

	assertCount("/api/v1/sessions?tenant=example", 10)
	assertCount("/api/v1/sessions?tenant=example&limit=12", 12)

	invalid := handler.request(t, http.MethodGet, "/api/v1/sessions?tenant=example&limit=101", "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid sessions limit status = %d, want 400", invalid.Code)
	}
}

func TestConsoleArchiveForcesFrameworkSummaryBeforePlatformArchive(t *testing.T) {
	framework := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = framework.Close() })
	const sessionKey = "example/support/web/conversation-archive-summary"
	if _, err := framework.CreateSession(context.Background(), agentsession.Key{
		AppName: "example/support", UserID: "user-archive", SessionID: sessionKey,
	}, nil); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	summaries := &summarySessionService{Service: framework}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.AgentSessions = staticAgentSessionProvider{service: summaries}
	})
	if _, err := handler.state.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: sessionKey,
		MessageID: "message-summary", Channel: "web", BindingID: "web-console", TraceID: "trace-summary",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply", SubjectID: "user-archive",
	}); err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	recorder := handler.request(t, http.MethodPost, "/api/v1/sessions/archive",
		`{"tenant_id":"example","session_key":"example/support/web/conversation-archive-summary"}`)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("archive response = %d: %s", recorder.Code, recorder.Body.String())
	}
	if summaries.enqueued != 1 || summaries.filterKey != agentsession.SummaryFilterKeyAllContents || !summaries.force {
		t.Fatalf("summary enqueue = count=%d filter=%q force=%v", summaries.enqueued, summaries.filterKey, summaries.force)
	}
	session, err := handler.state.GetSession(context.Background(), "example", sessionKey)
	if err != nil || session.Status != "archived" {
		t.Fatalf("platform archive = %+v, %v", session, err)
	}
}
func TestConsoleManagesTenantPlatformData(t *testing.T) {
	handler := testConsoleHandler(t)
	memoryService := memoryinmemory.NewMemoryService()
	if err := memoryService.AddMemory(context.Background(), agentmemory.UserKey{AppName: "example/support", UserID: "user-1"}, "prefers concise replies", []string{"preference"}); err != nil {
		t.Fatalf("AddMemory() fixture error = %v", err)
	}
	handler = testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.AgentMemory = staticAgentMemoryProvider{reader: memoryService}
	})
	knowledgeWrite := handler.request(t, http.MethodPost, "/api/v1/knowledge", `{"tenant_id":"example","app_code":"support","document_id":"faq-1","content":"Refunds are available within 30 days.","metadata":{"kind":"faq"}}`)
	if knowledgeWrite.Code != http.StatusAccepted {
		t.Fatalf("knowledge write status = %d: %s", knowledgeWrite.Code, knowledgeWrite.Body.String())
	}
	handler.knowledge.seed(storage.KnowledgeDocument{
		TenantID: "example", AppCode: "support", DocumentID: "faq-1", Name: "faq-1", Status: "ready", TotalChunks: 1,
		Metadata: map[string]string{"kind": "faq"},
	}, "Refunds are available within 30 days.")
	preferenceUser := identity.SessionUser{PlatformUserID: "user-1", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember, Status: "active"}}}
	memory := handler.requestAs(t, preferenceUser, http.MethodGet, "/api/v1/memory?tenant=example&app=support", "")
	if memory.Code != http.StatusOK || !strings.Contains(memory.Body.String(), "prefers concise replies") {
		t.Fatalf("memory response = %d: %s", memory.Code, memory.Body.String())
	}
	eventTime := time.Date(2024, 5, 7, 0, 0, 0, 0, time.UTC)
	if err := memoryService.AddMemory(
		context.Background(), agentmemory.UserKey{AppName: "example/support", UserID: "user-1"},
		"went hiking at Mt. Fuji", []string{"trip"},
		agentmemory.WithMetadata(&agentmemory.Metadata{
			Kind: agentmemory.KindEpisode, EventTime: &eventTime, Location: "Mt. Fuji", Participants: []string{"Alice"},
		}),
	); err != nil {
		t.Fatalf("AddMemory() episode fixture error = %v", err)
	}
	episodes := handler.requestAs(t, preferenceUser, http.MethodGet, "/api/v1/memory?tenant=example&app=support&kind=episode&order=event_time", "")
	if episodes.Code != http.StatusOK || !strings.Contains(episodes.Body.String(), "went hiking at Mt. Fuji") || !strings.Contains(episodes.Body.String(), "Mt. Fuji") || strings.Contains(episodes.Body.String(), "prefers concise replies") {
		t.Fatalf("episode memory response = %d: %s", episodes.Code, episodes.Body.String())
	}
	search := handler.requestAs(t, preferenceUser, http.MethodGet, "/api/v1/memory?tenant=example&app=support&query=concise", "")
	if search.Code != http.StatusOK || !strings.Contains(search.Body.String(), "prefers concise replies") {
		t.Fatalf("memory search response = %d: %s", search.Code, search.Body.String())
	}
	invalidKind := handler.requestAs(t, preferenceUser, http.MethodGet, "/api/v1/memory?tenant=example&app=support&kind=profile", "")
	if invalidKind.Code != http.StatusBadRequest {
		t.Fatalf("invalid kind status = %d, want 400: %s", invalidKind.Code, invalidKind.Body.String())
	}
	stored, err := memoryService.ReadMemories(context.Background(), agentmemory.UserKey{AppName: "example/support", UserID: "user-1"}, 20)
	if err != nil {
		t.Fatalf("ReadMemories() before delete error = %v", err)
	}
	var preferenceID string
	for _, entry := range stored {
		if entry.Memory != nil && entry.Memory.Memory == "prefers concise replies" {
			preferenceID = entry.ID
			break
		}
	}
	if preferenceID == "" {
		t.Fatal("preference fixture ID was not found")
	}
	deleted := handler.requestAs(t, preferenceUser, http.MethodDelete, "/api/v1/memory?tenant=example&app=support&memory_id="+url.QueryEscape(preferenceID), "")
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete own preference status = %d, want 204: %s", deleted.Code, deleted.Body.String())
	}
	remaining, err := memoryService.ReadMemories(context.Background(), agentmemory.UserKey{AppName: "example/support", UserID: "user-1"}, 20)
	if err != nil {
		t.Fatalf("ReadMemories() after delete error = %v", err)
	}
	for _, entry := range remaining {
		if entry.Memory != nil && entry.Memory.Memory == "prefers concise replies" {
			t.Fatal("deleted preference remained visible")
		}
	}
	memoryWrite := handler.request(t, http.MethodPut, "/api/v1/memory", `{}`)
	if memoryWrite.Code != http.StatusMethodNotAllowed {
		t.Fatalf("memory write status = %d, want 405", memoryWrite.Code)
	}
	const artifactSessionKey = "example/support/web/session-1"
	info := agentartifact.SessionInfo{AppName: "example/support", UserID: "user-1", SessionID: artifactSessionKey}
	if _, err := handler.state.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: artifactSessionKey, SubjectID: "user-1",
		MessageID: "artifact-message", Channel: "web", BindingID: "web-console", TraceID: "artifact-trace",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply",
	}); err != nil {
		t.Fatalf("seed artifact session: %v", err)
	}
	if version, err := handler.artifacts.SaveArtifact(context.Background(), info, "report.txt", &agentartifact.Artifact{Data: []byte("report"), MimeType: "text/plain"}); err != nil || version != 0 {
		t.Fatalf("SaveArtifact() = %d, %v", version, err)
	}
	listing := handler.request(t, http.MethodGet, "/api/v1/artifacts?tenant=example&app=support&session="+url.QueryEscape(artifactSessionKey), "")
	if listing.Code != http.StatusOK || !strings.Contains(listing.Body.String(), "report.txt") {
		t.Fatalf("artifact listing = %d: %s", listing.Code, listing.Body.String())
	}
	artifact := handler.request(t, http.MethodGet, "/api/v1/artifacts?tenant=example&app=support&session="+url.QueryEscape(artifactSessionKey)+"&filename=report.txt", "")
	if artifact.Code != http.StatusOK || artifact.Body.String() != "report" || artifact.Header().Get("Content-Type") != "text/plain" || artifact.Header().Get("X-Artifact-Version") != "0" {
		t.Fatalf("artifact response = %d: %s", artifact.Code, artifact.Body.String())
	}
	upload := handler.request(t, http.MethodPut, "/api/v1/artifacts", `{}`)
	if upload.Code != http.StatusMethodNotAllowed {
		t.Fatalf("artifact upload status = %d, want 405", upload.Code)
	}
}

func TestConsolePlatformDataEnforcesTenantPermissionAndLimits(t *testing.T) {
	handler := testConsoleHandler(t)
	member := identity.SessionUser{PlatformUserID: "member", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember}}}
	allowed := handler.requestAs(t, member, http.MethodGet, "/api/v1/memory?tenant=example&app=support", "")
	if allowed.Code != http.StatusOK {
		t.Fatalf("member memory read status = %d, want 200", allowed.Code)
	}
	denied := handler.requestAs(t, member, http.MethodGet, "/api/v1/memory?tenant=example&app=support&subject=user-1", "")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("cross-user memory status = %d, want 403", denied.Code)
	}
	admin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	adminDenied := handler.requestAs(t, admin, http.MethodGet, "/api/v1/memory?tenant=example&app=support&subject=member", "")
	if adminDenied.Code != http.StatusForbidden {
		t.Fatalf("tenant admin cross-user memory status = %d, want 403", adminDenied.Code)
	}
	unknown := handler.request(t, http.MethodGet, "/api/v1/artifacts?tenant=example&app=missing&session=session-1&filename=report.txt", "")
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown app artifact status = %d, want 404", unknown.Code)
	}
}

func TestConsoleCreateApplicationPublishesVersionOne(t *testing.T) {
	handler := testConsoleHandler(t)
	body := `{"tenant_id":"example","app_code":"bot","status":"active","model":{"provider_id":"primary","name":"support"},"storage":{"session":{"profile_id":"platform-postgres"},"memory":{"profile_id":"platform-postgres"},"knowledge":{"profile_id":"platform-pgvector"},"artifact":{"profile_id":"platform-postgres"}},"channels":[{"type":"telegram","binding_id":"new-bot"}]}`
	admin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	recorder := handler.requestAs(t, admin, http.MethodPost, "/api/v1/apps", body)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Application tenant.Snapshot `json:"application"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if response.Application.Config.TenantID != "example" || response.Application.Config.ConfigVersion != 1 {
		t.Fatalf("unexpected created application: %+v", response.Application)
	}
	if len(response.Application.Config.Tools.Allowed) != 0 {
		t.Fatalf("tenant-selectable tools = %+v, want none", response.Application.Config.Tools)
	}
	list := handler.requestAs(t, admin, http.MethodGet, "/api/v1/apps", "")
	var listing struct {
		Applications []tenant.Snapshot `json:"applications"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode applications: %v", err)
	}
	if len(listing.Applications) != 2 {
		t.Fatalf("expected 2 applications after create, got %d", len(listing.Applications))
	}
}

func TestConsoleCreateApplicationDefaultsToDraft(t *testing.T) {
	handler := testConsoleHandler(t)
	admin := identity.SessionUser{PlatformUserID: "draft-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	recorder := handler.requestAs(t, admin, http.MethodPost, "/api/v1/apps", `{"tenant_id":"example","app_code":"draft-bot","channels":[]}`)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create draft status = %d, want 201: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Application tenant.Snapshot `json:"application"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode draft response: %v", err)
	}
	if response.Application.Config.Status != config.AgentDraft {
		t.Fatalf("new application status = %q, want draft", response.Application.Config.Status)
	}
}

func TestConsoleUpdateApplicationPublishesNextVersion(t *testing.T) {
	handler := testConsoleHandler(t)
	body := `{
		"status":"disabled",
		"instruction":"updated",
		"model":{"provider_id":"primary","name":"support-v2"},
		"tools":{"allowed":[]},
		"storage":{"session":{"profile_id":"platform-postgres"},"memory":{"profile_id":"platform-postgres"},"knowledge":{"profile_id":"platform-pgvector"},"artifact":{"profile_id":"platform-postgres"}},
		"governance":{"max_tool_calls":2,"budget_units":50},
		"audit":{"retention_days":30},
		"channels":[{"type":"wecom","binding_id":"wecom-support"}]
	}`
	recorder := handler.request(t, http.MethodPut, "/api/v1/apps/example/support", body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Application tenant.Snapshot `json:"application"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode update response: %v", err)
	}
	updated := response.Application
	if updated.Config.ConfigVersion != 2 || updated.Config.Status != config.AgentDisabled {
		t.Fatalf("unexpected updated application: %+v", updated)
	}
	if len(updated.Config.Channels) != 1 || updated.Config.Channels[0].Type != config.ChannelWeCom {
		t.Fatalf("unexpected updated channels: %+v", updated.Config.Channels)
	}
	if updated.Config.Model.Name != "support-v2" || len(updated.Config.Tools.Allowed) != 0 ||
		updated.Config.Governance.MaxToolCalls != 2 || updated.Config.Governance.BudgetUnits != 50 ||
		updated.Config.Audit.RetentionDays != 30 {
		t.Fatalf("advanced policy was not published: %+v", updated.Config)
	}
}

func TestConsoleUpdateApplicationWithFailoverAndGenerationConfig(t *testing.T) {
	handler := testConsoleHandler(t)
	body := `{
		"status":"active",
		"instruction":"customer service bot",
		"model":{
			"provider_id":"primary",
			"name":"support",
			"failover_candidates":[{"provider_id":"primary","name":"support-v2"}],
			"generation":{
				"temperature":0.8,
				"max_tokens":1024,
				"thinking_enabled":true,
				"reasoning_effort":"medium"
			}
		},
		"tools":{"allowed":[]},
		"storage":{"session":{"profile_id":"platform-postgres"},"memory":{"profile_id":"platform-postgres"},"knowledge":{"profile_id":"platform-pgvector"},"artifact":{"profile_id":"platform-postgres"}},
		"channels":[]
	}`
	recorder := handler.request(t, http.MethodPut, "/api/v1/apps/example/support", body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Application tenant.Snapshot `json:"application"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode update response: %v", err)
	}
	cfg := response.Application.Config
	if len(cfg.Model.FailoverCandidates) != 1 || cfg.Model.FailoverCandidates[0].Name != "support-v2" {
		t.Fatalf("expected failover candidate support-v2, got: %+v", cfg.Model.FailoverCandidates)
	}
	if cfg.Model.Generation == nil || cfg.Model.Generation.Temperature == nil || *cfg.Model.Generation.Temperature != 0.8 {
		t.Fatalf("expected generation temperature 0.8, got: %+v", cfg.Model.Generation)
	}
	if cfg.Model.Generation.MaxTokens == nil || *cfg.Model.Generation.MaxTokens != 1024 {
		t.Fatalf("expected max_tokens 1024, got: %+v", cfg.Model.Generation)
	}
	if cfg.Model.Generation.ThinkingEnabled == nil || !*cfg.Model.Generation.ThinkingEnabled {
		t.Fatalf("expected thinking_enabled true, got: %+v", cfg.Model.Generation)
	}
	if cfg.Model.Generation.ReasoningEffort == nil || *cfg.Model.Generation.ReasoningEffort != "medium" {
		t.Fatalf("expected reasoning_effort medium, got: %+v", cfg.Model.Generation)
	}
}

func TestConsoleRejectsApplicationUsingUnmanagedProviderOrTool(t *testing.T) {
	handler := testConsoleHandler(t)
	admin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	for _, body := range []string{
		`{"tenant_id":"example","app_code":"bot","model":{"provider_id":"missing","name":"support"},"channels":[]}`,
		`{"tenant_id":"example","app_code":"bot","model":{"provider_id":"primary","name":"support"},"tools":{"allowed":["platform.shell"]},"channels":[]}`,
	} {
		recorder := handler.requestAs(t, admin, http.MethodPost, "/api/v1/apps", body)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("unmanaged policy status = %d, want 400: %s", recorder.Code, recorder.Body.String())
		}
	}
}

func TestConsoleUpdateUnknownApplicationReturnsNotFound(t *testing.T) {
	handler := testConsoleHandler(t)
	body := `{"status":"active","channels":[]}`
	recorder := handler.request(t, http.MethodPut, "/api/v1/apps/example/unknown", body)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestConsoleCreateApplicationRejectsInvalidBody(t *testing.T) {
	handler := testConsoleHandler(t)
	recorder := handler.requestAs(t, identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}, http.MethodPost, "/api/v1/apps", `{"tenant_id":"","app_code":"bot"}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestConsoleExecutionTraceAggregatesState(t *testing.T) {
	stateStore := storage.NewMemoryStateStore()
	event, err := stateStore.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: "example/support/telegram/conv-1",
		MessageID: "msg-1", Channel: string(channels.Telegram), BindingID: "example-support-bot", TraceID: "msg-1",
		Action: "agent_reply", Result: "queued", AuditDetail: "你好，这是测试回复。",
		OutboxType: "channel_reply", OutboxPayload: []byte(`{"channel":"telegram","conversation_id":"conv-1","text":"你好，这是测试回复。"}`),
		ExecutionTrace: &storage.AgentExecutionTrace{
			Status: "completed", RootAgentName: "assistant", RootInvocationID: "inv-1",
			Steps: []storage.ExecutionTraceStep{{StepID: "step-1", NodeID: "assistant#model", NodeType: "llm"}},
		},
	})
	if err != nil {
		t.Fatalf("record execution: %v", err)
	}
	attempts := storage.NewMemoryRetryTracker()
	if _, err := attempts.Increment(context.Background(), "example", "example/support/telegram/conv-1", "msg-1"); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.State = stateStore
		dependencies.Attempts = attempts
		completedAt := time.Date(2026, 9, 9, 1, 0, 1, 500000000, time.UTC)
		dependencies.ToolExecutions = fakeToolExecutionLister{records: []platformtool.ExecutionRecord{{
			RequestID: "msg-1", ToolCallID: "call-1", ToolName: "query_order", Status: platformtool.ExecutionCompleted,
			TraceID: "msg-1", StartedAt: time.Date(2026, 9, 9, 1, 0, 1, 0, time.UTC), CompletedAt: &completedAt,
		}}}
		dependencies.Claims = &fakeClaimLister{claims: []storage.Claim{
			{Channel: "telegram", BindingID: "example-support-bot", MessageID: "msg-1", Status: "processing", TraceID: "msg-1", UpdatedAt: time.Now()},
		}}
	})
	recorder := handler.request(t, http.MethodGet,
		"/api/v1/execution?tenant=example&channel=telegram&binding_id=example-support-bot&event_id=msg-1&session_key=example%2Fs"+
			"upport%2Ftelegram%2Fconv-1", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var trace map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &trace); err != nil {
		t.Fatalf("decode trace: %v", err)
	}
	if trace["attempts"] != float64(1) {
		t.Fatalf("unexpected attempts: %v", trace["attempts"])
	}
	if _, exists := trace["audit"]; exists {
		t.Fatalf("execution response must not synthesize audit as trace: %v", trace["audit"])
	}
	if trace["trace_id"] != "msg-1" || trace["status"] != "completed" {
		t.Fatalf("unified execution identity = trace_id=%v status=%v", trace["trace_id"], trace["status"])
	}
	agentTrace, ok := trace["agent_trace"].(map[string]any)
	if !ok || agentTrace["root_invocation_id"] != "inv-1" {
		t.Fatalf("unexpected framework trace: %v", trace["agent_trace"])
	}
	outbox, ok := trace["outbox"].([]any)
	if !ok || len(outbox) != 1 {
		t.Fatalf("unexpected outbox: %v", trace["outbox"])
	}
	toolExecutions, ok := trace["tool_executions"].([]any)
	if !ok || len(toolExecutions) != 1 {
		t.Fatalf("unexpected tool executions: %v", trace["tool_executions"])
	}
	toolExecution, ok := toolExecutions[0].(map[string]any)
	if !ok || toolExecution["tool_name"] != "query_order" || toolExecution["status"] != "completed" {
		t.Fatalf("unexpected tool execution: %v", toolExecutions[0])
	}
	for _, sensitive := range []string{"arguments_hash", "result_hash", "result_ciphertext", "idempotency_key"} {
		if _, exists := toolExecution[sensitive]; exists {
			t.Fatalf("tool execution leaked %s: %v", sensitive, toolExecution)
		}
	}
	_ = event
}

func TestConsoleListClaimsIncludesTraceSummary(t *testing.T) {
	stateStore := storage.NewMemoryStateStore()
	started := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	if err := stateStore.RecordExecutionTrace(context.Background(), storage.ExecutionTraceRecord{
		TenantID: "example", AppCode: "support", Channel: "telegram", BindingID: "example-support-bot", MessageID: "msg-1", TraceID: "trace-1",
		Trace: storage.AgentExecutionTrace{
			Status: "failed", StartedAt: started, EndedAt: started.Add(2 * time.Second),
			Usage: &storage.ExecutionTraceUsage{PromptTokens: 120, CompletionTokens: 42, TotalTokens: 162, CachedTokens: 80, ReasoningTokens: 10},
			Steps: []storage.ExecutionTraceStep{
				{StepID: "llm-1", NodeType: "llm", Failed: true},
				{StepID: "tool-1", NodeType: "tool"},
			},
		},
	}); err != nil {
		t.Fatalf("record execution trace: %v", err)
	}
	claimLister := &fakeClaimLister{claims: []storage.Claim{
		{AppCode: "support", Channel: "telegram", BindingID: "example-support-bot", MessageID: "msg-1", Status: "completed", TraceID: "trace-1", UpdatedAt: started.Add(2 * time.Second)},
		{AppCode: "support", Channel: "web", BindingID: "web-console", MessageID: "msg-2", Status: "processing", TraceID: "trace-2", UpdatedAt: started},
	}}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.State = stateStore
		dependencies.Claims = claimLister
	})
	recorder := handler.request(t, http.MethodGet, "/api/v1/claims?tenant=example&app=support", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Claims []map[string]any `json:"claims"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	if len(body.Claims) != 2 {
		t.Fatalf("claims = %#v", body.Claims)
	}
	if claimLister.lastApp != "support" {
		t.Fatalf("claim app filter = %q, want support", claimLister.lastApp)
	}
	first := body.Claims[0]
	for _, key := range []string{"app_code", "channel", "message_id", "status", "trace_id", "updated_at"} {
		if _, exists := first[key]; !exists {
			t.Fatalf("claim response missing snake_case key %q: %#v", key, first)
		}
	}
	for _, legacyKey := range []string{"Channel", "MessageID", "Status", "TraceID", "UpdatedAt"} {
		if _, exists := first[legacyKey]; exists {
			t.Fatalf("claim response contains legacy key %q: %#v", legacyKey, first)
		}
	}
	if first["failed"] != true {
		t.Fatalf("enriched claim = %#v", first)
	}
	if first["started_at"] == nil || first["ended_at"] == nil {
		t.Fatalf("trace timing missing from claim summary: %#v", first)
	}
	if _, exists := first["usage"]; exists {
		t.Fatalf("claim summary must not duplicate trace usage: %#v", first)
	}
	if _, exists := first["step_count"]; exists {
		t.Fatalf("claim summary must not duplicate trace steps: %#v", first)
	}
}

func TestConsoleChatEnqueuesWebMessage(t *testing.T) {
	producer := &recordingProducer{}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) { dependencies.Producer = producer })
	body := `{"tenant_id":"example","app_code":"support","conversation_id":"conv-1","text":"hello","request_id":"d8f46533-f06f-4279-aea6-b63c80c43189"}`
	recorder := handler.request(t, http.MethodPost, "/api/v1/chat", body)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if len(producer.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(producer.published))
	}
	payload, err := messaging.DecodeInboundPayload(producer.published[0])
	if err != nil {
		t.Fatalf("decode web envelope: %v", err)
	}
	if payload.Inbound.Channel != channels.Web || payload.BindingID != "web-console" {
		t.Fatalf("unexpected web payload: %+v", payload)
	}
}

func TestConsoleSystemReportsProbes(t *testing.T) {
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Nodes = staticNodeLister{{
			NodeID: "worker-a", BootID: "boot-1", Role: "worker", State: node.StateReady,
			BuildVersion: "test", LastHeartbeat: time.Now().UTC(),
		}}
	})
	recorder := handler.request(t, http.MethodGet, "/api/v1/system", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode system response: %v", err)
	}
	statuses, ok := response["status"].(map[string]any)
	if !ok || statuses["postgres"] != "ok" {
		t.Fatalf("unexpected probe statuses: %v", response["status"])
	}
	info, ok := response["info"].(map[string]any)
	if !ok || info["version"] != "test" {
		t.Fatalf("unexpected system info: %v", response["info"])
	}
	for _, legacyKey := range []string{"Version", "ListenAddress", "KafkaTopic", "KafkaBrokers", "RedisAddress", "ModelProviders"} {
		if _, exists := info[legacyKey]; exists {
			t.Fatalf("system response contains legacy key %q: %#v", legacyKey, info)
		}
	}
	nodes, ok := response["nodes"].([]any)
	if !ok || len(nodes) != 1 || nodes[0].(map[string]any)["node_id"] != "worker-a" {
		t.Fatalf("unexpected nodes: %v", response["nodes"])
	}
}

func TestConsoleSyncModels(t *testing.T) {
	synced := []ModelProviderInfo{
		{ID: "test-openai", Type: "openai", BaseURL: "https://api.example.com", Configured: true, Models: []ModelInfo{
			{Name: "model-a", Capabilities: &config.ModelCapabilities{ReasoningEfforts: []string{"low", "high"}}},
			{Name: "model-b"},
		}},
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.ModelSyncer = func(ctx context.Context) ([]ModelProviderInfo, error) {
			return synced, nil
		}
	})
	recorder := handler.request(t, http.MethodPost, "/api/v1/models/sync", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode sync response: %v", err)
	}
	providers, ok := response["model_providers"].([]any)
	if !ok || len(providers) != 1 {
		t.Fatalf("expected 1 model_provider, got: %v", response)
	}
	models, ok := providers[0].(map[string]any)["models"].([]any)
	if !ok || len(models) != 2 {
		t.Fatalf("expected structured model catalog, got: %v", providers[0])
	}
	capabilities, ok := models[0].(map[string]any)["capabilities"].(map[string]any)
	if !ok || len(capabilities["reasoning_efforts"].([]any)) != 2 {
		t.Fatalf("expected model capabilities in sync response, got: %v", models[0])
	}
}

func TestConsoleDeletesOnlyUnusedDiscoveredModels(t *testing.T) {
	removed := make([]string, 0, 1)
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.System.ModelProviders = []ModelProviderInfo{{
			ID: "primary", Type: "openai", Configured: true,
			Models: []ModelInfo{
				{Name: "support", Source: "discovered"},
				{Name: "unused", Source: "discovered"},
				{Name: "configured", Source: "configured"},
			},
		}}
		dependencies.ModelRemover = func(_ context.Context, providerID, modelName string) error {
			removed = append(removed, providerID+"/"+modelName)
			return nil
		}
	})

	inUse := handler.request(t, http.MethodDelete, "/api/v1/models?provider_id=primary&model=support", "")
	if inUse.Code != http.StatusConflict {
		t.Fatalf("delete in-use model status = %d, want 409: %s", inUse.Code, inUse.Body.String())
	}
	configured := handler.request(t, http.MethodDelete, "/api/v1/models?provider_id=primary&model=configured", "")
	if configured.Code != http.StatusConflict {
		t.Fatalf("delete configured model status = %d, want 409: %s", configured.Code, configured.Body.String())
	}
	deleted := handler.request(t, http.MethodDelete, "/api/v1/models?provider_id=primary&model=unused", "")
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete discovered model status = %d, want 204: %s", deleted.Code, deleted.Body.String())
	}
	if len(removed) != 1 || removed[0] != "primary/unused" {
		t.Fatalf("removed models = %v, want [primary/unused]", removed)
	}

	system := handler.request(t, http.MethodGet, "/api/v1/system", "")
	if system.Code != http.StatusOK {
		t.Fatalf("system status = %d: %s", system.Code, system.Body.String())
	}
	var body struct {
		Info SystemInfo `json:"info"`
	}
	if err := json.Unmarshal(system.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode system: %v", err)
	}
	if got := body.Info.ModelProviders[0].Models; len(got) != 2 || got[0].Name == "unused" || got[1].Name == "unused" {
		t.Fatalf("models after delete = %#v", got)
	}
}

func TestConsoleSPAHandlerFallsBackToIndex(t *testing.T) {
	files := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<!doctype html><title>console-root</title>")},
	}
	handler, err := NewConsoleSPAHandler(files)
	if err != nil {
		t.Fatalf("construct SPA handler: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/console/", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "console-root") {
		t.Fatalf("expected index fallback, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestConsoleSPAHandlerReturnsNotFoundForMissingStaticAsset(t *testing.T) {
	files := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<!doctype html><title>console-root</title>")},
	}
	handler, err := NewConsoleSPAHandler(files)
	if err != nil {
		t.Fatalf("construct SPA handler: %v", err)
	}
	for _, path := range []string{"/console/assets/app.js", "/console/brands/wecom.png"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("GET %s status = %d, want 404", path, recorder.Code)
		}
	}

	route := httptest.NewRecorder()
	handler.ServeHTTP(route, httptest.NewRequest(http.MethodGet, "/console/account", nil))
	if route.Code != http.StatusOK || !strings.Contains(route.Body.String(), "console-root") {
		t.Fatalf("SPA route fallback = %d %s", route.Code, route.Body.String())
	}
}

// Tenant authorization contract tests for INV-01/INV-02.

func tenantMember(existing ...identity.TenantRole) identity.SessionUser {
	return identity.SessionUser{PlatformUserID: "member-1", Tenants: existing}
}

func TestConsoleMemberSeesOnlyMemberTenants(t *testing.T) {
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		if err := store.UpsertTenant(context.Background(), "another", "Another"); err != nil {
			t.Fatal(err)
		}
		if err := store.ReplaceTenantModelGrants(context.Background(), "another", []identity.TenantModelGrant{{ProviderID: "primary", ModelName: "support"}}); err != nil {
			t.Fatal(err)
		}
		if err := dependencies.BackendProfiles.ReplaceTenantBackendProfiles(context.Background(), "another", []string{"platform-postgres", "platform-pgvector"}); err != nil {
			t.Fatal(err)
		}
	})
	// Seed another tenant through an actual tenant administrator; System Admin
	// alone must not be a tenant-business bypass.
	otherAdmin := identity.SessionUser{PlatformUserID: "another-admin", Tenants: []identity.TenantRole{{TenantID: "another", Role: identity.RoleAdmin, Status: "active"}}}
	create := handler.requestAs(t, otherAdmin, http.MethodPost, "/api/v1/apps",
		`{"tenant_id":"another","app_code":"bot","status":"active","model":{"provider_id":"primary","name":"support"},"storage":{"session":{"profile_id":"platform-postgres"},"memory":{"profile_id":"platform-postgres"},"knowledge":{"profile_id":"platform-pgvector"},"artifact":{"profile_id":"platform-postgres"}},"channels":[]}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("tenant admin create status = %d, want 201", create.Code)
	}

	member := tenantMember(identity.TenantRole{TenantID: "example", Role: identity.RoleMember})
	recorder := handler.requestAs(t, member, http.MethodGet, "/api/v1/apps", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("member list status = %d, want 200", recorder.Code)
	}
	var response struct {
		Applications []tenant.Snapshot `json:"applications"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if len(response.Applications) != 1 || response.Applications[0].Config.TenantID != "example" {
		t.Fatalf("member sees %+v, want only example", response.Applications)
	}
}

func TestConsoleNonMemberIsForbiddenFromTenantData(t *testing.T) {
	handler := testConsoleHandler(t)
	member := tenantMember(identity.TenantRole{TenantID: "example", Role: identity.RoleMember})

	// Reads outside the user's tenant memberships are forbidden.
	for _, path := range []string{
		"/api/v1/sessions?tenant=other",
		"/api/v1/claims?tenant=other",
		"/api/v1/audit?tenant=other",
		"/api/v1/outbox?tenant=other",
		"/api/v1/execution?tenant=other&channel=telegram&binding_id=other-bot&event_id=m-1",
	} {
		recorder := handler.requestAs(t, member, http.MethodGet, path, "")
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("GET %s status = %d, want 403", path, recorder.Code)
		}
	}
	// Writes outside the user's tenant memberships are forbidden.
	chat := handler.requestAs(t, member, http.MethodPost, "/api/v1/chat",
		`{"tenant_id":"other","app_code":"bot","conversation_id":"c","text":"hi","request_id":"d8f46533-f06f-4279-aea6-b63c80c43189"}`)
	if chat.Code != http.StatusForbidden {
		t.Fatalf("chat non-member status = %d, want 403", chat.Code)
	}
	create := handler.requestAs(t, member, http.MethodPost, "/api/v1/apps",
		`{"tenant_id":"other","app_code":"bot","channels":[]}`)
	if create.Code != http.StatusForbidden {
		t.Fatalf("create non-member status = %d, want 403", create.Code)
	}
}

func TestConsoleMemberIsReadOnlyButTenantAdminCanWrite(t *testing.T) {
	handler := testConsoleHandler(t)
	member := tenantMember(identity.TenantRole{TenantID: "example", Role: identity.RoleMember})
	admin := tenantMember(identity.TenantRole{TenantID: "example", Role: identity.RoleAdmin})

	// Members can read their tenant but cannot mutate it.
	read := handler.requestAs(t, member, http.MethodGet, "/api/v1/apps", "")
	if read.Code != http.StatusOK {
		t.Fatalf("member read status = %d, want 200", read.Code)
	}
	write := handler.requestAs(t, member, http.MethodPost, "/api/v1/apps",
		`{"tenant_id":"example","app_code":"new-bot","channels":[]}`)
	if write.Code != http.StatusForbidden {
		t.Fatalf("member create status = %d, want 403 (read-only)", write.Code)
	}
	chat := handler.requestAs(t, member, http.MethodPost, "/api/v1/chat",
		`{"tenant_id":"example","app_code":"support","conversation_id":"member-chat","text":"hello","request_id":"11111111-2222-4333-8444-555555555555"}`)
	if chat.Code != http.StatusAccepted {
		t.Fatalf("member chat status = %d, want 202: %s", chat.Code, chat.Body.String())
	}
	for _, path := range []string{
		"/api/v1/claims?tenant=example",
		"/api/v1/audit?tenant=example",
		"/api/v1/outbox?tenant=example",
		"/api/v1/execution?tenant=example&channel=web&binding_id=web-console&event_id=member-message",
		"/api/v1/channels/status?tenant=example&app=support",
		"/api/v1/system",
	} {
		denied := handler.requestAs(t, member, http.MethodGet, path, "")
		if denied.Code != http.StatusForbidden {
			t.Fatalf("member GET %s status = %d, want 403", path, denied.Code)
		}
	}

	// Tenant administrators can mutate their tenant.
	opCreate := handler.requestAs(t, admin, http.MethodPost, "/api/v1/apps",
		`{"tenant_id":"example","app_code":"op-bot","model":{"provider_id":"primary","name":"support"},"channels":[]}`)
	if opCreate.Code != http.StatusCreated {
		t.Fatalf("tenant admin create status = %d, want 201", opCreate.Code)
	}
}

func TestConsoleSystemAdminDoesNotInheritTenantBusinessAccess(t *testing.T) {
	handler := testConsoleHandler(t)
	admin := identity.SessionUser{PlatformUserID: "sys", IsSystemAdmin: true, Tenants: nil}

	recorder := handler.requestAs(t, admin, http.MethodPost, "/api/v1/apps",
		`{"tenant_id":"brand-new","app_code":"bot","model":{"provider_id":"primary","name":"support"},"channels":[]}`)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("system admin create status = %d, want 403", recorder.Code)
	}
	list := handler.requestAs(t, admin, http.MethodGet, "/api/v1/apps", "")
	if list.Code != http.StatusOK {
		t.Fatalf("system admin list status = %d, want 200", list.Code)
	}
	var response struct {
		Applications []tenant.Snapshot `json:"applications"`
	}
	_ = json.Unmarshal(list.Body.Bytes(), &response)
	if len(response.Applications) != 0 {
		t.Fatalf("system admin sees %d tenant apps without membership, want 0", len(response.Applications))
	}
}

func TestConsoleKnowledgeDocumentsAndFrameworkSearch(t *testing.T) {
	handler := testConsoleHandler(t)

	// Ingestion is durable and asynchronous; the console does not maintain a
	// second chunking/search implementation.
	postPayload := `{"tenant_id":"example","app_code":"support","document_id":"faq-policy","name":"退款政策.md","content":"# 退款政策\n\n用户购买后七天内无理由退款。\n\n# 换货说明\n\n商品存在质量问题支持换货。","chunk_size":50,"overlap":10}`
	writeResp := handler.request(t, http.MethodPost, "/api/v1/knowledge", postPayload)
	if writeResp.Code != http.StatusAccepted || !strings.Contains(writeResp.Body.String(), `"job_id":"job-1"`) {
		t.Fatalf("ingest knowledge status = %d, want 202 queued job: %s", writeResp.Code, writeResp.Body.String())
	}
	handler.knowledge.seed(storage.KnowledgeDocument{
		TenantID: "example", AppCode: "support", DocumentID: "faq-policy", Name: "退款政策.md",
		Status: "ready", TotalChunks: 2,
	}, "退款政策：用户购买后七天内无理由退款。换货说明：商品存在质量问题支持换货。")

	listResp := handler.request(t, http.MethodGet, "/api/v1/knowledge/documents?tenant=example&app=support", "")
	if listResp.Code != http.StatusOK {
		t.Fatalf("list documents status = %d, want 200: %s", listResp.Code, listResp.Body.String())
	}
	var docList struct {
		Documents []map[string]any `json:"documents"`
	}
	if err := json.Unmarshal(listResp.Body.Bytes(), &docList); err != nil {
		t.Fatalf("decode docList: %v", err)
	}
	if len(docList.Documents) != 1 {
		t.Fatalf("documents count = %d, want 1", len(docList.Documents))
	}
	doc := docList.Documents[0]
	if doc["document_id"] != "faq-policy" || doc["name"] != "退款政策.md" {
		t.Fatalf("unexpected doc: %#v", doc)
	}
	totalChunks, _ := doc["total_chunks"].(float64)
	if totalChunks <= 0 {
		t.Fatalf("total_chunks = %v, want > 0", totalChunks)
	}

	delResp := handler.request(t, http.MethodDelete, "/api/v1/knowledge?tenant=example&app=support&document_id=faq-policy", "")
	if delResp.Code != http.StatusNoContent {
		t.Fatalf("delete document status = %d, want 204: %s", delResp.Code, delResp.Body.String())
	}

	listResp2 := handler.request(t, http.MethodGet, "/api/v1/knowledge/documents?tenant=example&app=support", "")
	if listResp2.Code != http.StatusOK {
		t.Fatalf("list documents status = %d: %s", listResp2.Code, listResp2.Body.String())
	}
	var docList2 struct {
		Documents []map[string]any `json:"documents"`
	}
	_ = json.Unmarshal(listResp2.Body.Bytes(), &docList2)
	if len(docList2.Documents) != 0 {
		t.Fatalf("documents after delete = %d, want 0", len(docList2.Documents))
	}
}

func TestConsoleKnowledgeMultipartUploadQueuesBinaryDocument(t *testing.T) {
	queue := &recordingKnowledgeIngestQueue{}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.KnowledgeIngest = queue
	})
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for key, value := range map[string]string{
		"tenant_id": "example", "app_code": "support", "document_id": "policy", "name": "Policy",
		"chunk_size": "800", "overlap": "100",
	} {
		if err := form.WriteField(key, value); err != nil {
			t.Fatal(err)
		}
	}
	file, err := form.CreateFormFile("file", "policy.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("binary-pdf-fixture")); err != nil {
		t.Fatal(err)
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/knowledge", &body)
	request.Header.Set("Content-Type", form.FormDataContentType())
	sessionID, err := handler.store.Create(context.Background(), identity.SessionUser{PlatformUserID: "console-admin", IsSystemAdmin: true, Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	const csrf = "knowledge-upload-csrf"
	request.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	request.AddCookie(&http.Cookie{Name: "csrf_token", Value: csrf})
	request.Header.Set("X-CSRF-Token", csrf)
	recorder := httptest.NewRecorder()
	handler.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("upload status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if queue.request.TenantID != "example" || queue.request.AppCode != "support" || queue.request.DocumentID != "policy" || queue.request.Filename != "policy.pdf" || string(queue.request.Data) != "binary-pdf-fixture" {
		t.Fatalf("queued request = %#v", queue.request)
	}
}

func TestConsoleAcceptsActiveTenantHeaderWithCookieAuth(t *testing.T) {
	console := testConsoleHandler(t)
	user := identity.SessionUser{
		PlatformUserID: "tenant-admin",
		Tenants: []identity.TenantRole{
			{TenantID: "example", Role: identity.RoleAdmin, Status: "active"},
		},
	}
	sessionID, err := console.store.Create(context.Background(), user, time.Hour)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	// 1. Cookie auth + X-Active-Tenant header (no ?tenant= query parameter)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
	req.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	req.Header.Set("X-Active-Tenant", "example")
	rec := httptest.NewRecorder()
	console.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with cookie auth and X-Active-Tenant: %s", rec.Code, rec.Body.String())
	}

	// 2. Cookie auth + X-Active-Tenant for unassigned tenant -> 403 Forbidden
	reqForbidden := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
	reqForbidden.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	reqForbidden.Header.Set("X-Active-Tenant", "unassigned-tenant")
	recForbidden := httptest.NewRecorder()
	console.handler.ServeHTTP(recForbidden, reqForbidden)

	if recForbidden.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for unassigned tenant in X-Active-Tenant", recForbidden.Code)
	}

	// 3. listApplications filtered by X-Active-Tenant
	reqApps := httptest.NewRequest(http.MethodGet, "/api/v1/apps", nil)
	reqApps.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	reqApps.Header.Set("X-Active-Tenant", "example")
	recApps := httptest.NewRecorder()
	console.handler.ServeHTTP(recApps, reqApps)

	if recApps.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for listApplications with X-Active-Tenant", recApps.Code)
	}
	var appsBody struct {
		Applications []tenant.Snapshot `json:"applications"`
	}
	if err := json.Unmarshal(recApps.Body.Bytes(), &appsBody); err != nil {
		t.Fatalf("decode apps: %v", err)
	}
	if len(appsBody.Applications) != 1 || appsBody.Applications[0].Config.TenantID != "example" {
		t.Fatalf("apps count = %d, want 1 for tenant example", len(appsBody.Applications))
	}
}
