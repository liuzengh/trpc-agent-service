package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagemigration"
)

func newSecuredHandler(t *testing.T, store repository.Store, auditStore audit.Store, tokens string) http.Handler {
	t.Helper()
	credentials, err := ParseCredentials(tokens)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := NewAuthenticator(credentials)
	if err != nil {
		t.Fatal(err)
	}
	var options []Option
	if auditStore != nil {
		options = append(options, WithAudit(auditStore))
	}
	service, err := NewService(store, options...)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	return authenticator.Wrap(handler)
}

func migrationYAML(id string) []byte {
	return []byte(fmt.Sprintf(`schema_version: 1
tenants:
- tenant_id: %s
  name: %s
  enabled: true
  config_version: 1
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: true
    config: {instruction: Help the user.}
    model: {provider: mock, name: mock}
    tools: {allow: [echo], deny: [], require_approval: []}
    channels: [{binding_id: http, type: http, provider_account_id: local, enabled: true}]
    storage:
      session: &session_route
        type: postgres
        endpoint: postgres://source.example/runtime
        credential: {provider: env, key: SOURCE_DATABASE_DSN}
        migration_target: &target_route
          type: postgres
          endpoint: postgres://target.example/runtime
          credential: {provider: env, key: TARGET_DATABASE_DSN}
      memory: {type: postgres}
      summary: *session_route
      artifact: {type: postgres}
      knowledge: {type: postgres}
      audit: {type: postgres}
`, id, id))
}

func TestStorageMigrationAPIIsAuthenticatedTenantScopedAndRedacted(t *testing.T) {
	configStore := repository.NewMemoryStore()
	migrationStore := storagemigration.NewMemoryStore()
	service, err := NewService(configStore, WithMigrationStore(migrationStore))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Publish(WithActor(context.Background(), "seed"), "tenant-a", 0, migrationYAML("tenant-a")); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, _ := NewAuthenticator([]Credential{{Name: "ops", Token: "token", Tenants: map[string]bool{"tenant-a": true}}})
	secured := authenticator.Wrap(handler)
	body := []byte(`{"app_id":"assistant","domain":"session","expected_version":1}`)
	if response := call(secured, http.MethodPost, "/v1/tenants/tenant-a/storage/migrations", "", body); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated=%d", response.Code)
	}
	if response := call(secured, http.MethodPost, "/v1/tenants/tenant-b/storage/migrations", "token", body); response.Code != http.StatusForbidden {
		t.Fatalf("cross tenant=%d", response.Code)
	}
	response := call(secured, http.MethodPost, "/v1/tenants/tenant-a/storage/migrations", "token", body)
	if response.Code != http.StatusCreated {
		t.Fatalf("plan=%d %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "SOURCE_DATABASE_DSN") || strings.Contains(response.Body.String(), "TARGET_DATABASE_DSN") {
		t.Fatalf("response leaked route secret refs: %s", response.Body.String())
	}
	var created map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	jobID, _ := created["migration_id"].(string)
	if jobID == "" {
		t.Fatal("missing migration_id")
	}
	if duplicate := call(secured, http.MethodPost, "/v1/tenants/tenant-a/storage/migrations", "token", body); duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate=%d %s", duplicate.Code, duplicate.Body.String())
	}
	if listed := call(secured, http.MethodGet, "/v1/tenants/tenant-a/storage/migrations", "token", nil); listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), jobID) {
		t.Fatalf("list=%d %s", listed.Code, listed.Body.String())
	}
	if canceled := call(secured, http.MethodPost, "/v1/tenants/tenant-a/storage/migrations/"+jobID+"/cancel", "token", nil); canceled.Code != http.StatusOK {
		t.Fatalf("cancel=%d %s", canceled.Code, canceled.Body.String())
	}
}

func call(handler http.Handler, method, path, token string, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(string(body)))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestAdminAPIRequiresAuthentication(t *testing.T) {
	handler := newSecuredHandler(t, repository.NewMemoryStore(), nil, "ops=secret-token:*")
	for _, token := range []string{"", "wrong-token"} {
		for _, path := range []string{"/v1/tenants/tenant-a/configs", "/v1/tenants/tenant-a/configs/current", "/v1/tenants/tenant-a/apps/assistant/knowledge/search"} {
			if response := call(handler, http.MethodGet, path, token, nil); response.Code != http.StatusUnauthorized {
				t.Fatalf("token=%q path=%s code=%d", token, path, response.Code)
			}
		}
		if response := call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=0", token, tenantYAML("tenant-a", 1)); response.Code != http.StatusUnauthorized {
			t.Fatalf("publish token=%q code=%d", token, response.Code)
		}
	}
}

func TestAdminRolesRestrictWrites(t *testing.T) {
	viewer, err := ParseCredentials("read=token:tenant-a|viewer")
	if err != nil || len(viewer) != 1 || viewer[0].Role != RoleViewer {
		t.Fatalf("viewer credentials=%+v err=%v", viewer, err)
	}
	handler := newSecuredHandler(t, repository.NewMemoryStore(), nil, "read=token:tenant-a|viewer")
	if response := call(handler, http.MethodGet, "/v1/tenants/tenant-a/configs", "token", nil); response.Code != http.StatusOK {
		t.Fatalf("viewer read=%d", response.Code)
	}
	if response := call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/rollback?expected_version=1&target_version=1", "token", nil); response.Code != http.StatusForbidden {
		t.Fatalf("viewer write=%d", response.Code)
	}
}

func TestAdminAPITenantScopeEnforcement(t *testing.T) {
	handler := newSecuredHandler(t, repository.NewMemoryStore(), nil, "alice=token-a:tenant-a;root=root-token:*")
	// alice may publish to tenant-a.
	if response := call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=0", "token-a", tenantYAML("tenant-a", 1)); response.Code != http.StatusCreated {
		t.Fatalf("in-scope publish=%d %s", response.Code, response.Body.String())
	}
	// alice may not read or publish tenant-b, even though she knows its id.
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		path := "/v1/tenants/tenant-b/configs"
		var body []byte
		if method == http.MethodPost {
			path += "/publish?expected_version=0"
			body = tenantYAML("tenant-b", 1)
		}
		if response := call(handler, method, path, "token-a", body); response.Code != http.StatusForbidden {
			t.Fatalf("cross-scope %s=%d %s", method, response.Code, response.Body.String())
		}
	}
	if response := call(handler, http.MethodPost, "/v1/tenants/tenant-b/apps/assistant/knowledge/search", "token-a", []byte(`{"query":"secret"}`)); response.Code != http.StatusForbidden {
		t.Fatalf("cross-scope knowledge=%d %s", response.Code, response.Body.String())
	}
	// The URL tenant wins over any client-supplied payload tenant.
	if response := call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "token-a", tenantYAML("tenant-b", 2)); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("payload scope=%d %s", response.Code, response.Body.String())
	}
	// root reaches both tenants but never reads across them.
	if response := call(handler, http.MethodGet, "/v1/tenants/tenant-b/configs", "root-token", nil); response.Code != http.StatusOK || response.Body.String() != "[]\n" {
		t.Fatalf("root tenant-b list=%d %s", response.Code, response.Body.String())
	}
	if response := call(handler, http.MethodGet, "/v1/tenants/tenant-a/configs/current", "root-token", nil); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"version":1`) {
		t.Fatalf("current=%d %s", response.Code, response.Body.String())
	}
}

func TestAdminAPIConcurrentPublishSingleWinner(t *testing.T) {
	store := repository.NewMemoryStore()
	handler := newSecuredHandler(t, store, nil, "ops=token:*")
	if response := call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=0", "token", tenantYAML("tenant-a", 1)); response.Code != http.StatusCreated {
		t.Fatalf("seed=%d %s", response.Code, response.Body.String())
	}
	const racers = 8
	codes := make(chan int, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response := call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "token", tenantYAML("tenant-a", 2))
			codes <- response.Code
		}()
	}
	wg.Wait()
	close(codes)
	created, conflicted := 0, 0
	for code := range codes {
		switch code {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicted++
		default:
			t.Fatalf("unexpected code %d", code)
		}
	}
	if created != 1 || conflicted != racers-1 {
		t.Fatalf("created=%d conflicted=%d", created, conflicted)
	}
	if response := call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "token", tenantYAML("tenant-a", 2)); response.Code != http.StatusConflict {
		t.Fatalf("stale expected_version=%d", response.Code)
	}
}

func TestAdminAPIRollbackCreatesImmutableVersion(t *testing.T) {
	handler := newSecuredHandler(t, repository.NewMemoryStore(), nil, "ops=token:*")
	call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=0", "token", tenantYAML("tenant-a", 1))
	call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "token", tenantYAML("tenant-a", 2))
	response := call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/rollback?expected_version=2&target_version=1", "token", nil)
	if response.Code != http.StatusCreated {
		t.Fatalf("rollback=%d %s", response.Code, response.Body.String())
	}
	var rolled map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &rolled); err != nil {
		t.Fatal(err)
	}
	if rolled["version"] != float64(3) || rolled["rollback_of"] != float64(1) {
		t.Fatalf("rollback metadata=%v", rolled)
	}
	if rolled["created_by"] != "ops" {
		t.Fatalf("created_by=%v", rolled["created_by"])
	}
	response = call(handler, http.MethodGet, "/v1/tenants/tenant-a/configs", "token", nil)
	var versions []map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &versions); err != nil {
		t.Fatal(err)
	}
	if len(versions) != 3 {
		t.Fatalf("history=%v", versions)
	}
	// History is immutable: v1 still hashes its original payload.
	if versions[2]["version"] != float64(1) || versions[2]["content_hash"] == versions[0]["content_hash"] {
		t.Fatalf("history mutated=%v", versions)
	}
}

func TestAdminAPINeverLeaksSecretMaterial(t *testing.T) {
	const secretValue = "sk-live-deepseek-9f8e7d6c5b"
	t.Setenv("PR9_MODEL_KEY", secretValue)
	payload := []byte(fmt.Sprintf(`schema_version: 1
tenants:
- tenant_id: tenant-a
  name: tenant-a
  enabled: true
  config_version: 1
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: true
    config: {instruction: Help the user.}
    model:
      provider: deepseek
      name: deepseek-chat
      api_key: {provider: env, key: PR9_MODEL_KEY}
    tools: {allow: [echo], deny: [], require_approval: []}
    channels:
    - {binding_id: http, type: http, provider_account_id: local, enabled: true}
    storage:
      session: {type: inmemory}
      memory: {type: inmemory}
      summary: {type: inmemory}
      artifact: {type: inmemory}
      knowledge: {type: inmemory}
      audit: {type: inmemory}
`))
	handler := newSecuredHandler(t, repository.NewMemoryStore(), nil, "ops=token:*")
	responses := []*httptest.ResponseRecorder{
		call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/validate", "token", payload),
		call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=0", "token", payload),
		call(handler, http.MethodGet, "/v1/tenants/tenant-a/configs", "token", nil),
		call(handler, http.MethodGet, "/v1/tenants/tenant-a/configs/current", "token", nil),
		// An error path: the payload embeds a credential-looking literal and is
		// invalid YAML, so the parser error must not echo secret material.
		call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/validate", "token", []byte("api_key: "+secretValue+"\nbroken: [")),
	}
	for i, response := range responses {
		if strings.Contains(response.Body.String(), secretValue) {
			t.Fatalf("response %d leaks secret: %s", i, response.Body.String())
		}
	}
	if responses[1].Code != http.StatusCreated {
		t.Fatalf("publish=%d %s", responses[1].Code, responses[1].Body.String())
	}
	// The API projection must not expose the stored payload at all.
	for _, field := range []string{"config_yaml", "payload", "api_key"} {
		if strings.Contains(responses[2].Body.String(), field) {
			t.Fatalf("list response exposes %q: %s", field, responses[2].Body.String())
		}
	}
}

func TestAdminAPIPublishAndRollbackAudit(t *testing.T) {
	auditStore := audit.NewMemoryStore(nil)
	handler := newSecuredHandler(t, repository.NewMemoryStore(), auditStore, "ops=token:*")
	call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=0", "token", tenantYAML("tenant-a", 1))
	call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "token", tenantYAML("tenant-a", 2))
	call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/rollback?expected_version=2&target_version=1", "token", nil)
	call(handler, http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=99", "token", tenantYAML("tenant-a", 4))

	records := auditStore.Records("tenant-a")
	if len(records) != 4 {
		t.Fatalf("audit records=%d", len(records))
	}
	for i, record := range records {
		if record.TraceID == "" || record.CreatedAt.IsZero() {
			t.Fatalf("record %d missing trace/timestamp: %+v", i, record)
		}
		if record.Details["actor"] != "ops" {
			t.Fatalf("record %d actor=%v", i, record.Details["actor"])
		}
	}
	if records[0].Details["action"] != "config.publish" || records[0].Decision != "allow" || records[0].Details["new_version"] != uint64(1) {
		t.Fatalf("publish audit=%+v", records[0])
	}
	if records[2].Details["action"] != "config.rollback" || records[2].Details["new_version"] != uint64(3) {
		t.Fatalf("rollback audit=%+v", records[2])
	}
	if records[3].Decision != "error" || records[3].ErrorType == "" {
		t.Fatalf("conflict audit=%+v", records[3])
	}
}

func TestParseCredentialsRejectsMalformed(t *testing.T) {
	if _, err := ParseCredentials("missing-equals"); err == nil {
		t.Fatal("expected parse error")
	}
	if _, err := ParseCredentials("=:token:*"); err == nil {
		t.Fatal("expected empty name error")
	}
	credentials, err := ParseCredentials("alice=token-a:tenant-a, tenant-b ; root=root:*")
	if err != nil || len(credentials) != 2 {
		t.Fatalf("credentials=%v err=%v", credentials, err)
	}
	if !credentials[0].Allows("tenant-b") || credentials[0].Allows("tenant-c") || !credentials[1].Allows("anything") {
		t.Fatalf("scope=%+v", credentials)
	}
	// An empty token list rejects everything instead of failing open.
	authenticator, err := NewAuthenticator(nil)
	if err != nil {
		t.Fatal(err)
	}
	service, _ := NewService(repository.NewMemoryStore())
	handler, _ := NewHandler(service)
	if response := call(authenticator.Wrap(handler), http.MethodGet, "/v1/tenants/tenant-a/configs", "any", nil); response.Code != http.StatusUnauthorized {
		t.Fatalf("unconfigured admin API code=%d", response.Code)
	}
}

func TestWithActorStampsCreatedBy(t *testing.T) {
	store := repository.NewMemoryStore()
	service, _ := NewService(store)
	ctx := WithActor(context.Background(), "bootstrap")
	record, err := service.Publish(ctx, "tenant-a", 0, tenantYAML("tenant-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	if record.CreatedBy != "bootstrap" {
		t.Fatalf("created_by=%q", record.CreatedBy)
	}
	current, err := service.Current(ctx, "tenant-a")
	if err != nil || current.Version != 1 || current.CreatedBy != "bootstrap" {
		t.Fatalf("current=%+v err=%v", current, err)
	}
}
