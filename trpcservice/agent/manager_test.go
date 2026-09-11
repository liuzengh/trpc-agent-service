package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestMemoryServiceExposesExactlyTenantAllowedTools(t *testing.T) {
	tenantConfig := config.TenantConfig{
		TenantID: "tenant-a",
		Tools: config.ToolPolicy{
			Allow: []string{"memory_search", "memory_clear"},
			Deny:  []string{"memory_add"},
		},
		Data: config.DataConfig{Memory: config.BackendConfig{Type: "inmemory"}},
	}
	service, err := buildMemoryService(tenantConfig, "app")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	got := map[string]bool{}
	for _, memoryTool := range service.Tools() {
		got[memoryTool.Declaration().Name] = true
	}
	if len(got) != 2 || !got["memory_search"] || !got["memory_clear"] {
		t.Fatalf("memory tool surface = %v", got)
	}
}

func TestExternalMem0BackendWiresReaderIngestorAndAllowedTools(t *testing.T) {
	t.Setenv("TEST_MEM0_API_KEY", "not-logged")
	tenantConfig := config.TenantConfig{
		TenantID: "tenant-a",
		Tools:    config.ToolPolicy{Allow: []string{"memory_search"}},
		Data: config.DataConfig{Memory: config.BackendConfig{
			Type: "external", Provider: "mem0", Endpoint: "https://memory.example.test",
			Mode: "cloud", APIKeyEnv: "TEST_MEM0_API_KEY",
		}},
	}
	backend, err := buildMemoryBackend(tenantConfig, "tenant-a:assistant")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if backend.Service != nil || backend.Reader == nil || backend.Ingestor == nil {
		t.Fatalf("external memory capabilities = service:%T reader:%T ingestor:%T", backend.Service, backend.Reader, backend.Ingestor)
	}
	if names := ToolNames(backend.Tools); len(names) != 1 || names[0] != "memory_search" {
		t.Fatalf("external memory tools = %v", names)
	}
}

func TestObjectArtifactBackendRejectsUnavailableMetadataWithoutLeakingDSN(t *testing.T) {
	t.Setenv("TEST_S3_ACCESS", "access")
	t.Setenv("TEST_S3_SECRET", "secret")
	const credential = "artifact-canary-password"
	t.Setenv("TEST_ARTIFACT_DSN", "postgres://user:"+credential+"@localhost/%zz")
	tenantConfig := config.TenantConfig{TenantID: "tenant-a", Data: config.DataConfig{Artifact: config.BackendConfig{
		Type: "object", Provider: "s3", Bucket: "artifacts", Endpoint: "http://127.0.0.1:1",
		DSNEnv: "TEST_ARTIFACT_DSN",
		Region: "test", PathStyle: true, AccessKeyEnv: "TEST_S3_ACCESS", SecretKeyEnv: "TEST_S3_SECRET",
	}}}
	_, _, err := buildArtifactService(context.Background(), tenantConfig, "tenant-a:assistant")
	if err == nil || strings.Contains(err.Error(), credential) {
		t.Fatalf("object artifact error = %v", err)
	}
}

func TestManagerReusesInterleavedTenantRevisions(t *testing.T) {
	manager := NewManager()
	defer manager.Close()
	base := config.TenantConfig{
		TenantID: "tenant-a", Version: "v1", Enabled: true,
		App:   config.AppConfig{Name: "assistant", AgentName: "agent"},
		Model: config.ModelConfig{Provider: "mock", Name: "mock"},
		Data: config.DataConfig{
			Session: config.BackendConfig{Type: "inmemory"},
			Memory:  config.BackendConfig{Type: "disabled"},
		},
	}
	v1, releaseV1, err := manager.Acquire(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	releaseV1()
	v2Config := base
	v2Config.Version = "v2"
	v2, releaseV2, err := manager.Acquire(context.Background(), v2Config)
	if err != nil {
		t.Fatal(err)
	}
	releaseV2()
	if v1 == v2 {
		t.Fatal("different revisions unexpectedly shared a runtime")
	}
	v1Again, releaseAgain, err := manager.Acquire(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	releaseAgain()
	if v1Again != v1 {
		t.Fatal("interleaved historical revision was rebuilt instead of reused")
	}
}

func TestRedisBackendConstructionErrorDoesNotLeakDSN(t *testing.T) {
	const (
		envName    = "TEST_PLATFORM_REDIS_DSN"
		credential = "canary-password"
	)
	t.Setenv(envName, "redis://user:"+credential+"@localhost/%zz")
	tenantConfig := config.TenantConfig{
		TenantID: "tenant-a",
		Data: config.DataConfig{
			Session: config.BackendConfig{Type: "redis", DSNEnv: envName},
			Memory:  config.BackendConfig{Type: "redis", DSNEnv: envName},
		},
	}
	if _, err := buildSessionService(context.Background(), tenantConfig, "app", nil); err == nil || strings.Contains(err.Error(), credential) {
		t.Fatalf("session backend error = %v", err)
	}
	if _, err := buildMemoryService(tenantConfig, "app"); err == nil || strings.Contains(err.Error(), credential) {
		t.Fatalf("memory backend error = %v", err)
	}
}

func TestSQLSessionBackendRequiresConfiguredDSNEnv(t *testing.T) {
	tests := []struct {
		name    string
		dsnEnv  string
		wantEnv string
	}{
		{name: "empty reference", wantEnv: "<unset>"},
		{name: "missing value", dsnEnv: "TEST_PLATFORM_MISSING_SQL_DSN", wantEnv: "TEST_PLATFORM_MISSING_SQL_DSN"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.dsnEnv != "" {
				t.Setenv(test.dsnEnv, "")
			}
			tenantConfig := config.TenantConfig{Data: config.DataConfig{
				Session: config.BackendConfig{Type: "sql", DSNEnv: test.dsnEnv},
			}}
			_, err := buildSessionService(context.Background(), tenantConfig, "app", nil)
			if err == nil || !strings.Contains(err.Error(), test.wantEnv) {
				t.Fatalf("sql session backend error = %v, want safe env label %q", err, test.wantEnv)
			}
		})
	}
}

func TestSQLSessionBackendConnectionErrorDoesNotLeakDSN(t *testing.T) {
	const (
		envName    = "TEST_PLATFORM_SQL_DSN"
		credential = "canary-sql-password"
	)
	// Port 1 is expected to refuse immediately. The short context is a guard
	// against unusual local networking without requiring a PostgreSQL fixture.
	t.Setenv(envName, "postgres://user:"+credential+"@127.0.0.1:1/database?connect_timeout=1")
	tenantConfig := config.TenantConfig{Data: config.DataConfig{
		Session: config.BackendConfig{Type: "sql", DSNEnv: envName},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := buildSessionService(ctx, tenantConfig, "app", nil)
	if err == nil {
		t.Fatal("unavailable sql session backend unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), credential) || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("sql session backend error leaked DSN: %v", err)
	}
	if !strings.Contains(err.Error(), envName) {
		t.Fatalf("sql session backend error omitted safe env name: %v", err)
	}
}

func TestBuildRuntimeWiresSQLTurnSession(t *testing.T) {
	dsn, ok := os.LookupEnv("TEST_POSTGRES_DSN")
	if !ok || strings.TrimSpace(dsn) == "" {
		t.Skip("TEST_POSTGRES_DSN is not configured")
	}
	const envName = "TEST_MANAGER_SQL_SESSION_DSN"
	t.Setenv(envName, dsn)
	tenantConfig := config.TenantConfig{
		TenantID: "tenant-sql", Version: "v1", Enabled: true,
		App:   config.AppConfig{Name: "assistant", AgentName: "agent"},
		Model: config.ModelConfig{Provider: "mock", Name: "mock"},
		Data: config.DataConfig{
			Session: config.BackendConfig{Type: "sql", DSNEnv: envName},
			Memory:  config.BackendConfig{Type: "disabled"},
			Summary: config.BackendConfig{Type: "disabled"},
		},
	}
	runtime, err := buildRuntime(context.Background(), tenantConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Session == nil || runtime.TurnSession == nil {
		_ = runtime.close()
		t.Fatalf("sql runtime capabilities: session=%T turn=%T", runtime.Session, runtime.TurnSession)
	}
	if err := runtime.close(); err != nil {
		t.Fatalf("close sql runtime: %v", err)
	}
}

func TestSafeEnvReferenceDoesNotEchoInvalidValue(t *testing.T) {
	const literal = "postgres://user:canary-password@localhost/db"
	if got := safeEnvReference(literal); strings.Contains(got, "canary") || got != "dsn_env <invalid>" {
		t.Fatalf("safe env label = %q", got)
	}
	tenantConfig := config.TenantConfig{Data: config.DataConfig{
		Session: config.BackendConfig{Type: "sql", DSNEnv: literal},
	}}
	_, err := buildSessionService(context.Background(), tenantConfig, "app", nil)
	if err == nil || strings.Contains(err.Error(), "canary") || strings.Contains(err.Error(), literal) {
		t.Fatalf("invalid sql dsn_env error = %v", err)
	}
}

func TestSkillRepositoryResolution(t *testing.T) {
	if repo := NewSkillRepository(""); repo != nil {
		t.Fatal("empty root unexpectedly enabled skills")
	}
	if repo := NewSkillRepository("  "); repo != nil {
		t.Fatal("blank root unexpectedly enabled skills")
	}
	// A readable but empty root yields an enabled repository with no skills;
	// the platform treats repository presence and skill presence separately.
	if repo := NewSkillRepository(t.TempDir()); repo == nil || len(repo.Summaries()) != 0 {
		t.Fatalf("empty skills root = %v, summaries = %d", repo, len(repo.Summaries()))
	}
	repo := NewSkillRepository("../skill/skills")
	if repo == nil {
		t.Fatal("built-in skills repository not resolved")
	}
	names := map[string]bool{}
	for _, summary := range repo.Summaries() {
		names[summary.Name] = true
	}
	if !names["greeting"] {
		t.Fatalf("built-in skills missing greeting: %v", names)
	}
}

func TestBuildRuntimeWiresSkillsForOpenAIProvider(t *testing.T) {
	t.Setenv("TEST_PLATFORM_LLM_KEY", "test-key")
	tenantConfig := config.TenantConfig{
		TenantID: "tenant-skills", Version: "v1", Enabled: true,
		App:   config.AppConfig{Name: "assistant", AgentName: "agent", Instruction: "test"},
		Model: config.ModelConfig{Provider: "openai", Name: "qwen3.7-max-preview", Variant: "qwen", APIKeyEnv: "TEST_PLATFORM_LLM_KEY", Temperature: 0.3},
		Data: config.DataConfig{
			Session: config.BackendConfig{Type: "inmemory"},
			Memory:  config.BackendConfig{Type: "disabled"},
		},
	}
	runtime, err := buildRuntime(context.Background(), tenantConfig, NewSkillRepository("../skill/skills"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.close() }()
	if runtime.Runner == nil {
		t.Fatal("runtime runner is nil")
	}
	if runtime.TurnSession != nil {
		t.Fatal("in-memory session unexpectedly exposed strict turn capability")
	}
	// Skills stay optional: a runtime without a repository must still build.
	bare, err := buildRuntime(context.Background(), tenantConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := bare.close(); err != nil {
		t.Fatal(err)
	}
}

func TestConfiguredModelFallsBackToMockBeforePrimaryResponse(t *testing.T) {
	const apiKey = "test-dashscope-key"
	t.Setenv("TEST_DASHSCOPE_KEY", apiKey)
	var gotAuthorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"primary unavailable","type":"invalid_request_error"}}`))
	}))
	defer server.Close()

	configured, err := buildConfiguredModel(config.ModelConfig{
		Provider: "openai", Name: "qwen-primary", Variant: "qwen",
		BaseURL: server.URL + "/v1", APIKeyEnv: "TEST_DASHSCOPE_KEY",
		FallbackProvider: "mock",
	})
	if err != nil {
		t.Fatal(err)
	}
	if configured.Info().Name != "qwen-primary" {
		t.Fatalf("primary model priority lost: info=%+v", configured.Info())
	}
	responses, err := configured.GenerateContent(context.Background(), model.NewRequest([]model.Message{
		model.NewUserMessage("hello fallback"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	var content string
	for response := range responses {
		if response.Error != nil {
			t.Fatalf("fallback returned error: %+v", response.Error)
		}
		if len(response.Choices) > 0 {
			content += response.Choices[0].Message.Content
		}
	}
	if !strings.Contains(content, "[mock fallback]") || !strings.Contains(content, "hello fallback") {
		t.Fatalf("unexpected fallback content: %q", content)
	}
	if gotAuthorization != "Bearer "+apiKey {
		t.Fatal("primary model was not attempted before fallback")
	}
}

func TestConfiguredModelUsesMockFallbackWhenPrimaryKeyIsMissing(t *testing.T) {
	t.Setenv("TEST_MISSING_PRIMARY_KEY", "")
	configured, err := buildConfiguredModel(config.ModelConfig{
		Provider: "openai", Name: "qwen-primary", APIKeyEnv: "TEST_MISSING_PRIMARY_KEY",
		FallbackProvider: "mock",
	})
	if err != nil {
		t.Fatal(err)
	}
	if configured.Info().Name != "mock-fallback" {
		t.Fatalf("missing primary key did not select mock fallback: %+v", configured.Info())
	}
}
