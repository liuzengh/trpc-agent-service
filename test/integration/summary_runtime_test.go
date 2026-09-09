//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	openaiopt "github.com/openai/openai-go/option"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summaryruntime"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

type integrationSummaryTenantReader struct{ value *tenant.Tenant }

func (r integrationSummaryTenantReader) GetTenant(context.Context, string) (*tenant.Tenant, error) {
	return r.value, nil
}

type integrationSummaryVersionReader struct{ value *controlplane.ResolvedVersion }

func (r integrationSummaryVersionReader) LoadVersion(context.Context, string, string, string) (*controlplane.ResolvedVersion, error) {
	return r.value, nil
}

type integrationSummaryServices struct{ sessions session.Service }

func (s integrationSummaryServices) AcquireServices(context.Context, *tenant.Tenant) (session.Service, memory.Service, func(), error) {
	return s.sessions, memoryinmemory.NewMemoryService(), func() {}, nil
}

func TestSummaryRuntimePostgresRedisEndToEnd(t *testing.T) {
	db := openDatabase(t)
	ctx := context.Background()
	rawRedisURL := os.Getenv("TEST_REDIS_URL")
	if rawRedisURL == "" {
		t.Fatal("TEST_REDIS_URL is required for integration-tag tests")
	}
	v8Options, err := redisv8.ParseURL(rawRedisURL)
	if err != nil {
		t.Fatalf("parse TEST_REDIS_URL for lease and budget client: %v", err)
	}
	redisClient := redisv8.NewClient(v8Options)
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping real Redis summary backend: %v", err)
	}

	suffix := uuid.NewString()
	tenantID := "summary-e2e-" + suffix
	appID := "app-" + suffix
	versionID := "version-" + suffix
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,$2,'active','{}')`, tenantID, "summary e2e"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_apps(id,tenant_id,name,status) VALUES($1,$2,'support','active')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_versions(id,agent_app_id,version_number,config_snapshot,config_hash,status,created_by)
		VALUES($1,$2,1,'{}',$3,'published','integration')`, versionID, appID, fmt.Sprintf("%064x", 1)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM summary_checkpoints WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM summary_jobs WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM agent_versions WHERE id=$1`, versionID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM agent_apps WHERE id=$1`, appID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	sessionService, err := sessionredis.NewService(
		sessionredis.WithRedisClientURL(rawRedisURL),
		sessionredis.WithKeyPrefix("summary-e2e-"+suffix),
	)
	if err != nil {
		t.Fatalf("open real Redis Session service: %v", err)
	}
	t.Cleanup(func() { _ = sessionService.Close() })
	tenantValue := &tenant.Tenant{
		ID: tenantID, Status: tenant.TenantStatusActive,
		Storage: tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"},
	}
	physicalApp, err := storage.TenantScopedAppName(tenantValue, "support")
	if err != nil {
		t.Fatal(err)
	}
	sessionKey := session.Key{AppName: physicalApp, UserID: "owner-1", SessionID: "session-1"}
	incarnationID := uuid.NewString()
	stored, err := sessionService.CreateSession(ctx, sessionKey, session.StateMap{storage.SessionIncarnationStateKey: []byte(incarnationID)})
	if err != nil {
		t.Fatalf("create real Redis Session: %v", err)
	}
	firstAt := time.Date(2026, time.August, 31, 1, 0, 0, 0, time.UTC)
	for index, content := range []string{"need invoice", "which order", "order 42", "we will email it"} {
		role := model.RoleUser
		if index%2 == 1 {
			role = model.RoleAssistant
		}
		timestamp := firstAt.Add(time.Duration(index) * time.Second)
		if index == 3 {
			timestamp = firstAt.Add(2 * time.Second) // exact event ID disambiguates the shared timestamp.
		}
		item := &event.Event{
			ID: fmt.Sprintf("event-%d", index+1), Timestamp: timestamp,
			Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: role, Content: content}}}},
		}
		if err := sessionService.AppendEvent(ctx, stored, item); err != nil {
			t.Fatalf("append Redis Session event %d: %v", index+1, err)
		}
	}

	resolvedVersion := &controlplane.ResolvedVersion{
		TenantID: tenantID, AgentAppID: appID, AgentAppName: "support",
		VersionID: versionID, VersionNumber: 1,
		Snapshot: controlplane.VersionSnapshot{
			Agent: tenant.AgentConfig{Name: "support", DefaultModel: "summary-model"},
			Model: tenant.ModelConfig{Provider: "openai", ModelName: "summary-model"},
		},
	}
	var modelCalls atomic.Int32
	var requestMu sync.Mutex
	var capturedRequest struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content any    `json:"content"`
		} `json:"messages"`
	}
	fakeProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		modelCalls.Add(1)
		if request.Method != http.MethodPost || request.URL.Path != "/chat/completions" ||
			request.Header.Get("Authorization") != "Bearer test-only-summary-key" {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		var decoded struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20)).Decode(&decoded); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		requestMu.Lock()
		capturedRequest = decoded
		requestMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"summary-response","object":"chat.completion","created":1788140000,"model":"summary-model",
			"choices":[{"index":0,"message":{"role":"assistant","content":"customer asked about invoice; support explained the next step"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":10,"completion_tokens":7,"total_tokens":17}
		}`))
	}))
	t.Cleanup(fakeProvider.Close)
	sink := summarycoord.NewPostgresSink(db)
	runtime, err := summaryruntime.New(summaryruntime.RuntimeOptions{
		Tenants:  integrationSummaryTenantReader{value: tenantValue},
		Versions: integrationSummaryVersionReader{value: resolvedVersion},
		Services: integrationSummaryServices{sessions: sessionService},
		Redis:    redisClient, Checkpoints: sink, MinEvents: 1,
		MaxSummaryWords: 128, MaxOutputTokens: 256, SessionLockTTL: 5 * time.Second,
		ModelBuilder: func(_ context.Context, config tenant.ModelConfig, _ *tenant.Tenant, _ tenant.SecretResolver) (model.Model, error) {
			// Inject the external provider at the runtime's existing model
			// boundary. Tenant endpoint policy remains unchanged.
			return openai.New(config.ModelName, openai.WithAPIKey("test-only-summary-key"),
				openai.WithBaseURL(fakeProvider.URL),
				openai.WithOpenAIOptions(openaiopt.WithMaxRetries(0), openaiopt.WithHTTPClient(fakeProvider.Client()))), nil
		},
	})
	if err != nil {
		t.Fatalf("build production Summary runtime: %v", err)
	}
	store := summarycoord.NewPostgresStore(db)
	enqueued, err := store.Enqueue(ctx, summarycoord.EnqueueRequest{
		Key: summarycoord.Key{
			TenantID: tenantID, AgentAppID: appID, SessionOwnerID: sessionKey.UserID, SessionID: sessionKey.SessionID,
			SessionIncarnationID: incarnationID,
		},
		AgentVersionID: versionID, TargetEventSequence: 0,
	})
	if err != nil {
		t.Fatalf("enqueue deferred Summary target: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	pollers := make([]*summarycoord.Poller, 0, 2)
	done := make([]chan error, 0, 2)
	for _, owner := range []string{"summary-replica-a", "summary-replica-b"} {
		poller, err := summarycoord.NewPoller(summarycoord.PollerConfig{
			OwnerPrefix: owner, Concurrency: 1, PollInterval: 10 * time.Millisecond,
			LeaseTTL: 5 * time.Second, JobTimeout: 10 * time.Second,
			Store: store, Sink: sink, Generator: runtime, TargetResolver: runtime,
		})
		if err != nil {
			t.Fatal(err)
		}
		pollers = append(pollers, poller)
		result := make(chan error, 1)
		done = append(done, result)
		go func() { result <- poller.Run(runCtx) }()
	}
	_ = pollers
	deadline := time.Now().Add(15 * time.Second)
	var completed summarycoord.Job
	for time.Now().Before(deadline) {
		completed, err = store.Get(ctx, enqueued.Job.ID)
		if err == nil && completed.Status == summarycoord.StatusCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancelRun()
	for _, result := range done {
		if err := <-result; err != nil {
			t.Fatalf("stop Summary poller: %v", err)
		}
	}
	if completed.Status != summarycoord.StatusCompleted || completed.TargetEventSequence != 4 || completed.CompletedEventSequence != 4 {
		t.Fatalf("completed Summary job = %#v", completed)
	}
	if modelCalls.Load() != 1 {
		t.Fatalf("fake OpenAI-compatible request count = %d, want exactly one fenced execution", modelCalls.Load())
	}
	requestMu.Lock()
	requestModel, requestMessages := capturedRequest.Model, len(capturedRequest.Messages)
	requestRole := ""
	var requestContent []byte
	if requestMessages > 0 {
		requestRole = capturedRequest.Messages[0].Role
		requestContent, _ = json.Marshal(capturedRequest.Messages[0].Content)
	}
	requestMu.Unlock()
	if requestModel != "summary-model" || requestMessages != 1 || requestRole != "user" ||
		!strings.Contains(string(requestContent), "invoice") {
		t.Fatalf("fake OpenAI-compatible request model=%q messages=%d role=%q content=%s",
			requestModel, requestMessages, requestRole, requestContent)
	}

	checkpoint, found, err := sink.Get(ctx, summarycoord.Key{
		TenantID: tenantID, AgentAppID: appID, SessionOwnerID: sessionKey.UserID, SessionID: sessionKey.SessionID,
		SessionIncarnationID: incarnationID,
	})
	if err != nil || !found {
		t.Fatalf("read durable Summary checkpoint: found=%v err=%v", found, err)
	}
	if checkpoint.EventSequence != 4 || checkpoint.LastEventID != "event-4" ||
		!checkpoint.CutoffAt.Equal(firstAt.Add(2*time.Second)) {
		t.Fatalf("durable Summary checkpoint = %#v", checkpoint)
	}
	raw, err := sessionService.GetSession(ctx, sessionKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw.Summaries) != 0 {
		t.Fatalf("source Redis Session was unexpectedly dual-written: %#v", raw.Summaries)
	}
	overlay, err := summarycoord.NewCheckpointSessionService(sessionService, sink, tenantID, appID, physicalApp)
	if err != nil {
		t.Fatal(err)
	}
	hydrated, err := overlay.GetSession(ctx, sessionKey)
	if err != nil {
		t.Fatalf("hydrate Runner-visible Summary: %v", err)
	}
	summaryValue := hydrated.Summaries[session.SummaryFilterKeyAllContents]
	if summaryValue == nil || summaryValue.Summary != checkpoint.Content || summaryValue.Boundary == nil ||
		summaryValue.Boundary.LastEventID != "event-4" || !summaryValue.Boundary.CutoffAt.Equal(checkpoint.CutoffAt) {
		t.Fatalf("Runner-visible Summary = %#v", summaryValue)
	}
	if text, ok := overlay.GetSessionSummaryText(ctx, hydrated); !ok || text != checkpoint.Content {
		t.Fatalf("GetSessionSummaryText() = %q, %v", text, ok)
	}

	// Prove the final consumer, not merely the overlay data shape: append two
	// events after the checkpoint, execute a real tRPC LLMAgent through Runner,
	// and inspect the exact OpenAI-compatible request. Events covered by the
	// summary must be absent while the summary, post-cutoff delta and current
	// user turn must all be present.
	for index, content := range []string{"post-cutoff question", "post-cutoff answer"} {
		role := model.RoleUser
		if index == 1 {
			role = model.RoleAssistant
		}
		item := &event.Event{
			ID:        fmt.Sprintf("event-%d", index+5),
			Timestamp: firstAt.Add(time.Duration(index+3) * time.Second),
			Response:  &model.Response{Choices: []model.Choice{{Message: model.Message{Role: role, Content: content}}}},
		}
		if err := sessionService.AppendEvent(ctx, stored, item); err != nil {
			t.Fatalf("append post-cutoff event %d: %v", index+5, err)
		}
	}

	var agentRequestMu sync.Mutex
	var agentRequest struct {
		Messages []struct {
			Role    string `json:"role"`
			Content any    `json:"content"`
		} `json:"messages"`
	}
	agentProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/chat/completions" {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		var decoded struct {
			Messages []struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20)).Decode(&decoded); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		agentRequestMu.Lock()
		agentRequest = decoded
		agentRequestMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"agent-response","object":"chat.completion","created":1788140001,"model":"agent-model",
			"choices":[{"index":0,"message":{"role":"assistant","content":"next turn completed"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":20,"completion_tokens":4,"total_tokens":24}
		}`))
	}))
	t.Cleanup(agentProvider.Close)
	agentModel := openai.New("agent-model", openai.WithAPIKey("test-only-agent-key"),
		openai.WithBaseURL(agentProvider.URL),
		openai.WithOpenAIOptions(openaiopt.WithMaxRetries(0), openaiopt.WithHTTPClient(agentProvider.Client())))
	agentValue := llmagent.New("support",
		llmagent.WithModel(agentModel),
		llmagent.WithAddSessionSummary(true),
		llmagent.WithMessageBranchFilterMode(llmagent.BranchFilterModeAll),
		llmagent.WithMaxLLMCalls(1),
	)
	runnerValue := runner.NewRunner(physicalApp, agentValue, runner.WithSessionService(overlay))
	t.Cleanup(func() { _ = runnerValue.Close() })
	events, err := runnerValue.Run(ctx, sessionKey.UserID, sessionKey.SessionID,
		model.Message{Role: model.RoleUser, Content: "current user turn"})
	if err != nil {
		t.Fatalf("run next turn with durable Summary: %v", err)
	}
	completedRunner := false
	for item := range events {
		if item != nil && item.IsTerminalError() {
			t.Fatalf("next Runner turn returned terminal error: %#v", item.Error)
		}
		if item != nil && item.IsRunnerCompletion() {
			completedRunner = true
		}
	}
	if !completedRunner {
		t.Fatal("next Runner turn did not emit completion")
	}
	agentRequestMu.Lock()
	capturedMessages, _ := json.Marshal(agentRequest.Messages)
	agentRequestMu.Unlock()
	prompt := string(capturedMessages)
	for _, required := range []string{checkpoint.Content, "post-cutoff question", "post-cutoff answer", "current user turn"} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("next Runner request is missing %q: %s", required, prompt)
		}
	}
	for _, summarized := range []string{"need invoice", "which order", "order 42", "we will email it"} {
		if strings.Contains(prompt, summarized) {
			t.Fatalf("next Runner request retained summarized event %q: %s", summarized, prompt)
		}
	}
}
