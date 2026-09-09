package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestOpenAICompatibleAgentFactoryUsesServerOwnedProfile(t *testing.T) {
	requests := make(chan map[string]any, 1)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" || r.Header.Get("Authorization") != "Bearer fixture-secret" {
			http.Error(w, "unexpected provider request", http.StatusUnauthorized)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		requests <- payload
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"id":"fixture","object":"chat.completion","created":1699200000,"model":"fixture-model","choices":[{"index":0,"message":{"role":"assistant","content":"fixture reply"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}`)
	}))
	defer provider.Close()

	store := activeTestPlatform(t)
	store.mu.Lock()
	versions := store.versions[resourceKey("tenant-one", "deploy-one")]
	versions[0].Config = map[string]any{"provider_profile": "default-openai", "model": "fixture-model", "prompt": "Answer briefly."}
	store.versions[resourceKey("tenant-one", "deploy-one")] = versions
	store.mu.Unlock()
	adapter := NewFrameworkRunnerAdapter(store.DeploymentVersion, OpenAICompatibleAgentFactory(ModelProviderProfile{
		ID: "default-openai", BaseURL: provider.URL, APIKey: "fixture-secret", Model: "server-default",
	}))
	defer adapter.Close()
	response, err := adapter.Run(context.Background(), RunnerRequest{
		TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", VersionID: "deploy-one-v1",
		SessionID: "session-one", UserID: "user-one", RequestID: "request-openai", Input: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Output != "fixture reply" {
		t.Fatalf("output = %q", response.Output)
	}
	request := <-requests
	if request["model"] != "fixture-model" {
		t.Fatalf("provider model = %#v", request["model"])
	}
	encoded, _ := json.Marshal(request)
	if strings.Contains(string(encoded), "fixture-secret") {
		t.Fatal("provider credential leaked into request body")
	}
	_, err = adapter.Run(context.Background(), RunnerRequest{
		TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", VersionID: "deploy-one-v1",
		SessionID: "session-one", UserID: "user-one", RequestID: "request-openai-two", Input: "second request",
	})
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := <-requests
	secondPayload, _ := json.Marshal(secondRequest["messages"])
	if strings.Contains(string(secondPayload), "hello") || !strings.Contains(string(secondPayload), "second request") {
		t.Fatalf("upstream runner retained node-local history: %s", secondPayload)
	}
}

func TestOpenAICompatibleModelStreamsThroughPublicChatSSE(t *testing.T) {
	startedAt := time.Now()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" || r.Header.Get("Authorization") != "Bearer fixture-secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"id":"fixture","object":"chat.completion","created":1699200000,"model":"fixture-model","choices":[{"index":0,"message":{"role":"assistant","content":"fixture public reply"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`)
	}))
	defer provider.Close()

	client := newChannelTestClient(t, EchoRunner{})
	client.post("/api/v1/admin/agent-apps", `{"id":"app-model","name":"Model"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/admin/deployments", `{"id":"deploy-model","agent_app_id":"app-model"}`, nil, http.StatusCreated, nil)
	var version DeploymentVersion
	client.post("/api/v1/admin/deployments/deploy-model/versions", `{"config":{"provider_profile":"default-openai","model":"fixture-model","prompt":"Answer briefly."}}`, map[string]string{"Idempotency-Key": "model-version"}, http.StatusCreated, &version)
	client.post("/api/v1/admin/deployments/deploy-model/transition", `{"status":"published","version_id":"`+version.ID+`"}`, nil, http.StatusOK, nil)
	client.post("/api/v1/admin/deployments/deploy-model/transition", `{"status":"active"}`, nil, http.StatusOK, nil)
	if _, err := client.handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-model"}); err != nil {
		t.Fatal(err)
	}
	adapter := NewFrameworkRunnerAdapter(client.handler.platform.DeploymentVersion, OpenAICompatibleAgentFactory(ModelProviderProfile{ID: "default-openai", BaseURL: provider.URL, APIKey: "fixture-secret", Model: "fixture-model"}))
	client.handler.ConfigureRuntime(adapter, nil)
	client.post("/api/v1/chat/sessions", `{"app_id":"app-model","session_id":"session-model"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-model/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-model"}, http.StatusAccepted, nil)
	envelopes := readSSE(t, client, "/api/v1/chat/sessions/session-model/stream?request_id=request-model", nil)
	found := false
	for _, envelope := range envelopes {
		var data map[string]string
		_ = json.Unmarshal(envelope.Data, &data)
		if envelope.Type == "message.completed" && data["output"] == "fixture public reply" {
			found = true
		}
	}
	if !found {
		t.Fatalf("public SSE did not contain fixture response: %#v", envelopes)
	}
	metrics := client.handler.governance.Metrics("tenant-one")
	if metrics.ModelLatencyMS < 10 || metrics.ExecutionLatencyMS < metrics.ModelLatencyMS {
		t.Fatalf("model and execution latency were not measured at their boundaries: %#v", metrics)
	}
	trace, traceFound := client.handler.governance.Trace("tenant-one", "", "request-model")
	modelSpanFound := false
	for _, span := range trace.Spans {
		modelSpanFound = modelSpanFound || span.Name == "model.call"
	}
	if !traceFound || !modelSpanFound {
		t.Fatalf("model call trace missing: %#v, found = %v", trace, traceFound)
	}
	if elapsed := time.Since(startedAt); elapsed >= 5*time.Second {
		t.Fatalf("model workflow waited for an event completion notice: %v", elapsed)
	}
}

func TestResponsesModelStreamsThroughPublicChatSSE(t *testing.T) {
	requests := make(chan map[string]any, 1)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" || r.Header.Get("Authorization") != "Bearer fixture-secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		requests <- payload
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"message-1\",\"output_index\":0,\"content_index\":0,\"sequence_number\":1,\"delta\":\"fixture responses reply\"}\n\n")
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"response-1\",\"object\":\"response\",\"created_at\":1699200000,\"model\":\"gpt-5.6-luna\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5,\"input_tokens_details\":{},\"output_tokens_details\":{}}}}\n\n")
	}))
	defer provider.Close()

	client := newChannelTestClient(t, EchoRunner{})
	client.post("/api/v1/admin/agent-apps", `{"id":"app-responses","name":"Responses"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/admin/deployments", `{"id":"deploy-responses","agent_app_id":"app-responses"}`, nil, http.StatusCreated, nil)
	var version DeploymentVersion
	client.post("/api/v1/admin/deployments/deploy-responses/versions", `{"config":{"provider_profile":"default-openai","model":"gpt-5.6-luna","prompt":"Answer briefly.","generation_config":{"max_tokens":64,"temperature":0.2}}}`, map[string]string{"Idempotency-Key": "responses-version"}, http.StatusCreated, &version)
	client.post("/api/v1/admin/deployments/deploy-responses/transition", `{"status":"published","version_id":"`+version.ID+`"}`, nil, http.StatusOK, nil)
	client.post("/api/v1/admin/deployments/deploy-responses/transition", `{"status":"active"}`, nil, http.StatusOK, nil)
	if _, err := client.handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-responses"}); err != nil {
		t.Fatal(err)
	}
	adapter := NewFrameworkRunnerAdapter(client.handler.platform.DeploymentVersion, OpenAICompatibleAgentFactory(ModelProviderProfile{
		ID: "default-openai", BaseURL: provider.URL, APIKey: "fixture-secret", Model: "gpt-5.6-luna", Protocol: ModelProtocolResponses,
	}))
	client.handler.ConfigureRuntime(adapter, nil)
	client.post("/api/v1/chat/sessions", `{"app_id":"app-responses","session_id":"session-responses"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-responses/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-responses"}, http.StatusAccepted, nil)
	envelopes := readSSE(t, client, "/api/v1/chat/sessions/session-responses/stream?request_id=request-responses", nil)
	found := false
	for _, envelope := range envelopes {
		var data map[string]string
		_ = json.Unmarshal(envelope.Data, &data)
		if envelope.Type == "message.completed" && data["output"] == "fixture responses reply" {
			found = true
		}
	}
	if !found {
		t.Fatalf("public SSE did not contain Responses output: %#v", envelopes)
	}
	request := <-requests
	if request["model"] != "gpt-5.6-luna" || request["max_output_tokens"] != float64(64) || request["temperature"] != 0.2 || request["stream"] != true {
		t.Fatalf("Responses request = %#v", request)
	}
	encoded, _ := json.Marshal(request)
	if strings.Contains(string(encoded), "fixture-secret") {
		t.Fatal("provider credential leaked into request body")
	}
}

func TestResponsesModelCancellationClosesUnconsumedStream(t *testing.T) {
	providerStarted := make(chan struct{})
	providerCancelled := make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(providerStarted)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"message-1\",\"delta\":\"blocked\"}\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		close(providerCancelled)
	}))
	defer provider.Close()

	ctx, cancel := context.WithCancel(context.Background())
	output, err := newResponsesModel("gpt-5.6-luna", provider.URL, "fixture-secret").GenerateContent(ctx, &model.Request{
		Messages: []model.Message{{Role: model.RoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-providerStarted:
	case <-time.After(time.Second):
		t.Fatal("provider stream did not start")
	}
	cancel()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-output:
			if !ok {
				goto streamClosed
			}
		case <-deadline:
			t.Fatal("model output did not close after cancellation")
		}
	}

streamClosed:
	select {
	case <-providerCancelled:
	case <-time.After(time.Second):
		t.Fatal("provider stream was not closed after cancellation")
	}
}

func TestFrameworkRunnerAdapterStreamsAndReusesRunner(t *testing.T) {
	platform := activeTestPlatform(t)
	var factoryCalls atomic.Int64
	adapter := NewFrameworkRunnerAdapter(platform.DeploymentVersion, func(_ context.Context, version DeploymentVersion) (frameworkagent.Agent, error) {
		factoryCalls.Add(1)
		return serviceagent.NewDeterministicAgent(version.AgentAppID), nil
	})

	request := RunnerRequest{TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", SessionID: "session-one", UserID: "user-one", Input: "hello", RequestID: "request-one", VersionID: "deploy-one-v1"}
	first := collectRuntimeEvents(t, adapter, request)
	second := collectRuntimeEvents(t, adapter, request)
	if factoryCalls.Load() != 1 {
		t.Fatalf("factory calls = %d, want 1", factoryCalls.Load())
	}
	if len(first) != 3 || first[0].Type != "message.delta" || first[1].Type != "message.completed" || first[2].Type != "run.completed" {
		t.Fatalf("first events = %#v", first)
	}
	if first[0].Data["delta"] != "framework:hello" || first[1].Data["output"] != "framework:hello" {
		t.Fatalf("first event data = %#v", first)
	}
	if len(second) != 3 {
		t.Fatalf("second events = %#v", second)
	}
}

func TestGovernancePluginAuthorizesAtActualToolCallback(t *testing.T) {
	center := NewGovernanceCenter()
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-one", AllowedTools: []string{"search"}})
	manager := plugin.MustNewManager(&governanceRuntimePlugin{center: center})
	callbacks := manager.ToolCallbacks()
	request := RunnerRequest{TenantID: "tenant-one", AppID: "app-one", UserID: "user-one", SessionID: "session-one", RequestID: "request-one", TraceID: "trace-one"}
	ctx := withRunnerIdentity(context.Background(), request)
	if _, err := callbacks.RunBeforeTool(ctx, &tool.BeforeToolArgs{ToolName: "search", Arguments: []byte(`{"query":"safe"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := callbacks.RunBeforeTool(ctx, &tool.BeforeToolArgs{ToolName: "delete", Arguments: []byte(`{}`)}); err == nil {
		t.Fatal("disallowed Tool reached invocation")
	}
}

func TestGovernancePluginConsumesDangerousConfirmationAndRecordsToolCompletion(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	center := NewGovernanceCenter()
	center.now = func() time.Time { return now }
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-one", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"}})
	request := RunnerRequest{TenantID: "tenant-one", AppID: "app-one", UserID: "user-one", SessionID: "session-one", RequestID: "request-one", TraceID: "trace-one"}
	_, _ = center.Evaluate(context.Background(), GovernanceRequest{TenantID: request.TenantID, AgentAppID: request.AppID, UserID: request.UserID, SessionID: request.SessionID, RequestID: request.RequestID, RequiredTools: []string{"deploy"}})
	callbacks := plugin.MustNewManager(&governanceRuntimePlugin{center: center}).ToolCallbacks()
	ctx := withRunnerIdentity(context.Background(), request)
	args := &tool.BeforeToolArgs{ToolName: "deploy", Arguments: []byte(`{"target":"production"}`)}
	if _, err := callbacks.RunBeforeTool(ctx, args); !IsGovernanceError(err, "confirmation_required") {
		t.Fatalf("initial Tool callback error = %v", err)
	}
	pending := center.Confirmations("tenant-one")[0]
	if _, err := center.DecideConfirmation(context.Background(), "tenant-one", pending.ID, "operator", true); err != nil {
		t.Fatal(err)
	}
	if _, err := callbacks.RunBeforeTool(ctx, args); err != nil {
		t.Fatal(err)
	}
	now = now.Add(25 * time.Millisecond)
	if _, err := callbacks.RunAfterTool(ctx, &tool.AfterToolArgs{ToolName: "deploy", Arguments: args.Arguments, Result: "done"}); err != nil {
		t.Fatal(err)
	}
	if _, err := callbacks.RunBeforeTool(ctx, args); !IsGovernanceError(err, "confirmation_consumed") {
		t.Fatalf("repeated Tool callback error = %v", err)
	}
	if metrics := center.Metrics("tenant-one"); metrics.ToolLatencyMS != 25 {
		t.Fatalf("Tool execution latency = %dms, want 25ms", metrics.ToolLatencyMS)
	}
}

func TestGovernancePluginMeasuresModelCallSeparatelyFromExecution(t *testing.T) {
	center := NewGovernanceCenter()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	center.now = func() time.Time { return clock }
	callbacks := plugin.MustNewManager(&governanceRuntimePlugin{center: center}).ModelCallbacks()
	request := RunnerRequest{
		TenantID: "tenant-one", AppID: "app-one", UserID: "user-one", SessionID: "session-one",
		RequestID: "request-one", TraceID: "trace-one", Channel: ChannelTelegram,
	}
	ctx := withRunnerIdentity(context.Background(), request)
	before, err := callbacks.RunBeforeModel(ctx, &model.BeforeModelArgs{Request: &model.Request{}})
	if err != nil || before == nil || before.Context == nil {
		t.Fatalf("before model = %#v, %v", before, err)
	}
	clock = clock.Add(20 * time.Millisecond)
	if _, err := callbacks.RunAfterModel(before.Context, &model.AfterModelArgs{Response: &model.Response{IsPartial: true}}); err != nil {
		t.Fatal(err)
	}
	if metrics := center.Metrics("tenant-one"); metrics.ModelLatencyMS != 0 {
		t.Fatalf("partial response recorded model latency: %#v", metrics)
	}
	clock = clock.Add(15 * time.Millisecond)
	if _, err := callbacks.RunAfterModel(before.Context, &model.AfterModelArgs{Response: &model.Response{}}); err != nil {
		t.Fatal(err)
	}
	metrics := center.Metrics("tenant-one")
	if metrics.ModelLatencyMS != 35 || metrics.ExecutionLatencyMS != 0 {
		t.Fatalf("model and execution latency = %#v", metrics)
	}
	trace, found := center.Trace("tenant-one", "trace-one", "")
	if !found || len(trace.Spans) != 1 || trace.Spans[0].Name != "model.call" {
		t.Fatalf("model trace = %#v, found = %v", trace, found)
	}
}

func TestDefaultAgentFactoryInvokesConfiguredDeterministicToolAfterConfirmation(t *testing.T) {
	platform := activeTestPlatform(t)
	version, ok, err := platform.DeploymentVersion(context.Background(), DeploymentVersionRef{TenantID: "tenant-one", VersionID: "deploy-one-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("active deployment version not found")
	}
	version.Config = map[string]any{"runner": "framework", "tools": []any{"deploy"}, "deterministic_tool_call": "deploy"}
	center := NewGovernanceCenter()
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one",
		AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"},
	})
	adapter := NewFrameworkRunnerAdapter(func(_ context.Context, ref DeploymentVersionRef) (DeploymentVersion, bool, error) {
		if ref.TenantID != version.TenantID || ref.VersionID != version.ID {
			return DeploymentVersion{}, false, nil
		}
		return version, true, nil
	}, nil)
	adapter.SetGovernance(center)
	request := RunnerRequest{
		TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", VersionID: version.ID,
		SessionID: "session-one", UserID: "user-one", RequestID: "request-one", TraceID: "trace-one", Input: "ship",
	}
	first := collectRuntimeEvents(t, adapter, request)
	if len(first) != 1 || first[0].Type != "run.failed" || !strings.Contains(first[0].Data["error"], "confirmation_required") {
		t.Fatalf("first Tool run events = %#v", first)
	}
	if metrics := center.Metrics("tenant-one"); metrics.Active != 1 || metrics.Failed != 0 || metrics.Completed != 0 {
		t.Fatalf("confirmation reservation was reconciled too early: %#v", metrics)
	}
	confirmations := center.Confirmations("tenant-one")
	if len(confirmations) != 1 || confirmations[0].ToolName != "deploy" {
		t.Fatalf("pending confirmations = %#v", confirmations)
	}
	if _, err := center.DecideConfirmation(context.Background(), "tenant-one", confirmations[0].ID, "operator", true); err != nil {
		t.Fatal(err)
	}
	second := collectRuntimeEvents(t, adapter, request)
	if len(second) < 2 || second[len(second)-1].Type != "run.completed" {
		t.Fatalf("approved Tool run events = %#v", second)
	}
	confirmations = center.Confirmations("tenant-one")
	if len(confirmations) != 1 || confirmations[0].Status != ConfirmationCompleted {
		t.Fatalf("completed confirmations = %#v", confirmations)
	}
	if metrics := center.Metrics("tenant-one"); metrics.Active != 0 || metrics.Failed != 0 || metrics.Completed != 1 {
		t.Fatalf("approved retry accounting = %#v", metrics)
	}
}

func TestFrameworkRunnerAdapterRejectsUnknownVersionAndClose(t *testing.T) {
	adapter := NewFrameworkRunnerAdapter(func(context.Context, DeploymentVersionRef) (DeploymentVersion, bool, error) {
		return DeploymentVersion{}, false, nil
	}, nil)
	if _, err := adapter.RunEvents(context.Background(), RunnerRequest{VersionID: "missing"}); err == nil || err.Error() != "deployment_version_scope_mismatch" {
		t.Fatalf("unknown version error = %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.RunEvents(context.Background(), RunnerRequest{VersionID: "missing"}); err == nil || err.Error() != "framework_runtime_closed" {
		t.Fatalf("closed adapter error = %v", err)
	}
}

func TestFrameworkRunnerAdapterAppliesToolGovernanceAfterRunningPlainVersion(t *testing.T) {
	versions := map[string]DeploymentVersion{
		"plain-v1": {
			ID: "plain-v1", TenantID: "tenant-one", AgentAppID: "plain-app", DeploymentID: "plain", Active: true,
			Config: map[string]any{"runner": "plain"},
		},
		"tool-v1": {
			ID: "tool-v1", TenantID: "tenant-one", AgentAppID: "tool-app", DeploymentID: "tool", Active: true,
			Config: map[string]any{"runner": "tool", "tools": []any{"deploy"}, "deterministic_tool_call": "deploy"},
		},
	}
	adapter := NewFrameworkRunnerAdapter(func(_ context.Context, ref DeploymentVersionRef) (DeploymentVersion, bool, error) {
		version, ok := versions[ref.VersionID]
		return version, ok, nil
	}, nil)
	defer adapter.Close()
	center := NewGovernanceCenter()
	policy, err := center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "tool-app", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter.SetToolGovernance(center)

	plain := collectRuntimeEvents(t, adapter, RunnerRequest{
		TenantID: "tenant-one", AppID: "plain-app", DeploymentID: "plain", VersionID: "plain-v1",
		SessionID: "plain-session", UserID: "user-one", RequestID: "plain-request", Input: "hello",
	})
	if plain[len(plain)-1].Type != "run.completed" {
		t.Fatalf("plain events = %#v", plain)
	}
	tool := collectRuntimeEvents(t, adapter, RunnerRequest{
		TenantID: "tenant-one", AppID: "tool-app", DeploymentID: "tool", VersionID: "tool-v1",
		SessionID: "tool-session", UserID: "user-one", RequestID: "tool-request", Input: "deploy", PolicyRevision: policy.Revision,
	})
	if len(tool) != 1 || tool[0].Type != "run.failed" || !strings.Contains(tool[0].Data["error"], "confirmation_required") {
		t.Fatalf("governed Tool events = %#v", tool)
	}
}

func TestRuntimeStreamPropagatesTenantUserAndCompletion(t *testing.T) {
	platform := activeTestPlatform(t)
	runtime := NewRuntime(platform, NewFrameworkRunnerAdapter(platform.DeploymentVersion, nil), nil)
	events, err := runtime.Stream(context.Background(), TenantContext{TenantID: "tenant-one", UserID: "user-one", Role: RoleOperator}, GatewayRequest{
		AppID: "app-one", SessionID: "session-one", Input: "hello", RequestID: "request-one",
	})
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for event := range events {
		types = append(types, event.Type)
	}
	if len(types) != 3 || types[0] != "message.delta" || types[1] != "message.completed" || types[2] != "run.completed" {
		t.Fatalf("runtime event types = %#v", types)
	}
}

func TestFrameworkRunnerAdapterPreservesIdentity(t *testing.T) {
	platform := activeTestPlatform(t)
	seen := make(chan RunnerRequest, 1)
	adapter := NewFrameworkRunnerAdapter(platform.DeploymentVersion, func(_ context.Context, _ DeploymentVersion) (frameworkagent.Agent, error) {
		return identityCapturingAgent{seen: seen}, nil
	})
	request := RunnerRequest{TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", SessionID: "session-one", UserID: "user-one", Input: "hello", RequestID: "request-one", VersionID: "deploy-one-v1"}
	events := collectRuntimeEvents(t, adapter, request)
	got := <-seen
	if got != request {
		t.Fatalf("framework identity = %#v, want %#v", got, request)
	}
	for _, runtimeEvent := range events {
		for key, want := range map[string]string{
			"tenant_id": request.TenantID, "app_id": request.AppID, "deployment_id": request.DeploymentID,
			"version_id": request.VersionID, "session_id": request.SessionID, "request_id": request.RequestID,
		} {
			if runtimeEvent.Data[key] != want {
				t.Fatalf("event identity %q = %q, want %q", key, runtimeEvent.Data[key], want)
			}
		}
	}
}

func TestFrameworkRunnerAdapterIsolatesTenantVersions(t *testing.T) {
	versions := map[string]DeploymentVersion{
		"tenant-one-v1": {ID: "tenant-one-v1", TenantID: "tenant-one", AgentAppID: "app-one", DeploymentID: "deploy-one", Active: true},
		"tenant-two-v1": {ID: "tenant-two-v1", TenantID: "tenant-two", AgentAppID: "app-two", DeploymentID: "deploy-two", Active: true},
	}
	var factoryCalls atomic.Int64
	adapter := NewFrameworkRunnerAdapter(func(_ context.Context, ref DeploymentVersionRef) (DeploymentVersion, bool, error) {
		version, ok := versions[ref.VersionID]
		return version, ok, nil
	}, func(_ context.Context, version DeploymentVersion) (frameworkagent.Agent, error) {
		factoryCalls.Add(1)
		return serviceagent.NewDeterministicAgent(version.AgentAppID), nil
	})
	for _, request := range []RunnerRequest{
		{TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", SessionID: "session-one", UserID: "user-one", Input: "one", RequestID: "request-one", VersionID: "tenant-one-v1"},
		{TenantID: "tenant-two", AppID: "app-two", DeploymentID: "deploy-two", SessionID: "session-two", UserID: "user-two", Input: "two", RequestID: "request-two", VersionID: "tenant-two-v1"},
	} {
		_ = collectRuntimeEvents(t, adapter, request)
	}
	if factoryCalls.Load() != 2 {
		t.Fatalf("factory calls = %d, want 2", factoryCalls.Load())
	}
	if _, err := adapter.RunEvents(context.Background(), RunnerRequest{TenantID: "tenant-two", AppID: "app-two", DeploymentID: "deploy-two", SessionID: "session-one", UserID: "user-two", Input: "guess", RequestID: "request-three", VersionID: "tenant-one-v1"}); err == nil || err.Error() != "deployment_version_scope_mismatch" {
		t.Fatalf("cross-tenant version error = %v", err)
	}
}

func TestFrameworkRunnerAdapterEmitsCancellation(t *testing.T) {
	platform := activeTestPlatform(t)
	adapter := NewFrameworkRunnerAdapter(platform.DeploymentVersion, func(_ context.Context, _ DeploymentVersion) (frameworkagent.Agent, error) {
		return blockingAgent{}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := adapter.RunEvents(ctx, RunnerRequest{TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", SessionID: "session-one", UserID: "user-one", Input: "hello", RequestID: "request-one", VersionID: "deploy-one-v1"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	runtimeEvent, ok := <-events
	if !ok {
		t.Fatal("event channel closed without cancellation")
	}
	if runtimeEvent.Type != "run.cancelled" {
		t.Fatalf("event type = %q, want run.cancelled", runtimeEvent.Type)
	}
}

func TestFrameworkRunnerAdapterCloseCancelsActiveRun(t *testing.T) {
	platform := activeTestPlatform(t)
	adapter := NewFrameworkRunnerAdapter(platform.DeploymentVersion, func(_ context.Context, _ DeploymentVersion) (frameworkagent.Agent, error) {
		return blockingAgent{}, nil
	})
	events, err := adapter.RunEvents(context.Background(), RunnerRequest{TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", SessionID: "session-one", UserID: "user-one", Input: "hello", RequestID: "request-one", VersionID: "deploy-one-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	runtimeEvent, ok := <-events
	if !ok || runtimeEvent.Type != "run.cancelled" {
		t.Fatalf("close event = %#v, want run.cancelled", runtimeEvent)
	}
}

func TestFrameworkRunnerAdapterCancellationDoesNotLeakWithoutReader(t *testing.T) {
	platform := activeTestPlatform(t)
	adapter := NewFrameworkRunnerAdapter(platform.DeploymentVersion, func(_ context.Context, _ DeploymentVersion) (frameworkagent.Agent, error) {
		return blockingAgent{}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	events, err := adapter.RunEvents(ctx, RunnerRequest{TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", SessionID: "session-one", UserID: "user-one", Input: "hello", RequestID: "request-one", VersionID: "deploy-one-v1"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		adapter.mu.Lock()
		remaining := len(adapter.runs["deploy-one-v1"])
		adapter.mu.Unlock()
		if remaining == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("cancelled run remained registered; events = %#v", events)
}

func TestFrameworkRunnerAdapterRetiresInactiveVersion(t *testing.T) {
	platform := activeTestPlatform(t)
	adapter := NewFrameworkRunnerAdapter(platform.DeploymentVersion, nil)
	request := RunnerRequest{TenantID: "tenant-one", AppID: "app-one", DeploymentID: "deploy-one", SessionID: "session-one", UserID: "user-one", Input: "hello", RequestID: "request-one", VersionID: "deploy-one-v1"}
	_ = collectRuntimeEvents(t, adapter, request)
	deployment, ok, err := platform.deployment(context.Background(), "tenant-one", "deploy-one")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("deployment not found")
	}
	if _, _, ok, err := platform.transition(context.Background(), deployment, DeploymentPaused, ""); err != nil || !ok {
		t.Fatal("pause deployment")
	}
	if _, err := adapter.RunEvents(context.Background(), request); err == nil || err.Error() != "deployment_version_inactive" {
		t.Fatalf("inactive version error = %v", err)
	}
	ref := DeploymentVersionRef{TenantID: request.TenantID, VersionID: request.VersionID}
	adapter.RetireVersion(ref)
	if _, exists := adapter.runners[versionRefKey(ref)]; exists {
		t.Fatal("retired runner remains cached")
	}
}

func TestFrameworkChatSSEPersistsIdentityAndSupportsReplay(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	client.handler.ConfigureRuntime(NewFrameworkRunnerAdapter(client.handler.platform.DeploymentVersion, nil), nil)
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}
	envelopes := readSSE(t, client, "/api/v1/chat/sessions/session-one/stream?request_id=request-one", nil)
	if len(envelopes) < 4 || envelopes[len(envelopes)-1].Type != "run.completed" {
		t.Fatalf("framework SSE envelopes = %#v", envelopes)
	}
	for _, envelope := range envelopes {
		if envelope.RequestID != "request-one" || envelope.SessionID != "session-one" {
			t.Fatalf("framework SSE identity = %#v", envelope)
		}
	}
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusOK, nil)
}

func TestFrameworkChatFailureAndCancellation(t *testing.T) {
	failureClient := newChannelTestClient(t, nil)
	failureClient.activateApp("app-one", "deploy-one")
	failureClient.handler.ConfigureRuntime(NewFrameworkRunnerAdapter(failureClient.handler.platform.DeploymentVersion, func(_ context.Context, _ DeploymentVersion) (frameworkagent.Agent, error) {
		return failingFrameworkAgent{}, nil
	}), nil)
	failureClient.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	failureClient.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(failureClient, "session-one", "run.failed"); err != nil {
		t.Fatal(err)
	}

	cancelClient := newChannelTestClient(t, nil)
	cancelClient.activateApp("app-one", "deploy-one")
	cancelClient.handler.ConfigureRuntime(NewFrameworkRunnerAdapter(cancelClient.handler.platform.DeploymentVersion, func(_ context.Context, _ DeploymentVersion) (frameworkagent.Agent, error) {
		return blockingAgent{}, nil
	}), nil)
	cancelClient.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	cancelClient.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	cancelClient.post("/api/v1/chat/sessions/session-one/cancel", `{"request_id":"request-one"}`, nil, http.StatusOK, nil)
	if err := waitForChatEvent(cancelClient, "session-one", "run.cancelled"); err != nil {
		t.Fatal(err)
	}
}

func TestFrameworkChatPersistsWithStage2Stores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) DataStore
	}{
		{name: "redis", store: func(t *testing.T) DataStore {
			server := miniredis.RunT(t)
			return NewRedisStore(server.Addr())
		}},
		{name: "sqlite", store: func(t *testing.T) DataStore {
			store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "framework.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := test.store(t)
			if closer, ok := store.(interface{ Close() error }); ok {
				defer closer.Close()
			}
			client := newChannelTestClient(t, nil)
			client.handler.ConfigureDataStore(store)
			client.activateApp("app-one", "deploy-one")
			client.handler.ConfigureRuntime(NewFrameworkRunnerAdapter(client.handler.platform.DeploymentVersion, nil), nil)
			client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
			client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
			if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
				t.Fatal(err)
			}
			events, err := store.ListSessionEvents(context.Background(), "tenant-one", "session-one", 0)
			if err != nil || len(events) < 4 {
				t.Fatalf("persisted events = %#v, err = %v", events, err)
			}
		})
	}
}

type identityCapturingAgent struct {
	seen chan<- RunnerRequest
}

func (a identityCapturingAgent) Run(ctx context.Context, invocation *frameworkagent.Invocation) (<-chan *event.Event, error) {
	request, _ := RunnerIdentityFromContext(ctx)
	a.seen <- request
	results := make(chan *event.Event, 1)
	results <- event.NewResponseEvent(invocation.InvocationID, "identity-agent", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{Message: model.Message{
			Role: model.RoleAssistant, Content: "ok",
		}}},
	})
	close(results)
	return results, nil
}

func (identityCapturingAgent) Tools() []tool.Tool { return nil }
func (identityCapturingAgent) Info() frameworkagent.Info {
	return frameworkagent.Info{Name: "identity-agent"}
}
func (identityCapturingAgent) SubAgents() []frameworkagent.Agent        { return nil }
func (identityCapturingAgent) FindSubAgent(string) frameworkagent.Agent { return nil }

type blockingAgent struct{}

func (blockingAgent) Run(ctx context.Context, _ *frameworkagent.Invocation) (<-chan *event.Event, error) {
	results := make(chan *event.Event)
	go func() {
		<-ctx.Done()
		close(results)
	}()
	return results, nil
}

func (blockingAgent) Tools() []tool.Tool                       { return nil }
func (blockingAgent) Info() frameworkagent.Info                { return frameworkagent.Info{Name: "blocking-agent"} }
func (blockingAgent) SubAgents() []frameworkagent.Agent        { return nil }
func (blockingAgent) FindSubAgent(string) frameworkagent.Agent { return nil }

type failingFrameworkAgent struct{}

func (failingFrameworkAgent) Run(context.Context, *frameworkagent.Invocation) (<-chan *event.Event, error) {
	return nil, errors.New("framework failure")
}

func (failingFrameworkAgent) Tools() []tool.Tool {
	return nil
}

func (failingFrameworkAgent) Info() frameworkagent.Info {
	return frameworkagent.Info{Name: "failing-agent"}
}

func (failingFrameworkAgent) SubAgents() []frameworkagent.Agent {
	return nil
}

func (failingFrameworkAgent) FindSubAgent(string) frameworkagent.Agent {
	return nil
}

func collectRuntimeEvents(t *testing.T, adapter *FrameworkRunnerAdapter, request RunnerRequest) []RuntimeEvent {
	t.Helper()
	events, err := adapter.RunEvents(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var collected []RuntimeEvent
	for event := range events {
		collected = append(collected, event)
	}
	return collected
}
