package agent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestOpenAIProviderFactoryConcurrentConfigIsolation(t *testing.T) {
	servers := map[string]*httptest.Server{}
	for tenantID, response := range map[string]string{"tenant-a": "answer-a", "tenant-b": "answer-b"} {
		response := response
		servers[tenantID] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"object":"chat.completion","choices":[{"message":{"role":"assistant","content":"`+response+`"},"finish_reason":"stop"}]}`)
		}))
	}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()

	factory := OpenAIProviderFactory{
		Configs: ModelConfigResolverFunc(func(_ context.Context, tc tenant.TenantContext, spec AgentSpec) (ModelConfig, error) {
			suffix := tc.TenantID[len("tenant-"):]
			expectedTool := ToolSpec{Name: "lookup-" + suffix, Description: "lookup for " + suffix, InputSchema: map[string]any{"tenant": suffix}}
			if spec.SystemPrompt != "system-"+suffix || spec.ToolPolicyRef != "policy-"+suffix || spec.GuardrailRef != "guardrail-"+suffix || len(spec.Tools) != 1 || spec.Tools[0].Name != expectedTool.Name || spec.Tools[0].Description != expectedTool.Description || spec.Tools[0].InputSchema["tenant"] != suffix {
				return ModelConfig{}, fmt.Errorf("agent configuration crossed tenant boundary for %s", tc.TenantID)
			}
			return ModelConfig{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, ConfigVersion: tc.ConfigVersion, ConfigRef: spec.ModelConfigRef, Provider: spec.ModelProvider, Endpoint: servers[tc.TenantID].URL, Model: tc.AgentAppID, SecretRef: "secret/" + tc.TenantID}, nil
		}),
		Secrets: SecretResolverFunc(func(_ context.Context, tc tenant.TenantContext, ref string) (string, error) {
			if ref != "secret/"+tc.TenantID {
				return "", context.Canceled
			}
			return tc.TenantID + "-secret", nil
		}),
	}

	contexts := []tenant.TenantContext{validContext(), validContext()}
	contexts[1].TenantID = "tenant-b"
	contexts[1].AgentAppID = "agent-b"
	specs := []AgentSpec{validSpec(), validSpec()}
	specs[0].ModelProvider = "openai"
	specs[0].ModelConfigRef = "ref-a"
	specs[0].SystemPrompt = "system-a"
	specs[0].ToolPolicyRef = "policy-a"
	specs[0].GuardrailRef = "guardrail-a"
	specs[0].Tools = []ToolSpec{{Name: "lookup-a", Description: "lookup for a", InputSchema: map[string]any{"tenant": "a"}}}
	specs[1].TenantID = "tenant-b"
	specs[1].AgentAppID = "agent-b"
	specs[1].ModelProvider = "openai"
	specs[1].ModelConfigRef = "ref-b"
	specs[1].SystemPrompt = "system-b"
	specs[1].ToolPolicyRef = "policy-b"
	specs[1].GuardrailRef = "guardrail-b"
	specs[1].Tools = []ToolSpec{{Name: "lookup-b", Description: "lookup for b", InputSchema: map[string]any{"tenant": "b"}}}

	type isolationResult struct {
		index    int
		response ProviderResponse
		err      error
	}
	results := make([]ProviderResponse, 2)
	errs := make([]error, 2)
	out := make(chan isolationResult, len(contexts))
	var wait sync.WaitGroup
	for i := range contexts {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			provider, err := factory.Build(context.Background(), contexts[i], specs[i])
			if err != nil {
				out <- isolationResult{index: i, err: err}
				return
			}
			response, completeErr := provider.Complete(context.Background(), ProviderRequest{Messages: []Message{{Role: "user", Content: "hello"}}})
			out <- isolationResult{index: i, response: response, err: completeErr}
		}(i)
	}
	wait.Wait()
	close(out)
	for item := range out {
		results[item.index] = item.response
		errs[item.index] = item.err
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
	if results[0].Text != "answer-a" || results[1].Text != "answer-b" {
		t.Fatalf("config values crossed requests: %+v", results)
	}
}
