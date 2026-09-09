//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	openaiopt "github.com/openai/openai-go/option"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestModelFactoryDisablesProviderSDKRetries(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After-Ms", "1")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"temporary failure","type":"server_error"}}`))
	}))
	t.Cleanup(server.Close)
	t.Setenv("TRPC_OPENAI_BASE_URL", server.URL)
	factory := NewModelFactory()
	factory.endpointClient = func(string) (openaiopt.HTTPClient, error) { return server.Client(), nil }

	llm, err := factory.CreateModel(&tenant.ModelConfig{
		Provider:  "openai",
		ModelName: "gpt-4",
		APIKey:    "test-only-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	responses, err := llm.GenerateContent(ctx, &model.Request{
		Messages: []model.Message{{Role: model.RoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range responses {
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("one logical model call made %d HTTP attempts, want 1", got)
	}
}

func TestWorkerRejectsUnpinnedBudgetedVersion(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	agentConfig := tenant.AgentConfig{
		Name: "support", Type: "llm", DefaultModel: "gpt-4", MaxLLMCalls: 2,
	}
	versionModel := tenant.ModelConfig{
		Provider: "openai", ModelName: "gpt-4", MaxTokens: 1_000,
	}
	tenantConfig := &tenant.Tenant{
		ID:         "budgeted-tenant",
		Models:     []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt-4", APIKey: "test-only-key", MaxTokens: 1_000}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		Budget:     tenant.BudgetConfig{MaxTokensPerDay: 20_000, MaxTokensPerRequest: 16_384},
	}

	worker, err := NewWorkerWithOptionsContext(context.Background(), tenantConfig, nil, redisClient, Options{
		Agent: &agentConfig,
		Model: &versionModel,
	})
	if err == nil {
		_ = worker.Close()
		t.Fatal("worker accepted a hard-budget version without immutable model limits")
	}
}
