package worker

import (
	"context"
	"testing"
	"time"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestOpenAIModelFactory(t *testing.T) {
	t.Setenv("MODEL_KEY_TENANT_A", "test-key")
	factory := OpenAIModelFactory{}
	instance, err := factory.Model(context.Background(), tenant.ModelConfig{
		Provider:  "openai-compatible",
		Model:     "test-model",
		APIKeyRef: "env:MODEL_KEY_TENANT_A",
		BaseURL:   "https://example.test/v1",
		Timeout:   3 * time.Second,
	})
	if err != nil {
		t.Fatalf("Model() error = %v", err)
	}
	if instance.Info().Name != "test-model" {
		t.Fatalf("model name = %q, want test-model", instance.Info().Name)
	}
}

func TestOpenAIModelFactoryRejectsUnsupportedProvider(t *testing.T) {
	_, err := (OpenAIModelFactory{}).Model(context.Background(), tenant.ModelConfig{
		Provider: "unsupported",
		Model:    "test-model",
	})
	if err == nil {
		t.Fatal("Model() accepted unsupported provider")
	}
}
