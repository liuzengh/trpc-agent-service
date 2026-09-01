// Package agent hosts tenant-specific agents built on tRPC-Agent-Go
// (llmagent, graph, chain/parallel/cycle) and runner.Runner.
package agent

import (
	"fmt"
	"os"

	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// Model config selects the LLM backing the agent. At this stage it is read
// from environment variables; it will be moved into per-tenant model config
// (trpcservice/config + trpcservice/tenant) once the tenant base lands.
const (
	envAPIKey  = "MODEL_API_KEY"  // required, key of the OpenAI-compatible service
	envModel   = "MODEL_NAME"     // optional, e.g. "gpt-4o-mini", "deepseek-chat"
	envBaseURL = "MODEL_BASE_URL" // optional, OpenAI-compatible endpoint
)

const (
	appName      = "trpc-agent-service"
	agentName    = "assistant"
	defaultModel = "gpt-4o-mini"
)

// NewRunner builds the minimal walking-skeleton runner: one LLMAgent backed
// by an OpenAI-compatible model, streaming output, in-memory session backend.
// The session service is intentionally in-memory here and will be replaced by
// tenant-selected shared backends (redis/mysql/postgres) via the Storage
// Adapter later.
func NewRunner() (runner.Runner, error) {
	apiKey := os.Getenv(envAPIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("environment %s is required", envAPIKey)
	}

	modelName := os.Getenv(envModel)
	if modelName == "" {
		modelName = defaultModel
	}

	modelOpts := []openai.Option{openai.WithAPIKey(apiKey)}
	if baseURL := os.Getenv(envBaseURL); baseURL != "" {
		modelOpts = append(modelOpts, openai.WithBaseURL(baseURL))
	}
	llm := openai.New(modelName, modelOpts...)

	a := llmagent.New(agentName,
		llmagent.WithModel(llm),
		llmagent.WithInstruction("You are a helpful assistant."),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: true}),
	)

	return runner.NewRunner(appName, a,
		runner.WithSessionService(inmemory.NewSessionService()),
	), nil
}
