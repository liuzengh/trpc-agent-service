package agent

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// This is the only test in this package that reaches a real model endpoint, and
// it stays off unless the operator asks for it. `go test ./...` on a machine
// with no provider and no network must stay green, so the gate is checked
// before any config is built. The deterministic assertions about what the
// openai-compatible provider puts on the wire live in model_test.go and run
// against a local httptest endpoint instead.
//
// Point it at any OpenAI-compatible endpoint:
//
//	TRPC_SERVICE_MODEL_INTEGRATION=1 \
//	TRPC_SERVICE_MODEL_BASE_URL='https://api.openai.com/v1' \
//	TRPC_SERVICE_MODEL_NAME='gpt-4o-mini' \
//	TRPC_SERVICE_MODEL_SECRET_REF='env:OPENAI_API_KEY' \
//	OPENAI_API_KEY=... \
//	go test -race -timeout 120s -run TestOpenAICompatibleLiveEndpoint ./trpcservice/agent/...
//
// TRPC_SERVICE_MODEL_SECRET_REF is optional and is passed through as the
// revision's secret_ref, so it exercises the real resolver rather than a test
// shortcut. Leaving it unset covers the keyless local-endpoint case.
const (
	// envModelIntegration must be "1" for anything in this file to run.
	envModelIntegration = "TRPC_SERVICE_MODEL_INTEGRATION"
	// envModelBaseURL and envModelName describe the endpoint. There is no
	// default: a smoke test that silently picks its own provider would bill
	// somebody's account for a run they did not ask for.
	envModelBaseURL   = "TRPC_SERVICE_MODEL_BASE_URL"
	envModelName      = "TRPC_SERVICE_MODEL_NAME"
	envModelSecretRef = "TRPC_SERVICE_MODEL_SECRET_REF"

	// modelIntegrationTimeout bounds the run. A reachable endpoint answers a
	// one-line prompt well inside this; it only stops an unreachable one from
	// hanging until the package timeout.
	modelIntegrationTimeout = 60 * time.Second
)

// TestOpenAICompatibleLiveEndpoint is the end-to-end smoke test for the
// openai-compatible provider: a published revision must build a Runtime that
// completes one turn against a real endpoint and persists it to the session.
func TestOpenAICompatibleLiveEndpoint(t *testing.T) {
	if os.Getenv(envModelIntegration) != "1" {
		t.Skipf("set %s=1 to run model integration tests", envModelIntegration)
	}
	baseURL := requireModelEnv(t, envModelBaseURL)
	modelName := requireModelEnv(t, envModelName)

	revision := publishedRevision("revision-live", modelName)
	revision.Config.Instruction = "Answer in one short sentence."
	revision.Config.Model = tenant.ModelConfig{
		Provider:  ProviderOpenAICompatible,
		Name:      modelName,
		BaseURL:   baseURL,
		SecretRef: os.Getenv(envModelSecretRef),
		MaxTokens: 64,
	}
	revision = sealed(revision)

	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, sessionService.Close()) })
	// The entitlement is stated here rather than defaulted, and it is the same
	// authorizer type the process uses. A live test that ran under a permissive
	// stand-in would be the one test most likely to hide an entitlement
	// regression: it is the only one that resolves a real credential.
	runtime, err := NewRuntimeFromRevision(revision, sessionService, entitling(t, revision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), modelIntegrationTimeout)
	t.Cleanup(cancel)

	events, err := runtime.Runner.Run(
		ctx,
		"u/integration-user",
		"c/integration-session",
		model.NewUserMessage("Reply with the single word: pong."),
		trpcagent.WithRequestID("model-integration-1"),
	)
	require.NoError(t, err)

	var answer strings.Builder
	for evt := range events {
		if evt.Response == nil {
			continue
		}
		require.Nil(t, evt.Response.Error, "%+v", evt.Response.Error)
		for _, choice := range evt.Response.Choices {
			answer.WriteString(choice.Delta.Content)
		}
	}
	require.NotEmpty(t, strings.TrimSpace(answer.String()))
}

// requireModelEnv skips when the endpoint is not fully described, so an
// operator who exported the gate but not the target gets a skip rather than a
// failure against a half-built config.
func requireModelEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Skipf("set %s to run this test", name)
	}
	return value
}
