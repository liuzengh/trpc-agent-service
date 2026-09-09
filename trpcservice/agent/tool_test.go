package agent

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/stretchr/testify/require"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// tool_refs decides what the Runtime assembles, and the proof is what reaches
// the provider: the tools the revision named, under the names it named them by,
// in that order.
func TestRuntimeSendsResolvedToolsToTheModel(t *testing.T) {
	upstream := newRecordingUpstream(t, "hello from upstream")
	revision := toolRevision(
		upstream.server.URL,
		[]string{servicetool.RefAdd, servicetool.RefEcho},
		[]string{servicetool.PolicySafeTools},
	)

	runOneTurn(t, buildRuntime(t, revision), "ping")

	call := upstream.lastCall()
	require.Equal(t,
		[]string{servicetool.RefAdd, servicetool.RefEcho}, toolFunctionNames(t, call.body))

	declared := call.body["tools"].([]any)[0].(map[string]any)
	require.Equal(t, "function", declared["type"])
	function := declared["function"].(map[string]any)
	require.NotEmpty(t, function["description"])
	// The model is told how to call the tool, not merely that it exists.
	parameters := function["parameters"].(map[string]any)
	require.Contains(t, parameters["properties"], "a")
	require.Contains(t, parameters["properties"], "b")
}

// The compatibility guarantee for every revision published before tools
// existed: it names no tools, so the provider is offered none, and the request
// is the request it always was.
func TestRuntimeWithoutToolRefsSendsNoTools(t *testing.T) {
	upstream := newRecordingUpstream(t, "hello from upstream")

	runOneTurn(t, buildRuntime(t, toolRevision(upstream.server.URL, nil, nil)), "ping")

	require.NotContains(t, upstream.lastCall().body, "tools")
}

// A known policy that authorizes nothing is not an error, and a revision that
// names one without naming a tool still offers the model nothing.
func TestRuntimeWithPolicyButNoToolRefsSendsNoTools(t *testing.T) {
	upstream := newRecordingUpstream(t, "hello from upstream")
	revision := toolRevision(
		upstream.server.URL, nil, []string{servicetool.PolicySafeTools})

	runOneTurn(t, buildRuntime(t, revision), "ping")

	require.NotContains(t, upstream.lastCall().body, "tools")
}

// Every way a revision can fail to authorize its tools, asserted through the
// real build path. None of these may produce a Runtime with a reduced tool set:
// an agent that silently runs with fewer tools than its revision names is a
// different agent than the one that was published.
func TestNewRuntimeFromRevisionFailsClosedOnToolsAndPolicies(t *testing.T) {
	for name, testCase := range map[string]struct {
		toolRefs   []string
		policyRefs []string
		wantErr    error
	}{
		"unknown tool": {
			[]string{"builtin_shell"}, []string{servicetool.PolicySafeTools},
			servicetool.ErrUnknownTool,
		},
		"duplicate tool": {
			[]string{servicetool.RefEcho, servicetool.RefEcho},
			[]string{servicetool.PolicySafeTools},
			servicetool.ErrDuplicateRef,
		},
		"tools without policy": {
			[]string{servicetool.RefEcho}, nil,
			servicetool.ErrPolicyRequired,
		},
		"unknown policy": {
			[]string{servicetool.RefEcho}, []string{"builtin.everything"},
			servicetool.ErrUnknownPolicy,
		},
		// Fails even though it authorizes nothing, so a policy that is removed
		// from the binary cannot go unnoticed.
		"unknown policy without tools": {
			nil, []string{"builtin.everything"},
			servicetool.ErrUnknownPolicy,
		},
		"duplicate policy": {
			nil, []string{servicetool.PolicySafeTools, servicetool.PolicySafeTools},
			servicetool.ErrDuplicateRef,
		},
	} {
		t.Run(name, func(t *testing.T) {
			shared := sessioninmemory.NewSessionService()
			t.Cleanup(func() { require.NoError(t, shared.Close()) })

			revision := publishedRevision("revision-1", "echo-v1")
			revision.Config.ToolRefs = testCase.toolRefs
			revision.Config.PolicyRefs = testCase.policyRefs
			revision = sealed(revision)

			// Entitled to every ref it names, so what is left to refuse it is
			// the tool registry — which is the point: entitlement narrows what
			// a tenant may reach, it never stands in for tool authorization.
			runtime, err := NewRuntimeFromRevision(revision, shared, entitling(t, revision))
			require.Nil(t, runtime)
			require.ErrorIs(t, err, testCase.wantErr)
			// Enough to locate the revision, and nothing more.
			require.ErrorContains(t, err, `assemble tools for revision "revision-1"`)
		})
	}
}

// Authorization runs before anything is constructed. A revision the platform
// refuses must not reach a model, a Runner or a protocol adapter — and must not
// cause a credential to be resolved on the way to being refused.
func TestToolValidationPrecedesModelConstruction(t *testing.T) {
	shared := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, shared.Close()) })

	t.Run("before the model provider is resolved", func(t *testing.T) {
		revision := publishedRevision("revision-1", "echo-v1")
		revision.Config.ToolRefs = []string{"builtin_shell"}
		revision.Config.PolicyRefs = []string{servicetool.PolicySafeTools}
		revision.Config.Model.Provider = "anthropic"
		revision = sealed(revision)

		_, err := NewRuntimeFromRevision(revision, shared, entitling(t, revision))
		require.ErrorIs(t, err, servicetool.ErrUnknownTool)
		require.NotContains(t, err.Error(), "unsupported model provider")
	})

	t.Run("before a secret is resolved", func(t *testing.T) {
		revision := publishedRevision("revision-1", "test-model")
		revision.Config.ToolRefs = []string{servicetool.RefEcho}
		revision.Config.PolicyRefs = nil
		revision.Config.Model = tenant.ModelConfig{
			Provider:  ProviderOpenAICompatible,
			Name:      "test-model",
			BaseURL:   "https://api.example.com/v1",
			SecretRef: "env:TEST_ABSENT_MODEL_KEY",
		}
		revision = sealed(revision)

		_, err := NewRuntimeFromRevision(revision, shared, entitling(t, revision))
		require.ErrorIs(t, err, servicetool.ErrPolicyRequired)
		require.NotContains(t, err.Error(), "TEST_ABSENT_MODEL_KEY")
	})
}

// toolRevision publishes a revision that talks to a local endpoint and names
// the given tools and policies.
func toolRevision(
	serverURL string,
	toolRefs []string,
	policyRefs []string,
) tenant.AgentRevision {
	revision := publishedRevision("revision-1", "test-model")
	revision.Config.ToolRefs = toolRefs
	revision.Config.PolicyRefs = policyRefs
	revision.Config.Model = tenant.ModelConfig{
		Provider: ProviderOpenAICompatible,
		Name:     "test-model",
		BaseURL:  serverURL + "/v1",
	}
	return sealed(revision)
}

func buildRuntime(t *testing.T, revision tenant.AgentRevision) *Runtime {
	t.Helper()
	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, sessionService.Close()) })
	runtime, err := NewRuntimeFromRevision(revision, sessionService, entitling(t, revision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })
	return runtime
}

func runOneTurn(t *testing.T, runtime *Runtime, prompt string) {
	t.Helper()
	events, err := runtime.Runner.Run(
		context.Background(),
		"u/test-user",
		"c/test-session",
		model.NewUserMessage(prompt),
		trpcagent.WithRequestID("request-1"),
	)
	require.NoError(t, err)
	for range events { //nolint:revive // drain the run before asserting.
	}
}

// toolFunctionNames reads back the function names offered to the provider, in
// the order they were sent.
func toolFunctionNames(t *testing.T, body map[string]any) []string {
	t.Helper()
	declared, ok := body["tools"].([]any)
	require.True(t, ok, "request carried no tools: %v", body)
	names := make([]string, 0, len(declared))
	for _, entry := range declared {
		function, ok := entry.(map[string]any)["function"].(map[string]any)
		require.True(t, ok, "tool entry has no function: %v", entry)
		names = append(names, function["name"].(string))
	}
	return names
}
