package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	modelopenai "trpc.group/trpc-go/trpc-agent-go/model/openai"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// The deterministic provider is the bootstrap path and every other test in this
// package depends on it, so its construction is asserted on its own.
func TestNewModelBuildsDeterministicProvider(t *testing.T) {
	built, err := newModel(tenant.ModelConfig{
		Provider: ProviderDeterministic,
		Name:     "echo-test",
	})
	require.NoError(t, err)
	require.IsType(t, deterministicModel{}, built)
	require.Equal(t, "echo-test", built.Info().Name)
}

// An unknown provider must fail closed rather than fall back to a default.
func TestNewModelRejectsUnknownProvider(t *testing.T) {
	for _, provider := range []string{"", "openai", "anthropic", " deterministic"} {
		t.Run("provider="+provider, func(t *testing.T) {
			built, err := newModel(tenant.ModelConfig{Provider: provider, Name: "some-model"})
			require.Nil(t, built)
			require.ErrorContains(t, err, "unsupported model provider")
		})
	}
}

// The same rejection has to happen through the real build path, not only
// through the factory, so a revision naming an unsupported provider never
// produces a Runtime.
func TestNewRuntimeFromRevisionRejectsUnknownProvider(t *testing.T) {
	shared := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, shared.Close()) })

	revision := publishedRevision("revision-1", "gpt-4o-mini")
	revision.Config.Model.Provider = "anthropic"
	revision = sealed(revision)

	runtime, err := NewRuntimeFromRevision(revision, shared, entitling(t, revision))
	require.Nil(t, runtime)
	require.ErrorContains(t, err, `build model for revision "revision-1"`)
	require.ErrorContains(t, err, `unsupported model provider "anthropic"`)
}

func TestNewModelRejectsInvalidBaseURL(t *testing.T) {
	for name, testCase := range map[string]struct{ baseURL, wants string }{
		"empty":          {"", "base_url is required"},
		"no scheme":      {"127.0.0.1:8000/v1", "is not a valid URL"},
		"wrong scheme":   {"ftp://127.0.0.1/v1", "must use the http or https scheme"},
		"not a url":      {"not a url", "must use the http or https scheme"},
		"scheme only":    {"http://", "must include a host"},
		"unparseable":    {"http://[::1/v1", "is not a valid URL"},
		"relative path":  {"/v1", "must use the http or https scheme"},
		"embedded creds": {"https://user:sk-embedded-key@api.example.com/v1", "must not embed credentials"},
	} {
		t.Run(name, func(t *testing.T) {
			built, err := newModel(tenant.ModelConfig{
				Provider: ProviderOpenAICompatible,
				Name:     "test-model",
				BaseURL:  testCase.baseURL,
			})
			require.Nil(t, built)
			require.ErrorContains(t, err, testCase.wants)
			// url.Parse repeats its input, and a rejected base_url may be the
			// one an operator pasted a key into.
			require.NotContains(t, err.Error(), "sk-embedded-key")
		})
	}
}

// A secret_ref that names nothing resolvable must fail the build, and must not
// carry either the value or a mistakenly-inlined key into the error.
func TestNewModelRejectsUnresolvableSecretRef(t *testing.T) {
	t.Setenv("TRPC_SERVICE_TEST_EMPTY_KEY", "")

	for name, testCase := range map[string]struct{ secretRef, wants string }{
		"unset env":    {"env:TRPC_SERVICE_TEST_ABSENT_KEY", `"TRPC_SERVICE_TEST_ABSENT_KEY" is unset or empty`},
		"empty env":    {"env:TRPC_SERVICE_TEST_EMPTY_KEY", `"TRPC_SERVICE_TEST_EMPTY_KEY" is unset or empty`},
		"no var name":  {"env:", "names no environment variable"},
		"other scheme": {"vault:sk-inlined-key", "must use the env:VAR_NAME scheme"},
		"inlined key":  {"sk-inlined-key", "must use the env:VAR_NAME scheme"},
		// A key pasted in behind the scheme names no variable, and must be
		// rejected on its shape so it is never echoed back as a lookup failure.
		"key behind scheme": {"env:sk-inlined-key", "must name a valid environment variable"},
		"leading digit":     {"env:1KEY", "must name a valid environment variable"},
		"has equals":        {"env:KEY=VALUE", "must name a valid environment variable"},
		"has space":         {"env:MY KEY", "must name a valid environment variable"},
	} {
		t.Run(name, func(t *testing.T) {
			built, err := newModel(tenant.ModelConfig{
				Provider:  ProviderOpenAICompatible,
				Name:      "test-model",
				BaseURL:   "https://api.example.com/v1",
				SecretRef: testCase.secretRef,
			})
			require.Nil(t, built)
			require.ErrorContains(t, err, testCase.wants)
			require.NotContains(t, err.Error(), "sk-inlined-key")
		})
	}
}

// The value of a resolvable secret must not reach the error path either: a
// later failure (here, the base URL) must not report what was resolved.
func TestNewModelErrorNeverCarriesResolvedSecret(t *testing.T) {
	t.Setenv("TRPC_SERVICE_TEST_MODEL_KEY", "sk-resolved-value")

	built, err := newModel(tenant.ModelConfig{
		Provider:  ProviderOpenAICompatible,
		Name:      "test-model",
		BaseURL:   "ftp://api.example.com/v1",
		SecretRef: "env:TRPC_SERVICE_TEST_MODEL_KEY",
	})
	require.Nil(t, built)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "sk-resolved-value")
}

func TestGenerationConfigFromModelConfig(t *testing.T) {
	t.Run("carries temperature and max tokens", func(t *testing.T) {
		temperature := 0.25
		config := tenant.ModelConfig{Temperature: &temperature, MaxTokens: 321}

		generation := generationConfig(config)

		require.True(t, generation.Stream)
		require.NotNil(t, generation.Temperature)
		require.Equal(t, 0.25, *generation.Temperature)
		require.NotNil(t, generation.MaxTokens)
		require.Equal(t, 321, *generation.MaxTokens)
	})

	t.Run("copies rather than aliases the revision pointer", func(t *testing.T) {
		temperature := 0.25
		config := tenant.ModelConfig{Temperature: &temperature}

		generation := generationConfig(config)
		require.NotSame(t, &temperature, generation.Temperature)

		// A Runtime outlives the revision value it was built from; writing
		// through either pointer must not change the other.
		*generation.Temperature = 1.5
		require.Equal(t, 0.25, temperature)
	})

	t.Run("leaves unset fields nil", func(t *testing.T) {
		generation := generationConfig(tenant.ModelConfig{})

		require.True(t, generation.Stream)
		require.Nil(t, generation.Temperature)
		require.Nil(t, generation.MaxTokens)
	})
}

// This is the end-to-end assertion for the openai-compatible provider: a
// published revision builds a Runtime whose model dials the configured
// endpoint, authenticates with the resolved secret, and carries the revision's
// generation settings on the wire. It talks only to a local httptest server.
func TestOpenAICompatibleRuntimeCallsConfiguredEndpoint(t *testing.T) {
	t.Setenv("TEST_MODEL_API_KEY", "sk-resolved-value")
	// The ambient defaults are present but unreferenced: the revision's own
	// secret_ref is what authenticates, so stripping the inherited values must
	// not disturb the explicit path.
	setAmbientOpenAIEnv(t)

	upstream := newRecordingUpstream(t, "hello from upstream")

	temperature := 0.25
	revision := publishedRevision("revision-1", "test-model")
	revision.Config.Model = tenant.ModelConfig{
		Provider:    ProviderOpenAICompatible,
		Name:        "test-model",
		BaseURL:     upstream.server.URL + "/v1",
		SecretRef:   "env:TEST_MODEL_API_KEY",
		Temperature: &temperature,
		MaxTokens:   321,
	}
	revision = sealed(revision)

	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, sessionService.Close()) })
	runtime, err := NewRuntimeFromRevision(revision, sessionService, entitling(t, revision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	events, err := runtime.Runner.Run(
		context.Background(),
		"u/test-user",
		"c/test-session",
		model.NewUserMessage("ping"),
		trpcagent.WithRequestID("request-1"),
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
	require.Contains(t, answer.String(), "hello from upstream")

	call := upstream.lastCall()
	require.Equal(t, "/v1/chat/completions", call.path)
	// The resolved env secret, not the ref, reaches the endpoint — and it is
	// the only ambient-derived value that does.
	require.Equal(t, "Bearer sk-resolved-value", call.authorization)
	require.NotContains(t, call.headerDump(), ambientAPIKey)
	requireNoAmbientMetadata(t, call)
	require.Equal(t, "test-model", call.body["model"])
	require.Equal(t, true, call.body["stream"])
	require.Equal(t, 0.25, call.body["temperature"])
	// Upstream sends max_tokens as max_completion_tokens for the OpenAI variant.
	require.Equal(t, float64(321), call.body["max_completion_tokens"])
}

// An empty secret_ref means keyless, and keyless must mean no credential —
// not the process default. base_url belongs to the revision, so a tenant that
// can publish one chooses where requests go; if an unset secret_ref inherited
// the ambient OPENAI_API_KEY, publishing a revision would be enough to read an
// operator's credential off any endpoint the tenant controls.
//
// The endpoint here stands in for that attacker-controlled host: every ambient
// default is exported, and none may reach it.
func TestOpenAICompatibleRuntimeWithoutSecretRefSendsNoCredential(t *testing.T) {
	setAmbientOpenAIEnv(t)

	upstream := newRecordingUpstream(t, "hello from upstream")

	revision := publishedRevision("revision-1", "test-model")
	revision.Config.Model = tenant.ModelConfig{
		Provider: ProviderOpenAICompatible,
		Name:     "test-model",
		BaseURL:  upstream.server.URL + "/v1",
	}
	revision = sealed(revision)

	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, sessionService.Close()) })
	runtime, err := NewRuntimeFromRevision(revision, sessionService, entitling(t, revision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	events, err := runtime.Runner.Run(
		context.Background(),
		"u/test-user",
		"c/test-session",
		model.NewUserMessage("ping"),
		trpcagent.WithRequestID("request-1"),
	)
	require.NoError(t, err)
	for range events { //nolint:revive // drain the run before asserting.
	}

	call := upstream.lastCall()
	// No Authorization header at all, rather than an empty or placeholder one.
	require.Empty(t, call.authorization)
	require.False(t, call.hasHeader(authorizationHeader), "headers:\n%s", call.headerDump())
	// The ambient key must not have reached the endpoint by any other header
	// either, so the whole recorded request is checked for the value.
	require.NotContains(t, call.headerDump(), ambientAPIKey)
	requireNoAmbientMetadata(t, call)
	// base_url still comes from the revision, never from the process env.
	require.Equal(t, "/v1/chat/completions", call.path)
}

// Control for the two tests above. Their central assertion is that headers are
// absent, and an absence proves nothing unless the same environment would
// otherwise produce them — if openai-go stopped deriving these headers, or the
// env vars were misspelled here, those tests would still pass while covering
// nothing.
//
// So this builds the same upstream model directly, without
// newOpenAICompatibleModel, and asserts all three headers do arrive. It is the
// only test in this package that expects an ambient value on the wire.
func TestAmbientOpenAIEnvWouldOtherwiseReachTheEndpoint(t *testing.T) {
	setAmbientOpenAIEnv(t)

	upstream := newRecordingUpstream(t, "hello from upstream")

	undefended := modelopenai.New("test-model",
		modelopenai.WithBaseURL(upstream.server.URL+"/v1"))
	responses, err := undefended.GenerateContent(context.Background(), &model.Request{
		Messages:         []model.Message{model.NewUserMessage("ping")},
		GenerationConfig: model.GenerationConfig{Stream: true},
	})
	require.NoError(t, err)
	for range responses { //nolint:revive // drain the stream before asserting.
	}

	call := upstream.lastCall()
	require.Equal(t, "Bearer "+ambientAPIKey, call.authorization)
	require.Equal(t, ambientOrgID, call.headers.Get(organizationHeader))
	require.Equal(t, ambientProjectID, call.headers.Get(projectHeader))
}

// Ambient process defaults openai-go's DefaultClientOptions turns into request
// headers. Values are distinct so headerDump can tell them apart.
const (
	ambientAPIKey    = "sk-ambient-must-not-leak"
	ambientOrgID     = "org-ambient-must-not-leak"
	ambientProjectID = "proj-ambient-must-not-leak"
)

// setAmbientOpenAIEnv exports every environment default openai-go derives a
// header from. Each endpoint test sets all of them, so "this header is absent"
// is a statement about the code rather than about a variable that happened to
// be unset in the test process.
//
// OPENAI_WEBHOOK_SECRET is the remaining default and is deliberately not set:
// it sets no request header, so there is nothing here to assert about it.
func setAmbientOpenAIEnv(t *testing.T) {
	t.Helper()
	t.Setenv("OPENAI_API_KEY", ambientAPIKey)
	t.Setenv("OPENAI_ORG_ID", ambientOrgID)
	t.Setenv("OPENAI_PROJECT_ID", ambientProjectID)
}

// requireNoAmbientMetadata asserts the headers built from OPENAI_ORG_ID and
// OPENAI_PROJECT_ID reached the endpoint under neither their name nor their
// value. Unlike Authorization these are unconditional: no ModelConfig field
// models either one, so both callers assert this — the empty secret_ref path
// and the explicit one alike.
func requireNoAmbientMetadata(t *testing.T, call upstreamCall) {
	t.Helper()
	for _, header := range []string{organizationHeader, projectHeader} {
		require.False(t, call.hasHeader(header), "%s present in:\n%s", header, call.headerDump())
		require.Empty(t, call.headers.Get(header))
	}
	require.NotContains(t, call.headerDump(), ambientOrgID)
	require.NotContains(t, call.headerDump(), ambientProjectID)
}

// recordingUpstream is a local OpenAI-compatible endpoint. It records what the
// model actually sent and answers with a minimal SSE completion, so the
// assertions above need no network and no live provider.
type recordingUpstream struct {
	server *httptest.Server

	mu    sync.Mutex
	calls []upstreamCall
}

type upstreamCall struct {
	path          string
	authorization string
	headers       http.Header
	body          map[string]any
}

// hasHeader reports whether the endpoint received a header with this name, so
// a test can assert a header was absent rather than merely empty.
//
// The comparison is case-insensitive on purpose. net/http canonicalizes header
// keys, so the recorded map holds "Openai-Organization"; a test scanning those
// keys for the literal "OpenAI-Organization" would find nothing and pass even
// when the header did arrive.
func (c upstreamCall) hasHeader(name string) bool {
	for received := range c.headers {
		if strings.EqualFold(received, name) {
			return true
		}
	}
	return false
}

// headerDump renders every received header, so a test can assert a credential
// arrived through no header at all rather than only through Authorization.
func (c upstreamCall) headerDump() string {
	var dump strings.Builder
	for name, values := range c.headers {
		for _, value := range values {
			dump.WriteString(name)
			dump.WriteString(": ")
			dump.WriteString(value)
			dump.WriteString("\n")
		}
	}
	return dump.String()
}

func newRecordingUpstream(t *testing.T, reply string) *recordingUpstream {
	t.Helper()
	upstream := &recordingUpstream{}
	upstream.server = httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			var body map[string]any
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			upstream.mu.Lock()
			upstream.calls = append(upstream.calls, upstreamCall{
				path:          request.URL.Path,
				authorization: request.Header.Get(authorizationHeader),
				headers:       request.Header.Clone(),
				body:          body,
			})
			upstream.mu.Unlock()

			writer.Header().Set("Content-Type", "text/event-stream")
			writer.WriteHeader(http.StatusOK)
			writeChunk := func(payload string) {
				_, err := writer.Write([]byte("data: " + payload + "\n\n"))
				require.NoError(t, err)
			}
			writeChunk(`{"id":"chunk-1","object":"chat.completion.chunk",` +
				`"created":1,"model":"test-model","choices":[{"index":0,` +
				`"delta":{"role":"assistant","content":` + quote(reply) + `}}]}`)
			writeChunk(`{"id":"chunk-1","object":"chat.completion.chunk",` +
				`"created":1,"model":"test-model","choices":[{"index":0,` +
				`"delta":{},"finish_reason":"stop"}]}`)
			writeChunk("[DONE]")
		}))
	t.Cleanup(upstream.server.Close)
	return upstream
}

func (u *recordingUpstream) lastCall() upstreamCall {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.calls) == 0 {
		return upstreamCall{}
	}
	return u.calls[len(u.calls)-1]
}

func quote(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `""`
	}
	return string(encoded)
}
