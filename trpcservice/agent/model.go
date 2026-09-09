package agent

import (
	"errors"
	"fmt"
	"net/url"
	"os"

	openaiopt "github.com/openai/openai-go/option"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
	modelopenai "trpc.group/trpc-go/trpc-agent-go/model/openai"
)

// Supported model providers. A revision naming anything else fails to build:
// an unknown provider is a configuration the platform cannot execute, and
// falling back to some default would run the tenant's traffic against a model
// they did not ask for.
const (
	ProviderDeterministic    = "deterministic"
	ProviderOpenAICompatible = "openai-compatible"
)

// Headers openai-go's DefaultClientOptions fills from the process environment.
// Each is named here because it has to be removed, not merely left unset; see
// newOpenAICompatibleModel.
//
// authorizationHeader carries OPENAI_API_KEY. organizationHeader and
// projectHeader carry OPENAI_ORG_ID and OPENAI_PROJECT_ID, by way of
// option.WithOrganization and option.WithProject.
const (
	authorizationHeader = "Authorization"
	organizationHeader  = "OpenAI-Organization"
	projectHeader       = "OpenAI-Project"
)

// newModel builds the model for one immutable revision's ModelConfig.
//
// It is the single place that turns configuration into a model, so every
// provider gets the same treatment: validate first, construct second, and
// return an error rather than a half-configured model.
func newModel(config tenant.ModelConfig) (model.Model, error) {
	switch config.Provider {
	case ProviderDeterministic:
		return deterministicModel{name: config.Name}, nil
	case ProviderOpenAICompatible:
		return newOpenAICompatibleModel(config)
	default:
		return nil, fmt.Errorf("agent: unsupported model provider %q", config.Provider)
	}
}

// newOpenAICompatibleModel builds an upstream OpenAI-protocol model.
//
// base_url is required. The provider name says only "something that speaks the
// OpenAI protocol", so the endpoint is the part of the configuration that
// decides where a tenant's conversation content is sent. Left empty, the
// upstream client would quietly fall back to the process OPENAI_BASE_URL, or
// to api.openai.com — a revision whose destination is set by whatever the
// operator happened to export. It has to come from the revision.
//
// Because base_url is revision-controlled, every ambient value openai-go's
// DefaultClientOptions derives from the environment is a value a tenant can
// aim at a host they chose. The revision schema models none of them, so all
// three headers it can set are deleted:
//
//   - OpenAI-Organization, from OPENAI_ORG_ID
//   - OpenAI-Project, from OPENAI_PROJECT_ID
//   - Authorization, from OPENAI_API_KEY — only when secret_ref is empty
//
// The two metadata headers go on every request, keyed or not. They name the
// operator's OpenAI account, no ModelConfig field sets them, and an explicit
// secret_ref says which credential to send, not which organization to
// disclose. (OPENAI_WEBHOOK_SECRET is the remaining environment default and is
// not listed because it sets no request header.)
//
// Authorization is the conditional one. An empty secret_ref means keyless —
// never "borrow whatever credential the process happens to hold" — and passing
// no API key is not enough to get that, because both layers below fill an
// unset key from the environment: openai-go's DefaultClientOptions appends
// option.WithAPIKey(OPENAI_API_KEY) (client.go), and model/openai's New copies
// a variant's key env var into the request when the option is empty
// (openai.go). Nor can the key be cleared through the typed option:
// model/openai's New only forwards the API key when it is non-empty, so
// WithAPIKey("") is dropped and overrides nothing. The credential is removed
// one layer down instead, by deleting the header that carries it. An explicit
// secret_ref keeps its own resolved Authorization, and nothing else.
//
// Ordering makes the deletes reliable: WithOpenAIOptions is appended last by
// New, and openai-go applies request options in order, so they run after every
// default that may have set these headers.
//
// This is header suppression on this one client, not secret isolation. The
// process environment stays readable, and an explicit env:VAR_NAME ref is
// resolved for whichever revision names it.
//
// A keyless local endpoint — vLLM, Ollama, llama.cpp — is served by this, and
// an endpoint that does require a key answers 401 rather than misbehaving.
//
// The variant is deliberately not set: upstream infers it from base_url
// (DeepSeek, MiniMax and Kimi endpoints, else plain OpenAI), which is the
// behavior an "openai-compatible" provider wants.
func newOpenAICompatibleModel(config tenant.ModelConfig) (model.Model, error) {
	if err := validateBaseURL(config.BaseURL); err != nil {
		return nil, err
	}
	apiKey, err := resolveSecret(config.SecretRef)
	if err != nil {
		return nil, err
	}
	options := []modelopenai.Option{modelopenai.WithBaseURL(config.BaseURL)}
	// Unconditional: no revision field carries either value, so neither may be
	// sent to a revision-chosen endpoint.
	headerDeletes := []openaiopt.RequestOption{
		openaiopt.WithHeaderDel(organizationHeader),
		openaiopt.WithHeaderDel(projectHeader),
	}
	if apiKey != "" {
		options = append(options, modelopenai.WithAPIKey(apiKey))
	} else {
		headerDeletes = append(headerDeletes, openaiopt.WithHeaderDel(authorizationHeader))
	}
	options = append(options, modelopenai.WithOpenAIOptions(headerDeletes...))
	return modelopenai.New(config.Name, options...), nil
}

// validateBaseURL rejects an endpoint the platform should not dial.
//
// None of these errors repeat the value. url.Parse embeds its input in the
// error it returns, and base_url is the one non-secret field an operator can
// plausibly paste a credential into, so the value is described rather than
// echoed.
func validateBaseURL(raw string) error {
	if raw == "" {
		return fmt.Errorf(
			"agent: model base_url is required for provider %q", ProviderOpenAICompatible)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return errors.New("agent: model base_url is not a valid URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("agent: model base_url must use the http or https scheme")
	}
	if parsed.Host == "" {
		return errors.New("agent: model base_url must include a host")
	}
	if parsed.User != nil {
		// Credentials in the URL would be stored in the revision config, which
		// is plaintext and readable through the admin API.
		return errors.New("agent: model base_url must not embed credentials; use secret_ref")
	}
	return nil
}

// resolveSecret returns the credential named by ref, or "" when no ref is set.
//
// The syntax is not parsed here: secretref owns it, and the security manifest
// entitles refs through the same parser. Two parsers would be two sets of
// rules, and a ref that the entitlement check and the resolver read differently
// is a ref that is entitled as one variable and read as another.
//
// No error here contains the resolved value, and none repeats ref either: a ref
// that is not a reference is most likely a key that was pasted into the config
// by mistake, and an error message is exactly the wrong place for it to
// resurface.
//
// This runs after the RevisionAuthorizer, never before it — see
// newRuntimeFromRevision.
func resolveSecret(ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	name, err := secretref.EnvName(ref)
	if err != nil {
		return "", fmt.Errorf("agent: model secret_ref: %w", err)
	}
	// An exported-but-empty variable is treated as unset. It means the operator
	// asked for a credential that is not there, and continuing would send the
	// request unauthenticated under configuration that says otherwise.
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf(
			"agent: model secret_ref environment variable %q is unset or empty", name)
	}
	return value, nil
}

// generationConfig maps a revision's model settings onto generation
// parameters. Stream stays true: the protocol adapter serves SSE.
//
// Pointer fields are copied, never aliased, so a long-lived Runtime shares no
// mutable state with the revision value it was built from.
func generationConfig(config tenant.ModelConfig) model.GenerationConfig {
	generation := model.GenerationConfig{Stream: true}
	if config.Temperature != nil {
		temperature := *config.Temperature
		generation.Temperature = &temperature
	}
	// Zero means unset, matching the omitempty encoding; RevisionConfig.Validate
	// has already rejected negatives.
	if config.MaxTokens > 0 {
		maxTokens := config.MaxTokens
		generation.MaxTokens = &maxTokens
	}
	return generation
}
