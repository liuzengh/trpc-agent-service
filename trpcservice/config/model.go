package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const (
	// ModelProviderMock selects the deterministic local tutorial model.
	ModelProviderMock = "mock"
	// ModelProviderOpenAI selects tRPC-Agent-Go's OpenAI-compatible model.
	ModelProviderOpenAI = "openai"
)

// ModelConfig contains the model settings needed by the tutorial runtime.
// APIKey must never be logged or serialized into HTTP responses.
type ModelConfig struct {
	Provider string
	Name     string
	BaseURL  string
	APIKey   string
	Stream   bool
}

// LoadModelConfigFromEnv reads model settings from environment variables.
func LoadModelConfigFromEnv() (ModelConfig, error) {
	provider := strings.ToLower(strings.TrimSpace(os.Getenv("TRPC_AGENT_MODEL_PROVIDER")))
	if provider == "" {
		provider = ModelProviderMock
	}

	config := ModelConfig{
		Provider: provider,
		Name:     strings.TrimSpace(os.Getenv("TRPC_AGENT_MODEL_NAME")),
		BaseURL:  strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")),
		APIKey:   strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		Stream:   false,
	}

	if raw := strings.TrimSpace(os.Getenv("TRPC_AGENT_MODEL_STREAM")); raw != "" {
		stream, err := strconv.ParseBool(raw)
		if err != nil {
			return ModelConfig{}, fmt.Errorf(
				"parse TRPC_AGENT_MODEL_STREAM: %w",
				err,
			)
		}
		config.Stream = stream
	}

	switch config.Provider {
	case ModelProviderMock:
		return config, nil
	case ModelProviderOpenAI:
		if config.Name == "" {
			return ModelConfig{}, fmt.Errorf(
				"TRPC_AGENT_MODEL_NAME is required when model provider is openai",
			)
		}
		if config.APIKey == "" {
			return ModelConfig{}, fmt.Errorf(
				"OPENAI_API_KEY is required when model provider is openai",
			)
		}
		if err := validateBaseURL(config.BaseURL); err != nil {
			return ModelConfig{}, err
		}
		return config, nil
	default:
		return ModelConfig{}, fmt.Errorf(
			"unsupported TRPC_AGENT_MODEL_PROVIDER %q: use mock or openai",
			config.Provider,
		)
	}
}

func validateBaseURL(raw string) error {
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse OPENAI_BASE_URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("OPENAI_BASE_URL must use http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("OPENAI_BASE_URL must include a host")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf(
			"OPENAI_BASE_URL must not include user info, query parameters, or fragments",
		)
	}
	return nil
}
