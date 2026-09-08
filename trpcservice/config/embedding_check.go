package config

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// EmbeddingCheckConfig is only for the explicit preflight command, not a global
// runtime Knowledge configuration. Tenant revisions still select their own
// embedding model and require a purpose=embedding Secret grant.
type EmbeddingCheckConfig struct {
	Model      string
	BaseURL    string
	APIKey     string // private; never print or serialize this configuration
	Dimensions int
}

func LoadEmbeddingCheckConfigFromEnv() (EmbeddingCheckConfig, error) {
	c := EmbeddingCheckConfig{
		Model:   strings.TrimSpace(os.Getenv("TRPC_AGENT_EMBEDDING_MODEL")),
		BaseURL: strings.TrimSpace(os.Getenv("TRPC_AGENT_EMBEDDING_BASE_URL")),
		APIKey:  strings.TrimSpace(os.Getenv("TRPC_AGENT_EMBEDDING_API_KEY")),
	}
	if c.Model == "" || len(c.Model) > 256 || strings.IndexFunc(c.Model, unicode.IsControl) >= 0 {
		return c, errors.New("TRPC_AGENT_EMBEDDING_MODEL must contain a valid embedding model ID")
	}
	if c.APIKey == "" || len(c.APIKey) > 8192 || strings.IndexFunc(c.APIKey, unicode.IsControl) >= 0 {
		return c, errors.New("TRPC_AGENT_EMBEDDING_API_KEY is required; chat credentials are not reused")
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return c, errors.New("TRPC_AGENT_EMBEDDING_BASE_URL must explicitly specify an HTTP(S) API root without credentials, query or fragment")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return c, errors.New("TRPC_AGENT_EMBEDDING_BASE_URL has an invalid port")
		}
	}
	c.Dimensions, err = strconv.Atoi(strings.TrimSpace(os.Getenv("TRPC_AGENT_EMBEDDING_DIMENSIONS")))
	if err != nil || c.Dimensions < 1 || c.Dimensions > 65536 {
		return c, errors.New("TRPC_AGENT_EMBEDDING_DIMENSIONS must be the model output dimension, between 1 and 65536")
	}
	return c, nil
}
