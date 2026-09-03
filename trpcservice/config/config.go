// Package config loads tenant, model, channel, and storage backend settings.
package config

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// DefaultPath is the config file looked up in the working directory.
// The real file is gitignored; commit only config.example.yaml.
const DefaultPath = "config.yaml"

// Environment variables overriding the default tenant's model settings, kept
// for the pre-config-file development workflow.
const (
	envAPIKey  = "MODEL_API_KEY"
	envModel   = "MODEL_NAME"
	envBaseURL = "MODEL_BASE_URL"
)

// fileYAML is the on-disk shape; tenant.Context carries the yaml tags so
// Load and Save round-trip through the same schema.
type fileYAML struct {
	DefaultTenant string           `yaml:"default_tenant,omitempty"`
	Tenants       []tenant.Context `yaml:"tenants"`
}

// Config is the loaded and validated platform configuration.
type Config struct {
	DefaultTenant string
	Tenants       map[string]*tenant.Context
}

// Load reads the YAML config at path. If the file does not exist it falls
// back to a single "default" tenant built from MODEL_* environment
// variables, preserving the export-based development workflow.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return fromEnv(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var f fileYAML
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	cfg := &Config{Tenants: make(map[string]*tenant.Context, len(f.Tenants))}
	for i := range f.Tenants {
		t := f.Tenants[i]
		if t.ID == "" {
			return nil, fmt.Errorf("config %s: every tenant needs an id", path)
		}
		if _, dup := cfg.Tenants[t.ID]; dup {
			return nil, fmt.Errorf("config %s: duplicate tenant %q", path, t.ID)
		}
		cfg.Tenants[t.ID] = &t
	}
	cfg.DefaultTenant = f.DefaultTenant
	if cfg.DefaultTenant == "" && len(f.Tenants) > 0 {
		cfg.DefaultTenant = f.Tenants[0].ID
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	applyEnvOverride(cfg.Tenants[cfg.DefaultTenant])
	return cfg, nil
}

// Validate checks the invariants shared by Load, Save, and the Admin API:
// at least one tenant, complete model settings, complete wecom bindings,
// and an existing default tenant.
func (c *Config) Validate() error {
	if len(c.Tenants) == 0 {
		return fmt.Errorf("at least one tenant is required")
	}
	for id, t := range c.Tenants {
		if t.Model.APIKey == "" {
			return fmt.Errorf("tenant %q: model.api_key is required", id)
		}
		if t.Model.Name == "" {
			return fmt.Errorf("tenant %q: model.name is required", id)
		}
		if err := validateWeCom(id, t.Channels.WeCom); err != nil {
			return err
		}
	}
	if _, ok := c.Tenants[c.DefaultTenant]; !ok {
		return fmt.Errorf("default_tenant %q is not defined", c.DefaultTenant)
	}
	return nil
}

// Save atomically persists cfg to path (tmp file + rename), so a partial
// write can never leave an unloadable config behind. Tenants are written in
// sorted order for stable diffs.
func Save(path string, cfg *Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	ids := make([]string, 0, len(cfg.Tenants))
	for id := range cfg.Tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	f := fileYAML{DefaultTenant: cfg.DefaultTenant}
	for _, id := range ids {
		f.Tenants = append(f.Tenants, *cfg.Tenants[id])
	}
	data, err := yaml.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}

// validateWeCom fails fast on half-filled channel bindings: a declared
// wecom block must be complete, otherwise the callback would 403 at runtime
// with a much less obvious error.
func validateWeCom(tenantID string, w *tenant.WeComBinding) error {
	if w == nil {
		return nil
	}
	if w.CorpID == "" || w.CorpSecret == "" || w.AgentID <= 0 || w.Token == "" || w.EncodingAESKey == "" {
		return fmt.Errorf("tenant %q: channels.wecom is incomplete (corp_id, corp_secret, agent_id, token, encoding_aes_key are all required)", tenantID)
	}
	if len(w.EncodingAESKey) != 43 {
		return fmt.Errorf("tenant %q: channels.wecom.encoding_aes_key must be 43 characters", tenantID)
	}
	return nil
}

// WeComBinding returns the WeCom app binding of tenantID. Tenants without a
// binding reject wecom callbacks with a clear error.
func (c *Config) WeComBinding(tenantID string) (*tenant.WeComBinding, bool) {
	if t, ok := c.Tenants[tenantID]; ok && t.Channels.WeCom != nil {
		return t.Channels.WeCom, true
	}
	return nil, false
}

// fromEnv builds the legacy single-tenant config from environment variables.
func fromEnv(path string) (*Config, error) {
	apiKey := os.Getenv(envAPIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("no config at %s and %s unset; copy config.example.yaml to %s or export %s",
			path, envAPIKey, path, envAPIKey)
	}
	name := os.Getenv(envModel)
	if name == "" {
		name = "gpt-4o-mini"
	}
	t := &tenant.Context{
		ID:   "default",
		Name: "default",
		Model: tenant.ModelConfig{
			Name:    name,
			APIKey:  apiKey,
			BaseURL: os.Getenv(envBaseURL),
		},
	}
	return &Config{
		DefaultTenant: t.ID,
		Tenants:       map[string]*tenant.Context{t.ID: t},
	}, nil
}

// applyEnvOverride lets MODEL_* variables override the default tenant's
// model settings for local development convenience.
func applyEnvOverride(t *tenant.Context) {
	if v := os.Getenv(envAPIKey); v != "" {
		t.Model.APIKey = v
	}
	if v := os.Getenv(envModel); v != "" {
		t.Model.Name = v
	}
	if v := os.Getenv(envBaseURL); v != "" {
		t.Model.BaseURL = v
	}
}
