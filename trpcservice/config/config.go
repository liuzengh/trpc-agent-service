// Package config loads tenant, model, channel, and storage backend settings.
package config

import (
	"fmt"
	"os"

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

type modelYAML struct {
	Name    string `yaml:"name"`
	APIKey  string `yaml:"api_key"`
	BaseURL string `yaml:"base_url"`
}

type tenantYAML struct {
	ID    string    `yaml:"id"`
	Name  string    `yaml:"name"`
	Model modelYAML `yaml:"model"`
}

type fileYAML struct {
	DefaultTenant string       `yaml:"default_tenant"`
	Tenants       []tenantYAML `yaml:"tenants"`
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
	if len(f.Tenants) == 0 {
		return nil, fmt.Errorf("config %s: at least one tenant is required", path)
	}

	cfg := &Config{Tenants: make(map[string]*tenant.Context, len(f.Tenants))}
	for _, t := range f.Tenants {
		if t.ID == "" {
			return nil, fmt.Errorf("config %s: every tenant needs an id", path)
		}
		if _, dup := cfg.Tenants[t.ID]; dup {
			return nil, fmt.Errorf("config %s: duplicate tenant %q", path, t.ID)
		}
		if t.Model.APIKey == "" {
			return nil, fmt.Errorf("config %s: tenant %q: model.api_key is required", path, t.ID)
		}
		if t.Model.Name == "" {
			return nil, fmt.Errorf("config %s: tenant %q: model.name is required", path, t.ID)
		}
		cfg.Tenants[t.ID] = &tenant.Context{
			ID:   t.ID,
			Name: t.Name,
			Model: tenant.ModelConfig{
				Name:    t.Model.Name,
				APIKey:  t.Model.APIKey,
				BaseURL: t.Model.BaseURL,
			},
		}
	}

	cfg.DefaultTenant = f.DefaultTenant
	if cfg.DefaultTenant == "" {
		cfg.DefaultTenant = f.Tenants[0].ID
	}
	if _, ok := cfg.Tenants[cfg.DefaultTenant]; !ok {
		return nil, fmt.Errorf("config %s: default_tenant %q is not defined", path, cfg.DefaultTenant)
	}

	applyEnvOverride(cfg.Tenants[cfg.DefaultTenant])
	return cfg, nil
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
