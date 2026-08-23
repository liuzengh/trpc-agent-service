// Package config loads tenant, model, channel, and storage backend settings.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"gopkg.in/yaml.v3"
)

type Config struct {
	HTTPAddr     string              `json:"http_addr" yaml:"http_addr"`
	Database     Database            `json:"database" yaml:"database"`
	ModelDefault tenant.ModelProfile `json:"model_defaults" yaml:"model_defaults"`
	Tenants      []tenant.Tenant     `json:"tenants" yaml:"tenants"`
}

type Database struct {
	PostgresDSN   string `json:"postgres_dsn" yaml:"postgres_dsn"`
	RedisAddr     string `json:"redis_addr" yaml:"redis_addr"`
	QueueName     string `json:"queue_name" yaml:"queue_name"`
	ConsumerGroup string `json:"consumer_group" yaml:"consumer_group"`
}

func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	applyEnv(&cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func applyEnv(cfg *Config) {
	if v := os.Getenv("TRPC_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	if v := os.Getenv("TRPC_POSTGRES_DSN"); v != "" {
		cfg.Database.PostgresDSN = v
	}
	if v := os.Getenv("TRPC_REDIS_ADDR"); v != "" {
		cfg.Database.RedisAddr = v
	}
}

func (c Config) Validate() error {
	if c.HTTPAddr == "" {
		return errors.New("http_addr is required")
	}
	if len(c.Tenants) == 0 {
		return errors.New("at least one tenant is required")
	}
	seen := make(map[string]struct{}, len(c.Tenants))
	for i := range c.Tenants {
		t := &c.Tenants[i]
		if t.Model.Provider == "" {
			t.Model = c.ModelDefault
		}
		if t.Model.Timeout == "" {
			t.Model.Timeout = c.ModelDefault.Timeout
		}
		if t.Model.Timeout != "" {
			if _, err := time.ParseDuration(t.Model.Timeout); err != nil {
				return fmt.Errorf("tenant %s model timeout: %w", t.ID, err)
			}
		}
		if err := t.Validate(); err != nil {
			return fmt.Errorf("tenant[%d]: %w", i, err)
		}
		if _, ok := seen[t.ID]; ok {
			return fmt.Errorf("duplicate tenant id %q", t.ID)
		}
		seen[t.ID] = struct{}{}
	}
	return nil
}

func (c Config) TenantByID(id string) (tenant.Tenant, bool) {
	for _, item := range c.Tenants {
		if item.ID == id {
			if item.Model.Provider == "" {
				item.Model = c.ModelDefault
			}
			return item, true
		}
	}
	return tenant.Tenant{}, false
}
