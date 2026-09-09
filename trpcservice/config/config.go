// Package config loads node-level service configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const envPrefix = "TRPC_SERVICE_"

// Config contains node-level settings. Tenant-specific settings are managed by
// the tenant control plane and must not be added here.
type Config struct {
	ListenAddr        string        `yaml:"listen_addr"`
	RedisAddr         string        `yaml:"redis_addr"`
	MySQLDSN          string        `yaml:"mysql_dsn"`
	PGVectorDSN       string        `yaml:"pgvector_dsn"`
	Mem0BaseURL       string        `yaml:"mem0_base_url"`
	OTELEndpoint      string        `yaml:"otel_endpoint"`
	EmbeddingModel    string        `yaml:"embedding_model"`
	EmbeddingKeyRef   string        `yaml:"embedding_api_key_ref"`
	EmbeddingBaseURL  string        `yaml:"embedding_base_url"`
	EmbeddingDim      int           `yaml:"embedding_dimensions"`
	ILinkRouteKeys    []string      `yaml:"ilink_route_keys"`
	WecomBotRouteKeys []string      `yaml:"wecombot_route_keys"`
	WecomBotWSURL     string        `yaml:"wecombot_ws_url"`
	AdminUsername     string        `yaml:"admin_username"`
	AdminPasswordRef  string        `yaml:"admin_password_ref"`
	Debounce          time.Duration `yaml:"-"`
	ConfigCacheTTL    time.Duration `yaml:"-"`
	LockTTL           time.Duration `yaml:"-"`
	DedupInflightTTL  time.Duration `yaml:"-"`
	DedupDoneTTL      time.Duration `yaml:"-"`
}

type fileConfig struct {
	ListenAddr        string   `yaml:"listen_addr"`
	RedisAddr         string   `yaml:"redis_addr"`
	MySQLDSN          string   `yaml:"mysql_dsn"`
	PGVectorDSN       string   `yaml:"pgvector_dsn"`
	Mem0BaseURL       string   `yaml:"mem0_base_url"`
	OTELEndpoint      string   `yaml:"otel_endpoint"`
	EmbeddingModel    string   `yaml:"embedding_model"`
	EmbeddingKeyRef   string   `yaml:"embedding_api_key_ref"`
	EmbeddingBaseURL  string   `yaml:"embedding_base_url"`
	EmbeddingDim      *int     `yaml:"embedding_dimensions"`
	ILinkRouteKeys    []string `yaml:"ilink_route_keys"`
	WecomBotRouteKeys []string `yaml:"wecombot_route_keys"`
	WecomBotWSURL     string   `yaml:"wecombot_ws_url"`
	AdminUsername     string   `yaml:"admin_username"`
	AdminPasswordRef  string   `yaml:"admin_password_ref"`
	DebounceMS        *int     `yaml:"debounce_ms"`
	ConfigCacheTTL    string   `yaml:"config_cache_ttl"`
	LockTTL           string   `yaml:"lock_ttl"`
	DedupInflightTTL  string   `yaml:"dedup_inflight_ttl"`
	DedupDoneTTL      string   `yaml:"dedup_done_ttl"`
}

// Default returns safe development defaults. Backend connection settings may
// remain empty until the corresponding implementation task is enabled.
func Default() Config {
	return Config{
		ListenAddr:       ":8080",
		RedisAddr:        "127.0.0.1:6379",
		EmbeddingModel:   "text-embedding-3-small",
		EmbeddingKeyRef:  "env:TRPC_SERVICE_EMBEDDING_API_KEY",
		EmbeddingDim:     1536,
		AdminUsername:    "admin",
		AdminPasswordRef: "env:TRPC_SERVICE_ADMIN_PASSWORD",
		Debounce:         1500 * time.Millisecond,
		ConfigCacheTTL:   30 * time.Second,
		LockTTL:          30 * time.Second,
		DedupInflightTTL: 90 * time.Second,
		DedupDoneTTL:     24 * time.Hour,
	}
}

// Load reads an optional YAML file and applies TRPC_SERVICE_* environment
// overrides. An empty path loads defaults plus environment variables.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
		var raw fileConfig
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return Config{}, fmt.Errorf("decode config %q: %w", path, err)
		}
		if err := applyFile(&cfg, raw); err != nil {
			return Config{}, fmt.Errorf("validate config %q: %w", path, err)
		}
	}
	if err := applyEnvironment(&cfg); err != nil {
		return Config{}, err
	}
	if cfg.ListenAddr == "" {
		return Config{}, errors.New("listen address must not be empty")
	}
	if cfg.AdminPasswordRef != "" && !strings.HasPrefix(cfg.AdminPasswordRef, "env:") {
		return Config{}, errors.New("admin_password_ref must use env: prefix")
	}
	if cfg.EmbeddingDim <= 0 {
		return Config{}, errors.New("embedding_dimensions must be positive")
	}
	if cfg.EmbeddingKeyRef != "" && !strings.HasPrefix(cfg.EmbeddingKeyRef, "env:") {
		return Config{}, errors.New("embedding_api_key_ref must use env: prefix")
	}
	return cfg, nil
}

func applyFile(cfg *Config, raw fileConfig) error {
	setIfNotEmpty(&cfg.ListenAddr, raw.ListenAddr)
	setIfNotEmpty(&cfg.RedisAddr, raw.RedisAddr)
	setIfNotEmpty(&cfg.MySQLDSN, raw.MySQLDSN)
	setIfNotEmpty(&cfg.PGVectorDSN, raw.PGVectorDSN)
	setIfNotEmpty(&cfg.Mem0BaseURL, raw.Mem0BaseURL)
	setIfNotEmpty(&cfg.OTELEndpoint, raw.OTELEndpoint)
	setIfNotEmpty(&cfg.EmbeddingModel, raw.EmbeddingModel)
	setIfNotEmpty(&cfg.EmbeddingKeyRef, raw.EmbeddingKeyRef)
	setIfNotEmpty(&cfg.EmbeddingBaseURL, raw.EmbeddingBaseURL)
	if raw.EmbeddingDim != nil {
		cfg.EmbeddingDim = *raw.EmbeddingDim
	}
	if raw.ILinkRouteKeys != nil {
		cfg.ILinkRouteKeys = append([]string(nil), raw.ILinkRouteKeys...)
	}
	if raw.WecomBotRouteKeys != nil {
		cfg.WecomBotRouteKeys = append([]string(nil), raw.WecomBotRouteKeys...)
	}
	setIfNotEmpty(&cfg.WecomBotWSURL, raw.WecomBotWSURL)
	setIfNotEmpty(&cfg.AdminUsername, raw.AdminUsername)
	setIfNotEmpty(&cfg.AdminPasswordRef, raw.AdminPasswordRef)
	if raw.DebounceMS != nil {
		if *raw.DebounceMS < 0 {
			return errors.New("debounce_ms must be non-negative")
		}
		cfg.Debounce = time.Duration(*raw.DebounceMS) * time.Millisecond
	}
	var err error
	if cfg.ConfigCacheTTL, err = parseOptionalDuration(raw.ConfigCacheTTL, cfg.ConfigCacheTTL); err != nil {
		return fmt.Errorf("config_cache_ttl: %w", err)
	}
	if cfg.LockTTL, err = parseOptionalDuration(raw.LockTTL, cfg.LockTTL); err != nil {
		return fmt.Errorf("lock_ttl: %w", err)
	}
	if cfg.DedupInflightTTL, err = parseOptionalDuration(raw.DedupInflightTTL, cfg.DedupInflightTTL); err != nil {
		return fmt.Errorf("dedup_inflight_ttl: %w", err)
	}
	if cfg.DedupDoneTTL, err = parseOptionalDuration(raw.DedupDoneTTL, cfg.DedupDoneTTL); err != nil {
		return fmt.Errorf("dedup_done_ttl: %w", err)
	}
	return nil
}

func applyEnvironment(cfg *Config) error {
	stringOverrides := []struct {
		name   string
		target *string
	}{
		{"LISTEN_ADDR", &cfg.ListenAddr},
		{"REDIS_ADDR", &cfg.RedisAddr},
		{"MYSQL_DSN", &cfg.MySQLDSN},
		{"PGVECTOR_DSN", &cfg.PGVectorDSN},
		{"MEM0_BASE_URL", &cfg.Mem0BaseURL},
		{"OTEL_ENDPOINT", &cfg.OTELEndpoint},
		{"EMBEDDING_MODEL", &cfg.EmbeddingModel},
		{"EMBEDDING_API_KEY_REF", &cfg.EmbeddingKeyRef},
		{"EMBEDDING_BASE_URL", &cfg.EmbeddingBaseURL},
		{"ADMIN_USERNAME", &cfg.AdminUsername},
		{"ADMIN_PASSWORD_REF", &cfg.AdminPasswordRef},
		{"WECOMBOT_WS_URL", &cfg.WecomBotWSURL},
	}
	for _, override := range stringOverrides {
		if value, ok := os.LookupEnv(envPrefix + override.name); ok {
			*override.target = value
		}
	}

	if value, ok := os.LookupEnv(envPrefix + "DEBOUNCE_MS"); ok {
		ms, err := strconv.Atoi(value)
		if err != nil || ms < 0 {
			return fmt.Errorf("%sDEBOUNCE_MS must be a non-negative integer", envPrefix)
		}
		cfg.Debounce = time.Duration(ms) * time.Millisecond
	}
	if value, ok := os.LookupEnv(envPrefix + "EMBEDDING_DIMENSIONS"); ok {
		dimension, err := strconv.Atoi(value)
		if err != nil || dimension <= 0 {
			return fmt.Errorf("%sEMBEDDING_DIMENSIONS must be a positive integer", envPrefix)
		}
		cfg.EmbeddingDim = dimension
	}
	if value, ok := os.LookupEnv(envPrefix + "ILINK_ROUTE_KEYS"); ok {
		cfg.ILinkRouteKeys = nil
		for _, routeKey := range strings.Split(value, ",") {
			if routeKey = strings.TrimSpace(routeKey); routeKey != "" {
				cfg.ILinkRouteKeys = append(cfg.ILinkRouteKeys, routeKey)
			}
		}
	}
	if value, ok := os.LookupEnv(envPrefix + "WECOMBOT_ROUTE_KEYS"); ok {
		cfg.WecomBotRouteKeys = nil
		for _, routeKey := range strings.Split(value, ",") {
			if routeKey = strings.TrimSpace(routeKey); routeKey != "" {
				cfg.WecomBotRouteKeys = append(cfg.WecomBotRouteKeys, routeKey)
			}
		}
	}

	durationOverrides := []struct {
		name   string
		target *time.Duration
	}{
		{"CONFIG_CACHE_TTL", &cfg.ConfigCacheTTL},
		{"LOCK_TTL", &cfg.LockTTL},
		{"DEDUP_INFLIGHT_TTL", &cfg.DedupInflightTTL},
		{"DEDUP_DONE_TTL", &cfg.DedupDoneTTL},
	}
	for _, override := range durationOverrides {
		if value, ok := os.LookupEnv(envPrefix + override.name); ok {
			duration, err := parsePositiveDuration(value)
			if err != nil {
				return fmt.Errorf("%s%s: %w", envPrefix, override.name, err)
			}
			*override.target = duration
		}
	}
	return nil
}

func parseOptionalDuration(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	return parsePositiveDuration(value)
}

func parsePositiveDuration(value string) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", value, err)
	}
	if duration <= 0 {
		return 0, errors.New("duration must be positive")
	}
	return duration, nil
}

func setIfNotEmpty(target *string, value string) {
	if value != "" {
		*target = value
	}
}
