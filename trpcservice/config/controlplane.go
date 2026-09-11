package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ControlPlaneBackendInMemory = "inmemory"
	ControlPlaneBackendPostgres = "postgres"
)

// ControlPlaneConfig configures tenant and application metadata persistence.
type ControlPlaneConfig struct {
	Backend           string
	PostgresURL       string
	AutoMigrate       bool
	BootstrapTutorial bool
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
}

// LoadControlPlaneConfigFromEnv reads control-plane and PostgreSQL settings.
func LoadControlPlaneConfigFromEnv() (ControlPlaneConfig, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv(
		"TRPC_AGENT_CONTROL_PLANE_BACKEND",
	)))
	if backend == "" {
		backend = ControlPlaneBackendInMemory
	}
	cfg := ControlPlaneConfig{
		Backend:           backend,
		PostgresURL:       strings.TrimSpace(os.Getenv("TRPC_AGENT_POSTGRES_URL")),
		AutoMigrate:       true,
		BootstrapTutorial: false,
		MaxOpenConns:      20,
		MaxIdleConns:      5,
		ConnMaxLifetime:   30 * time.Minute,
	}
	var err error
	if cfg.AutoMigrate, err = parseBoolEnv(
		"TRPC_AGENT_POSTGRES_AUTO_MIGRATE",
		cfg.AutoMigrate,
	); err != nil {
		return ControlPlaneConfig{}, err
	}
	if cfg.BootstrapTutorial, err = parseBoolEnv(
		"TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL",
		cfg.BootstrapTutorial,
	); err != nil {
		return ControlPlaneConfig{}, err
	}
	if cfg.MaxOpenConns, err = parsePositiveIntEnv(
		"TRPC_AGENT_POSTGRES_MAX_OPEN_CONNS",
		cfg.MaxOpenConns,
	); err != nil {
		return ControlPlaneConfig{}, err
	}
	if cfg.MaxIdleConns, err = parseNonNegativeIntEnv(
		"TRPC_AGENT_POSTGRES_MAX_IDLE_CONNS",
		cfg.MaxIdleConns,
	); err != nil {
		return ControlPlaneConfig{}, err
	}
	if cfg.ConnMaxLifetime, err = parsePositiveDurationEnv(
		"TRPC_AGENT_POSTGRES_CONN_MAX_LIFETIME",
		cfg.ConnMaxLifetime,
	); err != nil {
		return ControlPlaneConfig{}, err
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		return ControlPlaneConfig{}, fmt.Errorf(
			"TRPC_AGENT_POSTGRES_MAX_IDLE_CONNS must not exceed max open connections",
		)
	}

	switch cfg.Backend {
	case ControlPlaneBackendInMemory:
		return cfg, nil
	case ControlPlaneBackendPostgres:
		if cfg.PostgresURL == "" {
			return ControlPlaneConfig{}, fmt.Errorf(
				"TRPC_AGENT_POSTGRES_URL is required when control plane backend is postgres",
			)
		}
		if err := validatePostgresURL(cfg.PostgresURL); err != nil {
			return ControlPlaneConfig{}, err
		}
		return cfg, nil
	default:
		return ControlPlaneConfig{}, fmt.Errorf(
			"unsupported TRPC_AGENT_CONTROL_PLANE_BACKEND %q: use inmemory or postgres",
			cfg.Backend,
		)
	}
}

func validatePostgresURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse TRPC_AGENT_POSTGRES_URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return fmt.Errorf("TRPC_AGENT_POSTGRES_URL must use postgres or postgresql")
	}
	if parsed.Host == "" || strings.Trim(parsed.Path, "/") == "" {
		return fmt.Errorf("TRPC_AGENT_POSTGRES_URL must include host and database")
	}
	return nil
}

func parseBoolEnv(name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", name, err)
	}
	return value, nil
}

func parsePositiveIntEnv(name string, fallback int) (int, error) {
	value, err := parseNonNegativeIntEnv(name, fallback)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return value, nil
}

func parseNonNegativeIntEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must not be negative", name)
	}
	return value, nil
}
