package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadControlPlaneConfigDefaults(t *testing.T) {
	clearControlPlaneEnvironment(t)
	cfg, err := LoadControlPlaneConfigFromEnv()
	if err != nil {
		t.Fatalf("load control plane config: %v", err)
	}
	if cfg.Backend != ControlPlaneBackendInMemory || !cfg.AutoMigrate || cfg.BootstrapTutorial {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.MaxOpenConns != 20 || cfg.MaxIdleConns != 5 ||
		cfg.ConnMaxLifetime != 30*time.Minute {
		t.Fatalf("unexpected pool defaults: %+v", cfg)
	}
}

func TestLoadControlPlaneConfigPostgres(t *testing.T) {
	clearControlPlaneEnvironment(t)
	t.Setenv("TRPC_AGENT_CONTROL_PLANE_BACKEND", "postgres")
	t.Setenv("TRPC_AGENT_POSTGRES_URL", "postgres://user:pass@localhost:5432/agents?sslmode=disable")
	t.Setenv("TRPC_AGENT_POSTGRES_AUTO_MIGRATE", "false")
	t.Setenv("TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL", "true")
	t.Setenv("TRPC_AGENT_POSTGRES_MAX_OPEN_CONNS", "12")
	t.Setenv("TRPC_AGENT_POSTGRES_MAX_IDLE_CONNS", "3")
	t.Setenv("TRPC_AGENT_POSTGRES_CONN_MAX_LIFETIME", "10m")
	cfg, err := LoadControlPlaneConfigFromEnv()
	if err != nil {
		t.Fatalf("load control plane config: %v", err)
	}
	if cfg.Backend != ControlPlaneBackendPostgres || cfg.AutoMigrate || !cfg.BootstrapTutorial ||
		cfg.MaxOpenConns != 12 || cfg.MaxIdleConns != 3 ||
		cfg.ConnMaxLifetime != 10*time.Minute {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadControlPlaneConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name      string
		backend   string
		url       string
		open      string
		idle      string
		wantError string
	}{
		{name: "unknown backend", backend: "mysql", wantError: "unsupported"},
		{name: "missing URL", backend: "postgres", wantError: "POSTGRES_URL"},
		{name: "wrong scheme", backend: "postgres", url: "mysql://localhost/db", wantError: "postgres or postgresql"},
		{name: "missing database", backend: "postgres", url: "postgres://localhost", wantError: "host and database"},
		{name: "zero open", open: "0", wantError: "must be positive"},
		{name: "negative idle", idle: "-1", wantError: "must not be negative"},
		{name: "idle exceeds open", open: "2", idle: "3", wantError: "must not exceed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearControlPlaneEnvironment(t)
			t.Setenv("TRPC_AGENT_CONTROL_PLANE_BACKEND", test.backend)
			t.Setenv("TRPC_AGENT_POSTGRES_URL", test.url)
			t.Setenv("TRPC_AGENT_POSTGRES_MAX_OPEN_CONNS", test.open)
			t.Setenv("TRPC_AGENT_POSTGRES_MAX_IDLE_CONNS", test.idle)
			_, err := LoadControlPlaneConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func clearControlPlaneEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"TRPC_AGENT_CONTROL_PLANE_BACKEND",
		"TRPC_AGENT_POSTGRES_URL",
		"TRPC_AGENT_POSTGRES_AUTO_MIGRATE",
		"TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL",
		"TRPC_AGENT_POSTGRES_MAX_OPEN_CONNS",
		"TRPC_AGENT_POSTGRES_MAX_IDLE_CONNS",
		"TRPC_AGENT_POSTGRES_CONN_MAX_LIFETIME",
	} {
		t.Setenv(key, "")
	}
}
