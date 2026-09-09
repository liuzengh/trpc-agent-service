package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	for _, k := range []string{"TRPC_HTTP_ADDR", "TRPC_PG_DSN", "TRPC_REDIS_ADDR",
		"TRPC_LOG_LEVEL", "TRPC_LOG_FORMAT", "TRPC_SECRETS_DIR"} {
		_ = os.Unsetenv(k)
	}

	cfg := Load()
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr default = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel default = %q, want info", cfg.LogLevel)
	}
	if cfg.RedisAddr != "localhost:6380" {
		t.Errorf("RedisAddr default = %q, want localhost:6380", cfg.RedisAddr)
	}
}

// TestKMSTokenRefDefaultsToDeployedName pins the fallback Load() uses when
// TRPC_KMS_TOKEN_REF is unset. It is a file name, not a credential; defaulting
// to it means a dropped env var still resolves to the deployed bootstrap token
// instead of a path that exists nowhere.
func TestKMSTokenRefDefaultsToDeployedName(t *testing.T) {
	_ = os.Unsetenv("TRPC_KMS_TOKEN_REF")
	if got := Load().KMSTokenRef; got != "kms-bootstrap-token" {
		t.Errorf("KMSTokenRef default = %q, want kms-bootstrap-token", got)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("TRPC_HTTP_ADDR", ":9090")
	t.Setenv("TRPC_LOG_FORMAT", "json")

	cfg := Load()
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
	if cfg.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want json", cfg.LogFormat)
	}
}

// TestMockChannelDefaultsOff pins the secure default: the mock callback is an
// unauthenticated message injector, so it takes an explicit opt-in.
func TestMockChannelDefaultsOff(t *testing.T) {
	_ = os.Unsetenv("TRPC_MOCK_CHANNEL")
	if got := Load().MockChannel; got != "false" {
		t.Errorf("MockChannel default = %q, want false", got)
	}
}

// TestAdminAddrDefaultsLoopback pins the secure default: the Admin API is
// internal only, so reaching it beyond the host takes an explicit
// TRPC_ADMIN_ADDR.
func TestAdminAddrDefaultsLoopback(t *testing.T) {
	_ = os.Unsetenv("TRPC_ADMIN_ADDR")
	if got := Load().AdminAddr; got != "127.0.0.1:8081" {
		t.Errorf("AdminAddr default = %q, want 127.0.0.1:8081", got)
	}
}

// TestMetricsAddrDefault pins the internal metrics listener address. It is a
// contract with artifacts no compiler checks: the k8s manifests set
// TRPC_METRICS_ADDR explicitly (kubelet probes and Prometheus scrape the pod
// IP, which a loopback bind would not serve) and compose Prometheus scrapes
// the host — a silent default change breaks scraping and rollouts while every
// build stays green.
func TestMetricsAddrDefault(t *testing.T) {
	_ = os.Unsetenv("TRPC_METRICS_ADDR")
	if got := Load().MetricsAddr; got != "127.0.0.1:8082" {
		t.Errorf("MetricsAddr default = %q, want 127.0.0.1:8082", got)
	}
	t.Setenv("TRPC_METRICS_ADDR", "127.0.0.1:9099")
	if got := Load().MetricsAddr; got != "127.0.0.1:9099" {
		t.Errorf("MetricsAddr = %q, want the TRPC_METRICS_ADDR value", got)
	}
}

// TestModelHostAllowlist covers the platform model-endpoint allowlist
// derivation: an explicit env list wins (trimmed, lowercased), otherwise the
// host of the platform's own default endpoint.
func TestModelHostAllowlist(t *testing.T) {
	_ = os.Unsetenv("TRPC_MODEL_BASE_URL_ALLOW")
	if got := Load().ModelHostAllowlist(); len(got) != 1 || got[0] != "api.deepseek.com" {
		t.Errorf("default allowlist = %v, want [api.deepseek.com]", got)
	}
	t.Setenv("TRPC_MODEL_BASE_URL_ALLOW", " model.corp.internal , model2.corp.internal ,")
	got := Load().ModelHostAllowlist()
	if len(got) != 2 || got[0] != "model.corp.internal" || got[1] != "model2.corp.internal" {
		t.Errorf("explicit allowlist = %v, want the trimmed hosts", got)
	}
	t.Setenv("TRPC_MODEL_BASE_URL", "https://selfhosted.corp.internal/v1")
	t.Setenv("TRPC_MODEL_BASE_URL_ALLOW", "")
	if got := Load().ModelHostAllowlist(); len(got) != 1 || got[0] != "selfhosted.corp.internal" {
		t.Errorf("endpoint-derived allowlist = %v, want [selfhosted.corp.internal]", got)
	}
}

func TestMustEnv(t *testing.T) {
	t.Setenv("TRPC_TEST_REQUIRED", "v")
	if _, err := MustEnv("TRPC_TEST_REQUIRED"); err != nil {
		t.Errorf("MustEnv on set var should succeed: %v", err)
	}
	if _, err := MustEnv("TRPC_TEST_MISSING"); err == nil {
		t.Error("MustEnv on missing var should fail")
	}
}

func TestFileResolver(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wecom-token"), []byte("  s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	r := NewFileResolver(dir)
	got, err := r.Resolve(context.Background(), "wecom-token")
	if err != nil {
		t.Fatal(err)
	}
	if got != "s3cret" {
		t.Errorf("Resolve = %q, want trimmed content", got)
	}

	if _, err := r.Resolve(context.Background(), "../etc/passwd"); err == nil {
		t.Error("path traversal ref should be rejected")
	}
	if _, err := r.Resolve(context.Background(), "not-exists"); err == nil {
		t.Error("missing file should return error")
	}
}
