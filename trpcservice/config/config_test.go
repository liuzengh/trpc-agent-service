package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr != ":8080" {
		t.Fatalf("ListenAddr = %q, want :8080", cfg.ListenAddr)
	}
	if cfg.Debounce != 1500*time.Millisecond {
		t.Fatalf("Debounce = %s, want 1.5s", cfg.Debounce)
	}
	if cfg.DedupDoneTTL != 24*time.Hour {
		t.Fatalf("DedupDoneTTL = %s, want 24h", cfg.DedupDoneTTL)
	}
}

func TestLoadYAMLAndEnvironmentOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte(`
listen_addr: ":9000"
redis_addr: "redis:6379"
mysql_dsn: "user:pass@tcp(mysql:3306)/agent"
debounce_ms: 700
config_cache_ttl: "45s"
lock_ttl: "20s"
dedup_inflight_ttl: "2m"
dedup_done_ttl: "12h"
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv(envPrefix+"LISTEN_ADDR", ":9100")
	t.Setenv(envPrefix+"DEBOUNCE_MS", "250")
	t.Setenv(envPrefix+"ILINK_ROUTE_KEYS", "bot-a, bot-b")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr != ":9100" {
		t.Errorf("ListenAddr = %q, want :9100", cfg.ListenAddr)
	}
	if cfg.RedisAddr != "redis:6379" {
		t.Errorf("RedisAddr = %q, want redis:6379", cfg.RedisAddr)
	}
	if cfg.Debounce != 250*time.Millisecond {
		t.Errorf("Debounce = %s, want 250ms", cfg.Debounce)
	}
	if cfg.ConfigCacheTTL != 45*time.Second {
		t.Errorf("ConfigCacheTTL = %s, want 45s", cfg.ConfigCacheTTL)
	}
	if cfg.DedupInflightTTL != 2*time.Minute {
		t.Errorf("DedupInflightTTL = %s, want 2m", cfg.DedupInflightTTL)
	}
	if len(cfg.ILinkRouteKeys) != 2 || cfg.ILinkRouteKeys[1] != "bot-b" {
		t.Errorf("ILinkRouteKeys = %v, want bot-a,bot-b", cfg.ILinkRouteKeys)
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	t.Setenv(envPrefix+"LOCK_TTL", "soon")
	_, err := Load("")
	if err == nil || !strings.Contains(err.Error(), "LOCK_TTL") {
		t.Fatalf("Load() error = %v, want LOCK_TTL validation error", err)
	}
}
