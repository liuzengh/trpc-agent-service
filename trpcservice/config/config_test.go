package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "server:\n  http_addr: \":8080\"\nlog:\n  level: \"debug\"\nrole: \"all\"\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.HTTPAddr != ":8080" {
		t.Errorf("Server.HTTPAddr = %q, want %q", cfg.Server.HTTPAddr, ":8080")
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q, want %q", cfg.Log.Level, "debug")
	}
	if cfg.Role != "all" {
		t.Errorf("Role = %q, want %q", cfg.Role, "all")
	}
}

func TestLoadBackendsFromYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "server:\n  http_addr: \":8080\"\n" +
		"mysql:\n  dsn: \"user:pass@tcp(127.0.0.1:3306)/platform\"\n" +
		"redis:\n  url: \"redis://127.0.0.1:6379/0\"\n" +
		"milvus:\n  address: \"127.0.0.1:19530\"\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MySQL.DSN != "user:pass@tcp(127.0.0.1:3306)/platform" {
		t.Errorf("MySQL.DSN = %q, want platform dsn", cfg.MySQL.DSN)
	}
	if cfg.Redis.URL != "redis://127.0.0.1:6379/0" {
		t.Errorf("Redis.URL = %q, want redis url", cfg.Redis.URL)
	}
	if cfg.Milvus.Address != "127.0.0.1:19530" {
		t.Errorf("Milvus.Address = %q, want 127.0.0.1:19530", cfg.Milvus.Address)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Error("Load on missing file should return an error")
	}
}

func TestDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Server.HTTPAddr == "" {
		t.Error("default Server.HTTPAddr should not be empty")
	}
	if cfg.Log.Level == "" {
		t.Error("default Log.Level should not be empty")
	}
	if cfg.Role == "" {
		t.Error("default Role should not be empty")
	}
}
