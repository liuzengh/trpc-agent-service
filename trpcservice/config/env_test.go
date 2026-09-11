package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDotEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(
		path,
		[]byte("TRPC_AGENT_TEST_DOTENV=from-file\n"),
		0o600,
	); err != nil {
		t.Fatalf("write dotenv fixture: %v", err)
	}
	t.Setenv("TRPC_AGENT_TEST_DOTENV", "")
	if err := os.Unsetenv("TRPC_AGENT_TEST_DOTENV"); err != nil {
		t.Fatalf("unset test environment: %v", err)
	}

	loaded, err := LoadDotEnv(path)
	if err != nil {
		t.Fatalf("load dotenv: %v", err)
	}
	if !loaded {
		t.Fatal("dotenv file was not reported as loaded")
	}
	if got := os.Getenv("TRPC_AGENT_TEST_DOTENV"); got != "from-file" {
		t.Fatalf("environment value = %q", got)
	}
}

func TestLoadDotEnvDoesNotOverrideProcessEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(
		path,
		[]byte("TRPC_AGENT_TEST_PRECEDENCE=from-file\n"),
		0o600,
	); err != nil {
		t.Fatalf("write dotenv fixture: %v", err)
	}
	t.Setenv("TRPC_AGENT_TEST_PRECEDENCE", "from-process")

	loaded, err := LoadDotEnv(path)
	if err != nil {
		t.Fatalf("load dotenv: %v", err)
	}
	if !loaded {
		t.Fatal("dotenv file was not reported as loaded")
	}
	if got := os.Getenv("TRPC_AGENT_TEST_PRECEDENCE"); got != "from-process" {
		t.Fatalf("environment value = %q", got)
	}
}

func TestLoadDotEnvMissingFileIsOptional(t *testing.T) {
	loaded, err := LoadDotEnv(filepath.Join(t.TempDir(), "missing.env"))
	if err != nil {
		t.Fatalf("load missing dotenv: %v", err)
	}
	if loaded {
		t.Fatal("missing dotenv file was reported as loaded")
	}
}

func TestLoadDotEnvRejectsMalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("BROKEN='unterminated\n"), 0o600); err != nil {
		t.Fatalf("write dotenv fixture: %v", err)
	}

	if _, err := LoadDotEnv(path); err == nil {
		t.Fatal("expected malformed dotenv error")
	}
}
