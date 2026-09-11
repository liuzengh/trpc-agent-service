package bootstrap

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigUsesV1Defaults(t *testing.T) {
	key := configTestEnvironment(t)
	t.Setenv("CONTROL_DATABASE_URL", "postgres://control:secret@localhost/control")
	t.Setenv("CONTROL_HTTP_ADDRESS", "")
	t.Setenv("CONTROL_SESSION_LIFETIME", "")
	t.Setenv("CONTROL_SESSION_COOKIE_NAME", "")
	t.Setenv("CONTROL_SESSION_COOKIE_DOMAIN", "")
	t.Setenv("CONTROL_SESSION_COOKIE_SECURE", "")
	t.Setenv("CONTROL_BOOTSTRAP_MODE", "")

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !bytes.Equal(config.ProfileCredentialKey, key) {
		t.Fatal("credential key was not decoded exactly")
	}
	if config.MigrationDatabaseURL != "postgres://control_migrator@localhost/control_test" {
		t.Fatal("migration database URL was not read independently")
	}
	if config.HTTPAddress != ":8080" {
		t.Fatalf("HTTPAddress = %q, want :8080", config.HTTPAddress)
	}
	if config.SessionLifetime != 24*time.Hour {
		t.Fatalf("SessionLifetime = %v, want 24h", config.SessionLifetime)
	}
	if config.SessionCookieName != "control_session" {
		t.Fatalf("SessionCookieName = %q, want control_session", config.SessionCookieName)
	}
	if !config.SessionCookieSecure {
		t.Fatal("SessionCookieSecure = false, want true")
	}
	if config.BootstrapMode != "disabled" {
		t.Fatalf("BootstrapMode = %q, want disabled", config.BootstrapMode)
	}
}

func TestLoadConfigRequiresDatabaseURL(t *testing.T) {
	configTestEnvironment(t)
	t.Setenv("CONTROL_DATABASE_URL", "")

	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "CONTROL_DATABASE_URL") {
		t.Fatal("expected missing database URL error")
	}
}

func TestLoadConfigRejectsInvalidSessionLifetime(t *testing.T) {
	configTestEnvironment(t)
	t.Setenv("CONTROL_DATABASE_URL", "postgres://control:secret@localhost/control")
	t.Setenv("CONTROL_SESSION_LIFETIME", "tomorrow")

	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "CONTROL_SESSION_LIFETIME") {
		t.Fatal("expected invalid session lifetime error")
	}
}

func TestLoadConfigRequiresBootstrapPasswordInAutoMode(t *testing.T) {
	configTestEnvironment(t)
	t.Setenv("CONTROL_DATABASE_URL", "postgres://control:secret@localhost/control")
	t.Setenv("CONTROL_BOOTSTRAP_MODE", "auto")
	t.Setenv("CONTROL_BOOTSTRAP_USERNAME", "root")
	t.Setenv("CONTROL_BOOTSTRAP_PASSWORD", "")

	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "CONTROL_BOOTSTRAP_PASSWORD") {
		t.Fatal("expected missing bootstrap password error")
	}
}

func TestLoadConfigRequiresProfileCredentialKey(t *testing.T) {
	configTestEnvironment(t)
	t.Setenv("CONTROL_DATABASE_URL", "postgres://localhost/test")
	for _, value := range []string{"", "not-base64", "YWJj"} {
		t.Setenv("CONTROL_PROFILE_CREDENTIAL_KEY", value)
		if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "CONTROL_PROFILE_CREDENTIAL_KEY") {
			t.Fatal("expected missing or invalid credential key error")
		}
	}
}

func configTestEnvironment(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONTROL_PROFILE_CREDENTIAL_KEY", base64.StdEncoding.EncodeToString(key))
	t.Setenv("CONTROL_DATABASE_URL", "postgres://localhost/control_test")
	t.Setenv("CONTROL_MIGRATION_DATABASE_URL", "postgres://control_migrator@localhost/control_test")
	t.Setenv("CONTROL_BOOTSTRAP_MODE", "disabled")
	t.Setenv("CONTROL_SESSION_LIFETIME", "")
	t.Setenv("CONTROL_SESSION_COOKIE_SECURE", "")
	digest, err := DeploymentContractDigestFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST", digest)
	return key
}

func TestLoadConfigRequiresIndependentMigrationDatabaseURL(t *testing.T) {
	configTestEnvironment(t)
	t.Setenv("CONTROL_MIGRATION_DATABASE_URL", "  ")
	if _, err := LoadConfig(); err == nil || err.Error() != "CONTROL_MIGRATION_DATABASE_URL is required" {
		t.Fatal("expected missing migration URL rather than fallback to runtime URL")
	}
}
