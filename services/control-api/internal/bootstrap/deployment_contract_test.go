package bootstrap

import (
	"context"
	"strings"
	"testing"
)

func TestLoadConfigRequiresExpectedDeploymentContractDigest(t *testing.T) {
	for _, value := range []string{"", "sha256:short", "sha256:" + strings.Repeat("A", 64), strings.Repeat("a", 64), "sha512:" + strings.Repeat("a", 64)} {
		t.Run(value, func(t *testing.T) {
			configTestEnvironment(t)
			t.Setenv("CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST", value)
			if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST") {
				t.Fatalf("LoadConfig() error = %v, want missing/invalid expected digest", err)
			}
		})
	}
}

func TestLoadConfigPreservesReleasePinnedDeploymentDigest(t *testing.T) {
	configTestEnvironment(t)
	want := "sha256:" + strings.Repeat("a", 64)
	t.Setenv("CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST", want)
	config, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.DeploymentExpectedContractDigest != want {
		t.Fatalf("expected digest = %q, want release-pinned %q", config.DeploymentExpectedContractDigest, want)
	}
}

func TestCheckedDeploymentContractAcceptsMatchingRelease(t *testing.T) {
	config := Config{}
	contract, err := deploymentPlatformContract(config)
	if err != nil {
		t.Fatal(err)
	}
	config.DeploymentExpectedContractDigest = contract.Digest
	got, err := checkedDeploymentPlatformContract(config)
	if err != nil || got.Digest != contract.Digest {
		t.Fatalf("checked contract = %#v, error = %v", got, err)
	}
}

func TestNewRejectsContractBeforeOpeningDatabase(t *testing.T) {
	for _, tc := range []struct {
		name      string
		expected  string
		wantError string
	}{
		{"missing", "", "CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST is required"},
		{"invalid", "invalid", "must be sha256:"},
		{"mismatch", "sha256:" + strings.Repeat("0", 64), "digest mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// This DSN always fails parsing if the database open path is reached.
			app, err := New(context.Background(), Config{
				DatabaseURL:                      "postgres://%zz",
				MigrationDatabaseURL:             "postgres://%zz",
				DeploymentExpectedContractDigest: tc.expected,
			})
			if app != nil || err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("New() app = %v, error = %v, want contract failure before database parsing", app, err)
			}
		})
	}
}

func TestNewMatchingContractReachesDatabaseOpen(t *testing.T) {
	contract, err := deploymentPlatformContract(Config{})
	if err != nil {
		t.Fatal(err)
	}
	app, err := New(context.Background(), Config{
		DatabaseURL:                      "postgres://%zz",
		MigrationDatabaseURL:             "postgres://%zz",
		DeploymentExpectedContractDigest: contract.Digest,
	})
	if app != nil || err == nil || !strings.Contains(err.Error(), "parse") || strings.Contains(err.Error(), "contract") {
		t.Fatalf("New() app = %v, error = %v, want database parse failure after contract gate", app, err)
	}
}

func TestDigestPreparationDoesNotLoadOtherProcessConfiguration(t *testing.T) {
	t.Setenv("CONTROL_DATABASE_URL", "")
	t.Setenv("CONTROL_PROFILE_CREDENTIAL_KEY", "")
	t.Setenv("CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST", "")
	got, err := DeploymentContractDigestFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	want, err := deploymentPlatformContract(Config{})
	if err != nil || got != want.Digest {
		t.Fatalf("digest = %q, error = %v, want %q", got, err, want.Digest)
	}
}
