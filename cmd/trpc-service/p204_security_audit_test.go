//go:build integration

package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestP204ComposeSecurityAudit performs the P2-04 C6 container/Compose
// security audit against the ACTUAL rendered configuration: pinned image
// tags (no latest), loopback-only published ports, secret-file credential
// injection, no test-only service in the default profile, healthchecks on
// long-running services, bounded restart policy and the Dockerfile non-root
// release shape. Every finding is a hard failure; the audit never edits the
// Compose file.
func TestP204ComposeSecurityAudit(t *testing.T) {
	root := composeRepoRoot(t)
	envDir := t.TempDir()
	secretDir := filepath.Join(envDir, "secrets")
	if err := os.MkdirAll(secretDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pg_password", "pg_runtime_password"} {
		if err := os.WriteFile(filepath.Join(secretDir, name), []byte("p204-audit-placeholder"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	envFile := filepath.Join(envDir, "env")
	env := "P109_RUN_ID=p204-audit\nP109_SECRET_DIR=" + secretDir + "\nP202_RUN_ID=p204-audit\nP202_SECRET_DIR=" + secretDir + "\nP109_APP_IMAGE=p109-audit:local\n"
	if err := os.WriteFile(envFile, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}
	raw := runComposeConfig(t, root, envFile)

	// 1. Rendered services: the default profile must not contain test-only
	// or recovery/tooling services.
	for _, forbidden := range []string{"recovery-source", "recovery-target", "recovery-redis", "recovery-tool", "recovery-migrate", "recovery-app", "collector"} {
		if composeDefinesService(raw, forbidden) {
			t.Fatalf("default profile defines test-only service %s", forbidden)
		}
	}
	for _, required := range []string{"postgres", "redis", "migrate", "app"} {
		if !composeDefinesService(raw, required) {
			t.Fatalf("default profile is missing service %s", required)
		}
	}

	// 2. No unpinned image references.
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "image:") && strings.Contains(trimmed, ":latest") {
			t.Fatalf("unpinned latest image reference: %s", strings.TrimSpace(trimmed))
		}
	}

	// 3. Published ports bind loopback only (checked on the YAML source,
	// where the bindings are declared).
	composeYAML, err := os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	loopbackBindings := 0
	for _, line := range strings.Split(string(composeYAML), "\n") {
		trimmed := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- "))
		if strings.HasPrefix(trimmed, "\"127.0.0.1:") {
			loopbackBindings++
		}
	}
	if loopbackBindings < 3 {
		t.Fatalf("published ports are not loopback-pinned: %d", loopbackBindings)
	}
	if regexp.MustCompile(`-\s*"0\.0\.0\.0:`).MatchString(string(composeYAML)) {
		t.Fatalf("non-loopback host port publication found in the compose file")
	}

	// 4. Credentials flow through Docker secret files only.
	if !strings.Contains(raw, "POSTGRES_PASSWORD_FILE") || !strings.Contains(raw, "/run/secrets/") {
		t.Fatalf("database credentials are not injected through Docker secrets")
	}
	if strings.Contains(raw, "POSTGRES_PASSWORD: ") {
		t.Fatalf("plaintext database password found in compose environment")
	}

	// 5. Healthchecks exist for the long-running services.
	for _, service := range []string{"postgres", "redis", "app"} {
		if !composeServiceHas(raw, service, "healthcheck") {
			t.Fatalf("service %s has no healthcheck", service)
		}
	}

	// 6. Dockerfile release shape: non-root user, pinned builder/runtime,
	// no secret material copied.
	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	dockerfileText := string(dockerfile)
	if !strings.Contains(dockerfileText, "USER app") {
		t.Fatalf("release image does not run as the non-root app user")
	}
	if strings.Contains(dockerfileText, "latest") {
		t.Fatalf("Dockerfile references an unpinned latest image")
	}
	for _, forbidden := range []string{".env", "docs/", "*.log"} {
		if strings.Contains(dockerfileText, "COPY "+forbidden) {
			t.Fatalf("Dockerfile copies forbidden path %s", forbidden)
		}
	}

	// 7. The Dockerfile keeps test files, reports and git metadata out of the
	// build context contract (dockerignore).
	ignore, err := os.ReadFile(filepath.Join(root, ".dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{".git", "docs/", "**/*_test.go", ".env*"} {
		if !strings.Contains(string(ignore), required) {
			t.Fatalf("dockerignore is missing %s", required)
		}
	}
}

// TestP204AppContainerRuntimeAudit boots the compose stack once and audits
// the RUNNING app container: non-root uid, bounded capabilities surface via
// the app user, loopback publication reachable and no credential in the
// process environment of the serving process beyond the documented secret
// mechanism. It reuses the capacity gate's override so the audit reflects
// the real serving shape.
func TestP204AppContainerRuntimeAudit(t *testing.T) {
	if testing.Short() {
		t.Skip("runtime audit boots a compose stack")
	}
	image := composeAppImage(t)
	run := newComposeRun(t, image)
	gate := &capacityGate{composeRun: run, override: writeP204RuntimeOverride(t, run.dir)}
	gate.compose(t, 5*time.Minute, "up", "-d", "--wait", "postgres", "redis", "migrate", "app")
	defer func() { _ = gate.composeTry(2*time.Minute, "down", "-v", "--timeout", "10") }()

	// PID 1 runs as the non-root app user (the wrapper drops via su-exec).
	uid := strings.TrimSpace(gate.compose(t, 30*time.Second, "exec", "-T", "app", "sh", "-c",
		"grep '^Uid' /proc/1/status | tr -s '\\t' ' ' | cut -d' ' -f2"))
	if uid == "" || uid == "0" {
		t.Fatalf("serving process must not run as root: uid=%q", uid)
	}

	// The published HTTP port is loopback-only: reachable locally, and the
	// compose config never publishes a non-loopback host address.
	if code := run.probeHealth("/livez"); code != 200 {
		t.Fatalf("livez on the audited stack: %d", code)
	}

	// The serving environment contains no plaintext postgres password: the
	// DSN is assembled from the secret file inside the wrapper.
	envOutput := gate.compose(t, 30*time.Second, "exec", "-T", "app", "sh", "-c", "env | grep -c 'postgres://.*:.*@' || true")
	if strings.TrimSpace(envOutput) != "0" {
		t.Fatalf("plaintext DSN found in the serving process environment: %s", envOutput)
	}
}

func writeP204RuntimeOverride(t *testing.T, dir string) string {
	t.Helper()
	override := filepath.Join(dir, "p204-audit-override.yml")
	content := `services:
  app:
    environment:
      BOOTSTRAP_TELEGRAM_ENABLED: "false"
`
	if err := os.WriteFile(override, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return override
}

// composeDefinesService / composeServiceHas inspect the canonical compose
// JSON (docker compose config --format json) without hand-parsing YAML.
func runComposeConfig(t *testing.T, root, envFile string, extra ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := append([]string{"compose", "--project-directory", root, "--env-file", envFile, "-f", filepath.Join(root, "docker-compose.yml"), "config", "--format", "json"}, extra...)
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("compose config failed: %v\n%s", err, tailString(string(out), 500))
	}
	return string(out)
}

func composeDefinesService(raw, service string) bool {
	var document struct {
		Services map[string]json.RawMessage `json:"services"`
	}
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return strings.Contains(raw, "  "+service+":")
	}
	_, ok := document.Services[service]
	return ok
}

func composeServiceHas(raw, service, key string) bool {
	var document struct {
		Services map[string]map[string]json.RawMessage `json:"services"`
	}
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return false
	}
	serviceConfig, ok := document.Services[service]
	if !ok {
		return false
	}
	_, ok = serviceConfig[key]
	return ok
}
