package main

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// TestP204SecretScan scans the P2-04-relevant source/config surface for
// high-signal credential patterns. It is a bounded negative test: the
// allowlist covers the documented synthetic placeholders; anything else
// matching a real-credential shape fails the scan. It does not claim to be a
// complete secret auditor (gitleaks/trivy stay UNAVAILABLE on this host).
func TestP204SecretScan(t *testing.T) {
	root := p204ScanRoot(t)
	targets := []string{
		"cmd/trpc-service",
		"cmd/trpc-migrate",
		"cmd/trpc-recovery",
		"trpcservice/admission",
		"trpcservice/recovery",
		"trpcservice/gateway",
		"trpcservice/ratelimit",
		"docker-compose.yml",
		"Dockerfile",
		"Dockerfile.recovery",
		".env.example",
		".dockerignore",
		"deploy/otel-collector.yaml",
	}
	patterns := map[string]*regexp.Regexp{
		"aws_access_key":   regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
		"private_key":      regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`),
		"generic_api_key":  regexp.MustCompile(`(?i)(api[_-]?key|apikey)\s*[:=]\s*["']?[A-Za-z0-9_\-]{24,}`),
		"bot_token":        regexp.MustCompile(`[0-9]{8,10}:AA[Ew][A-Za-z0-9_\-]{30,}`),
		"slack_token":      regexp.MustCompile(`xox[baprs]-[A-Za-z0-9\-]{10,}`),
		"password_literal": regexp.MustCompile(`(?i)(password|passwd|pwd)\s*[:=]\s*["'][^"']{8,}["']`),
	}
	allow := []*regexp.Regexp{
		regexp.MustCompile(`local-gate-placeholder`),
		regexp.MustCompile(`capacity-gate-placeholder`),
		regexp.MustCompile(`suite-placeholder`),
		regexp.MustCompile(`local-placeholder`),
		regexp.MustCompile(`p202-runtime-password`),                             // test fixture constant (documented synthetic)
		regexp.MustCompile(`p202suitepw`),                                       // test-only suite credential, /tmp env only
		regexp.MustCompile(`p006_|p009gc|p109-canary`),                          // pre-existing test fixture families
		regexp.MustCompile(`MODEL_API_KEY`),                                     // env var names are not values
		regexp.MustCompile(`(?i)password file|password_file|POSTGRES_PASSWORD`), // mechanism names
		regexp.MustCompile(`/run/secrets/`),                                     // Docker secret-file mechanism, never a literal
	}
	scanned := 0
	for _, target := range targets {
		abs := filepath.Join(root, target)
		info, err := os.Stat(abs)
		if err != nil {
			t.Fatalf("scan target missing: %s", target)
		}
		if !info.IsDir() {
			if scanOne(t, abs, patterns, allow) {
				scanned++
			}
			continue
		}
		entries, err := os.ReadDir(abs)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
				continue
			}
			if scanOne(t, filepath.Join(abs, entry.Name()), patterns, allow) {
				scanned++
			}
		}
	}
	if scanned < 20 {
		t.Fatalf("secret scan covered too few files: %d", scanned)
	}
}

// p204ScanRoot resolves the repository root from this test file's location.
func p204ScanRoot(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func scanOne(t *testing.T, path string, patterns map[string]*regexp.Regexp, allow []*regexp.Regexp) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "#") {
			continue
		}
		for name, pattern := range patterns {
			if !pattern.MatchString(line) {
				continue
			}
			allowed := false
			for _, a := range allow {
				if a.MatchString(line) {
					allowed = true
					break
				}
			}
			if !allowed {
				t.Fatalf("secret scan finding file=%s category=%s", filepath.Base(path), name)
			}
		}
	}
	return true
}
