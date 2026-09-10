package config

import (
	"path/filepath"
	"testing"
	"time"
)

// The deployment configs are load-bearing artifacts: scripts/e2e.sh,
// scripts/fault_drill.sh and deploy/README.md all make claims about what the
// stack does when it boots with them. Nothing else in the tree would notice if
// one of them stopped validating, or quietly drifted to a memory backend and
// invalidated the node-failure drill.
//
// They live here rather than in a shell script because Load is the real thing:
// parse, build, Validate, apply env overrides — the exact path cmd/trpc-service
// runs at boot. A shell check would have to spawn a process and infer success
// from its exit code and stderr, which is both slower and less precise about
// which invariant broke.
//
// Paths are relative to the package directory, which is where `go test` runs.
const deployDir = "../../deploy"

// loadDeployConfig loads one committed deployment config with the environment
// pinned, so an exported MODEL_BASE_URL on a developer's machine cannot make
// these assertions pass or fail by accident. Every env hook in Load is guarded
// by `!= ""`, so setting them empty is exactly "no override".
func loadDeployConfig(t *testing.T, rel string) *Config {
	t.Helper()
	for _, k := range []string{
		envAPIKey, envModel, envBaseURL,
		envStorageBackend, envStorageRedisURL,
		envAgentMessageTimeout, envAgentMaxConcurrency, envAgentMaxLLMCalls,
	} {
		t.Setenv(k, "")
	}
	path := filepath.Join(deployDir, rel)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%s): %v", path, err)
	}
	return cfg
}

// Both compose configs must run the real shared backend. A stack that silently
// fell back to memory would still answer messages and still look healthy, so
// the only thing anyone would notice is that drill D6 — kill the node, ask
// again, the new process remembers — stopped proving anything.
func TestComposeConfigsUseRedisNotMemory(t *testing.T) {
	for _, rel := range []string{
		"compose/config/config.yaml",
		"compose/config-observability/config.yaml",
	} {
		cfg := loadDeployConfig(t, rel)
		if got := cfg.Storage.Session.Backend; got != BackendRedis {
			t.Errorf("%s: storage.session.backend = %q, want %q: an in-process "+
				"backend makes the node-failure drill meaningless", rel, got, BackendRedis)
		}
		if got := cfg.Storage.Session.RedisURL; got == "" {
			t.Errorf("%s: redis_url is empty", rel)
		}
	}
}

// D7 needs a victim and a bystander: tenant demo saturates the per-tenant quota
// while demo-alt must still be served. That is the whole assertion — isolation,
// not merely rejection — so a compose config with one tenant would let the
// drill pass while measuring nothing.
func TestComposeConfigsCarryTwoTenantsForTheIsolationDrill(t *testing.T) {
	cfg := loadDeployConfig(t, "compose/config/config.yaml")
	if len(cfg.Tenants) != 2 {
		t.Fatalf("got %d tenants, want 2 (D7 needs one to flood and one to stay served)",
			len(cfg.Tenants))
	}
	for _, id := range []string{"demo", "demo-alt"} {
		tt, ok := cfg.Tenants[id]
		if !ok {
			t.Fatalf("tenant %q is missing", id)
		}
		// Both must point at the in-image fake model, or the drills cannot inject
		// a fault into the tenant they are measuring.
		if got := tt.Model.BaseURL; got != "http://fake-model:9009/v1" {
			t.Errorf("tenant %q base_url = %q, want the compose fake-model service", id, got)
		}
	}
}

// The demo tenant must NOT ship an output tripwire. The fake model's `ok` reply
// always contains 内部资料, deliberately split across two chunks, so a
// preconfigured output_blocked_keywords would truncate every baseline answer and
// leave the drills nothing healthy to compare against. scripts/e2e.sh installs
// it live instead, which also exercises the hot governance path.
func TestComposeDemoTenantHasNoOutputTripwirePreconfigured(t *testing.T) {
	cfg := loadDeployConfig(t, "compose/config/config.yaml")
	tt := cfg.Tenants["demo"]
	if tt == nil {
		t.Fatal("tenant demo is missing")
	}
	if n := len(tt.Guardrails.OutputBlockedKeywords); n != 0 {
		t.Fatalf("output_blocked_keywords = %v, want none: every baseline reply "+
			"would be truncated (see the comment in the config)", tt.Guardrails.OutputBlockedKeywords)
	}
	// The input tripwire must be there, though: e2e asserts a rejection without
	// having to configure one first.
	if len(tt.Guardrails.BlockedKeywords) == 0 {
		t.Error("blocked_keywords is empty; the input guardrail assertion in e2e has nothing to trigger")
	}
}

// The K8s config leaves audit.file out on purpose: audit.New("") is log-only, so
// the trail goes to stdout and the cluster's log pipeline, which is durable in a
// way an emptyDir file is not. That is only safe because of the fix behind
// TestEmptyPathIsLogOnlyNotSilent — before it, an empty path returned a nil
// logger and dropped every record. This test is the tripwire for anyone who
// "simplifies" audit.New back.
func TestKubernetesConfigIsLogOnlyAudit(t *testing.T) {
	cfg := loadDeployConfig(t, "k8s/config/config.yaml")
	if got := cfg.Audit.File; got != "" {
		t.Fatalf("audit.file = %q, want empty: the Deployment sets "+
			"readOnlyRootFilesystem and mounts no volume for it, so a path here "+
			"is a boot failure, not a trail", got)
	}
	if got := cfg.Storage.Session.Backend; got != BackendRedis {
		t.Errorf("storage.session.backend = %q, want %q", got, BackendRedis)
	}
}

// Exactly one tenant, and the reason is applyEnvOverride: Load calls it as
// applyEnvOverride(cfg.Tenants[cfg.DefaultTenant]), so MODEL_API_KEY from the
// Secret reaches the default tenant and nobody else. A second tenant here would
// boot fine and then call the provider with the literal placeholder key — a 401
// indistinguishable from a model outage. Adding a tenant means moving the whole
// file into a Secret (deploy/k8s/10-secret.example.yaml, shape B); this failure
// message is where that is written down.
func TestKubernetesConfigCarriesOneTenantUntilKeysMoveIntoASecret(t *testing.T) {
	cfg := loadDeployConfig(t, "k8s/config/config.yaml")
	if len(cfg.Tenants) != 1 {
		t.Fatalf("got %d tenants, want 1: MODEL_API_KEY overrides the default "+
			"tenant only, so every other tenant would run on the placeholder key. "+
			"Put the config in a Secret instead — see 10-secret.example.yaml shape B",
			len(cfg.Tenants))
	}
	tt := cfg.Tenants[cfg.DefaultTenant]
	if tt == nil {
		t.Fatalf("default_tenant %q is not defined", cfg.DefaultTenant)
	}
	// Non-empty, because Validate runs BEFORE applyEnvOverride in Load: an empty
	// placeholder would fail the boot even though the Secret supplies the real
	// key a moment later. The value itself is asserted nowhere — it is a
	// placeholder by design and the Secret replaces it.
	if tt.Model.APIKey == "" {
		t.Error("model.api_key is empty; Validate() runs before the env override, so this cannot boot")
	}
}

// The runtime envelope both stacks boot with. These are the documented defaults,
// and the drills retune them live over PUT /admin/settings rather than by
// editing a file — so if a config ever starts overriding them, the drill's
// expected numbers silently stop matching and the failure looks like a product
// regression.
func TestDeployConfigsBootWithTheDocumentedDefaults(t *testing.T) {
	for _, rel := range []string{
		"compose/config/config.yaml",
		"compose/config-observability/config.yaml",
		"k8s/config/config.yaml",
	} {
		cfg := loadDeployConfig(t, rel)
		if got := cfg.Agent.MessageTimeout; got != DefaultMessageTimeout {
			t.Errorf("%s: agent.message_timeout = %v, want the default %v", rel, got, DefaultMessageTimeout)
		}
		if got := cfg.Agent.MaxLLMCalls; got != DefaultMaxLLMCalls {
			t.Errorf("%s: agent.max_llm_calls = %d, want the default %d", rel, got, DefaultMaxLLMCalls)
		}
		if got := cfg.Agent.MaxConcurrencyPerTenant; got != 0 {
			t.Errorf("%s: agent.max_concurrency_per_tenant = %d, want 0 (unlimited; "+
				"D7 sets 1 live)", rel, got)
		}
	}
}

// The observability variant differs from the default compose config in exactly
// one place. Asserting that is the only thing standing between the two files and
// a silent drift, since neither can be exercised against a real collector here
// (the image cannot be pulled). If this test starts failing, one of the two was
// edited without the other.
func TestObservabilityConfigDiffersOnlyByTelemetry(t *testing.T) {
	base := loadDeployConfig(t, "compose/config/config.yaml")
	obs := loadDeployConfig(t, "compose/config-observability/config.yaml")

	if obs.Telemetry.Traces.Exporter != "otlp" || obs.Telemetry.Metrics.Exporter != "otlp" {
		t.Fatalf("telemetry exporters = %q/%q, want otlp/otlp: this config exists "+
			"only to turn them on", obs.Telemetry.Traces.Exporter, obs.Telemetry.Metrics.Exporter)
	}
	for _, e := range []struct{ name, got string }{
		{"traces.endpoint", obs.Telemetry.Traces.Endpoint},
		{"metrics.endpoint", obs.Telemetry.Metrics.Endpoint},
	} {
		if e.got != "http://otel-collector:4318" {
			t.Errorf("%s = %q, want the compose collector service on the OTLP/HTTP port", e.name, e.got)
		}
	}
	if base.Telemetry.Traces.Exporter == "otlp" {
		t.Error("the default compose config must not export: the collector image " +
			"is unreachable offline and a failed exporter would only add noise")
	}
	if got := obs.Telemetry.Metrics.Interval; got != 15*time.Second {
		t.Errorf("metrics.interval = %v, want 15s", got)
	}

	// Everything else is a copy, so compare the parts a drift would show up in.
	if len(obs.Tenants) != len(base.Tenants) {
		t.Errorf("tenant count = %d, want %d (same as the default config)", len(obs.Tenants), len(base.Tenants))
	}
	if obs.Storage.Session.Backend != base.Storage.Session.Backend ||
		obs.Storage.Session.RedisURL != base.Storage.Session.RedisURL {
		t.Errorf("storage = %+v, want the same as the default config %+v", obs.Storage.Session, base.Storage.Session)
	}
	if obs.Agent != base.Agent {
		t.Errorf("agent = %+v, want the same as the default config %+v", obs.Agent, base.Agent)
	}
	if obs.Audit.File != base.Audit.File {
		t.Errorf("audit.file = %q, want %q (same as the default config)", obs.Audit.File, base.Audit.File)
	}
}
