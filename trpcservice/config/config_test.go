package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const wecomYAML = `
default_tenant: demo
tenants:
  - id: demo
    name: Demo
    model:
      name: deepseek-chat
      api_key: sk-test
      base_url: https://api.deepseek.com
    channels:
      wecom:
        corp_id: wx5823bf96d3bd56c7
        corp_secret: secret
        agent_id: 218
        token: QDG6eK
        encoding_aes_key: jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C
  - id: plain
    name: Plain
    model:
      name: deepseek-chat
      api_key: sk-test
`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadWeComBinding(t *testing.T) {
	cfg, err := Load(writeConfig(t, wecomYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b, ok := cfg.WeComBinding("demo")
	if !ok {
		t.Fatal("demo should have a wecom binding")
	}
	if b.CorpID != "wx5823bf96d3bd56c7" || b.AgentID != 218 || b.Token != "QDG6eK" {
		t.Fatalf("binding = %+v", b)
	}
	if _, ok := cfg.WeComBinding("plain"); ok {
		t.Fatal("plain tenant must not have a binding")
	}
	if _, ok := cfg.WeComBinding("missing"); ok {
		t.Fatal("unknown tenant must not have a binding")
	}
}

func TestLoadWeComBindingIncomplete(t *testing.T) {
	_, err := Load(writeConfig(t, `
tenants:
  - id: demo
    model:
      name: m
      api_key: k
    channels:
      wecom:
        corp_id: wx123
        token: tk
`))
	if err == nil {
		t.Fatal("incomplete wecom binding must fail validation")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	src := writeConfig(t, wecomYAML)
	cfg, err := Load(src)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	again, err := Load(dst)
	if err != nil {
		t.Fatalf("reload saved config: %v", err)
	}
	if again.DefaultTenant != cfg.DefaultTenant || len(again.Tenants) != len(cfg.Tenants) {
		t.Fatalf("round trip mismatch: %+v", again)
	}
	b, ok := again.WeComBinding("demo")
	if !ok || b.CorpID != "wx5823bf96d3bd56c7" || b.AgentID != 218 {
		t.Fatalf("binding after round trip = %+v, %v", b, ok)
	}
	if _, ok := again.WeComBinding("plain"); ok {
		t.Fatal("plain tenant gained a binding after round trip")
	}
	if _, err := os.Stat(dst + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file must not survive Save")
	}
}

const storageRedisYAML = `
default_tenant: demo
storage:
  session:
    backend: redis
    redis_url: redis://127.0.0.1:6379
    key_prefix: "custom:"
    session_ttl: 72h
tenants:
  - id: demo
    model:
      name: m
      api_key: k
`

func clearStorageEnv(t *testing.T) {
	t.Helper()
	t.Setenv("STORAGE_SESSION_BACKEND", "")
	t.Setenv("STORAGE_SESSION_REDIS_URL", "")
}

func TestLoadStorageDefaults(t *testing.T) {
	clearStorageEnv(t)
	cfg, err := Load(writeConfig(t, wecomYAML)) // no storage section at all
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	s := cfg.Storage.Session
	if s.Backend != BackendMemory || s.KeyPrefix != DefaultKeyPrefix || s.RedisURL != "" || s.SessionTTL != 0 {
		t.Fatalf("defaults = %+v", s)
	}
}

func TestLoadStorageRedis(t *testing.T) {
	clearStorageEnv(t)
	cfg, err := Load(writeConfig(t, storageRedisYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	s := cfg.Storage.Session
	if s.Backend != BackendRedis || s.RedisURL != "redis://127.0.0.1:6379" ||
		s.KeyPrefix != "custom:" || s.SessionTTL != 72*time.Hour {
		t.Fatalf("storage = %+v", s)
	}
}

func TestLoadStorageInvalid(t *testing.T) {
	clearStorageEnv(t)
	cases := []struct{ name, doc string }{
		{"unknown backend", "storage:\n  session:\n    backend: mysql\n"},
		{"redis without url", "storage:\n  session:\n    backend: redis\n"},
		{"bad scheme", "storage:\n  session:\n    backend: redis\n    redis_url: http://x:6379\n"},
		{"bad ttl", "storage:\n  session:\n    session_ttl: abc\n"},
		{"negative ttl", "storage:\n  session:\n    session_ttl: -1h\n"},
	}
	for _, c := range cases {
		doc := "tenants:\n  - id: demo\n    model:\n      name: m\n      api_key: k\n" + c.doc
		if _, err := Load(writeConfig(t, doc)); err == nil {
			t.Fatalf("%s: must fail validation", c.name)
		}
	}
}

func TestStorageEnvOverride(t *testing.T) {
	t.Setenv("STORAGE_SESSION_BACKEND", "redis")
	t.Setenv("STORAGE_SESSION_REDIS_URL", "redis://env-host:6380")
	cfg, err := Load(writeConfig(t, wecomYAML)) // file says nothing about storage
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Storage.Session.Backend != BackendRedis || cfg.Storage.Session.RedisURL != "redis://env-host:6380" {
		t.Fatalf("env override not applied: %+v", cfg.Storage.Session)
	}
}

func TestSaveStorageRoundTrip(t *testing.T) {
	clearStorageEnv(t)
	cfg, err := Load(writeConfig(t, storageRedisYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	raw, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "backend: redis") || !strings.Contains(string(raw), "custom:") {
		t.Fatalf("storage section lost on save:\n%s", raw)
	}
	again, err := Load(dst)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if again.Storage != cfg.Storage {
		t.Fatalf("round trip mismatch: %+v vs %+v", again.Storage, cfg.Storage)
	}

	// Pure-default storage must stay out of the serialized file (clean diffs).
	def, err := Load(writeConfig(t, wecomYAML))
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	dst2 := filepath.Join(t.TempDir(), "saved2.yaml")
	if err := Save(dst2, def); err != nil {
		t.Fatalf("save default: %v", err)
	}
	raw2, err := os.ReadFile(dst2)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw2), "storage:") {
		t.Fatalf("default storage must be omitted, got:\n%s", raw2)
	}
}

func clearAgentEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AGENT_MESSAGE_TIMEOUT", "")
	t.Setenv("AGENT_MAX_CONCURRENCY_PER_TENANT", "")
	t.Setenv("AGENT_MAX_LLM_CALLS", "")
}

func TestLoadAgentDefaults(t *testing.T) {
	clearAgentEnv(t)
	cfg, err := Load(writeConfig(t, wecomYAML)) // no agent section at all
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Agent.MessageTimeout != DefaultMessageTimeout || cfg.Agent.MaxConcurrencyPerTenant != 0 ||
		cfg.Agent.MaxLLMCalls != DefaultMaxLLMCalls {
		t.Fatalf("agent defaults = %+v", cfg.Agent)
	}
	if DefaultMessageTimeout != 2*time.Minute {
		t.Fatalf("DefaultMessageTimeout = %v, want the 2m the gateway used to hardcode", DefaultMessageTimeout)
	}
	// Pinned because the framework reads a non-positive cap as "no limit": if
	// this ever became 0, a config without an agent section would silently
	// reopen the unbounded upstream-call loop (§4.5).
	if DefaultMaxLLMCalls <= 0 {
		t.Fatalf("DefaultMaxLLMCalls = %d, must stay positive", DefaultMaxLLMCalls)
	}
}

const agentYAMLDoc = `
default_tenant: demo
agent:
  message_timeout: 3s
  max_concurrency_per_tenant: 5
  max_llm_calls: 4
tenants:
  - id: demo
    model:
      name: m
      api_key: k
`

func TestLoadAgent(t *testing.T) {
	clearAgentEnv(t)
	cfg, err := Load(writeConfig(t, agentYAMLDoc))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Agent.MessageTimeout != 3*time.Second || cfg.Agent.MaxConcurrencyPerTenant != 5 ||
		cfg.Agent.MaxLLMCalls != 4 {
		t.Fatalf("agent = %+v", cfg.Agent)
	}
}

// TestZeroCallCapSelectsTheDefault pins the one place where max_llm_calls
// deliberately differs from max_concurrency_per_tenant: for the quota 0 means
// unlimited, for the call cap it means the documented default. Both the file
// and the environment take that reading, so no deployment can express
// "no cap" by accident — only by naming a large number.
func TestZeroCallCapSelectsTheDefault(t *testing.T) {
	tenants := "tenants:\n  - id: demo\n    model:\n      name: m\n      api_key: k\n"

	clearAgentEnv(t)
	cfg, err := Load(writeConfig(t, "agent:\n  max_llm_calls: 0\n"+tenants))
	if err != nil {
		t.Fatalf("a zero cap in the file must load, not fail: %v", err)
	}
	if cfg.Agent.MaxLLMCalls != DefaultMaxLLMCalls {
		t.Fatalf("file cap = %d, want the default %d", cfg.Agent.MaxLLMCalls, DefaultMaxLLMCalls)
	}

	clearAgentEnv(t)
	t.Setenv("AGENT_MAX_LLM_CALLS", "0")
	cfg, err = Load(writeConfig(t, tenants))
	if err != nil {
		t.Fatalf("a zero cap in the env must load, not fail: %v", err)
	}
	if cfg.Agent.MaxLLMCalls != DefaultMaxLLMCalls {
		t.Fatalf("env cap = %d, want the default %d", cfg.Agent.MaxLLMCalls, DefaultMaxLLMCalls)
	}
}

func TestLoadAgentInvalid(t *testing.T) {
	clearAgentEnv(t)
	tenants := "tenants:\n  - id: demo\n    model:\n      name: m\n      api_key: k\n"
	cases := map[string]string{
		"bad duration":      "agent:\n  message_timeout: soon\n" + tenants,
		"zero timeout":      "agent:\n  message_timeout: 0s\n" + tenants,
		"negative timeout":  "agent:\n  message_timeout: -1s\n" + tenants,
		"negative quota":    "agent:\n  max_concurrency_per_tenant: -1\n" + tenants,
		"non-integer quota": "agent:\n  max_concurrency_per_tenant: many\n" + tenants,
		"negative cap":      "agent:\n  max_llm_calls: -1\n" + tenants,
		"non-integer cap":   "agent:\n  max_llm_calls: many\n" + tenants,
	}
	for name, doc := range cases {
		if _, err := Load(writeConfig(t, doc)); err == nil {
			t.Fatalf("%s: must fail validation", name)
		}
	}
}

func TestAgentEnvOverride(t *testing.T) {
	t.Setenv("AGENT_MESSAGE_TIMEOUT", "45s")
	t.Setenv("AGENT_MAX_CONCURRENCY_PER_TENANT", "2")
	t.Setenv("AGENT_MAX_LLM_CALLS", "3")
	clearStorageEnv(t)
	cfg, err := Load(writeConfig(t, agentYAMLDoc)) // file says 3s / 5 / 4
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Agent.MessageTimeout != 45*time.Second || cfg.Agent.MaxConcurrencyPerTenant != 2 ||
		cfg.Agent.MaxLLMCalls != 3 {
		t.Fatalf("env override not applied: %+v", cfg.Agent)
	}
}

func TestAgentEnvInvalid(t *testing.T) {
	tenants := "tenants:\n  - id: demo\n    model:\n      name: m\n      api_key: k\n"
	cases := []struct{ name, key, val string }{
		{"bad timeout", "AGENT_MESSAGE_TIMEOUT", "soon"},
		{"bad quota", "AGENT_MAX_CONCURRENCY_PER_TENANT", "many"},
		{"bad call cap", "AGENT_MAX_LLM_CALLS", "many"},
	}
	for _, c := range cases {
		clearAgentEnv(t)
		t.Setenv(c.key, c.val)
		_, err := Load(writeConfig(t, tenants))
		if err == nil {
			t.Fatalf("%s: must fail", c.name)
		}
		// The variable name has to appear, otherwise an operator staring at a
		// crashing container cannot tell which env var is malformed.
		if !strings.Contains(err.Error(), c.key) {
			t.Fatalf("%s: error %q must name %s", c.name, err, c.key)
		}
	}
}

func TestSaveAgentRoundTrip(t *testing.T) {
	clearAgentEnv(t)
	cfg, err := Load(writeConfig(t, agentYAMLDoc))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	raw, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"agent:", "message_timeout: 3s", "max_concurrency_per_tenant: 5", "max_llm_calls: 4",
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("saved config lost %q:\n%s", want, raw)
		}
	}
	again, err := Load(dst)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if again.Agent != cfg.Agent {
		t.Fatalf("round trip mismatch: %+v vs %+v", again.Agent, cfg.Agent)
	}

	// Pure-default agent settings must stay out of the serialized file.
	def, err := Load(writeConfig(t, wecomYAML))
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	dst2 := filepath.Join(t.TempDir(), "saved2.yaml")
	if err := Save(dst2, def); err != nil {
		t.Fatalf("save default: %v", err)
	}
	raw2, err := os.ReadFile(dst2)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw2), "\nagent:") {
		t.Fatalf("default agent section must be omitted, got:\n%s", raw2)
	}
}

func TestSaveAgentPartialOmission(t *testing.T) {
	clearAgentEnv(t)
	// Only the quota differs from the default: the timeout must not be written
	// out, or every save would churn the file with the implicit default.
	cfg, err := Load(writeConfig(t, wecomYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	cfg.Agent.MaxConcurrencyPerTenant = 4
	dst := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	raw, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "max_concurrency_per_tenant: 4") {
		t.Fatalf("quota lost:\n%s", raw)
	}
	if strings.Contains(string(raw), "message_timeout") {
		t.Fatalf("default timeout must be omitted:\n%s", raw)
	}
	if strings.Contains(string(raw), "max_llm_calls") {
		t.Fatalf("default call cap must be omitted:\n%s", raw)
	}
	again, err := Load(dst)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if again.Agent.MessageTimeout != DefaultMessageTimeout || again.Agent.MaxConcurrencyPerTenant != 4 ||
		again.Agent.MaxLLMCalls != DefaultMaxLLMCalls {
		t.Fatalf("agent after round trip = %+v", again.Agent)
	}
}

const kfYAML = `
default_tenant: demo
tenants:
  - id: demo
    name: Demo
    model:
      name: deepseek-chat
      api_key: sk-test
    channels:
      wechat_kf:
        corp_id: wx5823bf96d3bd56c7
        secret: kf-secret
        token: QDG6eK
        encoding_aes_key: jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C
`

func TestLoadWeChatKfBinding(t *testing.T) {
	cfg, err := Load(writeConfig(t, kfYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b, ok := cfg.WeChatKfBinding("demo")
	if !ok {
		t.Fatal("demo should have a wechat_kf binding")
	}
	if b.CorpID != "wx5823bf96d3bd56c7" || b.Secret != "kf-secret" || b.Token != "QDG6eK" {
		t.Fatalf("binding = %+v", b)
	}
	if _, ok := cfg.WeChatKfBinding("missing"); ok {
		t.Fatal("unknown tenant must not have a binding")
	}

	// Save/Load round trip keeps the block.
	dst := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	again, err := Load(dst)
	if err != nil {
		t.Fatalf("reload saved config: %v", err)
	}
	if b2, ok := again.WeChatKfBinding("demo"); !ok || b2.Secret != "kf-secret" {
		t.Fatalf("binding after round trip = %+v, %v", b2, ok)
	}
}

func TestLoadWeChatKfBindingIncomplete(t *testing.T) {
	_, err := Load(writeConfig(t, `
default_tenant: demo
tenants:
  - id: demo
    model:
      name: m
      api_key: k
    channels:
      wechat_kf:
        corp_id: wx123
        token: tk
`))
	if err == nil {
		t.Fatal("incomplete wechat_kf binding must fail validation")
	}
}

const observabilityYAML = `
default_tenant: demo
log:
  level: debug
  json: true
audit:
  file: data/audit.jsonl
telemetry:
  traces:
    exporter: otlp
    endpoint: http://127.0.0.1:4318
  metrics:
    exporter: stdout
    interval: 30s
tenants:
  - id: demo
    model:
      name: deepseek-chat
      api_key: sk-test
    guardrails:
      max_input_bytes: 4096
      blocked_keywords: ["badword", "secret"]
      output_blocked_keywords: ["internal"]
`

func TestLoadObservability(t *testing.T) {
	cfg, err := Load(writeConfig(t, observabilityYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Log.Level != "debug" || !cfg.Log.JSON {
		t.Fatalf("log = %+v", cfg.Log)
	}
	if cfg.Audit.File != "data/audit.jsonl" {
		t.Fatalf("audit = %+v", cfg.Audit)
	}
	if cfg.Telemetry.Traces.Exporter != ExporterOTLP || cfg.Telemetry.Traces.Endpoint != "http://127.0.0.1:4318" {
		t.Fatalf("traces = %+v", cfg.Telemetry.Traces)
	}
	if cfg.Telemetry.Metrics.Exporter != ExporterStdout || cfg.Telemetry.Metrics.Interval != 30*time.Second {
		t.Fatalf("metrics = %+v", cfg.Telemetry.Metrics)
	}
	g := cfg.Tenants["demo"].Guardrails
	if g.MaxInputBytes != 4096 || len(g.BlockedKeywords) != 2 || g.OutputBlockedKeywords[0] != "internal" {
		t.Fatalf("guardrails = %+v", g)
	}
}

func TestLoadObservabilityDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, wecomYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Log.Level != DefaultLogLevel || cfg.Log.JSON {
		t.Fatalf("log defaults = %+v", cfg.Log)
	}
	if cfg.Audit.File != "" {
		t.Fatalf("audit default = %+v", cfg.Audit)
	}
	if cfg.Telemetry.Traces.Exporter != ExporterOff || cfg.Telemetry.Metrics.Exporter != ExporterOff {
		t.Fatalf("telemetry defaults = %+v", cfg.Telemetry)
	}
	if cfg.Telemetry.Metrics.Interval != DefaultMetricInterval {
		t.Fatalf("interval default = %v", cfg.Telemetry.Metrics.Interval)
	}
}

func TestLoadObservabilityInvalid(t *testing.T) {
	tenants := "tenants:\n  - id: demo\n    model:\n      name: m\n      api_key: k\n"
	cases := map[string]string{
		"bad log level":     "log:\n  level: verbose\n" + tenants,
		"bad exporter":      "telemetry:\n  traces:\n    exporter: jaeger\n" + tenants,
		"otlp without http": "telemetry:\n  metrics:\n    exporter: otlp\n    endpoint: 127.0.0.1:4318\n" + tenants,
		"bad interval":      "telemetry:\n  metrics:\n    interval: soon\n" + tenants,
		"negative interval": "telemetry:\n  metrics:\n    interval: -5s\n" + tenants,
		"negative bytes":    "tenants:\n  - id: demo\n    model:\n      name: m\n      api_key: k\n    guardrails:\n      max_input_bytes: -1\n",
		"empty keyword":     "tenants:\n  - id: demo\n    model:\n      name: m\n      api_key: k\n    guardrails:\n      blocked_keywords: [\"  \"]\n",
		"empty output kw":   "tenants:\n  - id: demo\n    model:\n      name: m\n      api_key: k\n    guardrails:\n      output_blocked_keywords: [\"\"]\n",
	}
	for name, y := range cases {
		if _, err := Load(writeConfig(t, y)); err == nil {
			t.Fatalf("%s: must fail validation", name)
		}
	}
}

func TestSaveObservabilityRoundTrip(t *testing.T) {
	cfg, err := Load(writeConfig(t, observabilityYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	raw, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"level: debug", "file: data/audit.jsonl", "endpoint: http://127.0.0.1:4318", "interval: 30s", "max_input_bytes: 4096", "blocked_keywords"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("saved config lost %q:\n%s", want, raw)
		}
	}
	again, err := Load(dst)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if again.Log != cfg.Log || again.Audit != cfg.Audit || again.Telemetry != cfg.Telemetry {
		t.Fatalf("round trip mismatch: %+v vs %+v", again.Telemetry, cfg.Telemetry)
	}
	if g := again.Tenants["demo"].Guardrails; g.MaxInputBytes != 4096 || len(g.BlockedKeywords) != 2 {
		t.Fatalf("guardrails after round trip = %+v", g)
	}

	// Pure-default observability must stay out of the serialized file.
	def, err := Load(writeConfig(t, wecomYAML))
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	dst2 := filepath.Join(t.TempDir(), "saved2.yaml")
	if err := Save(dst2, def); err != nil {
		t.Fatalf("save default: %v", err)
	}
	raw2, err := os.ReadFile(dst2)
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"log:", "audit:", "telemetry:", "guardrails:", "agent:"} {
		if strings.Contains(string(raw2), banned) {
			t.Fatalf("default observability must be omitted, got %q in:\n%s", banned, raw2)
		}
	}
}

// TestShippedExampleStaysUsable pins the one file every deployment starts from.
// Nothing else in the repo loads config.example.yaml, so a drift between the
// parser and the template — a renamed key, a newly required field, a default
// that moved — would surface only as a broken first run for the user.
func TestShippedExampleStaysUsable(t *testing.T) {
	// Env overrides are applied last, so a leftover export would rewrite the
	// very tenant this test asserts on.
	for _, k := range []string{
		"MODEL_API_KEY", "MODEL_NAME", "MODEL_BASE_URL",
		"STORAGE_SESSION_BACKEND", "STORAGE_SESSION_REDIS_URL",
		"AGENT_MESSAGE_TIMEOUT", "AGENT_MAX_CONCURRENCY_PER_TENANT", "AGENT_MAX_LLM_CALLS",
	} {
		t.Setenv(k, "")
	}

	cfg, err := Load(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatalf("the shipped template must load and validate: %v", err)
	}

	// Every block the template documents as optional really is: commented out,
	// they must resolve to the documented defaults rather than to zero values.
	if cfg.Agent.MessageTimeout != DefaultMessageTimeout || cfg.Agent.MaxConcurrencyPerTenant != 0 ||
		cfg.Agent.MaxLLMCalls != DefaultMaxLLMCalls {
		t.Fatalf("agent = %+v, want the documented defaults", cfg.Agent)
	}
	if cfg.Storage.Session.Backend != BackendMemory {
		t.Fatalf("session backend = %q, want %q", cfg.Storage.Session.Backend, BackendMemory)
	}
	if cfg.DefaultTenant != "demo" {
		t.Fatalf("default tenant = %q, want demo", cfg.DefaultTenant)
	}
	for _, id := range []string{"demo", "demo-alt"} {
		if _, ok := cfg.Tenants[id]; !ok {
			t.Fatalf("template lost tenant %q", id)
		}
	}

	// The template is committed to git and is what people are told to copy, so
	// it must never carry a credential that looks real.
	for id, tn := range cfg.Tenants {
		if !strings.HasPrefix(tn.Model.APIKey, "REPLACE_") {
			t.Fatalf("tenant %q ships a real-looking api key %q", id, tn.Model.APIKey)
		}
	}

	// Round trip: what Save writes for the loaded template must load back as the
	// same config. An operator editing tenants through the Admin API commits
	// exactly this path, so a field the template declared must not vanish.
	dst := filepath.Join(t.TempDir(), "roundtrip.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	back, err := Load(dst)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !reflect.DeepEqual(cfg, back) {
		t.Fatalf("template does not survive a save/load round trip:\nbefore = %+v\nafter  = %+v", cfg, back)
	}
}
