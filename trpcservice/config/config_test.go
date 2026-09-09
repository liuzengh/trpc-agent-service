package config

import (
	"os"
	"path/filepath"
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
	for _, banned := range []string{"log:", "audit:", "telemetry:", "guardrails:"} {
		if strings.Contains(string(raw2), banned) {
			t.Fatalf("default observability must be omitted, got %q in:\n%s", banned, raw2)
		}
	}
}
