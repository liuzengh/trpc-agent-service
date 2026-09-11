package tracing_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func read(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err = yaml.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func object(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatal("expected config object")
	}
	return m
}
func TestTracingCompositionIsolation(t *testing.T) {
	c := read(t, "../compose.tracing.yaml")
	services := object(t, c["services"])
	if len(services) != 3 {
		t.Fatal("unexpected deployment expansion")
	}
	pin := regexp.MustCompile(`^[a-z/-]+:[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$`)
	for name, v := range services {
		s := object(t, v)
		if !pin.MatchString(s["image"].(string)) {
			t.Fatal("unpinned image", name)
		}
		if s["read_only"] != true || s["mem_limit"] == nil || s["stop_grace_period"] == nil {
			t.Fatal("missing resource/lifecycle controls", name)
		}
		profiles := s["profiles"].([]any)
		if len(profiles) != 1 || profiles[0] != "tracing" {
			t.Fatal("stack starts implicitly")
		}
		for _, v := range s["ports"].([]any) {
			if !strings.HasPrefix(v.(string), "127.0.0.1:") {
				t.Fatal("public bind", name)
			}
		}
		for _, v := range s["volumes"].([]any) {
			p := v.(string)
			if strings.Contains(p, "postgres") || strings.Contains(p, "worker-runtime") || strings.Contains(p, ".env") {
				t.Fatal("business storage/config mounted")
			}
		}
	}
	if len(object(t, c["volumes"])) != 2 {
		t.Fatal("separate Tempo/Grafana volumes expected")
	}
}
func TestCollectorBoundsAndFiltering(t *testing.T) {
	c := read(t, "collector.yaml")
	processors := object(t, c["processors"])
	limiter := object(t, processors["memory_limiter"])
	if limiter["limit_mib"] != 128 || limiter["spike_limit_mib"] != 32 {
		t.Fatal("explicit collector bounds changed")
	}
	pipelines := object(t, object(t, c["service"])["pipelines"])
	if len(pipelines) != 1 {
		t.Fatal("unexpected telemetry pipeline")
	}
	p := object(t, pipelines["traces"])["processors"].([]any)
	want := []string{"memory_limiter", "filter/events", "transform/allowlist", "batch"}
	if len(p) != len(want) {
		t.Fatal(p)
	}
	for i := range want {
		if p[i] != want[i] {
			t.Fatal("processor order", p)
		}
	}
	exporters := object(t, c["exporters"])
	if len(exporters) != 1 || exporters["otlphttp/tempo"] == nil {
		t.Fatal("unexpected external/debug exporter")
	}
	e := object(t, exporters["otlphttp/tempo"])
	if e["endpoint"] != "http://trace-tempo:4318" || object(t, e["retry_on_failure"])["max_elapsed_time"] != "15s" {
		t.Fatal("unbounded or redirected export")
	}
	tls := object(t, object(t, object(t, object(t, read(t, "collector-tls.yaml")["receivers"])["otlp"])["protocols"])["http"])
	if object(t, tls["tls"])["min_version"] != "1.3" {
		t.Fatal("TLS minimum")
	}
	tempo := read(t, "tempo.yaml")
	if object(t, object(t, tempo["compactor"])["compaction"])["block_retention"] != "24h" {
		t.Fatal("retention must be explicit")
	}
}
func TestGrafanaQueryRequiresLogin(t *testing.T) {
	c := read(t, "../compose.tracing.yaml")
	g := object(t, object(t, c["services"])["trace-grafana"])
	env := object(t, g["environment"])
	for _, key := range []string{"GF_AUTH_ANONYMOUS_ENABLED", "GF_USERS_ALLOW_SIGN_UP", "GF_ANALYTICS_REPORTING_ENABLED"} {
		if env[key] != "false" {
			t.Fatal(key)
		}
	}
	if env["GF_SECURITY_ADMIN_PASSWORD"] != nil || env["GF_SECURITY_ADMIN_PASSWORD__FILE"] != "/run/secrets/grafana-admin-password" {
		t.Fatal("admin password must come from file")
	}
	ds := read(t, "grafana/provisioning/datasources/tempo.yaml")["datasources"].([]any)
	if len(ds) != 1 {
		t.Fatal("one datasource expected")
	}
	d := object(t, ds[0])
	if d["uid"] != "im-runtime-tempo" || d["access"] != "proxy" || d["editable"] != false || d["url"] != "http://trace-tempo:3200" {
		t.Fatal("datasource must use internal backend")
	}
}
