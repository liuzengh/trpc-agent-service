package config

import "testing"

func TestLoadTelemetryConfig(t *testing.T) {
	t.Setenv("TRPC_AGENT_OTEL_ENABLED", "true")
	t.Setenv("TRPC_AGENT_OTEL_ENDPOINT", "localhost:4317")
	t.Setenv("TRPC_AGENT_OTEL_SAMPLE_RATIO", "0.5")
	config, err := LoadTelemetryConfigFromEnv()
	if err != nil || !config.Enabled || config.SampleRatio != 0.5 {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}

func TestLoadTelemetryConfigRequiresEndpoint(t *testing.T) {
	t.Setenv("TRPC_AGENT_OTEL_ENABLED", "true")
	t.Setenv("TRPC_AGENT_OTEL_ENDPOINT", "")
	if _, err := LoadTelemetryConfigFromEnv(); err == nil {
		t.Fatal("expected endpoint error")
	}
}
