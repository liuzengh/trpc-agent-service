package config

import "testing"

func TestObservabilityDefaultsAndValidation(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	cfg, err := parseObservabilityFile(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Enabled || cfg.SpanQueueSize != 2048 || cfg.SpanBatchSize != 512 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
	if _, err := parseObservabilityFile(&observabilityConfigFile{Enabled: true, Endpoint: "relative"}); err == nil {
		t.Fatal("expected endpoint validation error")
	}
}
