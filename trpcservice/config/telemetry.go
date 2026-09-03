package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type TelemetryConfig struct {
	Enabled     bool
	ServiceName string
	Endpoint    string
	Insecure    bool
	SampleRatio float64
}

func LoadTelemetryConfigFromEnv() (TelemetryConfig, error) {
	enabled, err := parseBoolEnv("TRPC_AGENT_OTEL_ENABLED", false)
	if err != nil {
		return TelemetryConfig{}, err
	}
	insecure, err := parseBoolEnv("TRPC_AGENT_OTEL_INSECURE", true)
	if err != nil {
		return TelemetryConfig{}, err
	}
	config := TelemetryConfig{
		Enabled:     enabled,
		ServiceName: strings.TrimSpace(os.Getenv("TRPC_AGENT_OTEL_SERVICE_NAME")),
		Endpoint:    strings.TrimSpace(os.Getenv("TRPC_AGENT_OTEL_ENDPOINT")),
		Insecure:    insecure,
		SampleRatio: 1,
	}
	if config.ServiceName == "" {
		config.ServiceName = "trpc-agent-service"
	}
	if raw := strings.TrimSpace(os.Getenv("TRPC_AGENT_OTEL_SAMPLE_RATIO")); raw != "" {
		ratio, err := strconv.ParseFloat(raw, 64)
		if err != nil || ratio < 0 || ratio > 1 {
			return TelemetryConfig{}, fmt.Errorf("TRPC_AGENT_OTEL_SAMPLE_RATIO must be between 0 and 1")
		}
		config.SampleRatio = ratio
	}
	if config.Enabled && config.Endpoint == "" {
		return TelemetryConfig{}, fmt.Errorf("TRPC_AGENT_OTEL_ENDPOINT is required when telemetry is enabled")
	}
	return config, nil
}
