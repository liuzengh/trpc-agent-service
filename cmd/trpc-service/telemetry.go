package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	serviceotel "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry/otel"
)

func newRoleTelemetry(ctx context.Context, getenv func(string) string, role string, logger *roleLogger) (telemetry.Provider, error) {
	if getenv == nil || strings.TrimSpace(role) == "" || logger == nil {
		return nil, errors.New("invalid telemetry dependencies")
	}
	endpoint := strings.TrimSpace(getenv("TRPC_OTEL_ENDPOINT"))
	if endpoint == "" {
		return telemetry.Noop(), nil
	}
	config := serviceotel.Config{Endpoint: endpoint, ServiceName: "trpc-agent-service",
		ServiceVersion: strings.TrimSpace(valueOr(getenv("TRPC_SERVICE_VERSION"), "development")), Role: role,
		BatchTimeout: time.Second, ExportTimeout: 5 * time.Second, MetricInterval: 10 * time.Second,
		MaxQueueSize: 2048, MaxExportBatchSize: 512, Logger: logger}
	var err error
	if config.AllowInsecure, err = envBool(getenv, "TRPC_OTEL_ALLOW_INSECURE", false); err != nil {
		return nil, errors.New("invalid TRPC_OTEL_ALLOW_INSECURE")
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_OTEL_BATCH_TIMEOUT", &config.BatchTimeout, time.Millisecond},
		{"TRPC_OTEL_EXPORT_TIMEOUT", &config.ExportTimeout, time.Millisecond},
		{"TRPC_OTEL_METRIC_INTERVAL", &config.MetricInterval, 100 * time.Millisecond},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return nil, errors.New("invalid " + item.name)
		}
	}
	if config.MaxQueueSize, err = envInt(getenv, "TRPC_OTEL_MAX_QUEUE_SIZE", config.MaxQueueSize); err != nil || config.MaxQueueSize < 1 || config.MaxQueueSize > 65536 {
		return nil, errors.New("invalid TRPC_OTEL_MAX_QUEUE_SIZE")
	}
	if config.MaxExportBatchSize, err = envInt(getenv, "TRPC_OTEL_MAX_EXPORT_BATCH_SIZE", config.MaxExportBatchSize); err != nil ||
		config.MaxExportBatchSize < 1 || config.MaxExportBatchSize > config.MaxQueueSize {
		return nil, errors.New("invalid TRPC_OTEL_MAX_EXPORT_BATCH_SIZE")
	}
	if config.ServiceVersion == "" || len(config.ServiceVersion) > 128 || strings.ContainsAny(config.ServiceVersion, "\x00\r\n") {
		return nil, errors.New("invalid TRPC_SERVICE_VERSION")
	}
	return serviceotel.New(ctx, config)
}

func shutdownRoleTelemetry(provider telemetry.Provider, logger *roleLogger) {
	if provider == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := provider.Shutdown(ctx); err != nil && logger != nil {
		logger.Printf("telemetry shutdown degraded")
	}
}
