package config

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultOTelSpanQueue = 2048
	DefaultOTelSpanBatch = 512
)

type ObservabilityConfig struct {
	Enabled        bool
	Endpoint       string
	ServiceName    string
	SampleRatio    float64
	SpanQueueSize  int
	SpanBatchSize  int
	ScheduleDelay  time.Duration
	ExportTimeout  time.Duration
	MetricInterval time.Duration
}

type observabilityConfigFile struct {
	Enabled        bool    `json:"enabled,omitempty"`
	Endpoint       string  `json:"endpoint,omitempty"`
	ServiceName    string  `json:"service_name,omitempty"`
	SampleRatio    float64 `json:"sample_ratio,omitempty"`
	SpanQueueSize  int     `json:"span_queue_size,omitempty"`
	SpanBatchSize  int     `json:"span_batch_size,omitempty"`
	ScheduleDelay  string  `json:"schedule_delay,omitempty"`
	ExportTimeout  string  `json:"export_timeout,omitempty"`
	MetricInterval string  `json:"metric_interval,omitempty"`
}

func parseObservabilityFile(raw *observabilityConfigFile) (ObservabilityConfig, error) {
	cfg := ObservabilityConfig{ServiceName: "trpc-agent-service", SampleRatio: 1, SpanQueueSize: DefaultOTelSpanQueue, SpanBatchSize: DefaultOTelSpanBatch, ScheduleDelay: time.Second, ExportTimeout: 2 * time.Second, MetricInterval: 5 * time.Second}
	if raw != nil {
		cfg.Enabled, cfg.Endpoint, cfg.ServiceName = raw.Enabled, strings.TrimSpace(raw.Endpoint), valueOrDefault(raw.ServiceName, cfg.ServiceName)
		if raw.SampleRatio != 0 {
			cfg.SampleRatio = raw.SampleRatio
		}
		if raw.SpanQueueSize != 0 {
			cfg.SpanQueueSize = raw.SpanQueueSize
		}
		if raw.SpanBatchSize != 0 {
			cfg.SpanBatchSize = raw.SpanBatchSize
		}
		var err error
		if cfg.ScheduleDelay, err = parseOptionalDuration(raw.ScheduleDelay, cfg.ScheduleDelay); err != nil {
			return ObservabilityConfig{}, errors.New("observability schedule_delay is invalid")
		}
		if cfg.ExportTimeout, err = parseOptionalDuration(raw.ExportTimeout, cfg.ExportTimeout); err != nil {
			return ObservabilityConfig{}, errors.New("observability export_timeout is invalid")
		}
		if cfg.MetricInterval, err = parseOptionalDuration(raw.MetricInterval, cfg.MetricInterval); err != nil {
			return ObservabilityConfig{}, errors.New("observability metric_interval is invalid")
		}
	}
	if endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); endpoint != "" {
		cfg.Enabled, cfg.Endpoint = true, endpoint
	}
	if ratio := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG")); ratio != "" {
		value, err := strconv.ParseFloat(ratio, 64)
		if err != nil {
			return ObservabilityConfig{}, errors.New("OTEL_TRACES_SAMPLER_ARG is invalid")
		}
		cfg.SampleRatio = value
	}
	if err := cfg.Validate(); err != nil {
		return ObservabilityConfig{}, err
	}
	return cfg, nil
}

func (c ObservabilityConfig) Validate() error {
	if c.SampleRatio < 0 || c.SampleRatio > 1 {
		return errors.New("observability sample_ratio must be between 0 and 1")
	}
	if c.SpanQueueSize < 1 || c.SpanBatchSize < 1 || c.SpanBatchSize > c.SpanQueueSize {
		return errors.New("observability span queue and batch sizes are invalid")
	}
	if c.ScheduleDelay <= 0 || c.ExportTimeout <= 0 || c.MetricInterval <= 0 {
		return errors.New("observability durations must be positive")
	}
	if !c.Enabled {
		return nil
	}
	parsed, err := url.Parse(c.Endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return errors.New("observability endpoint must be an absolute HTTP(S) URL without credentials")
	}
	return nil
}
