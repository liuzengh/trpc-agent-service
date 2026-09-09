// Package telemetry is the P1-07 server-owned observability boundary: a
// TracerProvider, MeterProvider, structured JSON logger, safe error mapper,
// low-cardinality instrument registry and bounded idempotent shutdown.
// Telemetry is best-effort: it is never a business fact source and export
// failures never change business outcomes.
package telemetry

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Mode selects the export target. ModeNone (the production default) never
// resolves credentials, connects to a collector, starts export goroutines or
// probes; instrumentation uses safe no-op providers.
type Mode string

const (
	ModeNone Mode = "none"
	ModeOTLP Mode = "otlp"
)

const (
	defaultServiceName  = "trpc-agent-service"
	defaultSampleRatio  = 1.0
	defaultBatchSize    = 512
	defaultQueueSize    = 2048
	defaultExportWait   = 5 * time.Second
	defaultFlushTimeout = 10 * time.Second
	maxServiceVersion   = 64
)

var environments = map[string]bool{
	"local": true, "development": true, "staging": true, "production": true, "test": true,
}

// Config is the server-owned telemetry configuration. Invalid values fail
// closed at startup.
type Config struct {
	Mode            Mode
	Endpoint        string
	Environment     string
	ServiceVersion  string
	SampleRatio     float64
	BatchSize       int
	QueueSize       int
	ExportTimeout   time.Duration
	FlushTimeout    time.Duration
	InsecureLocalOK bool
}

func (c Config) WithDefaults() (Config, error) {
	if c.Mode == "" {
		c.Mode = ModeNone
	}
	if c.Mode != ModeNone && c.Mode != ModeOTLP {
		return Config{}, fmt.Errorf("telemetry: invalid mode %q", string(c.Mode))
	}
	if c.Environment == "" {
		c.Environment = "local"
	}
	if !environments[c.Environment] {
		return Config{}, errors.New("telemetry: environment is not allowlisted")
	}
	if len(c.ServiceVersion) > maxServiceVersion || strings.ContainsAny(c.ServiceVersion, "\x00\r\n") {
		return Config{}, errors.New("telemetry: service version is invalid")
	}
	if c.SampleRatio == 0 {
		c.SampleRatio = defaultSampleRatio
	}
	if c.SampleRatio < 0 || c.SampleRatio > 1 {
		return Config{}, errors.New("telemetry: sampling ratio must be within [0,1]")
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.BatchSize < 1 || c.BatchSize > 8192 {
		return Config{}, errors.New("telemetry: batch size out of bounds")
	}
	if c.QueueSize == 0 {
		c.QueueSize = defaultQueueSize
	}
	if c.QueueSize < 1 || c.QueueSize > 65536 {
		return Config{}, errors.New("telemetry: queue size out of bounds")
	}
	if c.ExportTimeout == 0 {
		c.ExportTimeout = defaultExportWait
	}
	if c.ExportTimeout < time.Second || c.ExportTimeout > time.Minute {
		return Config{}, errors.New("telemetry: export timeout out of bounds")
	}
	if c.FlushTimeout == 0 {
		c.FlushTimeout = defaultFlushTimeout
	}
	if c.FlushTimeout < time.Second || c.FlushTimeout > 5*time.Minute {
		return Config{}, errors.New("telemetry: flush timeout out of bounds")
	}
	if c.Mode == ModeOTLP {
		if strings.TrimSpace(c.Endpoint) == "" {
			return Config{}, errors.New("telemetry: OTLP endpoint is required")
		}
		// OTLP gRPC endpoints are bare host:port; an optional scheme:// prefix
		// is tolerated but stripped before validation.
		endpoint := c.Endpoint
		if parsed, err := url.Parse(endpoint); err == nil && parsed.Scheme != "" && parsed.Host != "" {
			endpoint = parsed.Host
		}
		host, port, splitErr := net.SplitHostPort(endpoint)
		if splitErr != nil || host == "" || port == "" {
			return Config{}, errors.New("telemetry: OTLP endpoint is invalid")
		}
		if c.InsecureLocalOK {
			ip := net.ParseIP(host)
			loopback := host == "localhost" || (ip != nil && ip.IsLoopback())
			if !loopback {
				return Config{}, errors.New("telemetry: insecure transport is only allowed for loopback endpoints")
			}
		}
	}
	return c, nil
}

// ParseBool is the bounded boolean env decoder shared by composition roots.
func ParseBool(raw string) (bool, error) {
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("telemetry: invalid boolean %q", raw)
	}
	return value, nil
}
