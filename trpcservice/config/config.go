// Package config loads tenant, model, channel, and storage backend settings.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration for the service.
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Log       LogConfig       `yaml:"log"`
	Role      string          `yaml:"role"`
	MySQL     MySQLConfig     `yaml:"mysql"`
	Redis     RedisConfig     `yaml:"redis"`
	Milvus    MilvusConfig    `yaml:"milvus"`
	MinIO     MinIOConfig     `yaml:"minio"`
	Secret    SecretConfig    `yaml:"secret"`
	Telemetry TelemetryConfig `yaml:"telemetry"`
}

// ServerConfig configures the HTTP server.
type ServerConfig struct {
	HTTPAddr string `yaml:"http_addr"`
}

// MySQLConfig configures the platform MySQL backend. An empty DSN keeps the
// service in memory mode (no persistence).
type MySQLConfig struct {
	DSN string `yaml:"dsn"`
}

// RedisConfig configures the shared Redis backend.
type RedisConfig struct {
	URL string `yaml:"url"`
}

// MilvusConfig configures the Milvus vector backend. An empty address keeps
// knowledge bases on the in-memory vector store (dev/test mode).
type MilvusConfig struct {
	Address  string `yaml:"address"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// MinIOConfig configures the S3-compatible object store that holds agent
// artifacts. An empty endpoint disables artifact persistence (runner runs
// without an artifact service).
type MinIOConfig struct {
	Endpoint  string `yaml:"endpoint"` // host:port, e.g. minio:9000
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Bucket    string `yaml:"bucket"` // default "artifacts"
	UseSSL    bool   `yaml:"use_ssl"`
}

// SecretConfig configures the credential store. MasterKey is the encryption
// master key (also settable via env TRPC_SECRET_MASTER_KEY, which wins). With
// MySQL the credential store is disabled when no master key is present, so
// plaintext is never written at rest.
type SecretConfig struct {
	MasterKey string `yaml:"master_key"`
}

// TelemetryConfig configures OpenTelemetry trace + metrics export. An empty
// OTLPEndpoint keeps the noop provider (no export, recording is a no-op).
type TelemetryConfig struct {
	OTLPEndpoint string `yaml:"otlp_endpoint"` // OTLP collector host:port (grpc)
	ServiceName  string `yaml:"service_name"`
}

// LogConfig configures structured logging.
type LogConfig struct {
	Level string `yaml:"level"`
}

// Default returns a Config populated with safe defaults.
func Default() *Config {
	return &Config{
		Server:    ServerConfig{HTTPAddr: ":8080"},
		Log:       LogConfig{Level: "info"},
		Role:      "all",
		Telemetry: TelemetryConfig{ServiceName: "trpc-agent-service"},
	}
}

// Load reads a YAML config file, applying defaults for any unset fields.
func Load(path string) (*Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %q: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}
	return cfg, nil
}
