package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"gopkg.in/yaml.v3"
)

const (
	// CurrentSchemaVersion is the only configuration schema accepted by this
	// version of the service.
	CurrentSchemaVersion = 1
	// EnvConfigPath selects the YAML configuration file used by LoadFromEnv.
	EnvConfigPath = "TRPC_AGENT_CONFIG"
)

// File is the validated, tenant-scoped service configuration. Treat a loaded
// File as startup-only input and do not mutate it concurrently. Hot reloaders
// must load a new File and atomically publish snapshots derived from it.
type File struct {
	SchemaVersion int             `json:"schema_version" yaml:"schema_version"`
	Tenants       []tenant.Tenant `json:"tenants" yaml:"tenants"`
}

// Load decodes exactly one YAML document, rejects unknown fields, and validates
// all tenants including disabled entries.
func Load(reader io.Reader) (*File, error) {
	if reader == nil {
		return nil, errors.New("config: nil reader")
	}
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)

	var file File
	if err := decoder.Decode(&file); err != nil {
		return nil, fmt.Errorf("config: decode YAML: %w", err)
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("config: decode trailing YAML: %w", err)
		}
		return nil, errors.New("config: multiple YAML documents are not allowed")
	}

	if err := file.Validate(); err != nil {
		return nil, err
	}
	return &file, nil
}

// LoadFile loads a configuration file from path.
func LoadFile(path string) (*File, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("config: path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %q: %w", path, err)
	}
	defer file.Close()

	loaded, err := Load(file)
	if err != nil {
		return nil, fmt.Errorf("config: load %q: %w", path, err)
	}
	return loaded, nil
}

// LoadFromEnv loads the file selected by TRPC_AGENT_CONFIG. SecretRef values
// remain unresolved; loading configuration never reads secret values.
func LoadFromEnv() (*File, error) {
	path := strings.TrimSpace(os.Getenv(EnvConfigPath))
	if path == "" {
		return nil, fmt.Errorf("config: %s is not set", EnvConfigPath)
	}
	return LoadFile(path)
}
