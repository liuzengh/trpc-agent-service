package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// ValidateBackendBindingConfig reuses the router configuration types without
// constructing clients, resolving credentials, dialing or initializing tables.
func ValidateBackendBindingConfig(b controlplane.BackendBinding) error {
	invalid := errors.New("invalid backend configuration")
	raw := bytes.TrimSpace(b.Config)
	if len(raw) > 0 && (raw[0] != '{' || !json.Valid(raw)) {
		return invalid
	}
	kind := strings.ToLower(b.BackendType)
	switch b.ResourceType {
	case "session":
		if kind == "startup_config" {
			return nil
		} // owned by deployment startup config
		var cfg sessionBackendConfig
		if decodeStorageConfig(raw, &cfg) != nil {
			return invalid
		}
		if cfg.TTL != "" {
			ttl, err := time.ParseDuration(cfg.TTL)
			if err != nil || ttl < 0 {
				return invalid
			}
		}
		switch kind {
		case "inmemory":
			return nil
		case "redis":
			if cfg.URL != "" || b.SecretRef != "" {
				return nil
			}
		case "postgres":
			if cfg.DSN != "" || b.SecretRef != "" {
				return nil
			}
		}
	case "memory":
		var cfg memoryBackendConfig
		if decodeStorageConfig(raw, &cfg) != nil || cfg.MemoryLimit < 0 {
			return invalid
		}
		switch kind {
		case "inmemory":
			return nil
		case "redis":
			if cfg.URL != "" || b.SecretRef != "" {
				return nil
			}
		case "postgres":
			if cfg.DSN != "" || b.SecretRef != "" {
				return nil
			}
		}
	case "knowledge":
		var cfg knowledgeBackendConfig
		if decodeStorageConfig(raw, &cfg) != nil || cfg.Dimensions < 0 || cfg.Dimensions > 65536 || cfg.MaxResults < 0 {
			return invalid
		}
		if kind == "inmemory" {
			return nil
		}
		if kind == "qdrant" && cfg.Host != "" && cfg.Port > 0 && cfg.Port <= 65535 && cfg.CollectionName != "" {
			return nil
		}
	case "artifact":
		var cfg artifactBackendConfig
		if decodeStorageConfig(raw, &cfg) != nil || cfg.Retries < 0 {
			return invalid
		}
		if kind == "inmemory" {
			return nil
		}
		if (kind == "s3" || kind == "minio") && cfg.Bucket != "" && b.SecretRef != "" {
			return nil
		}
	}
	return invalid
}
