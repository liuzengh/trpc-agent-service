package tenant

import (
	"errors"
	"fmt"
)

// ErrStorageBindingChange prevents a configuration update from bypassing the
// data migration protocol. Migration cutover owns its separate fenced CAS.
var ErrStorageBindingChange = errors.New("existing storage binding requires a data migration")

func validateStorageBindingUpdate(current, next StorageConfig) error {
	bindings := []struct {
		domain                         string
		currentBackend, currentProfile string
		nextBackend, nextProfile       string
	}{
		{"session", current.SessionBackend, current.SessionProfile, next.SessionBackend, next.SessionProfile},
		{"memory", current.MemoryBackend, current.MemoryProfile, next.MemoryBackend, next.MemoryProfile},
		{"knowledge", current.KnowledgeBackend, current.KnowledgeProfile, next.KnowledgeBackend, next.KnowledgeProfile},
		{"artifact", current.ArtifactBackend, current.ArtifactProfile, next.ArtifactBackend, next.ArtifactProfile},
	}
	for _, binding := range bindings {
		if binding.currentBackend == "" && binding.currentProfile == "" {
			continue
		}
		if binding.currentBackend != binding.nextBackend || binding.currentProfile != binding.nextProfile {
			return fmt.Errorf("%w: %s: %w", ErrInvalidTenantConfig, binding.domain, ErrStorageBindingChange)
		}
	}
	return nil
}
