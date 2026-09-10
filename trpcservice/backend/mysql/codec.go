package mysql

import "github.com/XnLemon/trpc-agent-service/trpcservice/backend"

type backendBindingJSON struct {
	Capability string            `json:"capability"`
	Provider   string            `json:"provider"`
	Endpoint   string            `json:"endpoint,omitempty"`
	Options    map[string]string `json:"options,omitempty"`
	SecretRef  string            `json:"secret_ref,omitempty"`
}

func encodeBackendBindings(bindings []backend.CapabilityBinding) ([]byte, error) {
	values := make([]backendBindingJSON, 0, len(bindings))
	for _, binding := range bindings {
		values = append(values, backendBindingJSON{
			Capability: string(binding.Capability),
			Provider:   binding.Provider,
			Endpoint:   binding.Endpoint,
			Options:    binding.Options,
			SecretRef:  binding.SecretRef,
		})
	}
	return encodeJSON(values)
}
