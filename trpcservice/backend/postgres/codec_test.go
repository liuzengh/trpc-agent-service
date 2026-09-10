package postgres

import (
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

func TestBackendPostgresBindingsCodec(t *testing.T) {
	encoded, err := encodeBackendBindings([]backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "primary"}}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded []backendBindingJSON
	if err := decodeJSON(encoded, &decoded); err != nil || len(decoded) != 1 || decoded[0].Options["namespace"] != "primary" {
		t.Fatalf("bindings decode = %+v, err=%v", decoded, err)
	}
}
