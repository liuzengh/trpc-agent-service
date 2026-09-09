package tenant_test

import (
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// legacyRevisionConfigJSON is a revision config as it was written before
// ModelConfig had a base_url field, in the byte order RevisionConfig marshals
// to. Both constants are golden values: they are not derived from the current
// struct, so a change that alters the encoding fails here instead of turning
// every stored revision into a digest mismatch at read time.
const (
	legacyRevisionConfigJSON = `{"agent_name":"support-agent",` +
		`"instruction":"Help the user.",` +
		`"model":{"provider":"deterministic","name":"echo-v1",` +
		`"secret_ref":"env:LEGACY_KEY","temperature":0.5,"max_tokens":256},` +
		`"tool_refs":["orders.read"]}`
	legacyRevisionConfigDigest = "cb477c19aca5dfdcdc4308394db45d3495373609fd9d8db45b5ffbe58d9f942b"
)

// A revision is immutable and its stored ConfigDigest is re-checked on read, so
// adding base_url had to add no bytes to a config that does not set it. If it
// did, every revision written before the field existed would come back as
// ErrConfigIntegrity.
func TestModelConfigBaseURLKeepsLegacyConfigsCompatible(t *testing.T) {
	var config tenant.RevisionConfig
	require.NoError(t, json.Unmarshal([]byte(legacyRevisionConfigJSON), &config))
	require.Equal(t, "", config.Model.BaseURL)

	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	require.Equal(t, legacyRevisionConfigJSON, string(encoded))

	digest, err := config.Digest()
	require.NoError(t, err)
	require.Equal(t, legacyRevisionConfigDigest, digest)
}

// A config that does set base_url must round-trip it and must land on a
// different digest, so the endpoint is part of what a revision pins.
func TestModelConfigBaseURLRoundTripsAndChangesDigest(t *testing.T) {
	var config tenant.RevisionConfig
	require.NoError(t, json.Unmarshal([]byte(legacyRevisionConfigJSON), &config))

	config.Model.BaseURL = "https://api.example.com/v1"
	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"base_url":"https://api.example.com/v1"`)

	var decoded tenant.RevisionConfig
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, "https://api.example.com/v1", decoded.Model.BaseURL)

	digest, err := config.Digest()
	require.NoError(t, err)
	require.NotEqual(t, legacyRevisionConfigDigest, digest)
}
