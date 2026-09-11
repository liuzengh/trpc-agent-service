// Package agentspecv1 embeds the public AgentSpec V1 JSON Schema so tests and
// consumers can validate the exact versioned protocol checked into the repo.
package agentspecv1

import _ "embed"

// Schema is the canonical AgentSpec V1 JSON Schema.
//
//go:embed agent-spec.schema.json
var Schema []byte
