package migrations

import (
	"strings"
	"testing"
)

func TestServiceSchemaPinsToolBindingDigest(t *testing.T) {
	baseline := serviceSchemaBaseline(t)
	for _, clause := range []string{
		"content_digest character(64)",
		"agent_app_revision_tool_content_digest_check",
	} {
		if !strings.Contains(baseline.Up, clause) {
			t.Fatalf("service schema missing MCP ToolRef binding clause %q", clause)
		}
	}
}
