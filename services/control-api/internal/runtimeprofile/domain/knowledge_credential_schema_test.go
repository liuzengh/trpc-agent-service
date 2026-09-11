package domain_test

import (
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

func TestManagedKnowledgeCredentialCanonicalSchema(t *testing.T) {
	schema := compilePublicSchema(t)
	for _, mode := range []string{"empty", "pair", "no_digest", "invalid_digest"} {
		t.Run(mode, func(t *testing.T) {
			r := map[string]any{"kind": "managed_knowledge", "backend_id": "vectors", "backend_revision": 1, "embedding": map[string]any{"model": "embed", "base_url": "https://embed.example", "dimensions": 3, "api_key_credential_id": "crd_99999999999999999999999999999999"}}
			if mode != "empty" {
				r["qdrant_api_key_credential_id"] = "crd_00000000000000000000000000000001"
				r["credential_audience_digest"] = "sha256:" + strings.Repeat("a", 64)
			}
			if mode == "no_digest" {
				delete(r, "credential_audience_digest")
			}
			if mode == "invalid_digest" {
				r["credential_audience_digest"] = "invalid"
			}
			raw, _ := json.Marshal(map[string]any{"schema_version": "v1", "credential_protocol_version": "v1", "models": map[string]any{}, "tools": map[string]any{}, "knowledge": map[string]any{"docs": r}, "storage": map[string]any{}})
			want := mode == "empty" || mode == "pair"
			c, report := domain.ValidateForPublication(raw, 1)
			if report.Valid != want {
				t.Fatal(report)
			}
			validateWithPublicSchema(t, schema, raw, want)
			if want {
				validateWithPublicSchema(t, schema, c.Document, true)
			}
		})
	}
}
