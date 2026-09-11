package domain_test

import (
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

func TestArtifactCredentialCanonicalSchema(t *testing.T) {
	schema := compilePublicSchema(t)
	for _, mode := range []string{"empty", "pair", "partial", "no_digest", "wrong_role"} {
		t.Run(mode, func(t *testing.T) {
			r := map[string]any{"kind": "managed_artifact", "backend_id": "artifacts", "backend_revision": 1}
			if mode != "empty" {
				r["access_key_id_credential_id"] = "crd_00000000000000000000000000000001"
				r["credential_audience_digest"] = "sha256:" + strings.Repeat("a", 64)
			}
			if mode == "pair" {
				r["secret_access_key_credential_id"] = "crd_00000000000000000000000000000002"
			}
			if mode == "no_digest" {
				delete(r, "credential_audience_digest")
			}
			if mode == "wrong_role" {
				r["kind"] = "managed_session"
			}
			role := "artifact"
			if mode == "wrong_role" {
				role = "session"
			}
			raw, _ := json.Marshal(map[string]any{"schema_version": "v1", "credential_protocol_version": "v1", "models": map[string]any{}, "tools": map[string]any{}, "knowledge": map[string]any{}, "storage": map[string]any{role: r}})
			want := mode != "no_digest" && mode != "wrong_role"
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
