package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestManagedMemoryCredentialCanonicalSharedSchema(t *testing.T) {
	schema := compilePublicSchema(t)
	for _, kind := range []string{"managed_memory", "managed_session", "managed_artifact"} {
		for _, fields := range []string{"none", "pair", "id_only", "digest_only", "bad_digest"} {
			t.Run(kind+"/"+fields, func(t *testing.T) {
				resource := map[string]any{"kind": kind, "backend_id": "pg", "backend_revision": 1}
				if fields == "pair" || fields == "id_only" || fields == "bad_digest" {
					resource["dsn_credential_id"] = "crd_00000000000000000000000000000001"
				}
				if fields == "pair" || fields == "digest_only" {
					resource["credential_audience_digest"] = "sha256:" + strings.Repeat("a", 64)
				}
				if fields == "bad_digest" {
					resource["credential_audience_digest"] = "invalid"
				}
				spec := map[string]any{"schema_version": "v1", "credential_protocol_version": "v1", "models": map[string]any{}, "tools": map[string]any{}, "knowledge": map[string]any{}, "storage": map[string]any{strings.TrimPrefix(kind, "managed_"): resource}}
				raw, err := json.Marshal(spec)
				if err != nil {
					t.Fatal(err)
				}
				want := fields == "none" || ((kind == "managed_memory" || kind == "managed_session") && fields == "pair")
				canonical, report := domain.ValidateForPublication(raw, 1)
				if report.Valid != want {
					t.Fatalf("domain valid=%v want=%v report=%+v", report.Valid, want, report)
				}
				validateWithPublicSchema(t, schema, raw, want)
				if want {
					validateWithPublicSchema(t, schema, canonical.Document, true)
				}
			})
		}
	}
}
