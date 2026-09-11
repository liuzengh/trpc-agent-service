package domain_test

import (
	"bytes"
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

func TestWorkspaceProfileContract(t *testing.T) {
	b := []byte(`{"schema_version":"v1","credential_protocol_version":"v1","models":{},"tools":{},"knowledge":{},"storage":{},"executors":{"shell":{"kind":"sdk_sandbox"}}}`)
	c, report := domain.ValidateForPublication(b, 1)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	validateWithPublicSchema(t, compilePublicSchema(t), c.Document, true)
	var s domain.Spec
	json.Unmarshal(c.Document, &s)
	if s.Executors["shell"].Kind != "sdk_sandbox" {
		t.Fatal(s)
	}
	for _, replacement := range []string{`{"kind":"host"}`, `{"kind":"sdk_sandbox","path":"/tmp"}`, `{"kind":"sdk_sandbox","env":{}}`, `null`} {
		bad := bytes.Replace(b, []byte(`{"kind":"sdk_sandbox"}`), []byte(replacement), 1)
		if _, report := domain.ValidateForPublication(bad, 1); report.Valid {
			t.Fatal("accepted", replacement)
		}
		validateWithPublicSchema(t, compilePublicSchema(t), bad, false)
	}
}
