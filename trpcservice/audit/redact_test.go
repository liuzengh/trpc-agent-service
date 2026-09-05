package audit

import (
	"strings"
	"testing"
)

func TestRedactorCoversCredentialFormatsAndConfiguredSecret(t *testing.T) {
	redactor := NewRedactorWithSecrets(256, []string{"environment-secret"})
	for _, input := range []string{
		"Authorization: Bearer token-value",
		"postgres://db-user:db-password@db.internal/app",
		"password=environment-secret",
		"-----BEGIN PRIVATE KEY-----",
	} {
		output := redactor.RedactString(input)
		if output == input || strings.Contains(output, "token-value") || strings.Contains(output, "db-password") || strings.Contains(output, "environment-secret") {
			t.Fatalf("sensitive input was not redacted: input=%q output=%q", input, output)
		}
	}
}

func TestRedactorBoundsUnicodeAndMetadataWithoutMutation(t *testing.T) {
	input := map[string]string{"tool_input": "prompt and history", "component": strings.Repeat("x", 128)}
	output := RedactMetadata(input)
	if input["tool_input"] == RedactedValue || output["tool_input"] != RedactedValue {
		t.Fatalf("metadata redaction mutated input or missed sensitive field: input=%v output=%v", input, output)
	}
	bounded := NewRedactor(16).RedactString(strings.Repeat("界", 32))
	if len(bounded) > 16 || !strings.Contains(bounded, "TRUNCATED") {
		t.Fatalf("unicode output exceeded bound: %q", bounded)
	}
	if Fingerprint("stable") != Fingerprint("stable") {
		t.Fatal("fingerprint changed across identical inputs")
	}
}
