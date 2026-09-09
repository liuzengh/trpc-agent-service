package redaction

import "testing"

func TestRedactorRemovesSystemSecretsAndAppliesTenantPatterns(t *testing.T) {
	r, err := New([]string{`customer-[0-9]+`})
	if err != nil {
		t.Fatal(err)
	}
	input := `Authorization: Bearer abc123 dsn=postgres://user:pass@db.example/app customer-42`
	output := r.Redact(input)
	for _, secret := range []string{"abc123", "user:pass", "customer-42"} {
		if contains(output, secret) {
			t.Fatalf("secret %q remained in %q", secret, output)
		}
	}
}

func contains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
