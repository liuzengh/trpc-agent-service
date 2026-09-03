package log

import (
	"bytes"
	"strings"
	"testing"
)

func TestRedactSecrets(t *testing.T) {
	input := `Authorization: Bearer abcdefghijklmnop api_key="secret-value" postgres://user:pass@db:5432/app`
	redacted := Redact(input)
	for _, secret := range []string{"abcdefghijklmnop", "secret-value", "user:pass"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("secret %q remains in %q", secret, redacted)
		}
	}
}

func TestRedactingWriterReportsOriginalLength(t *testing.T) {
	var target bytes.Buffer
	writer := NewRedactingWriter(&target)
	input := []byte("Bearer abcdefghijklmnop")
	written, err := writer.Write(input)
	if err != nil || written != len(input) || strings.Contains(target.String(), "abcdefghijklmnop") {
		t.Fatalf("written=%d target=%q err=%v", written, target.String(), err)
	}
}
