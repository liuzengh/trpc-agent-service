package log

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestMask(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"abc", "***"},
		{"abcdefgh", "ab***gh"},
		{"averylongsecretvalue1234", "ave***1234"},
	}
	for _, c := range cases {
		if got := Mask(c.in); got != c.want {
			t.Errorf("Mask(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestIsSensitiveKey(t *testing.T) {
	yes := []string{"api_key", "secret", "password", "mysql_dsn", "access_token", "master_key", "authorization"}
	no := []string{"tenant_id", "err", "session", "name", "latency_ms"}
	for _, k := range yes {
		if !isSensitiveKey(k) {
			t.Errorf("isSensitiveKey(%q) = false, want true", k)
		}
	}
	for _, k := range no {
		if isSensitiveKey(k) {
			t.Errorf("isSensitiveKey(%q) = true, want false", k)
		}
	}
}

// TestNewRedactsSecrets verifies the default logger masks sensitive attribute
// values end to end while leaving ordinary attributes intact.
func TestNewRedactsSecrets(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, nil)
	l := slog.New(&redactHandler{next: base})

	l.Info("dialing", "mysql_dsn", "root:hunter2@tcp(db:3306)/app", "tenant", "t1",
		"err", "boom")

	out := buf.String()
	if strings.Contains(out, "hunter2") {
		t.Errorf("secret leaked into log: %s", out)
	}
	if !strings.Contains(out, "root:***") && !strings.Contains(out, "***") {
		t.Errorf("masked dsn missing: %s", out)
	}
	if !strings.Contains(out, "t1") || !strings.Contains(out, "boom") {
		t.Errorf("ordinary attrs altered: %s", out)
	}
}

// TestNestedGroupRedaction walks into groups so secrets inside them are
// masked too.
func TestNestedGroupRedaction(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, nil)
	l := slog.New(&redactHandler{next: base})

	l.Info("op", "creds", slog.GroupValue(slog.String("token", "plain-secret"), slog.String("user", "alice")))

	out := buf.String()
	if strings.Contains(out, "plain-secret") {
		t.Errorf("nested secret leaked: %s", out)
	}
	if !strings.Contains(out, "alice") {
		t.Errorf("nested ordinary value altered: %s", out)
	}
}
