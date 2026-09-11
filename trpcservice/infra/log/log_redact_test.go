package log

import (
	"bytes"
	"errors"
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
	yes := []string{"api_key", "secret", "password", "mysql_dsn", "access_token", "master_key", "authorization", "access_key", "secret_key"}
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

// TestSanitizeTextStripsURLSecrets covers the credential-in-a-URL leak: a Redis
// URL, a DSN, or a signed webhook URL carries its secret inside an otherwise
// ordinary string, so key-based masking never sees it.
func TestSanitizeTextStripsURLSecrets(t *testing.T) {
	cases := []struct{ in, want, forbidden string }{
		{"redis://:hunter2@cache:6379/0", "redis://***@cache:6379/0", "hunter2"},
		{"postgres://user:hunter2@db:5432/app", "postgres://***@db:5432/app", "hunter2"},
		{"https://open.feishu.cn/hook?token=abc123&x=1", "https://open.feishu.cn/hook?token=***&x=1", "abc123"},
		{"wss://openws.work.weixin.qq.com?secret=zzz", "wss://openws.work.weixin.qq.com?secret=***", "zzz"},
		{"plain text with no secret", "plain text with no secret", ""},
	}
	for _, c := range cases {
		got := SanitizeText(c.in)
		if got != c.want {
			t.Errorf("SanitizeText(%q) = %q, want %q", c.in, got, c.want)
		}
		if c.forbidden != "" && strings.Contains(got, c.forbidden) {
			t.Errorf("SanitizeText(%q) leaked %q", c.in, c.forbidden)
		}
	}
}

// TestRedactsNonStringValues covers the values key-based masking used to miss:
// a secret handed over as `any` (slog.Any) and credentials embedded in an error
// message. Both used to reach the log verbatim.
func TestRedactsNonStringValues(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, nil)
	l := slog.New(&redactHandler{next: base})

	l.Info("op",
		"access_key", any("AKIA-secret-value"),
		"err", any(errors.New("dial redis://:hunter2@cache:6379: connection refused")),
		"endpoint", "redis://default:hunter3@cache:6379/0",
	)
	out := buf.String()
	for _, leaked := range []string{"AKIA-secret-value", "hunter2", "hunter3"} {
		if strings.Contains(out, leaked) {
			t.Errorf("%q leaked into log: %s", leaked, out)
		}
	}
	if !strings.Contains(out, "connection refused") {
		t.Errorf("the diagnostic part of the error was lost: %s", out)
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
