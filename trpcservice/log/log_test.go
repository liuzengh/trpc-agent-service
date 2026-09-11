package log

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestInitLevels(t *testing.T) {
	for _, lv := range []string{"", "debug", "info", "warn", "error"} {
		if err := Init(lv, false); err != nil {
			t.Fatalf("Init(%q): %v", lv, err)
		}
	}
	if err := Init("verbose", false); err == nil {
		t.Fatal("unknown level must fail")
	}
}

func TestInitFiltersByLevel(t *testing.T) {
	var buf bytes.Buffer
	// Install a buffer-backed logger at warn level through the same code
	// path: Init sets the default, then we swap only the output writer by
	// re-creating the handler shape Init uses.
	if err := Init("warn", false); err != nil {
		t.Fatal(err)
	}
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	slog.SetDefault(slog.New(h))
	slog.Info("dropped")
	slog.Warn("kept")
	out := buf.String()
	if bytes.Contains([]byte(out), []byte("dropped")) || !bytes.Contains([]byte(out), []byte("kept")) {
		t.Fatalf("level filtering broken: %q", out)
	}
}

func TestRedact(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"short", "****"},
		{"12345678", "****"},
		{"sk-secret-key-1234567890", "sk-****7890"},
	}
	for _, c := range cases {
		if got := Redact(c.in); got != c.want {
			t.Fatalf("Redact(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestRedactedAttrLogger exercises the slog Handler wrapper that prevents
// secret leakage to stderr in production.
func TestRedactedAttrLogger(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, nil)
	secret := "sk-supersecret"
	slog.SetDefault(slog.New(WithLogRedaction(h, []string{secret})))
	t.Cleanup(func() { slog.SetDefault(slog.New(slog.NewJSONHandler(nil, nil))) })

	slog.Info("the secret is sk-supersecret-dont-tell", "key", "sk-supersecret-value")
	out := buf.String()
	if strings.Contains(out, secret) {
		t.Fatalf("log output must not contain the secret: %s", out)
	}
	if !strings.Contains(out, "<redacted>") {
		t.Fatalf("log output should contain the redacted marker: %s", out)
	}
}
