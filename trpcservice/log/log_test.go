package log

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactStripsCredentialForms(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{"Authorization=Bearer abc123", "Authorization=[REDACTED]"},
		{"api_key=secret-value", "api_key=[REDACTED]"},
		{"token=abcdef", "token=[REDACTED]"},
		{"password=hunter2", "password=[REDACTED]"},
		{"postgres://trpc:super-secret@localhost/db", "postgres://trpc:[REDACTED]@localhost/db"},
		{`provider error: {"password":"secret_123","access_token": "tok_abc"}`, `provider error: {"password":"[REDACTED]","access_token": "[REDACTED]"}`},
		{`{"client_secret":"line\"quoted","message":"safe"}`, `{"client_secret":"[REDACTED]","message":"safe"}`},
		{`{'refresh-token': 'tok_abc', 'message': 'safe'}`, `{'refresh-token': '[REDACTED]', 'message': 'safe'}`},
		{"redis://:redis-password@127.0.0.1:6379/0", "redis://:[REDACTED]@127.0.0.1:6379/0"},
		{"amqps://worker:rabbit-secret@mq.example.com/vhost", "amqps://worker:[REDACTED]@mq.example.com/vhost"},
		{"sasl.password=kafka-secret,security.protocol=SASL_SSL", "sasl.password=[REDACTED],security.protocol=SASL_SSL"},
		{"order shipped", "order shipped"},
	}
	for _, testCase := range cases {
		if got := Redact(testCase.input); got != testCase.want {
			t.Fatalf("Redact(%q) = %q, want %q", testCase.input, got, testCase.want)
		}
	}
}

func TestRedactStripsCommonPII(t *testing.T) {
	t.Parallel()
	for _, input := range []string{
		"phone=13812345678",
		"identity=11010519900101123X",
		"card=6222021234567890123",
		"email=alice.support+prod@example.com",
	} {
		got := Redact(input)
		if got == input || !strings.Contains(got, "[REDACTED_PII]") {
			t.Fatalf("Redact(%q) = %q, want PII redaction", input, got)
		}
	}
}

func TestRedactingHandlerScrubsMessageAndAttributes(t *testing.T) {
	var buffer bytes.Buffer
	logger := slog.New(NewRedactingHandler(slog.NewTextHandler(&buffer, nil)))
	logger.InfoContext(context.Background(), "token=abcdef", "api_key", "leak")
	output := buffer.String()
	if strings.Contains(output, "abcdef") || strings.Contains(output, "leak") {
		t.Fatalf("log leaked secret: %s", output)
	}
	if !strings.Contains(output, "[REDACTED]") {
		t.Fatalf("log missing redaction: %s", output)
	}
}

func TestRedactingHandlerScrubsBoundAndGroupedAttributes(t *testing.T) {
	t.Parallel()
	var buffer bytes.Buffer
	base := NewRedactingHandler(slog.NewTextHandler(&buffer, nil))
	logger := slog.New(base.WithAttrs([]slog.Attr{
		slog.String("access_token", "bound-secret"),
		slog.String("endpoint", "postgres://user:db-password@db.example/app"),
	})).WithGroup("request")
	logger.InfoContext(context.Background(), "ok",
		slog.Group("auth",
			slog.String("authorization", "Bearer grouped-secret"),
			slog.String("detail", `{"password":"nested-secret","safe":"visible"}`),
		),
		slog.Int("attempt", 3),
	)
	output := buffer.String()
	for _, secret := range []string{"bound-secret", "db-password", "grouped-secret", "nested-secret"} {
		if strings.Contains(output, secret) {
			t.Fatalf("bound/grouped log leaked %q: %s", secret, output)
		}
	}
	if !strings.Contains(output, "visible") || !strings.Contains(output, "attempt=3") || !strings.Contains(output, "[REDACTED]") {
		t.Fatalf("redacted grouped output = %s", output)
	}
}

func TestRedactAttrHandlesGroupsSecretKeysAndResolvedValues(t *testing.T) {
	t.Parallel()
	secret := redactAttr(slog.String("client_secret", "do-not-log"))
	if secret.Value.String() != "[REDACTED]" {
		t.Fatalf("secret attr = %#v", secret)
	}
	group := redactAttr(slog.Group("outer",
		slog.String("token", "nested-token"),
		slog.String("safe", "password=inline-secret"),
	))
	attrs := group.Value.Group()
	if len(attrs) != 2 || attrs[0].Value.String() != "[REDACTED]" || strings.Contains(attrs[1].Value.String(), "inline-secret") {
		t.Fatalf("redacted group = %#v", attrs)
	}
	plain := redactAttr(slog.Int("count", 7))
	if plain.Value.Int64() != 7 {
		t.Fatalf("plain attr changed = %#v", plain)
	}
}

func TestNewRedactingHandlerNilFallsBackToDefaultHandler(t *testing.T) {
	t.Parallel()
	if handler := NewRedactingHandler(nil); handler == nil {
		t.Fatal("NewRedactingHandler(nil) returned nil")
	}
}
