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
