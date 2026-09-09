package main

import (
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telegramingress"
)

func TestConfigFromEnvRejectsMalformedControlValues(t *testing.T) {
	for _, values := range []map[string]string{
		{"TELEGRAM_EXPECTED_BOT_ID": "not-a-number"},
		{"TELEGRAM_EXPECTED_BOT_ID": "0"},
		{"TELEGRAM_POLL_TIMEOUT": "secret-value"},
		{"TELEGRAM_POLL_TIMEOUT": "0s"},
	} {
		if _, err := configFromEnv(func(key string) string { return values[key] }); !errors.Is(err, telegramingress.ErrConfiguration) {
			t.Fatalf("malformed control value accepted: %v", err)
		}
	}
	config, err := configFromEnv(func(key string) string { return "" })
	if err != nil || config.PollTimeout != 30*time.Second {
		t.Fatalf("poll timeout default = %v, err=%v", config.PollTimeout, err)
	}
}
