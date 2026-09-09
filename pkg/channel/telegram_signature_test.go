//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package channel

import (
	"net/http"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const testSecret = "telegram-webhook-secret-value-2026"

func telegramRequest(t *testing.T, headerValue string, setHeader bool) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if setHeader {
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", headerValue)
	}
	return req
}

func TestTelegram_VerifySignature_AcceptsMatchingSecret(t *testing.T) {
	a := NewTelegramAdapter()
	req := telegramRequest(t, testSecret, true)

	if err := a.VerifySignature(req, &tenant.ChannelBinding{Secret: testSecret}); err != nil {
		t.Fatalf("matching secret rejected: %v", err)
	}
}

func TestTelegram_VerifySignature_RejectsNilRequest(t *testing.T) {
	if err := NewTelegramAdapter().VerifySignature(nil, &tenant.ChannelBinding{Secret: testSecret}); err == nil {
		t.Fatal("nil request was accepted")
	}
}

func TestTelegramVerifySignatureRejectsDuplicateSecretHeaders(t *testing.T) {
	req := telegramRequest(t, testSecret, true)
	req.Header.Add("X-Telegram-Bot-Api-Secret-Token", testSecret)
	if err := NewTelegramAdapter().VerifySignature(req, &tenant.ChannelBinding{Secret: testSecret}); err == nil {
		t.Fatal("ambiguous duplicate secret headers accepted")
	}
}

func TestTelegramVerifySignatureRejectsMatchingWeakSecret(t *testing.T) {
	const weakSecret = "a"
	req := telegramRequest(t, weakSecret, true)
	if err := NewTelegramAdapter().VerifySignature(req, &tenant.ChannelBinding{Secret: weakSecret}); err == nil {
		t.Fatal("matching one-character webhook secret was accepted")
	}
}

func TestTelegram_VerifySignature_RejectsAttacks(t *testing.T) {
	tests := []struct {
		name        string
		headerValue string
		setHeader   bool
		secret      string
	}{
		{"wrong secret", "attacker-guess", true, testSecret},
		{"missing header", "", false, testSecret},
		{"empty header value", "", true, testSecret},
		{
			// A prefix must not pass: length has to be part of the comparison.
			name:        "secret prefix only",
			headerValue: testSecret[:10],
			setHeader:   true,
			secret:      testSecret,
		},
		{
			name:        "secret with trailing bytes",
			headerValue: testSecret + "x",
			setHeader:   true,
			secret:      testSecret,
		},
		{
			name:        "case altered secret",
			headerValue: strings.ToUpper(testSecret),
			setHeader:   true,
			secret:      testSecret,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := NewTelegramAdapter()
			req := telegramRequest(t, tc.headerValue, tc.setHeader)

			if err := a.VerifySignature(req, &tenant.ChannelBinding{Secret: tc.secret}); err == nil {
				t.Error("attack accepted: VerifySignature returned nil")
			}
		})
	}
}

// TestTelegram_VerifySignature_UnconfiguredSecretFailsClosed covers the
// misconfiguration path. A bare ConstantTimeCompare would accept a missing
// header when binding.Secret is empty, because empty compares equal to empty,
// so a tenant whose secret was never configured would accept unauthenticated
// webhooks. Verification must reject the binding instead.
func TestTelegram_VerifySignature_UnconfiguredSecretFailsClosed(t *testing.T) {
	tests := []struct {
		name        string
		headerValue string
		setHeader   bool
	}{
		{"no header at all", "", false},
		{"empty header value", "", true},
		{"attacker supplied header", "anything", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := NewTelegramAdapter()
			req := telegramRequest(t, tc.headerValue, tc.setHeader)

			err := a.VerifySignature(req, &tenant.ChannelBinding{Secret: ""})
			if err == nil {
				t.Fatal("unconfigured secret accepted the webhook: fail-open")
			}
			if !strings.Contains(err.Error(), "no valid webhook secret configured") {
				t.Errorf("rejected with %v, want an unconfigured-secret error", err)
			}
		})
	}
}
