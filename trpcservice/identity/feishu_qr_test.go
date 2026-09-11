package identity

import (
	"net/url"
	"strings"
	"testing"
)

func TestFeishuQRGoto(t *testing.T) {
	link, err := FeishuQRGoto(
		"https://passport.feishu.cn/suite/passport/oauth/authorize",
		"cli_test",
		"https://console.example.com/api/v1/auth/callback",
		"state-abc",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	parsed, err := url.Parse(link)
	if err != nil {
		t.Fatalf("goto is not a valid url: %v", err)
	}
	if !strings.HasPrefix(link, DefaultFeishuQRAuthorizeURL+"?") {
		t.Fatalf("goto does not start with authorize URL: %s", link)
	}
	q := parsed.Query()
	if got := q.Get("client_id"); got != "cli_test" {
		t.Errorf("client_id = %q, want cli_test", got)
	}
	if got := q.Get("redirect_uri"); got != "https://console.example.com/api/v1/auth/callback" {
		t.Errorf("redirect_uri = %q", got)
	}
	if got := q.Get("response_type"); got != "code" {
		t.Errorf("response_type = %q, want code", got)
	}
	if got := q.Get("state"); got != "state-abc" {
		t.Errorf("state = %q, want state-abc", got)
	}
}

func TestFeishuQRGotoValidation(t *testing.T) {
	cases := []struct {
		name, base, appID, redirect, state string
	}{
		{"empty base", "", "cli", "https://x", "s"},
		{"empty app", "https://a", "", "https://x", "s"},
		{"empty redirect", "https://a", "cli", "", "s"},
		{"empty state", "https://a", "cli", "https://x", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := FeishuQRGoto(c.base, c.appID, c.redirect, c.state); err == nil {
				t.Errorf("expected validation error for %s", c.name)
			}
		})
	}
}

func TestNormalizeFeishuURL(t *testing.T) {
	_, err := NormalizeFeishuURL("", DefaultFeishuQRSDKURL)
	if err != nil {
		t.Fatalf("empty value should fall back to default, got %v", err)
	}
	if _, err := NormalizeFeishuURL("http://example.com/sdk.js", DefaultFeishuQRSDKURL); err == nil {
		t.Errorf("non-loopback http must be rejected")
	}
	if _, err := NormalizeFeishuURL("https://lf-package-cn.feishucdn.com/sdk.js", DefaultFeishuQRSDKURL); err != nil {
		t.Errorf("https should be accepted, got %v", err)
	}
	if _, err := NormalizeFeishuURL("http://127.0.0.1:8080/sdk.js", DefaultFeishuQRSDKURL); err != nil {
		t.Errorf("loopback http should be accepted for local development, got %v", err)
	}
	for _, dangerous := range []string{
		"ftp://localhost/sdk.js",
		"javascript://localhost/sdk.js",
		"https://user@example.com/sdk.js",
		"https://example.com/sdk.js#fragment",
	} {
		if _, err := NormalizeFeishuURL(dangerous, DefaultFeishuQRSDKURL); err == nil {
			t.Errorf("dangerous URL %q must be rejected", dangerous)
		}
	}
}

func TestFeishuQRGotoPreservesExistingQuery(t *testing.T) {
	link, err := FeishuQRGoto("https://passport.feishu.cn/oauth/authorize?theme=dark", "cli", "https://app.example/callback", "state")
	if err != nil {
		t.Fatalf("FeishuQRGoto() error = %v", err)
	}
	parsed, err := url.Parse(link)
	if err != nil {
		t.Fatalf("parse goto: %v", err)
	}
	if got := parsed.Query().Get("theme"); got != "dark" {
		t.Fatalf("existing query value = %q, want dark", got)
	}
}
