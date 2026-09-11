package preflight

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

const configEpoch = "11111111-1111-4111-8111-111111111111"

func TestConfigCanonicalVectors(t *testing.T) {
	for _, tc := range []struct{ name, input, status, origin, digest string }{
		{"valid", "https://gateway.example.com", OriginValid, "https://gateway.example.com", "699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d"},
		{"equivalent", "https://GATEWAY.EXAMPLE.COM:443/", OriginValid, "https://gateway.example.com", "699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d"},
		{"not_public", "https://127.0.0.1", OriginNotPublic, "", "63c09ea92b63a02e1750e76d92f3d6ea76be142d7b6d3dd1b6c590936b9ff58a"},
		{"invalid", "https://gateway.example.com/secret-CANARY", OriginInvalid, "", "03f2b058b89dcf96f3b74459cf4851902e480cb112593a2e253c2d7527b03ed4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := NewConfig("pool", configEpoch, tc.input)
			if err != nil || cfg.OriginStatus != tc.status || cfg.Digest != "sha256:"+tc.digest || ValidateConfig(cfg) != nil {
				t.Fatalf("config=%+v err=%v", cfg, err)
			}
			if tc.origin == "" {
				if cfg.PublicOrigin != nil {
					t.Fatal("invalid origin retained")
				}
			} else if cfg.PublicOrigin == nil || *cfg.PublicOrigin != tc.origin {
				t.Fatal("wrong canonical origin")
			}
			b, _ := json.Marshal(cfg)
			if strings.Contains(string(b), "CANARY") {
				t.Fatal("raw origin leaked")
			}
		})
	}
}

func TestConfigStaticOrigins(t *testing.T) {
	cases := []struct {
		status string
		values []string
	}{
		{OriginValid, []string{"https://gateway.example.com:80", "https://gateway.example.com:88/", "https://gateway.example.com:8443", "HTTPS://GATEWAY.EXAMPLE.COM", "https://8.8.8.8", "https://[2606:4700:4700::1111]:8443", "https://xn--bcher-kva.example"}},
		{OriginInvalid, []string{"", "http://gateway.example.com", "https://u:p@gateway.example.com", "https://gateway.example.com?", "https://gateway.example.com#", "https://gateway.example.com/%2f", "https://gateway.example.com:444", "https://gateway.example.com:0443", "https://gateway.example.com:", "https://gateway.example.com.", "https://中文.example", "https://gateway_example.com", "https://-gateway.example.com", "https://gateway..example.com", "https://gateway.example.com/path", "https://gateway.example.com\\evil", " https://gateway.example.com", "https://[::1%25eth0]", "https://[gateway.example.com]"}},
		{OriginNotPublic, []string{"https://localhost", "https://host", "https://api.localhost", "https://api.local", "https://api.internal", "https://host.home.arpa", "https://127.1", "https://0177.0.0.1", "https://0x7f.0.0.1", "https://0.0.0.0", "https://10.0.0.1", "https://100.64.0.1", "https://127.0.0.1", "https://169.254.169.254", "https://172.16.0.1", "https://192.0.0.8", "https://192.0.2.1", "https://192.88.99.1", "https://192.168.1.1", "https://198.18.0.1", "https://198.51.100.1", "https://203.0.113.1", "https://224.0.0.1", "https://240.0.0.1", "https://255.255.255.255", "https://[::]", "https://[::1]", "https://[fc00::1]", "https://[fe80::1]", "https://[ff02::1]", "https://[::ffff:127.0.0.1]", "https://[2001:db8::1]", "https://[2002:0808:0808::1]", "https://[3fff::1]"}},
	}
	for _, group := range cases {
		for _, input := range group.values {
			t.Run(input, func(t *testing.T) {
				cfg, err := NewConfig("pool", configEpoch, input)
				if err != nil || cfg.OriginStatus != group.status || ValidateConfig(cfg) != nil {
					t.Fatalf("input=%q status=%q expected=%q err=%v", input, cfg.OriginStatus, group.status, err)
				}
				if group.status != OriginValid && cfg.PublicOrigin != nil {
					t.Fatal("retained invalid origin")
				}
			})
		}
	}
}

func TestConfigRejectsTampering(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ConfigSnapshot)
	}{
		{"scope", func(c *ConfigSnapshot) { c.ScopeID = "../bad" }},
		{"epoch", func(c *ConfigSnapshot) { c.SourceEpoch = "not-an-epoch" }},
		{"digest", func(c *ConfigSnapshot) { c.Digest = "sha256:" + strings.Repeat("0", 64) }},
		{"status", func(c *ConfigSnapshot) { c.OriginStatus = "CANARY" }},
		{"nil_valid", func(c *ConfigSnapshot) { c.PublicOrigin = nil }},
		{"noncanonical", func(c *ConfigSnapshot) { c.PublicOrigin = ptr("https://GATEWAY.EXAMPLE.COM:443/") }},
		{"invalid_with_origin", func(c *ConfigSnapshot) { c.OriginStatus = OriginInvalid }},
		{"notpublic_with_origin", func(c *ConfigSnapshot) { c.OriginStatus = OriginNotPublic }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, "https://gateway.example.com")
			tc.mutate(&cfg)
			if !errors.Is(ValidateConfig(cfg), ErrInvalid) {
				t.Fatal("accepted inconsistent configuration")
			}
		})
	}
	for _, fields := range [][2]string{{"../CANARY", configEpoch}, {"pool", "CANARY"}} {
		if _, err := NewConfig(fields[0], fields[1], "https://gateway.example.com"); err != ErrInvalid || strings.Contains(err.Error(), "CANARY") {
			t.Fatalf("err=%v", err)
		}
	}
}
