package bootstrap

import (
	"os"
	"path/filepath"
	"testing"
)

func TestChannelConfigurationIsExplicitAndClosed(t *testing.T) {
	cfg, err := loadChannelConfig("")
	if err != nil || cfg != nil {
		t.Fatal("unset Channel must remain unregistered")
	}
	for _, body := range []string{`{}`, `{"scope_id":"one","scope_id":"two"}`, `{"unknown":true}`, `{} {}`} {
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadChannelConfig(path); err == nil {
			t.Fatal("invalid config silently accepted")
		}
	}
}
func TestChannelPrivateFilesRequireOwnerOnlyMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key.json")
	if err := os.WriteFile(path, []byte(`{"field":"fixture"}`), 0644); err != nil {
		t.Fatal(err)
	}
	var shape struct {
		Field string `json:"field"`
	}
	if err := readConfigFile(path, true, &shape); err == nil {
		t.Fatal("world readable key file accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := readConfigFile(path, true, &shape); err != nil {
		t.Fatal(err)
	}
	if err := readConfigFile("relative.json", true, &shape); err == nil {
		t.Fatal("relative key path accepted")
	}
}
func TestChannelNATSRequiresAuthenticatedRestrictedTransport(t *testing.T) {
	for _, cfg := range []channelNATSConfig{{URL: "nats://127.0.0.1:4222"}, {URL: "nats://broker.internal:4222", User: "control", Password: "fixture"}, {URL: "nats://user:pass@127.0.0.1:4222", User: "control", Password: "fixture"}, {URL: "nats://127.0.0.1:4222/?token=fixture", User: "control", Password: "fixture"}} {
		if _, err := cfg.options(); err == nil {
			t.Fatal("invalid producer config accepted")
		}
	}
	for _, address := range []string{"nats://127.0.0.1:4222", "tls://broker.internal:4222"} {
		if _, err := (channelNATSConfig{URL: address, User: "control", Password: "fixture"}).options(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestChannelWorkloadConsumersAreClosedAndDiagnosticIsExplicit(t *testing.T) {
	valid := func() []channelWorkloadConfig {
		return []channelWorkloadConfig{{PrincipalID: "spiffe://example.test/gateway/one", InstanceID: "gateway_one", ScopeID: "pool", Audience: "control-channel-v1", Consumers: []string{"telegram_preflight"}}}
	}
	for _, tc := range []struct {
		name   string
		change func([]channelWorkloadConfig) []channelWorkloadConfig
		valid  bool
	}{
		{"wecom diagnostics only", func(v []channelWorkloadConfig) []channelWorkloadConfig {
			v[0].Consumers = []string{"wecom_preflight"}
			return v
		}, true},
		{"diagnostics only", func(v []channelWorkloadConfig) []channelWorkloadConfig { return v }, true},
		{"runtime plus diagnostics", func(v []channelWorkloadConfig) []channelWorkloadConfig {
			v[0].Consumers = []string{"wecom_connection", "telegram_webhook", "telegram_delivery", "telegram_registration", "telegram_preflight"}
			return v
		}, true},
		{"old runtime remains valid", func(v []channelWorkloadConfig) []channelWorkloadConfig {
			v[0].Consumers = []string{"telegram_registration"}
			return v
		}, true},
		{"unknown consumer", func(v []channelWorkloadConfig) []channelWorkloadConfig {
			v[0].Consumers = []string{"telegram_preflight_bot_token"}
			return v
		}, false},
		{"duplicate consumer", func(v []channelWorkloadConfig) []channelWorkloadConfig {
			v[0].Consumers = []string{"telegram_preflight", "telegram_preflight"}
			return v
		}, false},
		{"wrong scope", func(v []channelWorkloadConfig) []channelWorkloadConfig { v[0].ScopeID = "other"; return v }, false},
		{"wrong audience", func(v []channelWorkloadConfig) []channelWorkloadConfig { v[0].Audience = "other"; return v }, false},
		{"duplicate principal", func(v []channelWorkloadConfig) []channelWorkloadConfig {
			p := v[0]
			p.InstanceID = "two"
			return append(v, p)
		}, false},
		{"duplicate instance", func(v []channelWorkloadConfig) []channelWorkloadConfig {
			p := v[0]
			p.PrincipalID = "spiffe://example.test/gateway/two"
			return append(v, p)
		}, false},
		{"query in principal", func(v []channelWorkloadConfig) []channelWorkloadConfig {
			v[0].PrincipalID += "?token=fixture"
			return v
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ChannelConfig{ScopeID: "pool", Workloads: tc.change(valid())}
			err := cfg.validateWorkloads()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t err=%v", tc.valid, err)
			}
		})
	}
}
