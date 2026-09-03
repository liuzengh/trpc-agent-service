package config

import (
	"os"
	"path/filepath"
	"testing"
)

const wecomYAML = `
default_tenant: demo
tenants:
  - id: demo
    name: Demo
    model:
      name: deepseek-chat
      api_key: sk-test
      base_url: https://api.deepseek.com
    channels:
      wecom:
        corp_id: wx5823bf96d3bd56c7
        corp_secret: secret
        agent_id: 218
        token: QDG6eK
        encoding_aes_key: jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C
  - id: plain
    name: Plain
    model:
      name: deepseek-chat
      api_key: sk-test
`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadWeComBinding(t *testing.T) {
	cfg, err := Load(writeConfig(t, wecomYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b, ok := cfg.WeComBinding("demo")
	if !ok {
		t.Fatal("demo should have a wecom binding")
	}
	if b.CorpID != "wx5823bf96d3bd56c7" || b.AgentID != 218 || b.Token != "QDG6eK" {
		t.Fatalf("binding = %+v", b)
	}
	if _, ok := cfg.WeComBinding("plain"); ok {
		t.Fatal("plain tenant must not have a binding")
	}
	if _, ok := cfg.WeComBinding("missing"); ok {
		t.Fatal("unknown tenant must not have a binding")
	}
}

func TestLoadWeComBindingIncomplete(t *testing.T) {
	_, err := Load(writeConfig(t, `
tenants:
  - id: demo
    model:
      name: m
      api_key: k
    channels:
      wecom:
        corp_id: wx123
        token: tk
`))
	if err == nil {
		t.Fatal("incomplete wecom binding must fail validation")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	src := writeConfig(t, wecomYAML)
	cfg, err := Load(src)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	again, err := Load(dst)
	if err != nil {
		t.Fatalf("reload saved config: %v", err)
	}
	if again.DefaultTenant != cfg.DefaultTenant || len(again.Tenants) != len(cfg.Tenants) {
		t.Fatalf("round trip mismatch: %+v", again)
	}
	b, ok := again.WeComBinding("demo")
	if !ok || b.CorpID != "wx5823bf96d3bd56c7" || b.AgentID != 218 {
		t.Fatalf("binding after round trip = %+v, %v", b, ok)
	}
	if _, ok := again.WeComBinding("plain"); ok {
		t.Fatal("plain tenant gained a binding after round trip")
	}
	if _, err := os.Stat(dst + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file must not survive Save")
	}
}
