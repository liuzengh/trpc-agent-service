package natsadapter

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeclarations(t *testing.T) {
	topology, err := LoadTopology("../../../../../deploy/nats/streams.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range topology.configs() {
		if !s.DenyPurge || !s.DenyDelete || s.AllowMsgTTL || s.MaxAge != 0 {
			t.Fatal("full-history contract")
		}
	}
	generated, err := RenderServerConfig("../../../../../deploy/nats/permissions.yaml")
	if err != nil {
		t.Fatal(err)
	}
	existing, err := os.ReadFile("../../../../../deploy/nats/server.conf")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, existing) {
		t.Fatal("permissions.yaml/server.conf drift: regenerate with nats-config")
	}
	if bytes.Contains(generated, []byte("password: \"")) {
		t.Fatal("literal password rendered")
	}
}
func TestInvalidTopology(t *testing.T) {
	valid, err := os.ReadFile("../../../../../deploy/nats/streams.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"version: 2\nstreams: []\n", string(valid) + "unknown: true\n", string(valid) + "---\nversion: 1\n", strings.Repeat(" ", 65537) + string(valid), strings.Replace(string(valid), "control.channel-route.v1", "untrusted.*", 1)} {
		p := filepath.Join(t.TempDir(), "streams.yaml")
		os.WriteFile(p, []byte(body), 0600)
		if _, err := LoadTopology(p); err == nil {
			t.Fatalf("accepted malformed declaration")
		}
	}
}

func TestRunStreamMustFitWireContract(t *testing.T) {
	topology, err := LoadTopology("../../../../../deploy/nats/streams.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for i := range topology.Streams {
		if topology.Streams[i].Name == RunStream {
			topology.Streams[i].MaxMessageBytes = 16384
		}
	}
	if err := topology.Validate(); err == nil {
		t.Fatal("accepted Run stream limit below immutable wire contract")
	}
}
