package tenant

import (
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestDecodeSnapshotAcceptsCurrentCanonicalChecksum(t *testing.T) {
	t.Parallel()

	publishedAt := time.Date(2026, 9, 8, 6, 0, 0, 0, time.UTC)
	canonical, err := newSnapshot(config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 3,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-bot"}},
		Model:    config.ModelConfig{ProviderID: "primary", Name: "support-model"},
		Tools:    config.ToolPolicy{},
	}, publishedAt)
	if err != nil {
		t.Fatalf("newSnapshot() error = %v", err)
	}

	configJSON, err := marshalConfig(canonical.Config)
	if err != nil {
		t.Fatalf("marshalConfig() error = %v", err)
	}
	decoded, err := decodeSnapshot(3, configJSON, []byte(canonical.Checksum), publishedAt)
	if err != nil {
		t.Fatalf("decodeSnapshot() error = %v", err)
	}
	if decoded.Checksum != canonical.Checksum || decoded.Config.AppName() != "acme/support" || len(decoded.Config.Channels) != 1 {
		t.Fatalf("decoded snapshot = %#v, want current canonical snapshot", decoded)
	}
}

func TestDecodeSnapshotRejectsNonCanonicalOrTamperedChecksum(t *testing.T) {
	t.Parallel()

	publishedAt := time.Date(2026, 9, 8, 6, 0, 0, 0, time.UTC)
	const currentJSON = `{"tenant_id":"acme","app_code":"support","status":"active","config_version":4,"channels":[]}`

	if _, err := decodeSnapshot(4, []byte(currentJSON), []byte("0123456789abcdef"), publishedAt); err == nil {
		t.Fatal("decodeSnapshot() error = nil, want checksum mismatch")
	}
	const oldBaseShape = `{"tenant_id":"acme-retail","app_code":"sales-bot","status":"suspended","config_version":3,"channels":[],"instruction":"x"}`
	const oldBaseChecksum = "b3cdd6e1eef0ff026795d32c464299e8a3cc3e3abfb62dc3595843228e0a20f0"
	if _, err := decodeSnapshot(3, []byte(oldBaseShape), []byte(oldBaseChecksum), publishedAt); err == nil {
		t.Fatal("decodeSnapshot() accepted a development-era checksum")
	}
}
