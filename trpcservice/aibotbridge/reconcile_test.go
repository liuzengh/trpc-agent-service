package aibotbridge

import (
	"context"
	"errors"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"testing"
)

type reconcileClient struct{ closed bool }

func (c *reconcileClient) Close()                                      { c.closed = true }
func (c *reconcileClient) IsConnected() bool                           { return !c.closed }
func (c *reconcileClient) SendMarkdown(string, string) (string, error) { return "ack", nil }
func TestReconcileRotatesRollsBackDisablesAndPreservesOnFailure(t *testing.T) {
	t.Setenv("REVIEW_BOT_ID", "bot")
	t.Setenv("REVIEW_BOT_SECRET", "secret1")
	m := &Manager{ctx: context.Background(), adapter: channels.NewWeComAIBot(), clients: map[string]connection{}}
	var made []*reconcileClient
	fail := false
	m.connect = func(context.Context, config.ChannelConfig, string, string, string) (managedClient, error) {
		if fail {
			return nil, errors.New("sensitive provider failure")
		}
		c := &reconcileClient{}
		made = append(made, c)
		return c, nil
	}
	t.Cleanup(func() { _ = m.Close() })
	cfg := config.TenantConfig{TenantID: "t", Enabled: true, Channels: []config.ChannelConfig{{Type: channelType, BindingID: "b", Enabled: true, BotIDEnv: "REVIEW_BOT_ID", BotSecretEnv: "REVIEW_BOT_SECRET"}}}
	apply := func() {
		t.Helper()
		if err := m.Reconcile([]config.TenantConfig{cfg}); err != nil {
			t.Fatal(err)
		}
	}
	apply()
	apply()
	if len(made) != 1 {
		t.Fatal("unchanged transport rebuilt")
	}
	t.Setenv("REVIEW_BOT_SECRET", "secret2")
	fail = true
	if err := m.Reconcile([]config.TenantConfig{cfg}); err == nil || err.Error() == "sensitive provider failure" {
		t.Fatalf("failure not sanitized: %v", err)
	}
	if made[0].closed || m.clients["b"].client != made[0] {
		t.Fatal("failed rotation removed old transport")
	}
	fail = false
	apply()
	if !made[0].closed || len(made) != 2 {
		t.Fatal("rotation failed")
	}
	t.Setenv("REVIEW_BOT_SECRET", "secret1")
	apply()
	if !made[1].closed || len(made) != 3 {
		t.Fatal("rollback failed")
	}
	cfg.Channels[0].Enabled = false
	apply()
	if !made[2].closed || len(m.clients) != 0 {
		t.Fatal("disable retained transport")
	}
	cfg.Channels[0].Enabled = true
	apply()
	if len(made) != 4 {
		t.Fatal("reenable failed")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if !made[3].closed {
		t.Fatal("shutdown leaked transport")
	}
	if err := m.Reconcile([]config.TenantConfig{cfg}); err == nil {
		t.Fatal("closed manager accepted update")
	}
}

func TestReconcileFailedBatchClosesStagedClients(t *testing.T) {
	t.Setenv("REVIEW_BOT_A", "bot-a")
	t.Setenv("REVIEW_BOT_B", "bot-b")
	t.Setenv("REVIEW_BOT_SECRET", "secret")
	m := &Manager{ctx: context.Background(), adapter: channels.NewWeComAIBot(), clients: map[string]connection{}}
	staged := &reconcileClient{}
	m.connect = func(_ context.Context, b config.ChannelConfig, _, _, _ string) (managedClient, error) {
		if b.BindingID == "b" {
			return nil, errors.New("cannot authenticate")
		}
		return staged, nil
	}
	cfg := config.TenantConfig{TenantID: "tenant", Enabled: true, Channels: []config.ChannelConfig{
		{Type: channelType, BindingID: "a", Enabled: true, BotIDEnv: "REVIEW_BOT_A", BotSecretEnv: "REVIEW_BOT_SECRET"},
		{Type: channelType, BindingID: "b", Enabled: true, BotIDEnv: "REVIEW_BOT_B", BotSecretEnv: "REVIEW_BOT_SECRET"},
	}}
	if err := m.Reconcile([]config.TenantConfig{cfg}); err == nil {
		t.Fatal("batch should fail")
	}
	if !staged.closed || len(m.clients) != 0 {
		t.Fatal("failed batch leaked/published transport")
	}
}
