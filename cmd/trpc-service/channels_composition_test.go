package main

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

type staticSecretResolver string

func (r staticSecretResolver) Resolve(context.Context, string) (string, error) {
	return string(r), nil
}

func TestComposeTelegramRejectsTrailingCredentialJSON(t *testing.T) {
	tenants := []config.TenantConfig{{
		TenantID: "example", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type: config.ChannelTelegram, BindingID: "bot-1", CredentialRef: "env:BOT_1_CONFIG",
		}},
	}}
	valid := `{"bot_token":"bot-token"}`
	for _, trailing := range []string{
		` {"secret_token":"other","bot_token":"other"}`,
		` trailing-garbage`,
	} {
		_, err := decodeTelegramCredential(context.Background(), staticSecretResolver(valid+trailing), tenants[0].Channels[0])
		if err == nil || !strings.Contains(err.Error(), "exactly one JSON value") {
			t.Fatalf("decodeTelegramCredential() trailing %q error = %v, want exactly one JSON value", trailing, err)
		}
	}
}
