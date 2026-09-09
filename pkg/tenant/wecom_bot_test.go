package tenant

import (
	"strings"
	"testing"
)

func TestWeComBotBindingAdmission(t *testing.T) {
	valid := ChannelBinding{Type: "wecom_bot", AccountID: "bot-1", AgentApp: "support", Token: strings.Repeat("b", 32), AccessPolicy: ChannelAccessPolicy{AllowDirectMessages: true, AllowedUsers: []string{"user-1"}}}
	config := validConfig()
	config.Channels = []ChannelBinding{valid}
	if err := ValidateConfig(config); err != nil {
		t.Fatal(err)
	}
	config.Channels[0].Token, config.Channels[0].TokenRef = "", "env://TRPC_SECRET_BOT_BRIDGE"
	if err := ValidateConfig(config); err != nil {
		t.Fatalf("scoped bridge ref rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(*ChannelBinding)
	}{
		{"missing account", func(b *ChannelBinding) { b.AccountID = "" }},
		{"short bridge token", func(b *ChannelBinding) { b.Token = "short" }},
		{"unsafe bridge token", func(b *ChannelBinding) { b.Token = strings.Repeat("b", 32) + "\n" }},
		{"provider secret", func(b *ChannelBinding) { b.Secret = "bot-provider-secret" }},
		{"provider secret reference", func(b *ChannelBinding) { b.SecretRef = "env://TRPC_SECRET_PROVIDER" }},
		{"application AES", func(b *ChannelBinding) { b.EncodingAESKey = strings.Repeat("A", 43) }},
		{"application ID", func(b *ChannelBinding) { b.AppID = "1000002" }},
		{"tenant endpoint", func(b *ChannelBinding) { b.Config = map[string]string{"endpoint": "http://other"} }},
		{"both sources", func(b *ChannelBinding) { b.TokenRef = "env://TRPC_SECRET_BRIDGE" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			binding := valid
			test.change(&binding)
			config.Channels = []ChannelBinding{binding}
			if err := ValidateConfig(config); err == nil {
				t.Fatal("invalid smart bot config accepted")
			}
		})
	}
}
