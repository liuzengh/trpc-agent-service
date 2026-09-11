// IM adapter construction: adapts the binding-driven channels.Manager to the
// concrete wecom/feishu adapters (which live in their own packages).
//
// The admin channel page is the single place where an IM account is bound:
// binding.account_id carries the bot/app identity, binding.credential_ref and
// binding.verification_token_ref point into the secret store, and the Manager
// connects as soon as the binding is saved (no config IM section anymore).
package main

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels/wecom"
)

// buildAdapter satisfies channels.AdapterBuilder.
func buildAdapter(_ context.Context, b channels.ChannelBinding, secret string) (channels.Adapter, error) {
	switch b.Channel {
	case channels.ChannelWeCom:
		conn := wecom.NewConn(b.AccountID, secret)
		return wecom.New(b.TenantID, conn), nil
	case channels.ChannelFeishu:
		conn := feishu.NewConn(b.AccountID, secret)
		// bot_open_id is fetched from bot/v3/info at connect time (group
		// @-mention gating); empty falls back to accepting group messages.
		return feishu.New(b.TenantID, conn.BotOpenID(), conn), nil
	default:
		return nil, fmt.Errorf("unsupported channel %q", b.Channel)
	}
}

// webhookChannels are the channels this platform can serve over HTTP callbacks,
// with the verifier and adapter builder each one needs.
//
// WeCom is deliberately absent: its AI-bot product speaks only its own WSS
// protocol (the callback-URL mode belongs to a different WeCom product whose
// reply path is not implemented), so listing it in the config is a startup error
// rather than an endpoint that could never receive anything.
func webhookChannels() (map[string]channels.WebhookVerifier, map[string]channels.WebhookBuilder) {
	verifiers := map[string]channels.WebhookVerifier{
		channels.ChannelFeishu: feishu.WebhookVerifier(),
	}
	builders := map[string]channels.WebhookBuilder{
		channels.ChannelFeishu: func(_ context.Context, b channels.ChannelBinding, secret string) (channels.WebhookAdapter, error) {
			return feishu.NewWebhookAdapter(b.TenantID, b.AccountID, secret), nil
		},
	}
	return verifiers, builders
}
