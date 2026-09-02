// IM gateway wiring: builds the real WSS adapters (WeCom aibot SDK / Feishu
// Lark SDK) from config + secret store + bindings, and starts the bridge.
//
// Design (grill-confirmed): config declares WHICH bots to connect (account
// identity + credential_ref into the secret store); the binding store provides
// the runtime route (account -> tenant + agent). An enabled bot that is not
// bound yet is skipped with a warning — the operator binds it via the admin
// channel page, then restarts.
package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/liuzengh/trpc-agent-service/trpcservice/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

// wireIMGateway builds and starts the IM bridge when any platform is enabled.
// It returns an error only for misconfiguration the operator must fix (missing
// credential, unavailable secret store); an enabled bot that is simply not
// bound yet is skipped with a warning, not an error.
func wireIMGateway(ctx context.Context, b *bus.RedisBus, bindings channels.BindingStore, secrets secret.Store, im config.IMConfig) error {
	if !im.WeCom.Enabled && !im.Feishu.Enabled {
		return nil
	}
	gw := channels.NewGateway(b, bindings)

	if im.WeCom.Enabled {
		if err := attachWeCom(ctx, gw, bindings, secrets, im.WeCom); err != nil {
			return err
		}
	}
	if im.Feishu.Enabled {
		if err := attachFeishu(ctx, gw, bindings, secrets, im.Feishu); err != nil {
			return err
		}
	}

	go func() {
		if err := gw.Run(ctx); err != nil {
			slog.Error("IM gateway stopped", "err", err)
		}
	}()
	return nil
}

func attachWeCom(ctx context.Context, gw *channels.Gateway, bindings channels.BindingStore, secrets secret.Store, c config.WeComConfig) error {
	if c.BotID == "" || c.CredentialRef == "" {
		return fmt.Errorf("wecom: bot_id and credential_ref are required")
	}
	secretVal, err := resolveSecret(ctx, secrets, c.CredentialRef)
	if err != nil {
		return fmt.Errorf("wecom: %w", err)
	}
	conn := wecom.NewConn(c.BotID, secretVal)

	tenantID := resolveTenantID(ctx, bindings, channels.ChannelWeCom, c.BotID)
	if tenantID == "" {
		slog.Warn("wecom bot not bound yet, skipping connection", "bot_id", c.BotID)
		_ = conn.Close()
		return nil
	}
	gw.Attach(ctx, wecom.New(tenantID, conn), channels.Attach{AccountID: c.BotID})
	slog.Info("wecom gateway attached", "bot_id", c.BotID, "tenant", tenantID)
	return nil
}

func attachFeishu(ctx context.Context, gw *channels.Gateway, bindings channels.BindingStore, secrets secret.Store, c config.FeishuConfig) error {
	if c.AppID == "" || c.CredentialRef == "" {
		return fmt.Errorf("feishu: app_id and credential_ref are required")
	}
	secretVal, err := resolveSecret(ctx, secrets, c.CredentialRef)
	if err != nil {
		return fmt.Errorf("feishu: %w", err)
	}
	conn := feishu.NewConn(c.AppID, secretVal)

	tenantID := resolveTenantID(ctx, bindings, channels.ChannelFeishu, c.AppID)
	if tenantID == "" {
		slog.Warn("feishu app not bound yet, skipping connection", "app_id", c.AppID)
		_ = conn.Close()
		return nil
	}
	gw.Attach(ctx, feishu.New(tenantID, c.BotOpenID, conn), channels.Attach{AccountID: c.AppID})
	slog.Info("feishu gateway attached", "app_id", c.AppID, "tenant", tenantID)
	return nil
}

// resolveSecret fetches a credential value from the secret store by reference.
func resolveSecret(ctx context.Context, secrets secret.Store, ref string) (string, error) {
	if secrets == nil {
		return "", fmt.Errorf("secret store unavailable")
	}
	v, err := secrets.Get(ctx, ref)
	if err != nil {
		return "", fmt.Errorf("resolve credential %q: %w", ref, err)
	}
	return v, nil
}

// resolveTenantID finds the tenant an IM account is bound to, or "" when the
// account has no binding yet.
func resolveTenantID(ctx context.Context, bindings channels.BindingStore, channel, accountID string) string {
	if bindings == nil {
		return ""
	}
	list, err := bindings.List(ctx, "", channel)
	if err != nil {
		return ""
	}
	for _, b := range list {
		if b.AccountID == accountID {
			return b.TenantID
		}
	}
	return ""
}
