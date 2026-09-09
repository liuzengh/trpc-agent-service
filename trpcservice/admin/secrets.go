package admin

import (
	"context"
	"encoding/json"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func (s *Service) WithSecretAuthorizer(authorizer secret.Authorizer) *Service {
	s.secrets = authorizer
	return s
}

func (s *Service) authorizeSecret(ctx context.Context, tenantID, purpose, ref string) error {
	if s.secrets == nil {
		return secret.ErrForbidden
	}
	return s.secrets.Authorize(ctx, tenantID, purpose, ref)
}

func (s *Service) authorizeChannelSecrets(ctx context.Context, binding controlplane.ChannelBinding) error {
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(binding.Config, &cfg); err != nil {
		return invalidf("invalid channel config")
	}
	var fields map[string]string
	switch binding.ChannelType {
	case wecommcp.ChannelType:
		if _, err := wecommcp.ParseBinding(binding); err != nil {
			return invalidf("invalid WeCom MCP binding config")
		}
		for _, purpose := range []string{secret.WeComMCPRead, secret.WeComMCPSend} {
			if err := s.authorizeSecret(ctx, binding.TenantID, purpose, binding.SecretRef); err != nil {
				return err
			}
		}
		return nil
	case "telegram":
		if channels.MediaEnabled(binding) {
			var ref string
			if json.Unmarshal(cfg["bot_token_ref"], &ref) != nil {
				return invalidf("media credential reference required")
			}
			if err := s.authorizeSecret(ctx, binding.TenantID, secret.TelegramMedia, ref); err != nil {
				return err
			}
		}
		fields = map[string]string{"bot_token_ref": secret.TelegramBot, "webhook_secret_ref": secret.TelegramWebhook}
	case "wecom":
		fields = map[string]string{"callback_token_ref": secret.WeComCallback, "encoding_aes_key_ref": secret.WeComAES, "app_secret_ref": secret.WeComApp}
	case "http":
		return nil // HTTP has no provider secret; its caller auth lives outside tenant config.
	default:
		return invalidf("unsupported channel type")
	}
	for field, purpose := range fields {
		var ref string
		if err := json.Unmarshal(cfg[field], &ref); err != nil {
			return invalidf("channel credential reference is required")
		}
		if err := s.authorizeSecret(ctx, binding.TenantID, purpose, ref); err != nil {
			return err
		}
	}
	return nil
}

func validateChannelShape(binding controlplane.ChannelBinding) error {
	if channels.RealtimeChannel(binding.ChannelType) {
		if _, err := channels.ParseMessagePolicy(binding.Config); err != nil {
			return invalidf("invalid message policy")
		}
	}
	if binding.ChannelType == wecommcp.ChannelType {
		if _, err := wecommcp.ParseBinding(binding); err != nil {
			return invalidf("invalid WeCom MCP binding config")
		}
	}
	return nil
}
