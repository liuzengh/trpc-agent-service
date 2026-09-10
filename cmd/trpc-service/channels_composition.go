package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecombot"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
)

const (
	defaultTelegramBaseURL = "https://api.telegram.org"
)

type telegramBindingCredential struct {
	BotToken   string `json:"bot_token"`
	APIBaseURL string `json:"api_base_url,omitempty"`
}

func decodeTelegramCredential(ctx context.Context, secrets credential.SecretResolver, binding config.ChannelBinding) (telegramBindingCredential, error) {
	var value telegramBindingCredential
	if err := decodeBindingCredential(ctx, secrets, binding, &value); err != nil {
		return telegramBindingCredential{}, err
	}
	if _, err := telegram.NewPoller(telegram.PollerConfig{
		BotToken: value.BotToken, BaseURL: fallback(value.APIBaseURL, defaultTelegramBaseURL), HTTPClient: http.DefaultClient,
	}); err != nil {
		return telegramBindingCredential{}, fmt.Errorf("construct Telegram connector for %q: %w", binding.BindingID, err)
	}
	return value, nil
}

type weComBindingCredential struct {
	BotID    string `json:"bot_id"`
	Secret   string `json:"secret"`
	Endpoint string `json:"endpoint,omitempty"`
	Origin   string `json:"origin,omitempty"`
}

func decodeWeComCredential(ctx context.Context, secrets credential.SecretResolver, binding config.ChannelBinding) (weComBindingCredential, error) {
	var value weComBindingCredential
	if err := decodeBindingCredential(ctx, secrets, binding, &value); err != nil {
		return weComBindingCredential{}, err
	}
	if _, err := wecombot.NewClient(wecombot.Config{BotID: value.BotID, Secret: value.Secret, Endpoint: value.Endpoint, Origin: value.Origin}); err != nil {
		return weComBindingCredential{}, fmt.Errorf("construct WeCom smart bot for %q: %w", binding.BindingID, err)
	}
	return value, nil
}

type feishuBindingCredential struct {
	AppID      string `json:"app_id"`
	AppSecret  string `json:"app_secret"`
	APIBaseURL string `json:"api_base_url,omitempty"`
}

func decodeFeishuCredential(ctx context.Context, secrets credential.SecretResolver, binding config.ChannelBinding) (feishuBindingCredential, error) {
	var value feishuBindingCredential
	if err := decodeBindingCredential(ctx, secrets, binding, &value); err != nil {
		return feishuBindingCredential{}, err
	}
	if _, err := feishu.NewConnector(feishu.ConnectorConfig{AppID: value.AppID, AppSecret: value.AppSecret}); err != nil {
		return feishuBindingCredential{}, fmt.Errorf("construct Feishu connector for %q: %w", binding.BindingID, err)
	}
	return value, nil
}

func bindingsFor(tenants []config.TenantConfig, channelType string) []config.ChannelBinding {
	bindings := make([]config.ChannelBinding, 0)
	for _, tenantConfig := range tenants {
		for _, binding := range tenantConfig.Channels {
			if binding.Type != channelType {
				continue
			}
			bindings = append(bindings, binding)
		}
	}
	return bindings
}

func decodeBindingCredential(ctx context.Context, secrets credential.SecretResolver, binding config.ChannelBinding, target any) error {
	if secrets == nil {
		return fmt.Errorf("secret resolver is required for channel binding %q", binding.BindingID)
	}
	value, err := secrets.Resolve(ctx, binding.CredentialRef)
	if err != nil {
		return fmt.Errorf("resolve credentials for binding %q: %w", binding.BindingID, err)
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode credentials for binding %q: %w", binding.BindingID, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("credentials for binding %q must contain exactly one JSON value", binding.BindingID)
	}
	return nil
}

func fallback(value, defaultValue string) string {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	return strings.TrimSpace(value)
}

func required(getenv environment, name string) (string, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func csvRequired(getenv environment, name string) ([]string, error) {
	value, err := required(getenv, name)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("%s must contain at least one Kafka broker", name)
	}
	return brokers, nil
}

func optional(getenv environment, name, fallback string) string {
	if value := strings.TrimSpace(getenv(name)); value != "" {
		return value
	}
	return fallback
}
