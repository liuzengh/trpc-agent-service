package wecomws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Binding is the slice of a channel_binding row the wecomws channel serves.
// RoutesProvider projects tenant.Route down to it so this package stays
// decoupled from the tenant store.
type Binding struct {
	ID          string
	WebhookPath string
	Config      json.RawMessage
}

// RoutesProvider enumerates the bindings of one channel type that are
// currently servable (binding active + tenant active + app published), with
// the resolver's usual TTL / pub-sub invalidation semantics.
type RoutesProvider interface {
	RoutesByChannel(ctx context.Context, channel string) ([]Binding, error)
}

// bindingConfig is channel_binding.config for wecomws bindings: one bot per
// binding. The secret stays as a
// reference and is resolved through the SecretResolver at every (re)connect,
// so a rotation takes effect on the next reconnect.
type bindingConfig struct {
	BotID     string `json:"bot_id"`
	SecretRef string `json:"secret_ref"`
}

// ValidateBindingConfig is the admin API's write gate: the strict version of
// parseBindingConfig, returning the bot_id. Unknown fields are refused — the
// config jsonb lands verbatim in audit details, so e.g. a "secret" key would
// smuggle plaintext credentials into the audit trail while being silently
// ignored by the channel.
func ValidateBindingConfig(raw json.RawMessage) (botID string, err error) {
	if len(raw) == 0 {
		return "", errors.New("wecomws binding requires a config with non-empty bot_id and secret_ref")
	}
	var cfg bindingConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return "", fmt.Errorf("wecomws binding config only accepts bot_id and secret_ref: %w", err)
	}
	if cfg.BotID == "" || cfg.SecretRef == "" {
		return "", errors.New("wecomws binding config requires non-empty bot_id and secret_ref")
	}
	return cfg.BotID, nil
}

// parseBindingConfig enforces the two required fields; the admin API already
// refuses such rows up front, so a malformed row here means it was written
// out of band.
func parseBindingConfig(raw json.RawMessage) (bindingConfig, error) {
	var cfg bindingConfig
	if len(raw) == 0 {
		return cfg, errors.New("wecomws: binding config is empty")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("wecomws: parse binding config: %w", err)
	}
	if cfg.BotID == "" {
		return cfg, errors.New("wecomws: binding config misses bot_id")
	}
	if cfg.SecretRef == "" {
		return cfg, errors.New("wecomws: binding config misses secret_ref")
	}
	return cfg, nil
}
