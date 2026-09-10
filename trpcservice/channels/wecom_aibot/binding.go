// Package wecom_aibot implements the WebSocket channel for WeCom AI Bots.
package wecom_aibot

import (
	"context"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
)

// BindingLookup loads the current tenant-scoped Binding used to create one
// connection manager.
type BindingLookup interface {
	Get(context.Context, string, string) (*channels.Binding, error)
}

// Credentials contains the private AI Bot secret resolved from one Binding.
// It is intentionally distinct from the self-built-app WeCom credentials.
type Credentials struct {
	BotSecret string
}

// CredentialResolver resolves the secret scoped by a trusted Binding.
type CredentialResolver interface {
	Resolve(context.Context, channels.SecretScope) (Credentials, error)
}

// BindingConfig creates a connection only from a trusted routing target and
// the corresponding current Binding. It prevents BotID and SecretRef from
// being supplied by an inbound WebSocket frame.
type BindingConfig struct {
	Target            channels.RoutingTarget
	Bindings          BindingLookup
	Credentials       CredentialResolver
	Dispatcher        gateway.DispatchService
	Dialer            Dialer
	QueueSize         int
	HeartbeatInterval time.Duration
	ReconnectBase     time.Duration
	ReconnectMax      time.Duration
	ExecutionTimeout  time.Duration
}

// NewForBinding resolves BotID and Bot Secret from the current trusted
// Binding, then constructs a connection manager. Resolution failures are
// redacted so Secret Manager errors cannot cross the channel boundary.
func NewForBinding(ctx context.Context, config BindingConfig) (*Manager, error) {
	if ctx == nil || config.Bindings == nil || config.Credentials == nil || config.Dispatcher == nil {
		return nil, ErrInvalid
	}
	if err := config.Target.Validate(); err != nil || config.Target.Channel != channels.ChannelWeComAIBot {
		return nil, ErrInvalid
	}
	binding, err := config.Bindings.Get(ctx, config.Target.TenantID, config.Target.BindingID)
	if err != nil || binding == nil || !binding.CanAcceptInbound() || binding.Channel != channels.ChannelWeComAIBot || binding.Protocol.WeComAIBot == nil || binding.TenantID != config.Target.TenantID || binding.BindingID != config.Target.BindingID || binding.Version != config.Target.BindingVersion || binding.ConfigDigest != config.Target.ConfigDigest {
		return nil, ErrInvalid
	}
	credentials, err := config.Credentials.Resolve(ctx, channels.SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef})
	if err != nil || strings.TrimSpace(credentials.BotSecret) == "" {
		return nil, ErrNotReady
	}
	return New(Config{BotID: binding.Protocol.WeComAIBot.BotID, Secret: credentials.BotSecret, WSURL: binding.Protocol.WeComAIBot.WSURL, Target: config.Target, Dispatcher: config.Dispatcher, Dialer: config.Dialer, QueueSize: config.QueueSize, HeartbeatInterval: config.HeartbeatInterval, ReconnectBase: config.ReconnectBase, ReconnectMax: config.ReconnectMax, ExecutionTimeout: config.ExecutionTimeout})
}
