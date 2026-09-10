// Package outbound selects and creates the concrete IM reply provider.
package outbound

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

// WeComMessageSenderResolver returns the single live sender owned by the
// WeCom Channel adapter. Creating a second long connection for the same BotID
// causes WeCom to evict the inbound connection.
type WeComMessageSenderResolver interface {
	ResolveOutboundSender(context.Context, channels.BindingSnapshot) (wecom.MessageSender, error)
}

// Resolver selects the concrete provider for one durable Reply Outbox row.
type Resolver struct {
	store         *postgres.Store
	secrets       platformsecret.SecretProvider
	limiter       worker.ReplyRateLimiter
	wecomResolver WeComMessageSenderResolver
}

// NewResolver creates an IM outbound resolver for the single Channel owner.
// Provider clients remain binding-scoped values; the durable outbox owns
// retries and ordering.
func NewResolver(
	store *postgres.Store,
	secrets platformsecret.SecretProvider,
	limiter worker.ReplyRateLimiter,
	wecomResolvers ...WeComMessageSenderResolver,
) (*Resolver, error) {
	if store == nil || secrets == nil {
		return nil, errors.New("reply provider dependencies are required")
	}
	if len(wecomResolvers) > 1 {
		return nil, errors.New("only one wecom sender resolver is supported")
	}
	var wecomResolver WeComMessageSenderResolver
	if len(wecomResolvers) == 1 {
		wecomResolver = wecomResolvers[0]
	}
	return &Resolver{
		store:         store,
		secrets:       secrets,
		limiter:       limiter,
		wecomResolver: wecomResolver,
	}, nil
}

// ResolveReplyProvider revalidates the binding revision before returning the
// binding-scoped provider client used by the Channel reply sender.
func (r *Resolver) ResolveReplyProvider(
	ctx context.Context,
	delivery worker.ReplyDelivery,
) (worker.ReplyProvider, error) {
	if r == nil || r.store == nil || r.secrets == nil {
		return worker.ReplyProvider{}, errors.New("reply provider resolver is not initialized")
	}
	binding, err := r.store.ResolveBinding(
		ctx,
		delivery.Reply.TenantID,
		delivery.Reply.AppID,
		delivery.Reply.BindingID,
	)
	if err != nil {
		return worker.ReplyProvider{}, err
	}
	if binding.Status != channels.BindingActive {
		return worker.ReplyProvider{}, worker.ErrReplyBindingInactive
	}
	if binding.Channel != delivery.Reply.Channel {
		return worker.ReplyProvider{}, worker.ErrReplyBindingChanged
	}
	if binding.BindingRevision != delivery.Reply.BindingRevision {
		return worker.ReplyProvider{}, worker.ErrReplyBindingChanged
	}
	switch binding.Channel {
	case channels.ChannelWeCom:
		if r.wecomResolver == nil {
			return worker.ReplyProvider{}, errors.New("wecom shared sender resolver is required")
		}
		sender, err := r.wecomResolver.ResolveOutboundSender(ctx, binding.Snapshot())
		if err != nil {
			return worker.ReplyProvider{}, fmt.Errorf("resolve shared wecom sender: %w", err)
		}
		client, err := wecom.NewOutboundClientWithSender(sender)
		if err != nil {
			return worker.ReplyProvider{}, err
		}
		return worker.ReplyProvider{Client: client, Limiter: r.limiter}, nil
	case channels.ChannelFeishu:
		client, err := feishu.NewOutboundClient(ctx, r.secrets, binding.Snapshot())
		if err != nil {
			return worker.ReplyProvider{}, err
		}
		return worker.ReplyProvider{Client: client, Limiter: r.limiter}, nil
	default:
		return worker.ReplyProvider{}, fmt.Errorf("unsupported reply channel %q", binding.Channel)
	}
}

// Close releases no provider connection. The WeCom Channel adapter owns the
// shared long connection; Reply Outbox only creates lightweight wrappers.
func (r *Resolver) Close() error {
	return nil
}
