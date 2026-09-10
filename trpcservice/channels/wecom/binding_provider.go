package wecom

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

type bindingLookup interface {
	Get(context.Context, string, string) (*channels.Binding, error)
}

// BindingProvider resolves the active WeCom application from the durable reply
// target. It keeps access-token caches per Binding configuration in memory.
type BindingProvider struct {
	Bindings    bindingLookup
	Credentials CredentialResolver
	HTTPClient  *http.Client
	BaseURL     string
	Now         func() time.Time
	Attachments attachment.Reader

	mu        sync.Mutex
	providers map[string]*Provider
}

var _ outbox.Provider = (*BindingProvider)(nil)

// Deliver sends a reply through the Binding stored with the durable target.
func (p *BindingProvider) Deliver(ctx context.Context, value storage.ReplyOutbox) (string, error) {
	provider, err := p.provider(ctx, value)
	if err != nil {
		return "", err
	}
	return provider.Deliver(ctx, value)
}

// Reconcile delegates to the same Binding provider used for delivery.
func (p *BindingProvider) Reconcile(ctx context.Context, value storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	provider, err := p.provider(ctx, value)
	if err != nil {
		return outbox.DeliveryUnknown, "", err
	}
	return provider.Reconcile(ctx, value)
}

func (p *BindingProvider) provider(ctx context.Context, value storage.ReplyOutbox) (*Provider, error) {
	if p == nil || ctx == nil || p.Bindings == nil || p.Credentials == nil || value.TenantID == "" || value.ReplyTarget.BindingID == "" {
		return nil, invalidDelivery()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	binding, err := p.Bindings.Get(ctx, value.TenantID, value.ReplyTarget.BindingID)
	if err != nil {
		return nil, mapBindingLookupError(err)
	}
	if binding == nil || !binding.CanAcceptInbound() || binding.Channel != channels.ChannelWeCom || binding.Protocol.WeCom == nil {
		return nil, invalidDelivery()
	}
	credentials, err := p.Credentials.Resolve(ctx, channels.SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef})
	if err != nil {
		return nil, mapBindingLookupError(err)
	}
	if credentials.AppSecret == "" {
		return nil, invalidDelivery()
	}
	key := fmt.Sprintf("%s/%s/%d/%s", binding.TenantID, binding.BindingID, binding.Version, binding.ConfigDigest)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.providers == nil {
		p.providers = make(map[string]*Provider)
	}
	if provider := p.providers[key]; provider != nil {
		if provider.AppSecret == credentials.AppSecret {
			return provider, nil
		}
	}
	provider := &Provider{CorpID: binding.Protocol.WeCom.CorpID, AgentID: binding.Protocol.WeCom.AgentID, AppSecret: credentials.AppSecret, HTTPClient: p.HTTPClient, BaseURL: p.BaseURL, Now: p.Now, Attachments: p.Attachments}
	p.providers[key] = provider
	return provider, nil
}

func mapBindingLookupError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, channels.ErrNotFound) || errors.Is(err, channels.ErrInvalid) {
		return invalidDelivery()
	}
	return &outbox.DeliveryError{Class: "unavailable", Retryable: true}
}

func invalidDelivery() error { return &outbox.DeliveryError{Class: "invalid", Retryable: false} }
