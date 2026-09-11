package bootstrap

import (
	"context"
	"errors"
	"github.com/go-telegram/bot"
	protocol "github.com/liuzengh/trpc-agent-service/platform/im/telegram"
	use "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/accountuse"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	telegram "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/telegram"
	wecom "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/wecomadapter"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type controlDelivery struct {
	use        *use.Service
	owner      localConnectionOwner
	instance   string
	dispatcher *app.Dispatcher
	generation atomic.Int64
}

func (b *controlDelivery) InspectAccount(ctx context.Context, k app.AccountKey) (app.SendEligibility, error) {
	v, e := b.use.Directory.Lookup(ctx, k.AccountID)
	if e != nil || v.Account.Provider != k.Provider {
		return app.SendEligibility{}, d.ErrUnavailable
	}
	if k.Provider == "wecom" {
		return (connectionDeliveryEligibility{b.owner, b.instance}).InspectAccount(ctx, k)
	}
	return app.SendEligibility{Eligible: k.Provider == "telegram"}, nil
}

// This composition wrapper injects a trusted per-dispatch context. The existing
// Dispatcher, not Bootstrap, owns A1 -> Reserve -> A2 -> Send -> evidence.
func (b *controlDelivery) DispatchAccount(ctx context.Context, r d.ClaimRequest) (int, error) {
	v, e := b.use.Directory.Lookup(ctx, r.AccountID)
	if e != nil || v.Account.Provider != r.Provider || r.InstanceID != b.instance {
		return 0, d.ErrUnauthorized
	}
	if r.Provider == "wecom" {
		g, ok := b.owner.LocalOwner(r.AccountID)
		if !ok || r.Owner == nil || r.Owner.InstanceID != g.InstanceID || r.Owner.Epoch != g.Epoch || r.Owner.Revision != g.Revision || g.Revision != v.Account.ConnectionRevision {
			return 0, d.ErrUnauthorized
		}
	}
	p, _, e := b.use.Open(ctx, r.AccountID, r.Provider+"_delivery", v.Account.ConnectionRevision, b.generation.Add(1))
	if e != nil {
		return 0, d.ErrUnauthorized
	}
	defer p.Revoke()
	return b.dispatcher.DispatchAccount(context.WithValue(ctx, permitKey{}, p), r)
}

type controlSenders struct {
	artifacts      app.ArtifactReader
	telegramAPIURL string
	use            *use.Service
	wecom          *wecom.Provider
}

func (b controlSenders) Reserve(ctx context.Context, r app.SendRequest) (app.ReservedSender, error) {
	p := permitFrom(ctx)
	if p.Check() != nil || r.Claim.UseBinding != bindingDigest(p) {
		return nil, d.ErrUnauthorized
	}
	bound := p.Binding()
	if bound.AccountID != r.Claim.Target.AccountID || bound.Provider != r.Claim.Target.Provider || bound.TenantID != r.Claim.Target.TenantID {
		return nil, d.ErrUnauthorized
	}
	var sender app.ReservedSender
	var close func()
	var e error
	if bound.Provider == "wecom" {
		sender, e = b.wecom.Reserve(ctx, r)
	} else {
		v, err := b.use.Directory.Lookup(ctx, bound.AccountID)
		if err != nil {
			return nil, d.ErrUnauthorized
		}
		values, err := b.use.Resolve(ctx, p, v.Account, nil, nil)
		if err != nil || len(values) != 1 {
			return nil, d.ErrUnavailable
		}
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.MaxConnsPerHost = 2
		tr.ResponseHeaderTimeout = 5 * time.Second
		tr.DisableCompression = true
		close = tr.CloseIdleConnections
		h := &http.Client{Transport: tr, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		options := []bot.Option{bot.WithSkipGetMe(), bot.WithHTTPClient(5*time.Second, h)}
		endpoint, err := protocol.Endpoint(v.Account.Config.EndpointProfile, b.telegramAPIURL)
		if err != nil {
			close()
			return nil, d.ErrUnavailable
		}
		options = append(options, bot.WithServerURL(strings.TrimSuffix(endpoint, "/")))
		client, err := bot.New(values[0].Value, options...)
		values[0].Value = ""
		if err != nil {
			close()
			return nil, d.ErrUnavailable
		}
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		stop := context.AfterFunc(p.Context(), cancel)
		identity, err := client.GetMe(op)
		stop()
		cancel()
		if p.Check() != nil {
			close()
			return nil, d.ErrUnauthorized
		}
		if err != nil {
			close()
			// getMe is preparation, not sendMessage. A transport or server
			// failure proves no Final was sent but does not prove bad identity.
			// Keep it on the existing bounded preparation retry path.
			if errors.Is(err, bot.ErrorUnauthorized) || errors.Is(err, bot.ErrorForbidden) || errors.Is(err, bot.ErrorBadRequest) || errors.Is(err, bot.ErrorNotFound) {
				return nil, d.ErrUnauthorized
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, d.ErrUnavailable
		}
		if identity == nil || !identity.IsBot || strconv.FormatInt(identity.ID, 10) != v.Account.ProviderAccountID {
			close()
			return nil, d.ErrUnauthorized
		}
		provider, err := telegram.NewProvider(map[string]*bot.Bot{bound.AccountID: client}, b.artifacts)
		if err != nil {
			close()
			return nil, err
		}
		sender, e = provider.Reserve(ctx, r)
	}
	if e != nil || sender == nil {
		if close != nil {
			close()
		}
		return sender, e
	}
	return &permitSender{inner: sender, permit: p, proof: r.Claim.UseBinding, close: close}, nil
}

type permitSender struct {
	inner    app.ReservedSender
	permit   *c.Permit
	proof    string
	close    func()
	released atomic.Bool
}

func (s *permitSender) Release() {
	if s.released.CompareAndSwap(false, true) {
		s.inner.Release()
		if s.close != nil {
			s.close()
		}
	}
}
func (s *permitSender) SendFinal(ctx context.Context, a d.Attempt) d.Result {
	if s.released.Load() || s.permit.Check() != nil || a.UseBinding != s.proof || s.proof != bindingDigest(s.permit) {
		return d.Result{Certainty: d.CertaintyNotSent, ErrorClass: d.ErrorStaleOrigin}
	}
	op, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.permit.Context(), cancel)
	defer stop()
	if s.permit.Check() != nil {
		return d.Result{Certainty: d.CertaintyNotSent, ErrorClass: d.ErrorStaleOrigin}
	}
	return s.inner.SendFinal(op, a)
}
