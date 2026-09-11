package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	telegram "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/telegramadapter"
	admission "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/catalogpostgres"
	control "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/controlhttp"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	use "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/accountuse"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

type credentialTransport struct{ client *control.Client }

func (b credentialTransport) ResolveValues(ctx context.Context, a c.Account, r c.ResolveRequest) ([]use.Value, error) {
	values, e := b.client.Resolve(ctx, a, r)
	if e != nil {
		return nil, e
	}
	out := make([]use.Value, len(values))
	for i, v := range values {
		out[i] = use.Value{Purpose: v.Purpose, ID: v.ID, Version: v.Version, Value: v.Value}
	}
	return out, nil
}

type controlWeComCredentials struct{ use *use.Service }

func (b controlWeComCredentials) Resolve(context.Context, d.Account) (connection.CredentialMaterial, error) {
	return connection.CredentialMaterial{}, c.ErrUnauthorized
}
func (b controlWeComCredentials) ResolveOwned(ctx context.Context, a d.Account, g d.OwnerGrant) (connection.CredentialMaterial, error) {
	p, account, e := b.use.Open(ctx, a.ID, "wecom_connection", a.Revision, g.Epoch)
	if e != nil {
		return connection.CredentialMaterial{}, e
	}
	defer p.Revoke()
	if account.ProviderAccountID != a.BotID || account.Credentials[0].ID != a.CredentialRef {
		return connection.CredentialMaterial{}, c.ErrUnauthorized
	}
	values, e := b.use.Resolve(ctx, p, account, &g, nil)
	if e != nil || len(values) != 1 {
		return connection.CredentialMaterial{}, c.ErrUnauthorized
	}
	return connection.CredentialMaterial{Secret: values[0].Value}, nil
}

type permitKey struct{}

func permitFrom(ctx context.Context) *c.Permit { p, _ := ctx.Value(permitKey{}).(*c.Permit); return p }
func bindingDigest(p *c.Permit) string {
	raw, _ := json.Marshal(p.Binding())
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type accountGuardBridge struct {
	store   *pg.Store
	ingress bool
}

func (b accountGuardBridge) AccountContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	p := permitFrom(ctx)
	if p.Check() != nil {
		return nil, nil, c.ErrUnauthorized
	}
	deadline, ok := p.Context().Deadline()
	if !ok {
		return nil, nil, c.ErrUnauthorized
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	stop := context.AfterFunc(p.Context(), cancel)
	return bounded, func() { stop(); cancel() }, nil
}
func (b accountGuardBridge) VerifyAccount(ctx context.Context, tx pgx.Tx, provider, account, tenant string, generation *int64) (string, error) {
	p := permitFrom(ctx)
	if p.Check() != nil {
		return "", c.ErrUnauthorized
	}
	bound := p.Binding()
	kind := provider + "_delivery"
	if b.ingress {
		kind = provider + "_ingress"
		if provider == "telegram" {
			kind = "telegram_webhook"
			if bound.Kind == "telegram_receiver" {
				kind = "telegram_receiver"
			}
		}
	}
	if bound.Kind != kind || bound.Provider != provider || bound.AccountID != account || (tenant != "" && bound.TenantID != tenant) {
		return "", c.ErrUnauthorized
	}
	if e := b.store.Guard(ctx, tx, p, generation); e != nil {
		return "", e
	}
	return bindingDigest(p), nil
}
func (b accountGuardBridge) RecheckAccount(ctx context.Context, tx pgx.Tx) error {
	return b.store.RecheckExpiry(ctx, tx, permitFrom(ctx))
}

// A handler captures its immutable client lifetime. A newer catalog with the
// same account ID must never authorize a callback from this obsolete handler.
type boundAcceptor struct {
	use                  *use.Service
	next                 telegram.Acceptor
	account, provider    string
	revision, generation int64
	lifetime             context.Context
}

func (b boundAcceptor) AcceptInbound(ctx context.Context, in admission.Inbound) (admission.Receipt, error) {
	if b.lifetime.Err() != nil {
		return b.next.AcceptInbound(ctx, in)
	} // receipt-first; no permit => no new work
	kind := "wecom_ingress"
	if b.provider == "telegram" {
		kind = "telegram_webhook"
	}
	p, _, e := b.use.Open(ctx, b.account, kind, b.revision, b.generation)
	if e != nil {
		return b.next.AcceptInbound(ctx, in)
	}
	defer p.Revoke()
	stop := context.AfterFunc(b.lifetime, p.Revoke)
	defer stop()
	return b.next.AcceptInbound(context.WithValue(ctx, permitKey{}, p), in)
}

type dynamicTelegram struct {
	mu       sync.RWMutex
	handlers map[string]http.Handler
	use      *use.Service
	acceptor telegram.Acceptor
}

func (r *dynamicTelegram) Install(a c.Account, lifetime context.Context, secret string, generation int64) error {
	h, e := telegram.NewHandler(a.ID, secret, boundAcceptor{r.use, r.acceptor, a.ID, a.Provider, a.ConnectionRevision, generation, lifetime})
	if e != nil {
		return e
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if lifetime.Err() != nil {
		return c.ErrUnauthorized
	}
	r.handlers[a.ID] = h
	return nil
}
func (r *dynamicTelegram) Remove(id string) { r.mu.Lock(); delete(r.handlers, id); r.mu.Unlock() }
func (r *dynamicTelegram) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	id := strings.TrimPrefix(req.URL.Path, "/v1/telegram/")
	r.mu.RLock()
	h := r.handlers[id]
	r.mu.RUnlock()
	if h == nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "temporarily_unavailable", http.StatusServiceUnavailable)
		return
	}
	h.ServeHTTP(w, req)
}

type controlWeComFactory struct {
	base       wecomClientFactory
	use        *use.Service
	generation atomic.Int64
}

func (f *controlWeComFactory) New(ctx context.Context, a d.Account, g d.OwnerGrant, m connection.CredentialMaterial) (connection.Client, error) {
	v, e := f.use.Directory.Lookup(ctx, a.ID)
	if e != nil || v.Account.ConnectionRevision != a.Revision {
		return nil, c.ErrUnauthorized
	}
	lifetime, cancel := context.WithCancel(v.Context)
	b := f.base
	b.acceptor = boundAcceptor{f.use, f.base.acceptor, a.ID, "wecom", a.Revision, f.generation.Add(1), lifetime}
	client, e := b.New(ctx, a, g, m)
	if e != nil {
		cancel()
		return nil, e
	}
	return &revocableClient{Client: client, lifetime: lifetime, cancel: cancel}, nil
}

type revocableClient struct {
	connection.Client
	lifetime context.Context
	cancel   context.CancelFunc
}

func (c1 *revocableClient) Run(ctx context.Context) error {
	op, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(c1.lifetime, func() { c1.Client.Quiesce(); cancel() })
	defer stop()
	return c1.Client.Run(op)
}
func (c1 *revocableClient) ReserveFinal(ctx context.Context, t connection.ReplyTarget) (connection.ReservedSender, error) {
	if c1.lifetime.Err() != nil {
		return nil, connection.ErrSenderUnavailable
	}
	capable, ok := c1.Client.(connection.FinalClient)
	if !ok {
		return nil, connection.ErrSenderUnavailable
	}
	return capable.ReserveFinal(ctx, t)
}
func (c1 *revocableClient) Quiesce()                        { c1.cancel(); c1.Client.Quiesce() }
func (c1 *revocableClient) Close(ctx context.Context) error { c1.cancel(); return c1.Client.Close(ctx) }
