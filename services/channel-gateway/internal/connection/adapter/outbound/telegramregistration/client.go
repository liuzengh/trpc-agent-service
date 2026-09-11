// Package telegramregistration adapts explicit protocol calls to the receiver.
package telegramregistration

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	protocol "github.com/liuzengh/trpc-agent-service/platform/im/telegram"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/telegramruntime"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

type Factory struct{ ServerURL string }
type client struct{ api *protocol.Client }

func (f Factory) NewAccount(token string, a c.Account) (app.Remote, error) {
	endpoint, e := protocol.Endpoint(a.Config.EndpointProfile, f.ServerURL)
	if e != nil {
		return nil, e
	}
	return (Factory{ServerURL: endpoint}).New(token)
}
func (f Factory) New(token string) (app.Remote, error) {
	api, e := protocol.New(protocol.Options{Token: token, BaseURL: f.ServerURL})
	if e != nil {
		return nil, c.ErrInvalid
	}
	return &client{api}, nil
}
func stable(e error) error {
	if e == nil {
		return nil
	}
	var api *protocol.APIError
	if errors.As(e, &api) {
		if api.Code == 409 {
			return app.ErrPollingConflict
		}
		if api.Code == 429 {
			return &app.RateLimited{After: time.Duration(api.RetryAfter) * time.Second}
		}
	}
	return c.ErrUnavailable
}
func (c1 *client) Identity(ctx context.Context) (string, error) {
	id, e := c1.api.Identity(ctx)
	return id, stable(e)
}
func (c1 *client) Webhook(ctx context.Context) (string, error) {
	v, e := c1.api.WebhookInfo(ctx)
	return v.URL, stable(e)
}
func (c1 *client) Register(ctx context.Context, address, secret string) (bool, error) {
	e := c1.api.SetWebhook(ctx, address, secret, []string{"message", "callback_query"})
	return e == nil, stable(e)
}
func (c1 *client) DeleteWebhook(ctx context.Context) error { return stable(c1.api.DeleteWebhook(ctx)) }
func (c1 *client) Poll(ctx context.Context, offset int64, timeout int) ([]json.RawMessage, error) {
	v, e := c1.api.PollOnce(ctx, protocol.PollRequest{Offset: offset, Limit: 100, TimeoutSeconds: timeout, AllowedUpdates: []string{"message", "callback_query"}})
	return v, stable(e)
}
func (c1 *client) Close() { c1.api.Close() }
