// Package wecompreflight performs one explicitly authorized, bounded subscription.
// Unlike Telegram preflight this operation can replace an existing connection.
package wecompreflight

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
)

type Client struct {
	transport *http.Transport
	endpoint  string
}

func New() *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	tr.TLSHandshakeTimeout = 3 * time.Second
	tr.ResponseHeaderTimeout = 5 * time.Second
	tr.MaxResponseHeaderBytes = 8192
	tr.MaxConnsPerHost = 4
	tr.MaxIdleConnsPerHost = 4
	tr.MaxIdleConns = 4
	tr.DisableCompression = true
	return &Client{transport: tr, endpoint: wecom.DefaultURL}
}
func (c *Client) Close() {
	if c != nil && c.transport != nil {
		c.transport.CloseIdleConnections()
	}
}
func (c *Client) InspectConnection(ctx context.Context, secret app.Secret, botID string) (app.ConnectionProbeResult, error) {
	if ctx == nil || c == nil || c.transport == nil {
		return app.ConnectionProbeResult{}, app.ErrInvalid
	}
	if ctx.Err() != nil {
		return app.ConnectionProbeResult{}, app.ErrExpired
	}
	h := &http.Client{Transport: c.transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	result, err := wecom.ProbeAuthentication(ctx, wecom.Config{BotID: botID, Secret: secret.Reveal(), URL: c.endpoint, DialTimeout: 5 * time.Second, AckTimeout: 5 * time.Second}, wecom.WithHTTPClient(h))
	if ctx.Err() != nil {
		return app.ConnectionProbeResult{}, app.ErrExpired
	}
	if err != nil {
		return app.ConnectionProbeResult{Code: "PROVIDER_UNAVAILABLE"}, nil
	}
	return app.ConnectionProbeResult{Code: result.Code, Authenticated: result.Authenticated}, nil
}
