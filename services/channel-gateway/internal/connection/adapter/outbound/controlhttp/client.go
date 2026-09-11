// Package controlhttp implements the bounded, mutually authenticated Control
// transport. Connection use cases own local account/owner authorization before
// and after Resolve; successful HTTP alone never grants SDK or sending authority.
package controlhttp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

type Options struct {
	BaseURL, ScopeID, SourceEpoch, InstanceID string
	RootCAs                                   *x509.CertPool
	Certificate                               tls.Certificate
}
type Client struct {
	base                   *url.URL
	scope, epoch, instance string
	http                   *http.Client
	transport              *http.Transport
}

func New(o Options) (*Client, error) {
	u, err := url.Parse(o.BaseURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || o.RootCAs == nil || len(o.Certificate.Certificate) == 0 || o.Certificate.PrivateKey == nil || !c.ValidID(o.ScopeID) || !c.ValidEpoch(o.SourceEpoch) || !c.ValidID(o.InstanceID) {
		return nil, c.ErrInvalid
	}
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: o.RootCAs.Clone(), Certificates: []tls.Certificate{o.Certificate}}, TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 5 * time.Second, DisableCompression: true, MaxResponseHeaderBytes: 16 << 10, MaxIdleConns: 8, MaxIdleConnsPerHost: 4, MaxConnsPerHost: 8, IdleConnTimeout: 30 * time.Second}
	h := &http.Client{Transport: tr, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Client{base: u, scope: o.ScopeID, epoch: o.SourceEpoch, instance: o.InstanceID, http: h, transport: tr}, nil
}
func (c *Client) Close() { c.transport.CloseIdleConnections() }
func (cl *Client) request(ctx context.Context, method, path string, body []byte, limit int) ([]byte, error) {
	return cl.requestStatus(ctx, method, path, body, limit, http.StatusOK)
}
func (cl *Client) requestStatus(ctx context.Context, method, path string, body []byte, limit int, expected int) ([]byte, error) {
	if ctx == nil {
		return nil, c.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	u := *cl.base
	u.Path = path
	req, err := http.NewRequestWithContext(ctx, method, u.String(), strings.NewReader(string(body)))
	if err != nil {
		return nil, c.ErrInvalid
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := cl.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, c.ErrExpired
		}
		return nil, c.ErrUnavailable
	}
	defer res.Body.Close()
	// Never propagate response bodies, transport URLs or TLS errors: they can
	// include credentials or details that do not belong in domain diagnostics.
	if res.StatusCode != expected {
		switch res.StatusCode {
		case 401, 403:
			return nil, c.ErrUnauthorized
		case 409:
			return nil, c.ErrVersion
		default:
			return nil, c.ErrUnavailable
		}
	}
	if encoding := res.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, c.ErrInvalid
	}
	if res.ContentLength > int64(limit) {
		return nil, c.ErrInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, int64(limit)+1))
	if err != nil {
		return nil, c.ErrUnavailable
	}
	if len(raw) > limit {
		return nil, c.ErrInvalid
	}
	return raw, nil
}
func (cl *Client) Fetch(ctx context.Context) (c.Snapshot, error) {
	if ctx == nil {
		return c.Snapshot{}, c.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var result c.Snapshot
	raw, err := cl.request(ctx, http.MethodGet, "/internal/v1/channel-accounts/snapshot", nil, c.MaxSnapshotBytes)
	if err != nil {
		return result, err
	}
	if err = decode("account-snapshot.schema.json", raw, &result); err != nil {
		return c.Snapshot{}, err
	}
	if result.ScopeID != cl.scope || result.SourceEpoch != cl.epoch {
		return c.Snapshot{}, c.ErrIntegrity
	}
	if err = result.Validate(); err != nil {
		return c.Snapshot{}, err
	}
	if ctx.Err() != nil {
		return c.Snapshot{}, c.ErrExpired
	}
	return result, nil
}

type Value struct {
	Purpose string `json:"purpose"`
	ID      string `json:"credential_id"`
	Version int64  `json:"credential_version"`
	Value   string `json:"value"`
}
type resolveResponse struct {
	ScopeID            string  `json:"scope_id"`
	SourceEpoch        string  `json:"source_epoch"`
	TenantID           string  `json:"tenant_id"`
	AccountID          string  `json:"account_id"`
	ConnectionRevision int64   `json:"connection_revision"`
	Values             []Value `json:"values"`
}

// Resolve validates exact response identity/version/purpose. The caller must use
// its trusted account snapshot, and check its live UseToken/Owner around this
// call. This adapter neither accepts arbitrary references nor constructs clients.
func (cl *Client) Resolve(ctx context.Context, a c.Account, r c.ResolveRequest) ([]Value, error) {
	if ctx == nil {
		return nil, c.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.Validate(a); err != nil {
		return nil, err
	}
	if r.ScopeID != cl.scope || r.SourceEpoch != cl.epoch || r.Consumer.InstanceID != cl.instance {
		return nil, c.ErrUnauthorized
	}
	body, err := json.Marshal(r)
	if err != nil || len(body) > 16<<10 {
		return nil, c.ErrInvalid
	}
	if err = wire.Validate("credentials-resolve-request.schema.json", body); err != nil {
		return nil, protocolError(err)
	}
	path := "/internal/v1/tenants/" + url.PathEscape(a.TenantID) + "/channel-accounts/" + url.PathEscape(a.ID) + "/credentials:resolve"
	raw, err := cl.request(ctx, http.MethodPost, path, body, 64<<10)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	var res resolveResponse
	if err = decode("credentials-resolve-response.schema.json", raw, &res); err != nil {
		return nil, err
	}
	if res.ScopeID != r.ScopeID || res.SourceEpoch != r.SourceEpoch || res.TenantID != a.TenantID || res.AccountID != a.ID || res.ConnectionRevision != r.ConnectionRevision || len(res.Values) != len(r.Uses) {
		return nil, c.ErrIntegrity
	}
	for i, u := range r.Uses {
		v := res.Values[i]
		if u.Purpose != v.Purpose || u.ID != v.ID || u.Version != v.Version || v.Value == "" || len(v.Value) > 16<<10 {
			return nil, c.ErrIntegrity
		}
		if v.Purpose == "telegram.webhook_secret" && !c.ValidWebhookSecret(v.Value) {
			return nil, c.ErrInvalid
		}
	}
	if ctx.Err() != nil {
		return nil, c.ErrExpired
	}
	return res.Values, nil
}

var _ interface {
	Fetch(context.Context) (c.Snapshot, error)
} = (*Client)(nil)

func protocolError(err error) error {
	if errors.Is(err, wire.ErrInvalidDocument) {
		return c.ErrInvalid
	}
	return c.ErrUnavailable
}
func decode(name string, raw []byte, out any) error {
	if err := wire.Decode(name, raw, out); err != nil {
		return protocolError(err)
	}
	return nil
}
