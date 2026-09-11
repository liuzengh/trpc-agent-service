// Package workerhttp reads immutable committed Final evidence from Execution.
// Transport authentication failures never become permanent business rejections.
package workerhttp

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"go.opentelemetry.io/otel/trace"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type Options struct {
	BaseURL     string
	RootCAs     *x509.CertPool
	Certificate tls.Certificate
}
type Client struct {
	Tracer    trace.Tracer
	base      *url.URL
	http      *http.Client
	transport *http.Transport
}

func New(o Options) (*Client, error) {
	u, err := url.Parse(o.BaseURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || o.RootCAs == nil || len(o.Certificate.Certificate) == 0 || o.Certificate.PrivateKey == nil {
		return nil, d.ErrInvalid
	}
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: o.RootCAs.Clone(), Certificates: []tls.Certificate{o.Certificate}}, TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 5 * time.Second, DisableCompression: true, MaxResponseHeaderBytes: 16 << 10, MaxIdleConns: 8, MaxIdleConnsPerHost: 4, MaxConnsPerHost: 8, IdleConnTimeout: 30 * time.Second}
	h := &http.Client{Transport: tr, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Client{base: u, http: h, transport: tr}, nil
}
func (c *Client) Close() { c.transport.CloseIdleConnections() }
func (c *Client) VerifyCommittedFinal(ctx context.Context, i d.Intent, digest string) (authorization app.FinalAuthorization, resultErr error) {
	var zero app.FinalAuthorization
	if ctx == nil {
		return zero, d.ErrInvalid
	}
	ctx, span := telemetrytrace.Start(c.Tracer, ctx, "gateway.reply.verify", trace.WithSpanKind(trace.SpanKindClient))
	defer func() { telemetrytrace.End(span, resultErr) }()
	r := wire.FinalRequest{IntentID: i.ID, Digest: digest, AdmissionID: i.AdmissionID, RunID: i.RunID, AttemptID: i.AttemptID, CompletionID: i.CompletionID, ExecutionGeneration: i.ExecutionGeneration, Sequence: i.Sequence}
	body, err := wire.EncodeFinalRequest(r)
	if err != nil {
		return zero, d.ErrInvalid
	}
	u := *c.base
	u.Path = wire.FinalVerifyPath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return zero, d.ErrUnavailable
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	tracecontext.Capture(ctx).Inject(req.Header)
	res, err := c.http.Do(req)
	if err != nil {
		return zero, d.ErrUnavailable
	}
	defer res.Body.Close()
	// 409 is the authenticated owner's explicit durable mismatch. An absent
	// record, auth configuration failure or unknown response must be retried.
	if res.StatusCode == http.StatusConflict {
		return zero, d.ErrUnauthorized
	}
	if res.StatusCode != http.StatusOK {
		return zero, d.ErrUnavailable
	}
	if res.ContentLength > wire.MaxFinalProofBytes {
		return zero, d.ErrUnavailable
	}
	if enc := res.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return zero, d.ErrUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, wire.MaxFinalProofBytes+1))
	if err != nil {
		return zero, d.ErrUnavailable
	}
	proof, err := wire.DecodeFinalResponse(raw)
	if err != nil {
		return zero, d.ErrUnavailable
	}
	if proof.FinalRequest != r {
		return zero, d.ErrUnauthorized
	}
	return app.FinalAuthorization{IntentID: proof.IntentID, Digest: proof.Digest, AdmissionID: proof.AdmissionID, RunID: proof.RunID, AttemptID: proof.AttemptID, CompletionID: proof.CompletionID, ExecutionGeneration: proof.ExecutionGeneration, Sequence: proof.Sequence, TenantID: proof.TenantID, ManifestDigest: proof.ManifestDigest}, nil
}

var _ app.CommittedFinalVerifier = (*Client)(nil)
