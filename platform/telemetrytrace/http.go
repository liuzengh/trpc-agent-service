package telemetrytrace

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

var errExport = errors.New("trace export unavailable")

// This OTLP Client plugs into the official otlptrace exporter. Unlike the
// version-pinned stock HTTP client, it bounds response reads, rejects redirects,
// does not inherit endpoint/header environment variables, and never logs a
// Collector-supplied partial-success message or raw transport error.
type httpClient struct {
	endpoint  string
	client    *http.Client
	transport *http.Transport
}

func newHTTPClient(endpoint string, timeout time.Duration) *httpClient {
	tr := &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 2, MaxIdleConnsPerHost: 2, IdleConnTimeout: time.Minute,
		TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13},
	}
	return &httpClient{endpoint: endpoint, transport: tr, client: &http.Client{
		Transport: tr, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}
func (c *httpClient) Start(ctx context.Context) error { return ctx.Err() }
func (c *httpClient) Stop(context.Context) error      { c.transport.CloseIdleConnections(); return nil }
func (c *httpClient) UploadTraces(ctx context.Context, spans []*tracepb.ResourceSpans) error {
	body, err := proto.Marshal(&collector.ExportTraceServiceRequest{ResourceSpans: spans})
	if err != nil {
		return errExport
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return errExport
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := c.client.Do(req)
	if err != nil {
		return errExport
	}
	defer resp.Body.Close()
	const responseLimit = 64 * 1024
	raw, err := io.ReadAll(io.LimitReader(resp.Body, responseLimit+1))
	if err != nil || len(raw) > responseLimit || resp.StatusCode != http.StatusOK {
		return errExport
	}
	if len(raw) == 0 {
		return nil
	}
	var response collector.ExportTraceServiceResponse
	if proto.Unmarshal(raw, &response) != nil {
		return errExport
	}
	if p := response.PartialSuccess; p != nil && (p.RejectedSpans != 0 || p.ErrorMessage != "") {
		return errExport
	}
	return nil
}
