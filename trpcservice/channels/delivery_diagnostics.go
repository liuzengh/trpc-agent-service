package channels

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http/httptrace"
	"sync/atomic"
	"syscall"
)

// DeliveryDiagnostics contains only bounded metadata, never an endpoint, error
// string, credential, reply body or provider description. Phase describes the
// last observed HTTP milestone, not proof of provider acceptance or rejection.
type DeliveryDiagnostics struct {
	Kind       string
	Phase      string
	HTTPStatus int
}

// Safe also protects against an Adapter accidentally supplying free-form data.
func (d DeliveryDiagnostics) Safe() DeliveryDiagnostics {
	switch d.Kind {
	case "timeout", "canceled", "dns", "connect", "tls", "connection_reset", "unexpected_eof", "transport", "invalid_response", "provider_rejected":
	default:
		d.Kind = "transport"
	}
	switch d.Phase {
	case "request", "dns", "connect", "tls", "write_request", "wait_response", "response_headers", "response_body", "provider_response":
	default:
		d.Phase = "request"
	}
	if d.HTTPStatus < 100 || d.HTTPStatus > 599 {
		d.HTTPStatus = 0
	}
	return d
}

// HTTPDeliveryTrace observes a single request, including proxy/connection work.
// Hooks may run concurrently or after RoundTrip returns; only atomics are used.
// No goroutines or global trace hooks are created here.
type HTTPDeliveryTrace struct{ phase atomic.Uint32 }

func TraceHTTPDelivery(ctx context.Context) (context.Context, *HTTPDeliveryTrace) {
	t := &HTTPDeliveryTrace{}
	hooks := &httptrace.ClientTrace{
		DNSStart:          func(httptrace.DNSStartInfo) { t.advance(1) },
		ConnectStart:      func(string, string) { t.advance(2) },
		TLSHandshakeStart: func() { t.advance(3) },
		GotConn:           func(httptrace.GotConnInfo) { t.advance(4) },
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				t.advance(5)
			}
		},
		GotFirstResponseByte: func() { t.advance(6) },
	}
	return httptrace.WithClientTrace(ctx, hooks), t
}

func (t *HTTPDeliveryTrace) advance(next uint32) {
	for old := t.phase.Load(); old < next; old = t.phase.Load() {
		if t.phase.CompareAndSwap(old, next) {
			return
		}
	}
}

func (t *HTTPDeliveryTrace) Failure(err error) *DeliveryDiagnostics {
	phases := [...]string{"request", "dns", "connect", "tls", "write_request", "wait_response", "response_headers"}
	return &DeliveryDiagnostics{Kind: deliveryFailureKind(err), Phase: phases[t.phase.Load()]}
}

func deliveryFailureKind(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return "timeout"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns"
	}
	var cert *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	var record tls.RecordHeaderError
	if errors.As(err, &cert) || errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalidCert) || errors.As(err, &record) {
		return "tls"
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) {
		return "connect"
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return "connection_reset"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	return "transport"
}
