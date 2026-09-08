package channels

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func TestDeliveryFailureClassificationNeverUsesErrorText(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		kind string
	}{
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"cancel", context.Canceled, "canceled"},
		{"dns", &net.DNSError{Err: "private-query-canary", Name: "private-host-canary"}, "dns"},
		{"dns_timeout", &net.DNSError{IsTimeout: true}, "timeout"},
		{"connect", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, "connect"},
		{"reset", syscall.ECONNRESET, "connection_reset"},
		{"tls", &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}, "tls"},
		{"hostname", x509.HostnameError{}, "tls"},
		{"eof", io.EOF, "unexpected_eof"},
		{"short_body", io.ErrUnexpectedEOF, "unexpected_eof"},
		{"untyped", errors.New("TLS timeout password=private-error-canary"), "transport"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, observation := TraceHTTPDelivery(context.Background())
			d := observation.Failure(&url.Error{Op: "POST", URL: "https://api.telegram.org/botprivate-token-canary/sendMessage", Err: tc.err})
			if d.Kind != tc.kind || d.Phase != "request" {
				t.Fatalf("diagnostic=%+v", d)
			}
			err := &DeliveryError{Cause: errors.New("Telegram delivery outcome unknown"), Unknown: true, Diagnostics: d}
			if strings.Contains(err.Error(), "canary") || !strings.Contains(err.Error(), "kind="+tc.kind) {
				t.Fatal("unsafe or missing diagnostic")
			}
		})
	}
}

func TestHTTPDeliveryHooksAreConcurrentAndPreserveParent(t *testing.T) {
	parentCalled := false
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{DNSStart: func(httptrace.DNSStartInfo) { parentCalled = true }})
	ctx, observation := TraceHTTPDelivery(ctx)
	hooks := httptrace.ContextClientTrace(ctx)
	hooks.DNSStart(httptrace.DNSStartInfo{})
	if !parentCalled || observation.Failure(io.EOF).Phase != "dns" {
		t.Fatal("parent HTTP trace lost")
	}
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hooks.ConnectStart("tcp", "private-address")
			hooks.TLSHandshakeStart()
			hooks.GotConn(httptrace.GotConnInfo{})
			hooks.WroteRequest(httptrace.WroteRequestInfo{})
			hooks.GotFirstResponseByte()
			_ = observation.Failure(io.EOF)
		}()
	}
	wg.Wait()
	if observation.Failure(io.EOF).Phase != "response_headers" {
		t.Fatal("HTTP milestones moved backwards")
	}
}

func TestDeliveryDiagnosticFieldsAreBounded(t *testing.T) {
	d := DeliveryDiagnostics{Kind: "secret-canary", Phase: "https://private-canary", HTTPStatus: 999999}.Safe()
	if d.Kind != "transport" || d.Phase != "request" || d.HTTPStatus != 0 {
		t.Fatal("unbounded metadata allowed")
	}
	legacy := &DeliveryError{Cause: errors.New("legacy"), Unknown: true}
	if legacy.Error() != "legacy" || !errors.Is(legacy, legacy.Cause) {
		t.Fatal("legacy error compatibility lost")
	}
}
