//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package channel

import (
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"unicode/utf8"
)

// DeliveryIdempotencyKeyHeader is the standard header used when an adapter
// can pass a durable Outbox chunk identity to a provider or egress proxy.
// Telegram and WeChat Work currently ignore unknown headers, but forwarding
// the key makes deduplication available without changing the Adapter seam.
const DeliveryIdempotencyKeyHeader = "Idempotency-Key"

const maxDeliveryIDBytes = 128

// doProviderRequest records whether the transport may have dispatched the
// request. GotConn runs before dispatch; WroteRequest can arrive after Do
// returns and is only a fallback. A failure after acquiring a connection has
// an unknown outcome, while DNS and connection-setup failures remain retryable.
func doProviderRequest(client *http.Client, req *http.Request) (resp *http.Response, mayHaveDispatched bool, err error) {
	if client == nil || req == nil {
		return nil, false, fmt.Errorf("provider HTTP request is missing client or request")
	}
	var dispatched atomic.Bool
	trace := &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			dispatched.Store(true)
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			dispatched.Store(true)
		},
	}
	request := req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err = client.Do(request)
	return resp, dispatched.Load(), err
}

// providerTransportFailure converts a transport failure into the narrowest
// durable classification supported by the evidence available from net/http.
// The returned errors intentionally omit the underlying network text because
// it can contain credential-bearing provider URLs or proxy details.
func providerTransportFailure(provider string, mayHaveDispatched bool) error {
	transportErr := retryableTransportError(provider)
	if mayHaveDispatched {
		return UnknownDeliveryError(transportErr)
	}
	return transportErr
}

func setDeliveryIdempotencyKey(req *http.Request, msg *OutboundMessage) error {
	if req == nil || msg == nil || msg.DeliveryID == "" {
		return nil
	}
	if len(msg.DeliveryID) > maxDeliveryIDBytes || !utf8.ValidString(msg.DeliveryID) ||
		strings.ContainsAny(msg.DeliveryID, "\x00\r\n") {
		return fmt.Errorf("delivery idempotency key is invalid")
	}
	req.Header.Set(DeliveryIdempotencyKeyHeader, msg.DeliveryID)
	return nil
}

func setStreamDeliveryIdempotencyKey(req *http.Request, chunk *StreamChunk) error {
	if req == nil || chunk == nil || chunk.DeliveryID == "" {
		return nil
	}
	if len(chunk.DeliveryID) > maxDeliveryIDBytes || !utf8.ValidString(chunk.DeliveryID) ||
		strings.ContainsAny(chunk.DeliveryID, "\x00\r\n") {
		return fmt.Errorf("delivery idempotency key is invalid")
	}
	req.Header.Set(DeliveryIdempotencyKeyHeader, chunk.DeliveryID)
	return nil
}

// providerRedirectPolicy keeps credential-bearing provider requests on the
// expected HTTPS origin. The standard http.Client follows redirects by
// default; accepting a redirect to another host could disclose a bot token,
// access token, or provider secret in the redirected URL.
func providerRedirectPolicy(expectedHost string) func(*http.Request, []*http.Request) error {
	return func(request *http.Request, _ []*http.Request) error {
		if request == nil || request.URL == nil ||
			!strings.EqualFold(request.URL.Scheme, "https") ||
			!strings.EqualFold(request.URL.Hostname(), expectedHost) ||
			(request.URL.Port() != "" && request.URL.Port() != "443") ||
			request.URL.User != nil {
			return fmt.Errorf("provider redirect target is not trusted")
		}
		return nil
	}
}
