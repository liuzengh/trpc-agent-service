package wecom

import "net/http"

// handshakeClient preserves the official SDK's HTTP/1 field spelling. Although
// header names are case-insensitive by RFC, the WeCom endpoint returns HTTP 404
// for Go's canonical Sec-Websocket-* spelling. Do not mutate the caller's client,
// transport, request, TLS verification, proxy, timeout or redirect settings.
func handshakeClient(source *http.Client) *http.Client {
	if source == nil {
		source = http.DefaultClient
	}
	client := *source
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = handshakeTransport{base: base}
	return &client
}

type handshakeTransport struct{ base http.RoundTripper }

func (t handshakeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	for _, key := range []string{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Extensions", "Sec-WebSocket-Protocol"} {
		if values := request.Header.Values(key); len(values) > 0 {
			request.Header.Del(key)
			request.Header[key] = values
		}
	}
	return t.base.RoundTrip(request)
}
func (t handshakeTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
