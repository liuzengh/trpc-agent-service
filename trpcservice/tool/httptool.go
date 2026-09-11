package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// The HTTP tool: the only way a model's output can reach a network, and
// therefore the piece the approved plan surrounds with the most rules
// ("受控工具"): the URL, method and headers belong to the admin-authored
// binding, never to the model; the arguments are schema-checked before the
// request exists; the connection may not leave for anywhere that looks like
// loopback, link-local, private space or a metadata service; redirects are
// never followed; and a request whose arrival cannot be known is Unknown, not
// a retry.

// HTTPPolicy is the per-binding behaviour the governor and the HTTP tool
// share. It is copied from the tool_bindings row at assembly time; the
// governor re-reads the row before each call to catch a revocation, but the
// retry/unknown classification uses this snapshot, because "was this call
// allowed to retry" cannot change mid-call.
type HTTPPolicy struct {
	SideEffect string // none | read | write
	Idempotent bool
}

// retryable is true when a second attempt cannot duplicate an effect: reads
// and explicitly-idempotent calls. The approved plan's "安全重试计数受限"
// starts here — at most one retry, and only for these.
func (p HTTPPolicy) retryable() bool {
	return p.SideEffect != "write" || p.Idempotent
}

// unsafeOnNoResponse is true when a request that may have been sent and
// never answered must be classified Unknown instead of Failed.
func (p HTTPPolicy) unsafeOnNoResponse() bool {
	return p.SideEffect == "write" && !p.Idempotent
}

// HTTPSpec is the admin-authored template, stored as JSON in
// tool_bindings.spec.
type HTTPSpec struct {
	// Description is what the model is told about the tool.
	Description string `json:"description,omitempty"`
	Method      string `json:"method"`
	URL         string `json:"url"`
	// Headers are literal, non-secret values (Content-Type, an API version).
	// Credentials never live here: they are references in the binding's
	// secret_refs map and are resolved through the allowlist in
	// trpcservice/secrets.
	Headers map[string]string `json:"headers,omitempty"`
	// AllowHosts and AllowCIDRs are the explicit escape hatches the approved
	// plan allows ("内部 API 仅允许管理员显式配置的精确域名/CIDR"). A host
	// must match exactly — no wildcards, no suffixes — so "internal.svc"
	// cannot be reached by registering "evil-internal.svc".
	AllowHosts []string `json:"allow_hosts,omitempty"`
	AllowCIDRs []string `json:"allow_cidrs,omitempty"`
	// MaxResponseBytes bounds what is read back; default 256 KiB.
	MaxResponseBytes int64 `json:"max_response_bytes,omitempty"`
}

// httpSpec is the compiled, checked form.
type httpSpec struct {
	HTTPSpec
	parsed     *url.URL
	allowHosts map[string]struct{}
	allowCIDRs []*net.IPNet
	maxBody    int64
}

const defaultMaxResponseBytes = 256 << 10

// CompileHTTPSpec validates the template once, at assembly time, so an
// unusable spec fails the execution loudly instead of failing on the first
// model-chosen call.
func CompileHTTPSpec(raw json.RawMessage) (*httpSpec, error) {
	var spec HTTPSpec
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return nil, fmt.Errorf("tool: http spec: %w", err)
	}
	spec.Method = strings.ToUpper(strings.TrimSpace(spec.Method))
	if spec.Method != http.MethodGet && spec.Method != http.MethodPost {
		return nil, fmt.Errorf("tool: http spec: method %q is not supported (GET or POST)", spec.Method)
	}
	if spec.URL == "" {
		return nil, errors.New("tool: http spec: url is required")
	}
	u, err := url.Parse(spec.URL)
	if err != nil {
		return nil, fmt.Errorf("tool: http spec: url: %w", err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, fmt.Errorf("tool: http spec: scheme %q is not supported (https, or http only for an internal service named in allow_hosts and allow_cidrs)", u.Scheme)
	}
	if u.Host == "" || u.Hostname() == "" {
		return nil, errors.New("tool: http spec: url has no host")
	}
	if u.User != nil {
		return nil, errors.New("tool: http spec: url must not carry userinfo; use secret_refs for credentials")
	}
	if u.Fragment != "" {
		return nil, errors.New("tool: http spec: url must not carry a fragment")
	}
	compiled := &httpSpec{HTTPSpec: spec, parsed: u, allowHosts: map[string]struct{}{}, maxBody: spec.MaxResponseBytes}
	if compiled.maxBody <= 0 {
		compiled.maxBody = defaultMaxResponseBytes
	}
	for _, h := range spec.AllowHosts {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" {
			continue
		}
		if strings.Contains(h, "*") || strings.Contains(h, "/") {
			return nil, fmt.Errorf("tool: http spec: allow_hosts entry %q must be an exact host, not a pattern", h)
		}
		compiled.allowHosts[h] = struct{}{}
	}
	for _, c := range spec.AllowCIDRs {
		_, ipnet, err := net.ParseCIDR(strings.TrimSpace(c))
		if err != nil {
			return nil, fmt.Errorf("tool: http spec: allow_cidrs entry %q: %w", c, err)
		}
		compiled.allowCIDRs = append(compiled.allowCIDRs, ipnet)
	}
	if u.Scheme == "http" && (len(compiled.allowHosts) == 0 || len(compiled.allowCIDRs) == 0) {
		// Plain http only exists for explicitly blessed internal services;
		// without both an exact host and a CIDR blessing, the only thing
		// plain http could reach is something the address rules already
		// refuse, so it is refused at compile time with the reason.
		return nil, errors.New("tool: http spec: plain http requires allow_hosts and allow_cidrs naming the internal service explicitly")
	}
	return compiled, nil
}

// HTTPTool is a compiled HTTP binding, ready to be wrapped by the governor.
type HTTPTool struct {
	name         string
	description  string
	schema       *InputSchema
	spec         *httpSpec
	secretHeader map[string]string
	policy       HTTPPolicy
	client       *http.Client
}

// NewHTTPTool compiles the client. secretHeader is already resolved by the
// caller (through the secrets allowlist); this package never reads the
// environment itself.
func NewHTTPTool(name, description string, schema *InputSchema, spec *httpSpec, secretHeader map[string]string, policy HTTPPolicy) *HTTPTool {
	t := &HTTPTool{
		name:         name,
		description:  description,
		schema:       schema,
		spec:         spec,
		secretHeader: secretHeader,
		policy:       policy,
	}
	t.client = &http.Client{
		// No ambient proxy: net/http would otherwise honour HTTP_PROXY and
		// route the request through a host that never passed the address
		// checks below.
		//
		// DialContext is the address-checking dialer and the only dialer:
		// DialTLSContext is deliberately left unset, because a custom
		// DialTLSContext that returns a plain TCP connection would be used
		// *instead of* the transport's own TLS handshake, silently turning an
		// https binding into plaintext (measured while writing this file: the
		// first draft did exactly that).
		Transport: &http.Transport{
			Proxy:       nil,
			DialContext: t.dialVerified,
			// DialTLSContext: unset on purpose; see above.
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 0, // the caller's context is the deadline
			DisableKeepAlives:     true,
			MaxIdleConns:          1,
		},
		// A redirect is a second request to an address the binding did not
		// name; the response is returned as-is and classified as a failure.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return t
}

// Declaration implements frameworktool.Tool.
func (h *HTTPTool) Declaration() *frameworktool.Declaration {
	desc := h.description
	if desc == "" {
		desc = h.spec.Description
	}
	if desc == "" {
		desc = "Call the configured HTTP endpoint."
	}
	return &frameworktool.Declaration{
		Name:        h.name,
		Description: desc,
		InputSchema: h.schema.Declaration(),
	}
}

// inputSchema exposes the compiled schema to the governor's Wrap, which
// re-validates every call's arguments against it.
func (h *HTTPTool) inputSchema() *InputSchema { return h.schema }

// Call implements frameworktool.CallableTool. It never sees anything the
// model chose beyond the arguments: method, URL and headers come from the
// compiled spec.
func (h *HTTPTool) Call(ctx context.Context, jsonArgs []byte) (any, error) {
	args := bytes.TrimSpace(jsonArgs)
	if len(args) == 0 {
		args = []byte("{}")
	}
	// Validate the request shape once, before any attempt: nothing has left
	// the process, so a bad request is Failed — the model gets to see it and
	// try again.
	if _, err := h.buildRequest(ctx, args); err != nil {
		return nil, &CallError{Outcome: Failed, ErrorType: "bad_arguments", Err: err}
	}

	attempts := 1
	if h.policy.retryable() {
		attempts = 2
	}
	var lastCall *CallError
	for attempt := 1; attempt <= attempts; attempt++ {
		// The request is rebuilt per attempt, not reused: an http.Request's
		// body is consumed by the first Do, and reusing it makes the retry
		// fail with a body-length mismatch instead of reaching the upstream
		// (measured: the 5xx retry test caught exactly this).
		req, err := h.buildRequest(ctx, args)
		if err != nil {
			return nil, &CallError{Outcome: Failed, ErrorType: "bad_arguments", Err: err}
		}
		result, callErr := h.once(ctx, req)
		if callErr == nil {
			return result, nil
		}
		lastCall = callErr
		if !callErr.retryableStatus || attempt == attempts {
			return nil, callErr
		}
		// One retry, with a short pause: enough to step over a connection
		// reset, not enough to hide a sick upstream from the latency the
		// journal records.
		select {
		case <-ctx.Done():
			return nil, h.transportOutcome(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil, lastCall
}

// once performs one physical attempt. The request is rebuilt so the retry
// starts from a clean body. Every conclusion is also reported to the attempt
// recorder when one is attached, so the ledger shows each time the network
// was actually touched.
func (h *HTTPTool) once(ctx context.Context, req *http.Request) (any, *CallError) {
	resp, err := h.client.Do(req)
	if err != nil {
		// Transport-level failure. Whether this may be ambiguous depends on
		// the policy, and on the error: a refused connection never left the
		// machine, a timeout after the request went out may have arrived.
		if h.policy.unsafeOnNoResponse() && ambiguousTransportError(err) {
			res := h.transportOutcome(err)
			recordAttempt(ctx, 0, res.Outcome, res.ErrorType, res.Error())
			return nil, res
		}
		res := &CallError{Outcome: Failed, ErrorType: "transport", Err: err}
		recordAttempt(ctx, 0, res.Outcome, res.ErrorType, res.Error())
		return nil, res
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, h.spec.maxBody+1))
	if err != nil {
		if h.policy.unsafeOnNoResponse() {
			res := &CallError{Outcome: Unknown, ErrorType: "response_read",
				Err: fmt.Errorf("tool: reading the response of a write call failed, so whether it was processed is unknown: %w", err)}
			recordAttempt(ctx, resp.StatusCode, res.Outcome, res.ErrorType, res.Error())
			return nil, res
		}
		res := &CallError{Outcome: Failed, ErrorType: "response_read", Err: err}
		recordAttempt(ctx, resp.StatusCode, res.Outcome, res.ErrorType, res.Error())
		return nil, res
	}
	if int64(len(body)) > h.spec.maxBody {
		res := &CallError{Outcome: Failed, ErrorType: "response_too_large",
			Err: fmt.Errorf("tool: response exceeds %d bytes", h.spec.maxBody)}
		recordAttempt(ctx, resp.StatusCode, res.Outcome, res.ErrorType, res.Error())
		return nil, res
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		recordAttempt(ctx, resp.StatusCode, Succeeded, "", "")
		return h.decodeResult(resp, body), nil
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		res := &CallError{Outcome: Failed, ErrorType: "redirected",
			Err: fmt.Errorf("tool: upstream answered %d; redirects are not followed", resp.StatusCode)}
		recordAttempt(ctx, resp.StatusCode, res.Outcome, res.ErrorType, res.Error())
		return nil, res
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// A 4xx means the request was received and refused: no effect
		// happened, and repeating it cannot help.
		res := &CallError{Outcome: Failed, ErrorType: "http_4xx",
			Err: fmt.Errorf("tool: upstream answered %d: %s", resp.StatusCode, truncate(string(body), 200))}
		recordAttempt(ctx, resp.StatusCode, res.Outcome, res.ErrorType, res.Error())
		return nil, res
	default:
		res := &CallError{Outcome: Failed, ErrorType: "http_5xx", retryableStatus: true,
			Err: fmt.Errorf("tool: upstream answered %d: %s", resp.StatusCode, truncate(string(body), 200))}
		if h.policy.unsafeOnNoResponse() {
			// A 5xx on a write may still have taken effect before the error
			// was produced; the conservative reading is "unknown".
			res.Outcome = Unknown
		}
		recordAttempt(ctx, resp.StatusCode, res.Outcome, res.ErrorType, res.Error())
		return nil, res
	}
}

// decodeResult turns a 2xx body into the tool result. JSON stays JSON
// (numbers preserved as text by json.Number, so an id like 9007199254740993
// survives); anything else is returned as a length-limited string.
func (h *HTTPTool) decodeResult(resp *http.Response, body []byte) any {
	ct := resp.Header.Get("Content-Type")
	if strings.Contains(ct, "json") || json.Valid(body) {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err == nil {
			return v
		}
	}
	return map[string]any{"status": resp.StatusCode, "body": truncate(string(body), 4000)}
}

// transportOutcome classifies a transport error under the policy: a write
// that may have arrived is Unknown; anything else is Failed.
func (h *HTTPTool) transportOutcome(err error) *CallError {
	if h.policy.unsafeOnNoResponse() && ambiguousTransportError(err) {
		return &CallError{Outcome: Unknown, ErrorType: "transport_ambiguous", Err: fmt.Errorf(
			"tool: %v; the request may have reached the upstream, so its effect is unknown and is not retried", err)}
	}
	return &CallError{Outcome: Failed, ErrorType: "transport", Err: err}
}

// ambiguousTransportError reports whether err could have happened after the
// request left this process. A DNS failure or a refused connection could
// not; a timeout, a reset or an EOF after the request may have.
func ambiguousTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := err.Error()
	for _, frag := range []string{"EOF", "connection reset", "broken pipe", "unexpected EOF", "server closed"} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

// buildRequest turns validated-shape arguments into the one request the spec
// allows. GET puts scalar arguments in the query string; POST sends the
// argument object as the JSON body.
func (h *HTTPTool) buildRequest(ctx context.Context, args []byte) (*http.Request, error) {
	u := *h.spec.parsed
	var body io.Reader
	if h.spec.Method == http.MethodGet {
		values, err := scalarQuery(args)
		if err != nil {
			return nil, err
		}
		u.RawQuery = values.Encode()
	} else {
		body = bytes.NewReader(args)
	}
	req, err := http.NewRequestWithContext(ctx, h.spec.Method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("tool: build request: %w", err)
	}
	if h.spec.Method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range h.spec.Headers {
		req.Header.Set(k, v)
	}
	// Secret headers are set last and may not be overridden by literals: the
	// resolved credential is the point of the header.
	for k, v := range h.secretHeader {
		req.Header.Set(k, v)
	}
	return req, nil
}

// scalarQuery flattens a top-level object of scalars into query parameters.
// Nested values are refused rather than stringified: a URL a model can grow
// without bound is a denial-of-service surface, and GET bindings are declared
// with scalar parameters anyway.
func scalarQuery(args []byte) (url.Values, error) {
	dec := json.NewDecoder(bytes.NewReader(args))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("tool: GET arguments must be a JSON object: %w", err)
	}
	values := url.Values{}
	for k, v := range m {
		switch t := v.(type) {
		case string:
			values.Set(k, t)
		case bool:
			values.Set(k, fmt.Sprintf("%t", t))
		case json.Number:
			values.Set(k, t.String())
		case nil:
			values.Set(k, "")
		default:
			return nil, fmt.Errorf("tool: GET argument %q must be a string, number, boolean or null", k)
		}
	}
	return values, nil
}

// dialVerified is the transport's dialer and the SSRF boundary. It resolves
// the host itself, checks every candidate address against the deny rules,
// and dials only an address that passed — so the IP that gets connected is
// the IP that was checked, and a DNS answer that changes between check and
// connect (rebinding) cannot slip through.
func (h *HTTPTool) dialVerified(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("tool: dial %q: %w", addr, err)
	}
	d := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 0}
	if ip := net.ParseIP(host); ip != nil {
		if err := h.addressAllowed(host, ip); err != nil {
			return nil, err
		}
		return d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("tool: resolve %q: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("tool: resolve %q: no addresses", host)
	}
	var lastErr error
	for _, candidate := range ips {
		if err := h.addressAllowed(host, candidate.IP); err != nil {
			lastErr = err
			continue
		}
		conn, err := d.DialContext(ctx, network, net.JoinHostPort(candidate.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// addressAllowed is the address policy: loopback, link-local (which includes
// the 169.254.169.254 metadata address), private, unspecified, multicast and
// carrier-grade-NAT addresses are refused unless the binding names this exact
// host in allow_hosts or this network in allow_cidrs. Everything else — the
// public internet — is allowed.
func (h *HTTPTool) addressAllowed(host string, ip net.IP) error {
	return h.spec.addressAllowed(host, ip)
}

// addressAllowed is the policy itself, kept on the compiled spec so it is
// testable without a transport and cannot drift between the dialer and any
// later caller.
func (s *httpSpec) addressAllowed(host string, ip net.IP) error {
	if _, ok := s.allowHosts[strings.ToLower(host)]; ok {
		return nil
	}
	for _, cidr := range s.allowCIDRs {
		if cidr.Contains(ip) {
			return nil
		}
	}
	refuse := func(what string) error {
		return fmt.Errorf(
			"tool: refusing to connect to %s address %s (host %q): add the exact host to allow_hosts or the network to allow_cidrs if this internal service is intended",
			what, ip, host)
	}
	// 100.64.0.0/10 is not in net.IP.IsPrivate, so it needs its own check.
	// It is used for carrier-grade NAT and, in some clouds, for internal
	// service addresses — either way it is not the public internet.
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 100 && ip4[1]&0xC0 == 64 {
		return refuse("carrier-grade NAT")
	}
	switch {
	case ip.IsUnspecified():
		return refuse("unspecified")
	case ip.IsMulticast() || ip.IsInterfaceLocalMulticast():
		return refuse("multicast")
	case ip.Equal(net.IPv4(169, 254, 169, 254)) || ip.Equal(net.ParseIP("fd00:ec2::254")):
		return refuse("cloud metadata")
	case ip.IsLoopback():
		return refuse("loopback")
	case ip.IsLinkLocalUnicast():
		return refuse("link-local")
	case ip.IsPrivate():
		return refuse("private")
	}
	return nil
}
