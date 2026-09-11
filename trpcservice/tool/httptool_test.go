package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// specFor builds a compiled spec against a test server. The server listens on
// loopback, which the address policy refuses by default — so every test that
// talks to one must name it in allow_hosts, exactly as the operator of an
// internal service would. That is deliberate: it keeps the tests running
// through the same gate production does.
func specFor(t *testing.T, srv *httptest.Server, mutate func(*HTTPSpec)) *httpSpec {
	t.Helper()
	host := srv.Listener.Addr().(*net.TCPAddr).IP.String()
	spec := HTTPSpec{
		Method:     http.MethodPost,
		URL:        srv.URL + "/call",
		AllowHosts: []string{host},
		AllowCIDRs: []string{host + "/32"},
	}
	if mutate != nil {
		mutate(&spec)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := CompileHTTPSpec(raw)
	if err != nil {
		t.Fatalf("compile spec: %v", err)
	}
	return compiled
}

func schemaFor(t *testing.T, raw string) *InputSchema {
	t.Helper()
	sch, err := CompileInputSchema(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("compile schema: %v", err)
	}
	return sch
}

func TestCompileHTTPSpecRefusals(t *testing.T) {
	cases := []struct {
		name string
		spec string
		want string
	}{
		{"no method", `{"url":"https://example.com/x"}`, "method"},
		{"bad method", `{"method":"DELETE","url":"https://example.com/x"}`, "method"},
		{"no url", `{"method":"GET"}`, "url"},
		{"bad scheme", `{"method":"GET","url":"ftp://example.com/x"}`, "scheme"},
		{"userinfo", `{"method":"GET","url":"https://user:pass@example.com/x"}`, "userinfo"},
		{"fragment", `{"method":"GET","url":"https://example.com/x#frag"}`, "fragment"},
		{"wildcard host", `{"method":"GET","url":"https://example.com/x","allow_hosts":["*.example.com"]}`, "exact host"},
		{"host with path", `{"method":"GET","url":"https://example.com/x","allow_hosts":["example.com/x"]}`, "exact host"},
		{"bad cidr", `{"method":"GET","url":"https://example.com/x","allow_cidrs":["10.0.0.0/99"]}`, "allow_cidrs"},
		{"unknown field", `{"method":"GET","url":"https://example.com/x","nope":1}`, "unknown field"},
		{"http without blessing", `{"method":"GET","url":"http://internal.svc/x"}`, "allow_hosts"},
	}
	for _, c := range cases {
		_, err := CompileHTTPSpec(json.RawMessage(c.spec))
		if err == nil {
			t.Fatalf("%s: compiled, want refusal", c.name)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: error %q does not mention %q", c.name, err, c.want)
		}
	}
	// HTTP is allowed only for a service the operator named exactly.
	if _, err := CompileHTTPSpec(json.RawMessage(
		`{"method":"GET","url":"http://internal.svc/x","allow_hosts":["internal.svc"],"allow_cidrs":["10.1.0.0/16"]}`)); err != nil {
		t.Fatalf("blessed internal http refused: %v", err)
	}
}

func TestAddressPolicyClassification(t *testing.T) {
	spec := &httpSpec{allowHosts: map[string]struct{}{"blessed.internal": {}}, allowCIDRs: nil}
	cases := []struct {
		host string
		ip   string
		ok   bool
		why  string
	}{
		{"public.example", "93.184.216.34", true, ""},
		{"public.example", "2606:2800:220:1::1", true, ""},
		{"localhost", "127.0.0.1", false, "loopback"},
		{"localhost", "::1", false, "loopback"},
		{"internal", "10.1.2.3", false, "private"},
		{"internal", "192.168.1.1", false, "private"},
		{"internal", "172.16.0.1", false, "private"},
		{"internal", "fd00::1", false, "private"},
		{"metadata", "169.254.169.254", false, "cloud metadata"},
		{"linklocal", "169.254.1.1", false, "link-local"},
		{"nated", "100.64.0.1", false, "carrier-grade NAT"},
		{"zero", "0.0.0.0", false, "unspecified"},
		{"multi", "224.0.0.1", false, "multicast"},
		{"blessed.internal", "10.9.9.9", true, ""},
	}
	for _, c := range cases {
		err := spec.addressAllowed(c.host, net.ParseIP(c.ip))
		if c.ok && err != nil {
			t.Fatalf("%s %s refused: %v", c.host, c.ip, err)
		}
		if !c.ok {
			if err == nil {
				t.Fatalf("%s %s allowed, want refusal (%s)", c.host, c.ip, c.why)
			}
			if c.why != "" && !strings.Contains(err.Error(), c.why) {
				t.Fatalf("%s %s: error %q does not mention %q", c.host, c.ip, err, c.why)
			}
		}
	}
}

func TestHTTPToolPOSTSendsArgumentsAndSecretHeaders(t *testing.T) {
	var (
		gotBody    string
		gotAuth    string
		gotLiteral string
		gotMethod  string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		gotAuth = r.Header.Get("Authorization")
		gotLiteral = r.Header.Get("X-Api-Version")
		gotMethod = r.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"echoed":true,"n":9007199254740993}`))
	}))
	defer srv.Close()

	spec := specFor(t, srv, func(s *HTTPSpec) {
		s.Headers = map[string]string{"X-Api-Version": "2024-01", "Authorization": "literal-must-lose"}
	})
	// The secret header must override the literal one: the resolved
	// credential is the point of the header.
	tool := NewHTTPTool("lookup", "look things up",
		schemaFor(t, `{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`),
		spec, map[string]string{"Authorization": "Bearer tok"}, HTTPPolicy{SideEffect: "read"})

	result, err := tool.Call(context.Background(), []byte(`{"text":"alpha"}`))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if gotMethod != http.MethodPost || !strings.Contains(gotBody, `"text":"alpha"`) {
		t.Fatalf("method/body = %s / %s", gotMethod, gotBody)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("Authorization = %q, want the resolved secret", gotAuth)
	}
	if gotLiteral != "2024-01" {
		t.Fatalf("literal header = %q", gotLiteral)
	}
	m, ok := result.(map[string]any)
	if !ok || m["echoed"] != true {
		t.Fatalf("result = %#v", result)
	}
	if n, ok := m["n"].(json.Number); !ok || n.String() != "9007199254740993" {
		t.Fatalf("large integer lost precision: %#v", m["n"])
	}
}

func TestHTTPToolGETScalarQuery(t *testing.T) {
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.RawQuery
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	spec := specFor(t, srv, func(s *HTTPSpec) {
		s.Method = http.MethodGet
		s.URL = srv.URL + "/search"
	})
	tool := NewHTTPTool("search", "", schemaFor(t, `{"type":"object"}`), spec, nil, HTTPPolicy{SideEffect: "read"})
	if _, err := tool.Call(context.Background(), []byte(`{"q":"hé llo","n":3,"flag":true}`)); err != nil {
		t.Fatalf("call: %v", err)
	}
	if !strings.Contains(gotURL, "q=h%C3%A9+llo") || !strings.Contains(gotURL, "n=3") || !strings.Contains(gotURL, "flag=true") {
		t.Fatalf("query = %q", gotURL)
	}
	// Nested values are refused: a model must not be able to grow the URL.
	var ce *CallError
	if _, err := tool.Call(context.Background(), []byte(`{"nested":{"a":1}}`)); err == nil {
		t.Fatal("nested GET argument accepted")
	} else if !errorAs(err, &ce) || ce.Outcome != Failed || ce.ErrorType != "bad_arguments" {
		t.Fatalf("nested arg error = %#v", err)
	}
}

func TestHTTPToolReadRetriesOnceOn5xxAndWriteDoesNot(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "sick", http.StatusBadGateway)
	}))
	defer srv.Close()

	read := NewHTTPTool("read", "", schemaFor(t, `{"type":"object"}`), specFor(t, srv, nil), nil, HTTPPolicy{SideEffect: "read"})
	if _, err := read.Call(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("5xx read call reported success")
	} else {
		var ce *CallError
		if !errorAs(err, &ce) || ce.Outcome != Failed || ce.ErrorType != "http_5xx" {
			t.Fatalf("read 5xx classified as %#v", err)
		}
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("read 5xx attempts = %d, want exactly 2 (one retry)", got)
	}

	hits.Store(0)
	write := NewHTTPTool("write", "", schemaFor(t, `{"type":"object"}`), specFor(t, srv, nil), nil,
		HTTPPolicy{SideEffect: "write", Idempotent: false})
	if _, err := write.Call(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("5xx write call reported success")
	} else {
		var ce *CallError
		if !errorAs(err, &ce) || ce.Outcome != Unknown {
			t.Fatalf("non-idempotent write 5xx classified as %#v, want Unknown", err)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("write 5xx attempts = %d, want exactly 1 (no retry)", got)
	}
}

func TestHTTPToolTimeoutsClassifiedByPolicy(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer func() { close(block); srv.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	write := NewHTTPTool("write", "", schemaFor(t, `{"type":"object"}`), specFor(t, srv, nil), nil,
		HTTPPolicy{SideEffect: "write"})
	_, err := write.Call(ctx, []byte(`{}`))
	var ce *CallError
	if !errorAs(err, &ce) || ce.Outcome != Unknown {
		t.Fatalf("timed-out write classified as %#v, want Unknown", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel2()
	read := NewHTTPTool("read", "", schemaFor(t, `{"type":"object"}`), specFor(t, srv, nil), nil,
		HTTPPolicy{SideEffect: "read"})
	_, err = read.Call(ctx2, []byte(`{}`))
	if !errorAs(err, &ce) || ce.Outcome != Failed {
		t.Fatalf("timed-out read classified as %#v, want Failed", err)
	}
}

func TestHTTPToolDoesNotFollowRedirects(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/elsewhere", http.StatusFound)
	}))
	defer redirector.Close()

	tool := NewHTTPTool("hop", "", schemaFor(t, `{"type":"object"}`), specFor(t, redirector, nil), nil,
		HTTPPolicy{SideEffect: "read"})
	_, err := tool.Call(context.Background(), []byte(`{}`))
	var ce *CallError
	if !errorAs(err, &ce) || ce.ErrorType != "redirected" {
		t.Fatalf("redirect classified as %#v, want a failure", err)
	}
	if got := targetHits.Load(); got != 0 {
		t.Fatalf("the redirect target was called %d times; redirects must not be followed", got)
	}
}

func TestHTTPToolResponseLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"big":"` + strings.Repeat("a", 2048) + `"}`))
	}))
	defer srv.Close()

	spec := specFor(t, srv, func(s *HTTPSpec) { s.MaxResponseBytes = 512 })
	tool := NewHTTPTool("big", "", schemaFor(t, `{"type":"object"}`), spec, nil, HTTPPolicy{SideEffect: "read"})
	_, err := tool.Call(context.Background(), []byte(`{}`))
	var ce *CallError
	if !errorAs(err, &ce) || ce.ErrorType != "response_too_large" {
		t.Fatalf("oversized response classified as %#v", err)
	}
}

func TestHTTPToolRefusesLoopbackWithoutBlessing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	host := srv.Listener.Addr().(*net.TCPAddr).IP.String()
	// The binding blesses a *different* host: compilation passes (http is
	// allowed for a named internal service), and the refusal then has to come
	// from the address policy at dial time — which is the layer this test is
	// about.
	raw := fmt.Sprintf(`{"method":"POST","url":"%s/call","allow_hosts":["localhost"],"allow_cidrs":["10.0.0.0/8"]}`, srv.URL)
	compiled, err := CompileHTTPSpec(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	tool := NewHTTPTool("unblessed", "", schemaFor(t, `{"type":"object"}`), compiled, nil, HTTPPolicy{SideEffect: "read"})
	_, err = tool.Call(context.Background(), []byte(`{}`))
	if err == nil {
		t.Fatal("a loopback call without allow_hosts succeeded")
	}
	if !strings.Contains(err.Error(), "loopback") || !strings.Contains(err.Error(), host) {
		t.Fatalf("refusal %q does not name the reason and the address", err)
	}
	var ce *CallError
	if !errorAs(err, &ce) || ce.Outcome != Failed {
		t.Fatalf("ssrf refusal classified as %#v, want Failed (nothing left the process)", err)
	}
}

// errorAs is errors.As under a local name, so the assertions below read
// left-to-right without shadowing the errors import at every call site.
func errorAs(err error, target any) bool { return errors.As(err, target) }
