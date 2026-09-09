package wecom

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// stubBindings serves channels.OutboundBinding rows from a map; err (when set)
// makes every lookup fail, standing in for an unavailable binding store.
type stubBindings struct {
	rows map[string]channels.OutboundBinding
	err  error
}

func (s stubBindings) BindingByID(_ context.Context, id string) (channels.OutboundBinding, error) {
	if s.err != nil {
		return channels.OutboundBinding{}, s.err
	}
	b, ok := s.rows[id]
	if !ok {
		return channels.OutboundBinding{}, fmt.Errorf("unknown binding %s", id)
	}
	return b, nil
}

// identityFake fakes gettoken and message/send per corp identity: it counts
// gettoken calls per corpid, answers a distinct token per corpid, accepts only
// the registered corpsecret, and can fail the next send under one chosen
// identity with 40014.
type identityFake struct {
	mu         sync.Mutex
	secrets    map[string]string // corpid → accepted corpsecret
	tokenCalls map[string]int
	sendBodies []string
	failOnce   map[string]bool // corpid whose next send answers 40014
}

func (f *identityFake) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cgi-bin/gettoken", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		corpid := q.Get("corpid")
		f.mu.Lock()
		f.tokenCalls[corpid]++
		want, ok := f.secrets[corpid]
		f.mu.Unlock()
		if !ok || q.Get("corpsecret") != want {
			_, _ = w.Write([]byte(`{"errcode":40001,"errmsg":"invalid secret"}`))
			return
		}
		_, _ = fmt.Fprintf(w, `{"errcode":0,"access_token":"tok-%s","expires_in":7200}`, corpid)
	})
	mux.HandleFunc("/cgi-bin/message/send", func(w http.ResponseWriter, r *http.Request) {
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		corpid := strings.TrimPrefix(r.URL.Query().Get("access_token"), "tok-")
		f.mu.Lock()
		f.sendBodies = append(f.sendBodies, string(body))
		fail := f.failOnce[corpid]
		delete(f.failOnce, corpid)
		f.mu.Unlock()
		if fail {
			_, _ = w.Write([]byte(`{"errcode":40014,"errmsg":"invalid access_token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	return mux
}

func (f *identityFake) tokens(corpid string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tokenCalls[corpid]
}

func (f *identityFake) sends() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sendBodies)
}

func (f *identityFake) lastBody() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sendBodies[len(f.sendBodies)-1]
}

func newIdentityFake() *identityFake {
	return &identityFake{
		secrets: map[string]string{
			testCorpID: "corp-secret",
			"corpB":    "corp-secret-b",
			"corpC":    "corp-secret-c",
		},
		tokenCalls: map[string]int{},
		failOnce:   map[string]bool{},
	}
}

// bindingChannel builds a channel whose global identity is the env one and
// whose per-binding identities come from the stub provider.
func bindingChannel(t *testing.T, apiBase string, bindings channels.BindingProvider) *Channel {
	t.Helper()
	c, err := New(Config{
		CorpID: testCorpID, AgentID: 1000002,
		TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: apiBase,
		Bindings: bindings,
	}, mapResolver{
		"tok": testToken, "aes": testAESKey, "secret": "corp-secret",
		"secret-b": "corp-secret-b", "secret-c": "corp-secret-c",
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func bindingRow(id, config string) channels.OutboundBinding {
	return channels.OutboundBinding{ID: id, Config: json.RawMessage(config)}
}

// A binding carrying its own corp/agent/secret must send under that identity:
// gettoken with the binding's corpid and secret, message/send with the
// binding's agentid.
func TestSendUsesBindingOutboundIdentity(t *testing.T) {
	fake := newIdentityFake()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := bindingChannel(t, srv.URL, stubBindings{rows: map[string]channels.OutboundBinding{
		"b1": bindingRow("b1", `{"corp_id":"corpB","agent_id":2000003,"secret_ref":"secret-b"}`),
	}})

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好", BindingID: "b1",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.tokens("corpB"); n != 1 {
		t.Fatalf("binding identity must fetch its own token once, got %d", n)
	}
	if n := fake.tokens(testCorpID); n != 0 {
		t.Fatalf("global identity must stay untouched, got %d fetches", n)
	}
	body := fake.lastBody()
	if !strings.Contains(body, `"agentid":2000003`) {
		t.Fatalf("send must carry the binding's agentid: %s", body)
	}
	if strings.Contains(body, `"agentid":1000002`) {
		t.Fatalf("send leaked the global agentid: %s", body)
	}
}

// A config naming only some fields falls back to the env-global identity per
// field: here the binding's app answers under the global corp and secret.
func TestSendBindingPartialConfigFallsBackPerField(t *testing.T) {
	fake := newIdentityFake()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := bindingChannel(t, srv.URL, stubBindings{rows: map[string]channels.OutboundBinding{
		"b1": bindingRow("b1", `{"agent_id":2000003}`),
	}})

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好", BindingID: "b1",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.tokens(testCorpID); n != 1 {
		t.Fatalf("absent corp_id/secret_ref must fall back to the global identity, got %d fetches", n)
	}
	if body := fake.lastBody(); !strings.Contains(body, `"agentid":2000003`) {
		t.Fatalf("send must carry the binding's agentid: %s", body)
	}
}

// An empty config keeps the env-global single-identity behavior.
func TestSendEmptyBindingConfigUsesGlobalIdentity(t *testing.T) {
	fake := newIdentityFake()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := bindingChannel(t, srv.URL, stubBindings{rows: map[string]channels.OutboundBinding{
		"b1": bindingRow("b1", `{}`),
		"b2": {ID: "b2"}, // no config jsonb at all
	}})

	for _, bindingID := range []string{"b1", "b2"} {
		if err := c.Send(t.Context(), channels.OutboundMessage{
			Channel: "wecom", MsgID: bindingID, UserID: "zhangsan", Text: "你好", BindingID: bindingID,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n := fake.tokens(testCorpID); n != 1 {
		t.Fatalf("both bindings share the global identity: want 1 fetch, got %d", n)
	}
	if body := fake.lastBody(); !strings.Contains(body, `"agentid":1000002`) {
		t.Fatalf("send must carry the global agentid: %s", body)
	}
}

// Tokens are cached per identity: two bindings of different corps fetch one
// token each, and repeat sends hit the cache.
func TestSendCachesTokenPerIdentity(t *testing.T) {
	fake := newIdentityFake()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := bindingChannel(t, srv.URL, stubBindings{rows: map[string]channels.OutboundBinding{
		"b1": bindingRow("b1", `{"corp_id":"corpB","secret_ref":"secret-b"}`),
		"b2": bindingRow("b2", `{"corp_id":"corpC","secret_ref":"secret-c"}`),
	}})

	for i := 0; i < 2; i++ {
		for _, bindingID := range []string{"b1", "b2"} {
			if err := c.Send(t.Context(), channels.OutboundMessage{
				Channel: "wecom", MsgID: fmt.Sprintf("%s-%d", bindingID, i),
				UserID: "zhangsan", Text: "你好", BindingID: bindingID,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := fake.tokens("corpB"); n != 1 {
		t.Fatalf("corpB token must be fetched once and cached, got %d", n)
	}
	if n := fake.tokens("corpC"); n != 1 {
		t.Fatalf("corpC token must be fetched once and cached, got %d", n)
	}
	if n := fake.sends(); n != 4 {
		t.Fatalf("want 4 sends, got %d", n)
	}
}

// A 40014 under one identity invalidates only that identity's cache entry: the
// other identity's token survives and is not re-fetched.
func TestSendExpiredTokenInvalidatesOnlyItsIdentity(t *testing.T) {
	fake := newIdentityFake()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := bindingChannel(t, srv.URL, stubBindings{rows: map[string]channels.OutboundBinding{
		"b1": bindingRow("b1", `{"corp_id":"corpB","secret_ref":"secret-b"}`),
	}})
	ctx := t.Context()

	// Warm both identities.
	if err := c.Send(ctx, channels.OutboundMessage{
		Channel: "wecom", MsgID: "g1", UserID: "zhangsan", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(ctx, channels.OutboundMessage{
		Channel: "wecom", MsgID: "b1", UserID: "zhangsan", Text: "你好", BindingID: "b1",
	}); err != nil {
		t.Fatal(err)
	}

	// The binding's token is rejected once: refresh and retry under the same
	// identity.
	fake.mu.Lock()
	fake.failOnce["corpB"] = true
	fake.mu.Unlock()
	if err := c.Send(ctx, channels.OutboundMessage{
		Channel: "wecom", MsgID: "b2", UserID: "zhangsan", Text: "你好", BindingID: "b1",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.tokens("corpB"); n != 2 {
		t.Fatalf("rejected token must trigger exactly one refresh, got %d fetches", n)
	}

	// The global identity was not invalidated by the binding's 40014.
	if err := c.Send(ctx, channels.OutboundMessage{
		Channel: "wecom", MsgID: "g2", UserID: "zhangsan", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.tokens(testCorpID); n != 1 {
		t.Fatalf("global token must survive another identity's invalidation, got %d fetches", n)
	}
}

// An unresolvable binding fails the send instead of silently replying under
// the global identity — one tenant's message must never go out as another
// corp's bot.
func TestSendUnknownBindingFailsClosed(t *testing.T) {
	fake := newIdentityFake()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := bindingChannel(t, srv.URL, stubBindings{rows: map[string]channels.OutboundBinding{}})

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好", BindingID: "gone",
	})
	if err == nil || !strings.Contains(err.Error(), "resolve binding gone") {
		t.Fatalf("unknown binding must fail the send, got %v", err)
	}
	if n := fake.sends(); n != 0 {
		t.Fatalf("failed identity resolution must not reach the platform, got %d sends", n)
	}
}

// A binding store outage fails the send the same way.
func TestSendBindingLookupErrorFailsClosed(t *testing.T) {
	fake := newIdentityFake()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := bindingChannel(t, srv.URL, stubBindings{err: fmt.Errorf("pg down")})

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好", BindingID: "b1",
	})
	if err == nil || !strings.Contains(err.Error(), "pg down") {
		t.Fatalf("binding lookup error must surface, got %v", err)
	}
	if n := fake.sends(); n != 0 {
		t.Fatalf("failed identity resolution must not reach the platform, got %d sends", n)
	}
}

// A config jsonb the adapter cannot parse fails the send.
func TestSendMalformedBindingConfigFailsClosed(t *testing.T) {
	fake := newIdentityFake()
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := bindingChannel(t, srv.URL, stubBindings{rows: map[string]channels.OutboundBinding{
		"b1": bindingRow("b1", `{"corp_id":`),
	}})

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好", BindingID: "b1",
	})
	if err == nil || !strings.Contains(err.Error(), "config") {
		t.Fatalf("malformed binding config must fail the send, got %v", err)
	}
	if n := fake.sends(); n != 0 {
		t.Fatalf("failed identity resolution must not reach the platform, got %d sends", n)
	}
}

func TestValidateBindingConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  string
		wantErr string
	}{
		{"empty", "", ""},
		{"empty object", `{}`, ""},
		{"full identity", `{"corp_id":"corpB","agent_id":2000003,"secret_ref":"secret-b"}`, ""},
		{"corp only", `{"corp_id":"corpB"}`, ""},
		{"unknown field", `{"secret":"plaintext-credential"}`, "corp_id, agent_id and secret_ref"},
		{"wrong type", `{"agent_id":"not-a-number"}`, "corp_id, agent_id and secret_ref"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBindingConfig(json.RawMessage(tc.config))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("valid config rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error mentioning %q, got %v", tc.wantErr, err)
			}
		})
	}
}
