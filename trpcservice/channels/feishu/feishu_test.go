package feishu

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// The values every test in this package builds a binding from. They are fixed
// so that a test asserting on a derived id is asserting on one input set.
const (
	testTenantID   = "acme"
	testAgentAppID = "support-bot"
	testBindingID  = "feishu-main"
	testAppID      = "cli_a1b2c3d4"
	testSecretRef  = "env:TRPC_SERVICE_FEISHU_APP_SECRET"
	testSecret     = "secret-value-not-a-real-credential"
	testTenantKey  = "tk_2f1a9c"
	testOpenID     = "ou_c0ffee"
	testChatID     = "oc_1234"
	testMessageID  = "om_abcdef"
)

func testBinding() Binding {
	return Binding{
		TenantID:   testTenantID,
		AgentAppID: testAgentAppID,
		BindingID:  testBindingID,
		AppID:      testAppID,
		SecretRef:  testSecretRef,
	}
}

// grantingAuthorizer entitles exactly one (tenant, reference) pair and records
// every question it was asked, in order.
type grantingAuthorizer struct {
	tenantID string
	ref      string

	mu    sync.Mutex
	asked []string
}

func (a *grantingAuthorizer) AuthorizeSecretRef(tenantID, ref string) error {
	a.mu.Lock()
	a.asked = append(a.asked, tenantID+"|"+ref)
	a.mu.Unlock()
	if tenantID != a.tenantID || ref != a.ref {
		return security.ErrNotEntitled
	}
	return nil
}

func (a *grantingAuthorizer) questions() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.asked...)
}

func testAuthorizer() *grantingAuthorizer {
	return &grantingAuthorizer{tenantID: testTenantID, ref: testSecretRef}
}

// The routes the fake platform serves. The two connection ones are only used by
// the Serve tests, which install their own handlers for them.
const (
	replyPathPrefix = "/open-apis/im/v1/messages/"
	bootstrapPath   = "/callback/ws/endpoint"
	socketPath      = "/ws"
)

// platform is a stand-in for the open platform: the startup calls, the reply call and
// the connection bootstrap, each answerable per test, with every request recorded.
type platform struct {
	server *httptest.Server

	mu       sync.Mutex
	requests []recordedRequest
	answers  map[string]http.HandlerFunc
}

type recordedRequest struct {
	method string
	path   string
	auth   string
	body   []byte
}

func newPlatform(t *testing.T) *platform {
	t.Helper()
	p := &platform{answers: map[string]http.HandlerFunc{}}
	mux := http.NewServeMux()
	mux.HandleFunc(tokenPath, p.route(tokenPath, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"code": 0, "tenant_access_token": "t-live"})
	}))
	mux.HandleFunc(botInfoPath, p.route(botInfoPath, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"code": 0, "bot": map[string]any{"open_id": "ou_bot"},
		})
	}))
	mux.HandleFunc(tenantQueryPath, p.route(tenantQueryPath, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"code": 0,
			"data": map[string]any{"tenant": map[string]any{"tenant_key": testTenantKey}},
		})
	}))
	mux.HandleFunc(replyPathPrefix, p.route(replyPathPrefix, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"code": 0,
			"data": map[string]any{"message_id": "om_reply"},
		})
	}))

	unserved := func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no connection in this test", http.StatusNotFound)
	}
	mux.HandleFunc(bootstrapPath, p.route(bootstrapPath, unserved))
	mux.HandleFunc(socketPath, p.route(socketPath, unserved))
	p.server = httptest.NewServer(mux)
	t.Cleanup(p.server.Close)
	return p
}

func (p *platform) answer(path string, handler http.HandlerFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.answers[path] = handler
}

func (p *platform) route(path string, fallback http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r)
		p.mu.Lock()
		p.requests = append(p.requests, recordedRequest{
			method: r.Method,
			path:   r.URL.Path,
			auth:   r.Header.Get("Authorization"),
			body:   body,
		})
		handler := p.answers[path]
		p.mu.Unlock()
		if handler == nil {
			handler = fallback
		}
		handler(w, r)
	}
}

func (p *platform) seen() []recordedRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]recordedRequest(nil), p.requests...)
}

func (p *platform) seenPath(prefix string) []recordedRequest {
	var matched []recordedRequest
	for _, request := range p.seen() {
		if strings.HasPrefix(request.path, prefix) {
			matched = append(matched, request)
		}
	}
	return matched
}

func readAll(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, maxResponseBytes))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func testConfig(p *platform, authorizer security.SecretRefAuthorizer) Config {
	return Config{
		Binding:    testBinding(),
		Authorizer: authorizer,
		Getenv: func(name string) string {
			if name == "TRPC_SERVICE_FEISHU_APP_SECRET" {
				return testSecret
			}
			return ""
		},
		baseURL: p.server.URL,
	}
}

func newTestClient(t *testing.T, p *platform) *Client {
	t.Helper()
	client, err := New(context.Background(), testConfig(p, testAuthorizer()))
	require.NoError(t, err)
	return client
}

func TestBindingValidateRefusesWhatCouldNotOwnADurableRow(t *testing.T) {

	cases := []struct {
		name   string
		mutate func(*Binding)
	}{
		{"missing tenant", func(b *Binding) { b.TenantID = "" }},
		{"tenant is not a resource id", func(b *Binding) { b.TenantID = "Acme Corp" }},
		{"missing agent app", func(b *Binding) { b.AgentAppID = "" }},
		{"missing binding", func(b *Binding) { b.BindingID = "" }},
		{"missing external app", func(b *Binding) { b.AppID = "" }},
		{"external app has a path separator", func(b *Binding) { b.AppID = "cli_a/../b" }},
		{"external app is oversized", func(b *Binding) { b.AppID = strings.Repeat("a", 257) }},
		{"missing secret reference", func(b *Binding) { b.SecretRef = "" }},
		{"secret reference is a pasted secret", func(b *Binding) { b.SecretRef = "s3cr3t" }},
		{"secret reference uses another scheme", func(b *Binding) { b.SecretRef = "file:/etc/secret" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			binding := testBinding()
			test.mutate(&binding)
			err := binding.Validate()
			require.ErrorIs(t, err, ErrConfig)
		})
	}
	require.NoError(t, testBinding().Validate())
}

func TestNewAsksTheAuthorizerBeforeItReadsTheSecret(t *testing.T) {
	p := newPlatform(t)
	authorizer := &grantingAuthorizer{tenantID: testTenantID, ref: "env:SOMETHING_ELSE"}
	var read []string
	cfg := testConfig(p, authorizer)
	cfg.Getenv = func(name string) string {
		read = append(read, name)
		return testSecret
	}
	client, err := New(context.Background(), cfg)
	require.ErrorIs(t, err, security.ErrNotEntitled)
	require.Nil(t, client)
	require.Empty(t, read, "the environment was read before entitlement")
	require.Empty(t, p.seen(), "the platform was called before entitlement")
	require.Equal(t, []string{testTenantID + "|" + testSecretRef}, authorizer.questions())
}

func TestNewRefusesAConfigurationItCannotRunOn(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		want   error
	}{
		{
			name:   "no authorizer",
			mutate: func(c *Config) { c.Authorizer = nil },
			want:   ErrConfig,
		},
		{
			name:   "invalid binding",
			mutate: func(c *Config) { c.Binding.AppID = "" },
			want:   ErrConfig,
		},
		{
			name:   "entitled variable is unset",
			mutate: func(c *Config) { c.Getenv = func(string) string { return "" } },
			want:   ErrConfig,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			p := newPlatform(t)
			cfg := testConfig(p, testAuthorizer())
			test.mutate(&cfg)
			_, err := New(context.Background(), cfg)
			require.ErrorIs(t, err, test.want)
		})
	}
}

func TestNewRefusesAnAppThePlatformDoesNotConfirm(t *testing.T) {

	cases := []struct {
		name  string
		setup func(*platform)
	}{
		{
			name: "the credential is refused",
			setup: func(p *platform) {
				p.answer(tokenPath, func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{"code": 10003, "msg": "app not found"})
				})
			},
		},
		{
			name: "the credential exchange returns no token",
			setup: func(p *platform) {
				p.answer(tokenPath, func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{"code": 0})
				})
			},
		},
		{
			name: "the answer states no code at all",
			setup: func(p *platform) {
				p.answer(tokenPath, func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{"tenant_access_token": "t-live"})
				})
			},
		},
		{
			name: "the platform answers with a server error",
			setup: func(p *platform) {
				p.answer(tokenPath, func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusBadGateway)
				})
			},
		},
		{
			name: "the answer is not JSON",
			setup: func(p *platform) {
				p.answer(tokenPath, func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("<html>maintenance</html>"))
				})
			},
		},
		{
			name: "the app has no published bot",
			setup: func(p *platform) {
				p.answer(botInfoPath, func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{"code": 0, "bot": map[string]any{}})
				})
			},
		},
		{
			name: "the app is installed in no enterprise",
			setup: func(p *platform) {
				p.answer(tenantQueryPath, func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{
						"code": 0,
						"data": map[string]any{"tenant": map[string]any{"tenant_key": ""}},
					})
				})
			},
		},
		{
			name: "the enterprise key is not a plain identifier",
			setup: func(p *platform) {
				p.answer(tenantQueryPath, func(w http.ResponseWriter, _ *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{
						"code": 0,
						"data": map[string]any{"tenant": map[string]any{"tenant_key": "tk/../other"}},
					})
				})
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			p := newPlatform(t)
			test.setup(p)
			_, err := New(context.Background(), testConfig(p, testAuthorizer()))
			require.ErrorIs(t, err, ErrPlatform)
			for _, quoted := range []string{"app not found", "maintenance", "<html>"} {
				require.NotContains(t, err.Error(), quoted, "the error quoted the platform")
			}
		})
	}
}

func TestNewConfirmsTheAppAndKeepsTheEnterpriseItReported(t *testing.T) {
	p := newPlatform(t)
	client := newTestClient(t, p)
	require.Equal(t, testTenantKey, client.tenantKey)
	calls := p.seen()
	require.Len(t, calls, 3)
	require.Equal(t, http.MethodPost, calls[0].method)
	require.Equal(t, tokenPath, calls[0].path)
	require.Contains(t, string(calls[0].body), testSecret,
		"the credential exchange did not carry the app secret")
	for _, call := range calls[1:] {
		require.Equal(t, "Bearer t-live", call.auth, call.path)
		require.NotContains(t, string(call.body), testSecret, call.path)
	}
}

func TestDerivedIdentitiesAreStableScopedAndStorable(t *testing.T) {
	binding := testBinding()
	principal := principalID(binding, testTenantKey, testOpenID)
	session := directSessionID(binding, testTenantKey, principal, testChatID)
	require.NoError(t, tenant.ValidateResourceID("principal id", principal))
	require.NoError(t, tenant.ValidateResourceID("session id", session))
	require.Equal(t, principal, principalID(binding, testTenantKey, testOpenID),
		"principal id is not deterministic")
	require.Equal(t, session, directSessionID(binding, testTenantKey, principal, testChatID),
		"session id is not deterministic")

	scopes := []struct {
		name   string
		mutate func(*Binding, *string, *string)
	}{
		{"another tenant", func(b *Binding, _, _ *string) { b.TenantID = "other" }},
		{"another agent app", func(b *Binding, _, _ *string) { b.AgentAppID = "other" }},
		{"another binding", func(b *Binding, _, _ *string) { b.BindingID = "other" }},
		{"another external app", func(b *Binding, _, _ *string) { b.AppID = "cli_other" }},
		{"another enterprise", func(_ *Binding, key, _ *string) { *key = "tk_other" }},
		{"another user", func(_ *Binding, _, user *string) { *user = "ou_other" }},
		{"a boundary shifted between fields", func(b *Binding, _, user *string) {
			b.AppID = testAppID[:len(testAppID)-1]
			*user = testAppID[len(testAppID)-1:] + testOpenID
		}},
	}
	for _, test := range scopes {
		t.Run(test.name, func(t *testing.T) {
			other, key, user := testBinding(), testTenantKey, testOpenID
			test.mutate(&other, &key, &user)
			require.NotEqual(t, principal, principalID(other, key, user),
				"a different scope produced the same principal id")
		})
	}
	require.NotEqual(t, session, directSessionID(binding, testTenantKey, principal, "oc_other"),
		"two chats produced the same session id")
}

func TestReplyUUIDIsDerivedFromTheMessageAndFitsThePlatformField(t *testing.T) {
	binding := testBinding()
	uuid := replyUUID(binding, testTenantKey, testMessageID)
	require.Len(t, uuid, 32, "the platform allows 50")
	require.Equal(t, uuid, replyUUID(binding, testTenantKey, testMessageID),
		"a redelivery would double-post")
	require.NotEqual(t, uuid, replyUUID(binding, testTenantKey, "om_other"),
		"one answer would be swallowed")
}

func TestTargetRoundTripsOnlyInsideItsOwnScope(t *testing.T) {
	binding := testBinding()
	target, err := encodeTarget(binding, testTenantKey, testMessageID)
	require.NoError(t, err)
	require.Equal(t, channels.ChannelFeishu, target.Channel)
	require.EqualValues(t, targetVersion, target.Version)
	require.NoError(t, target.Validate(), "the encoded target is not storable")
	got, err := decodeTarget(binding, testTenantKey, target)
	require.NoError(t, err)
	require.Equal(t, testMessageID, got)

	cases := []struct {
		name    string
		binding Binding
		key     string
		mutate  func(*channels.DeliveryTarget)
	}{
		{"another channel", binding, testTenantKey, func(target *channels.DeliveryTarget) {
			target.Channel = channels.ChannelWeCom
		}},
		{"another version", binding, testTenantKey, func(target *channels.DeliveryTarget) {
			target.Version = targetVersion + 1
		}},
		{"an unreadable payload", binding, testTenantKey, func(target *channels.DeliveryTarget) {
			target.Payload = []byte(`{"tenant_id":`)
		}},
		{"a payload for another tenant", func() Binding {
			other := binding
			other.TenantID = "other"
			return other
		}(), testTenantKey, nil},
		{"a payload for another binding", func() Binding {
			other := binding
			other.BindingID = "other"
			return other
		}(), testTenantKey, nil},

		{"a payload for another agent app", func() Binding {
			other := binding
			other.AgentAppID = "other"
			return other
		}(), testTenantKey, nil},
		{"a payload for another external app", func() Binding {
			other := binding
			other.AppID = "cli_other"
			return other
		}(), testTenantKey, nil},
		{"a payload from another enterprise", binding, "tk_other", nil},
		{"an unconfirmed enterprise", binding, "", nil},
		{"a message id that is a path", binding, testTenantKey, func(target *channels.DeliveryTarget) {
			target.Payload = []byte(`{"tenant_id":"acme","agent_app_id":"support-bot",` +
				`"binding_id":"feishu-main","app_id":"cli_a1b2c3d4",` +
				`"tenant_key":"tk_2f1a9c","message_id":"../../chats/oc_victim"}`)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			stored, err := encodeTarget(binding, testTenantKey, testMessageID)
			require.NoError(t, err)
			if test.mutate != nil {
				test.mutate(&stored)
			}
			_, err = decodeTarget(test.binding, test.key, stored)
			require.ErrorIs(t, err, ErrTargetInvalid)
		})
	}
}
