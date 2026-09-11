package identity

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
)

// fakeWeCom simulates the WeCom token and getuserinfo endpoints.
type fakeWeCom struct {
	server         *httptest.Server
	tokenCalls     atomic.Int64
	userCalls      atomic.Int64
	corpID         string
	corpSecret     string
	userByCode     map[string]string
	rejectGetToken bool
}

func newFakeWeCom(t *testing.T, corpID, corpSecret string) *fakeWeCom {
	t.Helper()
	fake := &fakeWeCom{
		corpID:     corpID,
		corpSecret: corpSecret,
		userByCode: map[string]string{"good-code": "zhangsan"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/cgi-bin/gettoken", func(writer http.ResponseWriter, request *http.Request) {
		fake.tokenCalls.Add(1)
		if fake.rejectGetToken {
			writeWeComJSON(writer, 40001, "invalid corpsecret")
			return
		}
		query := request.URL.Query()
		if query.Get("corpid") != corpID || query.Get("corpsecret") != corpSecret {
			writeWeComJSON(writer, 40001, "invalid credential")
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"errcode": 0, "errmsg": "ok", "access_token": "tok-123", "expires_in": 7200})
	})
	mux.HandleFunc("/cgi-bin/auth/getuserinfo", func(writer http.ResponseWriter, request *http.Request) {
		fake.userCalls.Add(1)
		query := request.URL.Query()
		if query.Get("access_token") != "tok-123" {
			writeWeComJSON(writer, 40014, "invalid access_token")
			return
		}
		code := query.Get("code")
		userID, ok := fake.userByCode[code]
		if !ok {
			writeWeComJSON(writer, 40029, "invalid code")
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"errcode": 0, "errmsg": "ok", "userid": userID})
	})
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func writeWeComJSON(writer http.ResponseWriter, errcode int, errmsg string) {
	_ = json.NewEncoder(writer).Encode(map[string]any{"errcode": errcode, "errmsg": errmsg})
}

func testWeComProvider(fake *fakeWeCom) *WeComProvider {
	provider, err := NewWeComProvider(WeComConfig{
		ProviderID:  "wecom-main",
		DisplayName: "企业微信",
		CorpID:      fake.corpID,
		AgentID:     1000001,
		Secret:      fake.corpSecret,
		AuthBaseURL: fake.server.URL,
		APIBaseURL:  fake.server.URL,
		RedirectURI: "https://console.example.com/callback",
		HTTPClient:  fake.server.Client(),
		TokenCache:  credential.NewMemoryTokenCache(),
	})
	if err != nil {
		panic(err) // config above is always valid in tests
	}
	return provider
}

func TestWeComBeginBuildsAuthorizeURL(t *testing.T) {
	fake := newFakeWeCom(t, "wx-corp", "secret-1")
	provider := testWeComProvider(fake)

	authURL, err := provider.Begin(AuthRequest{State: "state-abc"})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("Begin() URL = %q, parse error = %v", authURL, err)
	}
	query := parsed.Query()
	if query.Get("appid") != "wx-corp" {
		t.Fatalf("appid = %q, want wx-corp", query.Get("appid"))
	}
	if query.Get("agentid") != "1000001" {
		t.Fatalf("agentid = %q, want 1000001", query.Get("agentid"))
	}
	if query.Get("state") != "state-abc" {
		t.Fatalf("state = %q, want state-abc", query.Get("state"))
	}
	if query.Get("login_type") != "CorpApp" || !strings.Contains(parsed.Path, "/wwlogin/sso/login") {
		t.Fatalf("authorize URL = %q, want CorpApp QR login", authURL)
	}
	if !strings.Contains(query.Get("redirect_uri"), "console.example.com") {
		t.Fatalf("redirect_uri = %q", query.Get("redirect_uri"))
	}
}

func TestWeComExchangeSuccessAndTokenCaching(t *testing.T) {
	fake := newFakeWeCom(t, "wx-corp", "secret-1")
	provider := testWeComProvider(fake)

	identity, err := provider.Exchange(context.Background(), AuthExchange{Code: "good-code"})
	if err != nil {
		t.Fatalf("Exchange() error = %v", err)
	}
	if identity.EnterpriseID != "wx-corp" || identity.SubjectID != "zhangsan" || identity.DisplayName != "zhangsan" || identity.ProviderID != "wecom-main" {
		t.Fatalf("Exchange() = %+v, want wecom-main/wx-corp/zhangsan", identity)
	}
	if fake.tokenCalls.Load() != 1 {
		t.Fatalf("gettoken calls = %d, want 1", fake.tokenCalls.Load())
	}

	// Second exchange must reuse the cached access_token.
	if _, err := provider.Exchange(context.Background(), AuthExchange{Code: "good-code"}); err != nil {
		t.Fatalf("second Exchange() error = %v", err)
	}
	if fake.tokenCalls.Load() != 1 {
		t.Fatalf("gettoken calls after cache = %d, want still 1", fake.tokenCalls.Load())
	}
	if fake.userCalls.Load() != 2 {
		t.Fatalf("getuserinfo calls = %d, want 2", fake.userCalls.Load())
	}
}

func TestWeComExchangeInvalidCode(t *testing.T) {
	fake := newFakeWeCom(t, "wx-corp", "secret-1")
	provider := testWeComProvider(fake)
	if _, err := provider.Exchange(context.Background(), AuthExchange{Code: "bad-code"}); err == nil {
		t.Fatal("Exchange(bad-code) error = nil, want failure")
	}
}

func TestWeComExchangeEmptyCode(t *testing.T) {
	fake := newFakeWeCom(t, "wx-corp", "secret-1")
	provider := testWeComProvider(fake)
	if _, err := provider.Exchange(context.Background(), AuthExchange{}); err == nil {
		t.Fatal("Exchange(empty code) error = nil, want failure")
	}
}

func TestWeComGetTokenFailure(t *testing.T) {
	fake := newFakeWeCom(t, "wx-corp", "secret-1")
	fake.rejectGetToken = true
	provider := testWeComProvider(fake)
	if _, err := provider.Exchange(context.Background(), AuthExchange{Code: "good-code"}); err == nil {
		t.Fatal("Exchange() error = nil when gettoken rejects, want failure")
	}
}

func TestWeComProviderRequiresConfig(t *testing.T) {
	if _, err := NewWeComProvider(WeComConfig{}); err == nil {
		t.Fatal("NewWeComProvider({}) error = nil, want failure for missing config")
	}
}

// Token-cache TTL and concurrency are covered by the shared credential package.
// This test verifies concurrent Exchange calls remain race-free.
func TestWeComExchangeConcurrent(t *testing.T) {
	fake := newFakeWeCom(t, "wx-corp", "secret-1")
	provider := testWeComProvider(fake)

	var wait sync.WaitGroup
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := provider.Exchange(context.Background(), AuthExchange{Code: "good-code"}); err != nil {
				t.Errorf("concurrent Exchange() error = %v", err)
			}
		}()
	}
	wait.Wait()
}
