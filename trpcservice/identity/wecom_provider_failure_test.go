package identity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
)

func providerForWeComServer(t *testing.T, server *httptest.Server) *WeComProvider {
	t.Helper()
	provider, err := NewWeComProvider(WeComConfig{
		ProviderID: "wecom-main", CorpID: "corp", AgentID: 1, Secret: "secret", RedirectURI: "https://console.example/callback",
		AuthBaseURL: server.URL, APIBaseURL: server.URL, HTTPClient: server.Client(), TokenCache: credential.NewMemoryTokenCache(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func TestWeComExchangeRejectsProviderResponseFailures(t *testing.T) {
	tests := []struct {
		name, want string
		reply      func(http.ResponseWriter)
	}{
		{name: "status", reply: func(w http.ResponseWriter) { http.Error(w, "upstream", http.StatusBadGateway) }, want: "getuserinfo HTTP 502"},
		{name: "JSON", reply: func(w http.ResponseWriter) { _, _ = w.Write([]byte("{")) }, want: "decode getuserinfo"},
		{name: "empty user", reply: func(w http.ResponseWriter) { _ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0}) }, want: "empty userid"},
		{name: "rejected", reply: func(w http.ResponseWriter) { writeWeComJSON(w, 40029, "invalid code") }, want: "40029"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/cgi-bin/gettoken" {
					_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "access_token": "token", "expires_in": 7200})
					return
				}
				test.reply(w)
			}))
			defer server.Close()
			_, err := providerForWeComServer(t, server).Exchange(context.Background(), AuthExchange{Code: "code"})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestWeComExchangeRefreshesExpiredTokenOnce(t *testing.T) {
	var tokenCalls, userCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			call := tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "access_token": fmt.Sprintf("token-%d", call), "expires_in": 7200})
		case "/cgi-bin/auth/getuserinfo":
			userCalls.Add(1)
			if r.URL.Query().Get("access_token") == "token-1" {
				writeWeComJSON(w, 42001, "expired")
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "userid": "user-1"})
		}
	}))
	defer server.Close()
	got, err := providerForWeComServer(t, server).Exchange(context.Background(), AuthExchange{Code: "code"})
	if err != nil {
		t.Fatal(err)
	}
	if got.SubjectID != "user-1" || tokenCalls.Load() != 2 || userCalls.Load() != 2 {
		t.Fatalf("got %+v, token=%d user=%d; want refreshed success", got, tokenCalls.Load(), userCalls.Load())
	}
}

func TestWeComExchangeReturnsRefreshFailure(t *testing.T) {
	var tokenCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			if tokenCalls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "access_token": "stale", "expires_in": 7200})
				return
			}
			writeWeComJSON(w, 40001, "invalid secret")
		case "/cgi-bin/auth/getuserinfo":
			writeWeComJSON(w, 40014, "invalid token")
		}
	}))
	defer server.Close()
	_, err := providerForWeComServer(t, server).Exchange(context.Background(), AuthExchange{Code: "code"})
	if err == nil || !strings.Contains(err.Error(), "gettoken error 40001") || tokenCalls.Load() != 2 {
		t.Fatalf("error = %v, token calls = %d; want one failed refresh", err, tokenCalls.Load())
	}
}

func TestWeComExchangeHonorsCanceledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "access_token": "token", "expires_in": 7200})
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := providerForWeComServer(t, server).Exchange(ctx, AuthExchange{Code: "code"})
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("error = %v, want context canceled", err)
	}
}
