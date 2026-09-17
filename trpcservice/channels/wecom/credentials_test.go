package wecom

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type wecomSecretResolver func(context.Context, channel.ReplyDestination) (secrets.SecretValue, error)

func (f wecomSecretResolver) Resolve(ctx context.Context, destination channel.ReplyDestination) (secrets.SecretValue, error) {
	return f(ctx, destination)
}

type wecomTokenHTTPFunc func(*http.Request) (*http.Response, error)

func (f wecomTokenHTTPFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

func TestTokenProviderCachesAndForceRefreshes(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	tokenCalls := 0
	provider := &TokenProvider{BaseURL: "https://unit.test", Now: func() time.Time { return now },
		Secrets: wecomSecretResolver(func(context.Context, channel.ReplyDestination) (secrets.SecretValue, error) {
			return secrets.SecretValue{Bytes: []byte(`{"corp_id":"corp","corp_secret":"secret","agent_id":218}`), Version: 2}, nil
		}), Client: wecomTokenHTTPFunc(func(request *http.Request) (*http.Response, error) {
			tokenCalls++
			if request.URL.Query().Get("corpid") != "corp" || request.URL.Query().Get("corpsecret") != "secret" {
				t.Fatalf("query=%v", request.URL.Query())
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"errcode":0,"access_token":"token","expires_in":7200}`))}, nil
		})}
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "wecom", ChannelBindingID: "binding", ExternalAccountID: "corp", ConfigVersion: 3}
	for _, force := range []bool{false, false, true} {
		token, agentID, err := provider.ResolveWeComAccessToken(context.Background(), destination, force)
		if err != nil || token != "token" || agentID != 218 {
			t.Fatalf("token=%q agent=%d err=%v", token, agentID, err)
		}
	}
	if tokenCalls != 2 {
		t.Fatalf("token_calls=%d", tokenCalls)
	}
}

func TestTokenProviderRejectsUnknownOrMismatchedFields(t *testing.T) {
	for _, raw := range []string{`{"corp_id":"other","corp_secret":"secret","agent_id":218}`, `{"corp_id":"corp","corp_secret":"secret","agent_id":218,"token":"unexpected"}`} {
		provider := &TokenProvider{Secrets: wecomSecretResolver(func(context.Context, channel.ReplyDestination) (secrets.SecretValue, error) {
			return secrets.SecretValue{Bytes: []byte(raw), Version: 1}, nil
		})}
		_, _, err := provider.ResolveWeComAccessToken(context.Background(), channel.ReplyDestination{TenantID: "tenant", Channel: "wecom", ChannelBindingID: "binding", ExternalAccountID: "corp", ConfigVersion: 1}, false)
		if err != runtime.ErrVersionMismatch {
			t.Fatalf("raw=%s err=%v", raw, err)
		}
	}
}
