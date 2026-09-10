package feishu

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

type feishuSecretResolver func(context.Context, channel.ReplyDestination) (secrets.SecretValue, error)

func (f feishuSecretResolver) Resolve(ctx context.Context, destination channel.ReplyDestination) (secrets.SecretValue, error) {
	return f(ctx, destination)
}

type feishuHTTPFunc func(*http.Request) (*http.Response, error)

func (f feishuHTTPFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

func TestCredentialProviderCachesAndForceRefreshesToken(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	secretCalls, tokenCalls := 0, 0
	provider := &CredentialProvider{BaseURL: "https://unit.test", Now: func() time.Time { return now },
		Secrets: feishuSecretResolver(func(context.Context, channel.ReplyDestination) (secrets.SecretValue, error) {
			secretCalls++
			return secrets.SecretValue{Bytes: []byte(`{"app_id":"app","app_secret":"secret"}`), Version: 4}, nil
		}), Client: feishuHTTPFunc(func(request *http.Request) (*http.Response, error) {
			tokenCalls++
			if request.URL.Path != "/open-apis/auth/v3/tenant_access_token/internal" {
				t.Fatalf("path=%s", request.URL.Path)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"code":0,"tenant_access_token":"token","expire":7200}`))}, nil
		})}
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app", ConfigVersion: 7}
	for _, force := range []bool{false, false, true} {
		token, err := provider.ResolveFeishuMediaAccessToken(context.Background(), destination, force)
		if err != nil || token != "token" {
			t.Fatalf("token=%q err=%v", token, err)
		}
	}
	if secretCalls != 3 || tokenCalls != 2 {
		t.Fatalf("secret_calls=%d token_calls=%d", secretCalls, tokenCalls)
	}
}

func TestCredentialProviderRejectsUnknownOrMismatchedFields(t *testing.T) {
	for _, raw := range []string{`{"app_id":"other","app_secret":"secret"}`, `{"app_id":"app","app_secret":"secret","token":"unexpected"}`} {
		provider := &CredentialProvider{Secrets: feishuSecretResolver(func(context.Context, channel.ReplyDestination) (secrets.SecretValue, error) {
			return secrets.SecretValue{Bytes: []byte(raw), Version: 1}, nil
		})}
		_, err := provider.ResolveFeishuSendCredentials(context.Background(), channel.ReplyDestination{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app", ConfigVersion: 1})
		if err != runtime.ErrVersionMismatch {
			t.Fatalf("raw=%s err=%v", raw, err)
		}
	}
}
