package telegrampreflight

import (
	"context"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
	"net/http"
	"strings"
	"testing"
)

func TestPreflightUsesAccountEndpoint(t *testing.T) {
	for _, profile := range []string{"", "test"} {
		calls := 0
		c := &Client{transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			want := "https://api.telegram.org"
			if profile == "test" {
				want = "http://channel-lab:8080"
			}
			if r.URL.Scheme+"://"+r.URL.Host != want {
				t.Fatalf("endpoint %s", r.URL)
			}
			calls++
			if strings.HasSuffix(r.URL.Path, "getMe") {
				return response(identityJSON), nil
			}
			return response(`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0}}`), nil
		})}
		got, e := c.Inspect(context.Background(), app.ProbeRequest{EndpointProfile: profile, Token: app.NewSecret(fixtureToken), ExpectedIdentity: "123"})
		if e != nil || got.IdentityCode != "BOT_IDENTITY_MATCH" || calls != 2 {
			t.Fatal(got, e, calls)
		}
	}
}
