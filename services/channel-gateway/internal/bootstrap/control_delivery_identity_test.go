package bootstrap

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	use "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/accountuse"
	refresh "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/catalogrefresh"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type identityDirectory struct{ view refresh.View }

func (s identityDirectory) Lookup(context.Context, string) (refresh.View, error) { return s.view, nil }

type identityCredentials struct{}

func (identityCredentials) ResolveValues(context.Context, c.Account, c.ResolveRequest) ([]use.Value, error) {
	return []use.Value{{Purpose: "telegram.bot_token", ID: "token", Version: 1, Value: "123:synthetic-fixture-token"}}, nil
}

// Exercise the actual bootstrap Reserve call site: a getMe transport failure
// happens before sendMessage, so it must consume the retryable preparation
// budget rather than permanently terminate an unsent Final.
func TestControlTelegramIdentityPreparation(t *testing.T) {
	for _, tc := range []struct {
		name, body         string
		disconnect, revoke bool
		want               error
	}{
		{name: "transport_eof", disconnect: true, want: d.ErrUnavailable},
		{name: "server_error", body: `{"ok":false,"error_code":500,"description":"temporary"}`, want: d.ErrUnavailable},
		{name: "rate_limit", body: `{"ok":false,"error_code":429,"description":"temporary","parameters":{"retry_after":1}}`, want: d.ErrUnavailable},
		{name: "unauthorized", body: `{"ok":false,"error_code":401,"description":"denied"}`, want: d.ErrUnauthorized},
		{name: "forbidden", body: `{"ok":false,"error_code":403,"description":"denied"}`, want: d.ErrUnauthorized},
		{name: "wrong_identity", body: `{"ok":true,"result":{"id":999,"is_bot":true,"first_name":"test"}}`, want: d.ErrUnauthorized},
		{name: "revoked_permit", body: `{"ok":true,"result":{"id":123,"is_bot":true,"first_name":"test"}}`, revoke: true, want: d.ErrUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := controlSnapshot().Accounts[0]
			p, err := c.NewPermit(context.Background(), c.UseBinding{ScopeID: "pool", SourceEpoch: controlEpoch, InstanceID: "gw", InstanceEpoch: controlBoot, TenantID: a.TenantID, Provider: a.Provider, AccountID: a.ID, Kind: "telegram_delivery", ConnectionRevision: 1, QualificationGeneration: 1, ClientGeneration: 1}, time.Now().Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			defer p.Revoke()
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/bot123:synthetic-fixture-token/getMe" {
					t.Errorf("unexpected API call: %s", r.URL.Path)
				}
				if tc.revoke {
					p.Revoke()
				}
				if tc.disconnect {
					conn, _, e := w.(http.Hijacker).Hijack()
					if e != nil {
						t.Error(e)
						return
					}
					_ = conn.Close()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			service := &use.Service{Directory: identityDirectory{refresh.View{Account: a, Context: context.Background(), Qualification: c.Qualification{Generation: 1}}}, Credentials: identityCredentials{}}
			req := app.SendRequest{Claim: d.Claim{UseBinding: bindingDigest(p), Target: d.Target{TenantID: a.TenantID, Provider: a.Provider, AccountID: a.ID}}}
			sender, err := (controlSenders{telegramAPIURL: server.URL, use: service}).Reserve(context.WithValue(context.Background(), permitKey{}, p), req)
			if sender != nil {
				sender.Release()
				t.Fatal("unexpected sender")
			}
			if !errors.Is(err, tc.want) || calls.Load() != 1 {
				t.Fatalf("Reserve err=%v want=%v getMe_calls=%d", err, tc.want, calls.Load())
			}
		})
	}
}
