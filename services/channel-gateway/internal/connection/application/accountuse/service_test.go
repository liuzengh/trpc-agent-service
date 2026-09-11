package accountuse

import (
	"context"
	refresh "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/catalogrefresh"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	"testing"
	"time"
)

type fixtureDirectory struct{ view refresh.View }

func (f *fixtureDirectory) Lookup(ctx context.Context, id string) (refresh.View, error) {
	return f.view, nil
}

type credentialFunc func(context.Context, c.Account, c.ResolveRequest) ([]Value, error)

func (f credentialFunc) ResolveValues(ctx context.Context, a c.Account, r c.ResolveRequest) ([]Value, error) {
	return f(ctx, a, r)
}

type ownerFunc func(context.Context, d.OwnerGrant) error

func (f ownerFunc) Check(ctx context.Context, g d.OwnerGrant) error { return f(ctx, g) }
func TestWeComResolverRequiresRealOwnerBeforeAndAfter(t *testing.T) {
	for _, mode := range []string{"success", "missing", "foreign", "lost_before", "lost_after", "rotation", "source_cancel"} {
		t.Run(mode, func(t *testing.T) {
			lifetime, stop := context.WithCancel(context.Background())
			defer stop()
			a := c.Account{ID: "account", TenantID: "tenant", Provider: "wecom", ProviderAccountID: "bot", Revision: 1, ConnectionRevision: 1, Enabled: true, Config: c.Config{BotID: "bot"}, Credentials: []c.Credential{{Purpose: "wecom.bot_secret", ID: "secret", Version: 1, Configured: true}}}
			directory := &fixtureDirectory{refresh.View{Account: a, Qualification: c.Qualification{Generation: 1}, Context: lifetime}}
			permit, e := c.NewPermit(lifetime, c.UseBinding{ScopeID: "pool", SourceEpoch: "00000000-0000-4000-8000-000000000001", InstanceID: "gw", InstanceEpoch: "00000000-0000-4000-8000-000000000002", TenantID: "tenant", Provider: "wecom", AccountID: "account", Kind: "wecom_connection", ConnectionRevision: 1, QualificationGeneration: 1, ClientGeneration: 1}, time.Now().Add(30*time.Second))
			if e != nil {
				t.Fatal(e)
			}
			defer permit.Revoke()
			reads, checks := 0, 0
			service := &Service{Directory: directory, Owner: ownerFunc(func(ctx context.Context, g d.OwnerGrant) error {
				checks++
				deadline, _ := ctx.Deadline()
				if time.Until(deadline) > 2*time.Second {
					t.Fatal("owner check exceeded separate lease budget")
				}
				if mode == "lost_before" || (mode == "lost_after" && checks == 2) {
					return c.ErrUnauthorized
				}
				return nil
			}), Credentials: credentialFunc(func(ctx context.Context, a c.Account, r c.ResolveRequest) ([]Value, error) {
				reads++
				if mode == "rotation" {
					directory.view.Account.ConnectionRevision++
				}
				if mode == "source_cancel" {
					stop()
				}
				return []Value{{Purpose: "wecom.bot_secret", ID: "secret", Version: 1, Value: "synthetic"}}, nil
			})}
			owner := &d.OwnerGrant{AccountID: "account", InstanceID: "gw", Revision: 1, Epoch: 1}
			if mode == "missing" {
				owner = nil
			}
			if mode == "foreign" {
				owner.AccountID = "another"
			}
			values, e := service.Resolve(context.Background(), permit, a, owner, nil)
			if mode == "success" {
				if e != nil || len(values) != 1 || reads != 1 || checks != 2 {
					t.Fatalf("success reads=%d checks=%d e=%v", reads, checks, e)
				}
			} else if e == nil || values != nil {
				t.Fatal("revoked use released credentials")
			}
			if (mode == "missing" || mode == "foreign" || mode == "lost_before") && reads != 0 {
				t.Fatal("unauthorized credential request")
			}
		})
	}
}
