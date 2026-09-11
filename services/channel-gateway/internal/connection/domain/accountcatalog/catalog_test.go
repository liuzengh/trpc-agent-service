package accountcatalog_test

import (
	"context"
	"errors"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	"strings"
	"testing"
	"time"
)

func account() c.Account {
	return c.Account{TenantID: "tnt_test", ID: "cha_test", Provider: "telegram", ProviderAccountID: "123", Revision: 1, ConnectionRevision: 1, Enabled: true, Config: c.Config{WebhookPath: "/v1/telegram/cha_test"}, Credentials: []c.Credential{{Purpose: "telegram.bot_token", ID: "ccr_token", Version: 1, Configured: true}, {Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1, Configured: true}}}
}
func snapshot() c.Snapshot {
	s := c.Snapshot{SchemaVersion: 1, ScopeID: "pool", SourceEpoch: "00000000-0000-4000-8000-000000000001", Revision: 1, Complete: true, Accounts: []c.Account{account()}}
	s.Digest, _ = s.ComputedDigest()
	return s
}
func TestSnapshotAndVersions(t *testing.T) {
	s := snapshot()
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*c.Snapshot){func(s *c.Snapshot) { s.Complete = false }, func(s *c.Snapshot) { s.Accounts[0].MinRouteGeneration = -1 }, func(s *c.Snapshot) { s.Accounts[0].Credentials[0].Configured = false }, func(s *c.Snapshot) { s.Accounts = append(s.Accounts, s.Accounts[0]) }, func(s *c.Snapshot) { s.Accounts[0].ProviderAccountID = "0123" }} {
		s = snapshot()
		mutate(&s)
		s.Digest, _ = s.ComputedDigest()
		if s.Validate() == nil {
			t.Fatal("invalid snapshot accepted")
		}
	}
	s = snapshot()
	s.Accounts[0].MinRouteGeneration = 1
	if !errors.Is(s.Validate(), c.ErrIntegrity) {
		t.Fatal("digest change accepted")
	}
	a := account()
	next := account()
	next.MinRouteGeneration = 4
	if c.CheckSuccessor(a, next, true) != nil {
		t.Fatal("floor requires reconnect")
	}
	next.Revision = 2
	if c.CheckSuccessor(a, next, true) != nil {
		t.Fatal("metadata requires reconnect")
	}
	next.Credentials[0].Version = 2
	if c.CheckSuccessor(a, next, true) == nil {
		t.Fatal("credential changed at same connection revision")
	}
	next.ConnectionRevision = 2
	if c.CheckSuccessor(a, next, true) != nil {
		t.Fatal("legitimate rotation refused")
	}
}
func TestOrdering(t *testing.T) {
	for _, x := range []struct {
		start, current, received int64
		known, digest            string
		want                     c.Classification
		bad                      bool
	}{{10, 11, 10, "a", "a", c.Superseded, false}, {11, 11, 10, "", "a", "", true}, {10, 11, 11, "b", "b", c.Same, false}, {10, 11, 12, "", "c", c.Advance, false}, {10, 11, 10, "a", "x", "", true}} {
		got, e := c.Classify(x.start, x.current, x.received, x.known, x.digest)
		if (e != nil) != x.bad || got != x.want {
			t.Fatalf("classification=%s error=%v", got, e)
		}
	}
}
func TestCredentialUseAndSecretValidation(t *testing.T) {
	a := account()
	epoch := int64(1)
	r := c.ResolveRequest{SchemaVersion: 1, ScopeID: "pool", SourceEpoch: snapshot().SourceEpoch, ConnectionRevision: 1, Consumer: c.Consumer{Kind: "telegram_registration", InstanceID: "gw", RegistrationEpoch: &epoch}, Uses: []c.Use{{Purpose: "telegram.bot_token", ID: "ccr_token", Version: 1}, {Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1}}}
	if r.Validate(a) != nil {
		t.Fatal("registration invalid")
	}
	r.Consumer.OwnerEpoch = &epoch
	if r.Validate(a) == nil {
		t.Fatal("fake wecom owner allowed")
	}
	r.Consumer.OwnerEpoch = nil
	r.Uses = r.Uses[:1]
	if r.Validate(a) == nil {
		t.Fatal("partial registration allowed")
	}
	for _, s := range []string{"", strings.Repeat("a", 257), "x\n", "中文", "x/y"} {
		if c.ValidWebhookSecret(s) {
			t.Fatal("invalid secret accepted")
		}
	}
	for _, s := range []string{"a", "_-", strings.Repeat("a", 256)} {
		if !c.ValidWebhookSecret(s) {
			t.Fatal("valid secret rejected")
		}
	}
}
func TestPermitRevocation(t *testing.T) {
	b := c.UseBinding{ScopeID: "pool", SourceEpoch: snapshot().SourceEpoch, InstanceID: "gw", InstanceEpoch: snapshot().SourceEpoch, TenantID: "tnt_test", Provider: "telegram", AccountID: "cha_test", Kind: "telegram_delivery", ConnectionRevision: 1, QualificationGeneration: 1, ClientGeneration: 1}
	p, e := c.NewPermit(context.Background(), b, time.Now().Add(time.Second))
	if e != nil || p.Check() != nil {
		t.Fatal(e)
	}
	p.Revoke()
	if p.Check() == nil {
		t.Fatal("revoked permit valid")
	}
}
