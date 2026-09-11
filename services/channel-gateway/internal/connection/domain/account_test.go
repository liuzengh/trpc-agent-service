package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

func TestAccountValidatesNonSecretIdentityAndRevision(t *testing.T) {
	a := domain.Account{ID: "account-1", BotID: "bot-1", CredentialRef: "secret://wecom/account-1", Revision: 1, Enabled: true}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*domain.Account){
		func(a *domain.Account) { a.ID = "" },
		func(a *domain.Account) { a.ID = strings.Repeat("a", 129) },
		func(a *domain.Account) { a.BotID = "bot\n1" },
		func(a *domain.Account) { a.BotID = "\xff" },
		func(a *domain.Account) { a.CredentialRef = "" },
		func(a *domain.Account) { a.CredentialRef = "a\nb" },
		func(a *domain.Account) { a.CredentialRef = strings.Repeat("a", 2049) },
		func(a *domain.Account) { a.Revision = 0 },
	} {
		candidate := a
		change(&candidate)
		if err := candidate.Validate(); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("accepted invalid account: %#v %v", candidate, err)
		}
	}
	a.Enabled = false
	a.CredentialRef = strings.Repeat("a", 2048)
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseInputsBoundTTLButDoNotTrustCallerClocks(t *testing.T) {
	for _, ttl := range []time.Duration{100 * time.Millisecond, time.Second, 10 * time.Minute} {
		if err := domain.ValidateLeaseTTL(ttl); err != nil {
			t.Fatal(err)
		}
	}
	for _, ttl := range []time.Duration{0, -time.Second, 100*time.Millisecond - time.Nanosecond, 10*time.Minute + time.Nanosecond} {
		if err := domain.ValidateLeaseTTL(ttl); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("ttl=%v: %v", ttl, err)
		}
	}
	g := domain.OwnerGrant{AccountID: "a", BotID: "b", InstanceID: "instance-1", Epoch: 1, Revision: 1}
	if err := g.Validate(); err != nil {
		t.Fatal(err)
	}
	g.Epoch = 0
	if err := g.Validate(); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
}
