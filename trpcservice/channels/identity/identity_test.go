package identity_test

import (
	"context"
	"errors"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretmemory "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
)

func TestMapperDerivesStableScopedIDs(t *testing.T) {
	mapper, binding := fixture(t, "tenant-a")
	dm := channel.ProviderEvent{Channel: "feishu", ExternalAccountID: "account", ExternalUserID: "user", ConversationType: "dm"}
	first, err := mapper.Map(context.Background(), binding, dm)
	if err != nil {
		t.Fatal(err)
	}
	second, err := mapper.Map(context.Background(), binding, dm)
	if err != nil || first != second || len(first.UserID) != 46 || len(first.SessionID) != 46 {
		t.Fatalf("first=%#v second=%#v err=%v", first, second, err)
	}
	group := dm
	group.ConversationType = "group"
	group.ExternalChatID = "chat"
	groupResult, err := mapper.Map(context.Background(), binding, group)
	if err != nil || groupResult.UserID != first.UserID || groupResult.SessionID == first.SessionID {
		t.Fatalf("group=%#v err=%v", groupResult, err)
	}
}

func TestMapperSeparatesTenantsAccountsAndCanonicalFields(t *testing.T) {
	mapperA, bindingA := fixture(t, "tenant-a")
	mapperB, bindingB := fixture(t, "tenant-b")
	bindingA.ExternalAccountID = "ab"
	bindingB.ExternalAccountID = "ab"
	event := channel.ProviderEvent{Channel: "feishu", ExternalAccountID: "ab", ExternalUserID: "c", ConversationType: "dm"}
	a, _ := mapperA.Map(context.Background(), bindingA, event)
	b, _ := mapperB.Map(context.Background(), bindingB, event)
	if a == b {
		t.Fatal("tenant-scoped keys produced equal IDs")
	}
	bindingA.ExternalAccountID = "a"
	event.ExternalAccountID = "a"
	event.ExternalUserID = "bc"
	c, err := mapperA.Map(context.Background(), bindingA, event)
	if err != nil || c.UserID == a.UserID {
		t.Fatalf("length-prefix collision result=%#v err=%v", c, err)
	}
}

func TestMapperFailsClosedForUntrustedOrIncompleteInput(t *testing.T) {
	mapper, binding := fixture(t, "tenant-a")
	event := channel.ProviderEvent{Channel: "feishu", ExternalAccountID: "account", ExternalUserID: "user", ConversationType: "group"}
	if _, err := mapper.Map(context.Background(), binding, event); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("missing group chat=%v", err)
	}
	event.ConversationType = "dm"
	event.ExternalAccountID = "forged"
	if _, err := mapper.Map(context.Background(), binding, event); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("account mismatch=%v", err)
	}
	binding.IdentitySecretRef.Version++
	event.ExternalAccountID = "account"
	if _, err := mapper.Map(context.Background(), binding, event); err == nil {
		t.Fatal("missing rotated key was accepted")
	}
}

func fixture(t *testing.T, tenantID string) (identity.Mapper, channel.VerifiedBinding) {
	t.Helper()
	provider := secretmemory.New()
	binding := channel.VerifiedBinding{
		TenantID: tenantID, AgentAppID: "app", ChannelBindingID: "binding", Channel: "feishu", ExternalAccountID: "account",
		TenantVersion: 1, BindingVersion: 1,
		IdentitySecretRef: secrets.SecretRef{Ref: "secret://identity/" + tenantID, Version: 1},
		SessionSecretRef:  secrets.SecretRef{Ref: "secret://session/" + tenantID, Version: 1},
	}
	provider.Put(secrets.Scope{TenantID: tenantID, Subject: tenantID, Purpose: secrets.PurposeTenantIdentity, ResourceID: tenantID, ResourceVersion: 1}, binding.IdentitySecretRef, []byte("identity-key-"+tenantID))
	provider.Put(secrets.Scope{TenantID: tenantID, Subject: tenantID, Purpose: secrets.PurposeTenantSession, ResourceID: tenantID, ResourceVersion: 1}, binding.SessionSecretRef, []byte("session-key-"+tenantID))
	return identity.Mapper{Secrets: provider}, binding
}
