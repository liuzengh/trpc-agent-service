package web

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

func resolveTestWeComUser(t *testing.T, store *identity.MemoryIdentityStore, corpID, userID string) identity.PlatformUser {
	t.Helper()
	providerID := "test-wecom-" + corpID
	if err := store.UpsertLoginProvider(context.Background(), identity.ProviderDescriptor{
		ProviderID: providerID, Type: identity.ProviderWeCom, DisplayName: "企业微信",
	}, corpID); err != nil {
		t.Fatalf("UpsertLoginProvider() error = %v", err)
	}
	user, err := store.ResolveLoginIdentity(context.Background(), identity.Identity{
		ProviderID: providerID, ProviderType: identity.ProviderWeCom,
		EnterpriseID: corpID, SubjectID: userID,
	})
	if err != nil {
		t.Fatalf("ResolveLoginIdentity() error = %v", err)
	}
	return user
}
