package identity

import (
	"context"
	"testing"
)

func registerTestWeComProvider(t *testing.T, store LoginProviderRegistrar, _ string, providerID, corpID string) {
	t.Helper()
	if err := store.UpsertLoginProvider(context.Background(), ProviderDescriptor{
		ProviderID: providerID, Type: ProviderWeCom, DisplayName: "企业微信",
	}, corpID); err != nil {
		t.Fatalf("UpsertLoginProvider() error = %v", err)
	}
}

func testWeComIdentity(_ string, providerID, corpID, userID string) Identity {
	return Identity{
		ProviderID: providerID, ProviderType: ProviderWeCom,
		EnterpriseID: corpID, SubjectID: userID,
	}
}
