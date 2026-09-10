package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

func TestAccountProfileEditsPlatformNameAndListsLoginIdentity(t *testing.T) {
	var platformUserID string
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		user, err := store.CreateLocalUser(context.Background(), "ming", "旧名称", "ming@example.com", "hash", false)
		if err != nil {
			t.Fatal(err)
		}
		platformUserID = user.PlatformUserID
	})
	principal := identity.SessionUser{PlatformUserID: platformUserID, DisplayName: "旧名称"}
	updated := handler.requestAs(t, principal, http.MethodPut, "/api/v1/account/profile", `{"display_name":"新名称"}`)
	if updated.Code != http.StatusOK {
		t.Fatalf("update profile = %d %s", updated.Code, updated.Body.String())
	}
	var profile accountProfileResponse
	if err := json.Unmarshal(updated.Body.Bytes(), &profile); err != nil {
		t.Fatal(err)
	}
	if profile.DisplayName != "新名称" || profile.PlatformUserID != platformUserID || len(profile.LoginMethods) != 1 || profile.LoginMethods[0].ProviderType != identity.ProviderLocal {
		t.Fatalf("profile = %+v", profile)
	}
	invalid := handler.requestAs(t, principal, http.MethodPut, "/api/v1/account/profile", `{"display_name":"   "}`)
	if invalid.Code != http.StatusBadRequest || !strings.Contains(invalid.Body.String(), "display name") {
		t.Fatalf("empty display name = %d %s", invalid.Code, invalid.Body.String())
	}
}
