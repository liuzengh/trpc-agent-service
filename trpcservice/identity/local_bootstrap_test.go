package identity

import (
	"context"
	"testing"
)

func TestBootstrapLocalSystemAdminRunsOnlyWithoutUsableAdmin(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryIdentityStore()
	bootstrap := LocalBootstrap{Username: "root.admin", Password: "temporary-password-123", DisplayName: "系统管理员"}
	if err := BootstrapLocalSystemAdmin(ctx, store, bootstrap); err != nil {
		t.Fatal(err)
	}
	credential, user, err := store.LookupLocalCredential(ctx, "root.admin")
	if err != nil || !credential.MustChangePassword || !VerifyLocalPassword(credential.PasswordHash, bootstrap.Password) {
		t.Fatalf("bootstrap credential = %#v user=%#v err=%v", credential, user, err)
	}
	if admin, err := store.IsSystemAdmin(ctx, user.PlatformUserID); err != nil || !admin {
		t.Fatalf("bootstrap system admin = %v, %v", admin, err)
	}

	// Deployment credentials are ignored after initialization. They cannot
	// create a second administrator or reset the existing password.
	if err := BootstrapLocalSystemAdmin(ctx, store, LocalBootstrap{Username: "backdoor", Password: "another-password-123"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LookupLocalCredential(ctx, "backdoor"); err == nil {
		t.Fatal("bootstrap must be inert once a usable system administrator exists")
	}
	credentialAfter, _, err := store.LookupLocalCredential(ctx, "root.admin")
	if err != nil || credentialAfter.PasswordHash != credential.PasswordHash {
		t.Fatal("bootstrap must not reset an initialized administrator")
	}
}

func TestBootstrapLocalSystemAdminAllowsUnconfiguredExternalRecoveryPath(t *testing.T) {
	store := NewMemoryIdentityStore()
	if err := BootstrapLocalSystemAdmin(context.Background(), store, LocalBootstrap{}); err != nil {
		t.Fatalf("unconfigured bootstrap = %v", err)
	}
}
