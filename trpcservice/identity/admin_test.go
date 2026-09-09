package identity

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Admin credentials in these tests are 36 characters: past the 32-character
// floor, and visibly longer than the chat keys next door in identity_test.go.
const (
	platformKey = "admin-key-platform-0123456789abcdef01"
	tenantKey   = "admin-key-tenant-a-0123456789abcdef01"
)

// The two roles have opposite requirements about the tenant field, and both
// directions have to be enforced. A platform admin carrying a tenant is the
// dangerous one: it would compare unequal to every tenant it is meant to
// administer, and a later "well, it has a tenant, so scope by it" would silently
// become the rule.
func TestAdminIdentityValidatesRoleShape(t *testing.T) {
	for _, tc := range []struct {
		name     string
		identity AdminIdentity
		wantErr  string
	}{
		{
			name:     "platform admin",
			identity: AdminIdentity{Role: RolePlatformAdmin, PrincipalID: "ops"},
		},
		{
			name: "tenant admin",
			identity: AdminIdentity{
				Role: RoleTenantAdmin, PrincipalID: "ops", TenantID: "tenant-a",
			},
		},
		{
			name: "platform admin with a tenant",
			identity: AdminIdentity{
				Role: RolePlatformAdmin, PrincipalID: "ops", TenantID: "tenant-a",
			},
			wantErr: "must not carry a tenant",
		},
		{
			name:     "tenant admin without a tenant",
			identity: AdminIdentity{Role: RoleTenantAdmin, PrincipalID: "ops"},
			wantErr:  "tenant id",
		},
		{
			name:     "no principal",
			identity: AdminIdentity{Role: RolePlatformAdmin},
			wantErr:  "principal id",
		},
		{
			name:     "blank principal",
			identity: AdminIdentity{Role: RolePlatformAdmin, PrincipalID: "   "},
			wantErr:  "principal id",
		},
		{
			name:     "unknown role",
			identity: AdminIdentity{Role: "root", PrincipalID: "ops"},
			wantErr:  "unsupported admin role",
		},
		{
			name:     "empty role",
			identity: AdminIdentity{PrincipalID: "ops"},
			wantErr:  "unsupported admin role",
		},
		{
			name: "chat-shaped role name",
			identity: AdminIdentity{
				Role: "chat", PrincipalID: "ops", TenantID: "tenant-a",
			},
			wantErr: "unsupported admin role",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.identity.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// AllowsTenant is the comparison the Admin API short-circuits on, so an
// identity that does not validate must answer no rather than answering from its
// fields.
func TestAdminIdentityAllowsTenant(t *testing.T) {
	platform := AdminIdentity{Role: RolePlatformAdmin, PrincipalID: "ops"}
	require.True(t, platform.IsPlatformAdmin())
	require.True(t, platform.AllowsTenant("tenant-a"))
	require.True(t, platform.AllowsTenant("tenant-b"))
	// Even a platform admin cannot reach an id that is not one.
	require.False(t, platform.AllowsTenant(""))
	require.False(t, platform.AllowsTenant("  "))

	bound := AdminIdentity{
		Role: RoleTenantAdmin, PrincipalID: "ops", TenantID: "tenant-a",
	}
	require.False(t, bound.IsPlatformAdmin())
	require.True(t, bound.AllowsTenant("tenant-a"))
	require.False(t, bound.AllowsTenant("tenant-b"))
	// Exact comparison: no prefixes, no case folding, no padding.
	require.False(t, bound.AllowsTenant("tenant-a2"))
	require.False(t, bound.AllowsTenant("TENANT-A"))
	require.False(t, bound.AllowsTenant("tenant-a "))
	require.False(t, bound.AllowsTenant(""))

	// A malformed identity grants nothing, whatever its fields say.
	malformed := AdminIdentity{Role: RolePlatformAdmin, TenantID: "tenant-a"}
	require.False(t, malformed.IsPlatformAdmin())
	require.False(t, malformed.AllowsTenant("tenant-a"))
}

// The control-plane floor is 32 characters, twice the chat floor. A key that
// would be accepted for chat must not be accepted here.
func TestNewStaticAdminAPIKeyAuthenticatorRequiresAStrongKey(t *testing.T) {
	for _, key := range []string{
		"short",
		strings.Repeat("k", minCredentialLength), // fine for chat
		strings.Repeat("k", minAdminCredentialLength-1), // one short
	} {
		_, err := NewStaticAdminAPIKeyAuthenticator(map[string]AdminIdentity{
			key: {Role: RolePlatformAdmin, PrincipalID: "ops"},
		})
		require.ErrorContains(t, err, "at least 32 characters")
		require.NotContains(t, err.Error(), key, "the refusal echoed the key")
	}

	// The empty key never reaches the floor: it is refused first as a credential
	// that could not be presented at all, which is the more accurate of the two
	// complaints. See TestNewStaticAdminAPIKeyAuthenticatorRejectsAnUncarryableKey.
	_, err := NewStaticAdminAPIKeyAuthenticator(map[string]AdminIdentity{
		"": {Role: RolePlatformAdmin, PrincipalID: "ops"},
	})
	require.ErrorContains(t, err, "cannot be sent as a Bearer credential")

	_, err = NewStaticAdminAPIKeyAuthenticator(map[string]AdminIdentity{
		strings.Repeat("k", minAdminCredentialLength): {
			Role: RolePlatformAdmin, PrincipalID: "ops",
		},
	})
	require.NoError(t, err)
}

// A grant that cannot be scoped is refused at construction rather than at the
// first request it would have mis-scoped.
func TestNewStaticAdminAPIKeyAuthenticatorRejectsAnInvalidGrant(t *testing.T) {
	_, err := NewStaticAdminAPIKeyAuthenticator(map[string]AdminIdentity{
		platformKey: {Role: RolePlatformAdmin, PrincipalID: "ops", TenantID: "tenant-a"},
	})
	require.ErrorContains(t, err, "invalid admin API key grant")
	require.NotContains(t, err.Error(), platformKey)

	_, err = NewStaticAdminAPIKeyAuthenticator(nil)
	require.ErrorContains(t, err, "at least one admin API key is required")
}

// Authentication is the whole trust boundary of the control plane, so every way
// of not presenting a valid credential has to end in the same refusal.
func TestStaticAdminAPIKeyAuthenticatorFailsClosed(t *testing.T) {
	authenticator, err := NewStaticAdminAPIKeyAuthenticator(map[string]AdminIdentity{
		platformKey: {Role: RolePlatformAdmin, PrincipalID: "ops"},
		tenantKey: {
			Role: RoleTenantAdmin, PrincipalID: "tenant-a-ops", TenantID: "tenant-a",
		},
	})
	require.NoError(t, err)

	granted, err := authenticator.AuthenticateAdmin(context.Background(), platformKey)
	require.NoError(t, err)
	require.Equal(t, RolePlatformAdmin, granted.Role)
	require.Equal(t, "ops", granted.PrincipalID)
	require.Empty(t, granted.TenantID)

	granted, err = authenticator.AuthenticateAdmin(context.Background(), tenantKey)
	require.NoError(t, err)
	require.Equal(t, RoleTenantAdmin, granted.Role)
	require.Equal(t, "tenant-a", granted.TenantID)

	for _, tc := range []struct {
		name  string
		token string
	}{
		{name: "empty", token: ""},
		{name: "unknown", token: "admin-key-unknown---0123456789abcdef01"},
		{name: "too short to be admin", token: "0123456789abcdef"},
		{name: "prefix of a real key", token: platformKey[:len(platformKey)-1]},
		{name: "real key with padding", token: platformKey + " "},
		{name: "bearer prefix left on", token: "Bearer " + platformKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := authenticator.AuthenticateAdmin(context.Background(), tc.token)
			require.ErrorIs(t, err, ErrUnauthenticated)
		})
	}

	// A nil receiver, a nil context and a cancelled context all refuse. None of
	// them is a state a caller should be able to turn into an identity.
	var nilAuthenticator *StaticAdminAPIKeyAuthenticator
	_, err = nilAuthenticator.AuthenticateAdmin(context.Background(), platformKey)
	require.ErrorIs(t, err, ErrUnauthenticated)

	//nolint:staticcheck // passing nil is the point.
	_, err = authenticator.AuthenticateAdmin(nil, platformKey)
	require.ErrorIs(t, err, ErrUnauthenticated)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = authenticator.AuthenticateAdmin(cancelled, platformKey)
	require.Error(t, err)
}

// A chat credential and an admin credential are different types, and no value
// satisfies both interfaces. This is a compile-time claim, so it is asserted at
// compile time.
func TestAdminAndChatAuthenticatorsAreDistinctTypes(t *testing.T) {
	admin, err := NewStaticAdminAPIKeyAuthenticator(map[string]AdminIdentity{
		platformKey: {Role: RolePlatformAdmin, PrincipalID: "ops"},
	})
	require.NoError(t, err)
	var _ AdminAuthenticator = admin
	_, isChat := any(admin).(Authenticator)
	require.False(t, isChat, "the admin authenticator also satisfies Authenticator")

	chat, err := NewStaticAPIKeyAuthenticator(map[string]Identity{
		platformKey: {
			TenantID: "tenant-a", PrincipalID: "ops", AllowedAppIDs: []string{"assistant"},
		},
	})
	require.NoError(t, err)
	var _ Authenticator = chat
	_, isAdmin := any(chat).(AdminAuthenticator)
	require.False(t, isAdmin, "the chat authenticator also satisfies AdminAuthenticator")
}
