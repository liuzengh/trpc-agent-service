package identity

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

const (
	testAPIKey    = "identity-test-key-0123456789"
	testOtherKey  = "identity-other-key-0123456789"
	testUnknown   = "identity-unknown-key-01234567"
	testShortKey  = "too-short"
	testPrincipal = "principal-1"
)

func TestStaticAPIKeyAuthenticatorStoresDigestsOnly(t *testing.T) {
	authenticator := newTestAuthenticator(t)

	// The long-lived map is keyed by digest, so no plaintext credential
	// survives construction.
	stored, ok := authenticator.identities[sha256.Sum256([]byte(testAPIKey))]
	require.True(t, ok)
	require.Equal(t, "tenant-a", stored.TenantID)
	require.Len(t, authenticator.identities, 2)

	authenticated, err := authenticator.Authenticate(context.Background(), testAPIKey)
	require.NoError(t, err)
	require.Equal(t, "tenant-a", authenticated.TenantID)
	require.Equal(t, testPrincipal, authenticated.PrincipalID)
	require.Equal(t, []string{"assistant", "reporter"}, authenticated.AllowedAppIDs)
}

func TestStaticAPIKeyAuthenticatorFailsClosed(t *testing.T) {
	authenticator := newTestAuthenticator(t)

	for name, token := range map[string]string{
		"unknown key": testUnknown,
		"empty token": "",
		"short token": testShortKey,
		"key prefix":  testAPIKey[:len(testAPIKey)-1],
	} {
		t.Run(name, func(t *testing.T) {
			authenticated, err := authenticator.Authenticate(context.Background(), token)
			require.ErrorIs(t, err, ErrUnauthenticated)
			require.Zero(t, authenticated)
		})
	}

	var missingContext context.Context
	_, err := authenticator.Authenticate(missingContext, testAPIKey)
	require.ErrorIs(t, err, ErrUnauthenticated)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = authenticator.Authenticate(cancelled, testAPIKey)
	require.ErrorIs(t, err, context.Canceled)

	var uninitialised *StaticAPIKeyAuthenticator
	_, err = uninitialised.Authenticate(context.Background(), testAPIKey)
	require.ErrorIs(t, err, ErrUnauthenticated)
}

func TestNewStaticAPIKeyAuthenticatorRejectsWeakConfiguration(t *testing.T) {
	valid := Identity{
		TenantID:      "tenant-a",
		PrincipalID:   testPrincipal,
		AllowedAppIDs: []string{"assistant"},
	}

	_, err := NewStaticAPIKeyAuthenticator(nil)
	require.ErrorContains(t, err, "at least one API key")

	_, err = NewStaticAPIKeyAuthenticator(map[string]Identity{testShortKey: valid})
	require.ErrorContains(t, err, "at least 16 characters")

	_, err = NewStaticAPIKeyAuthenticator(map[string]Identity{
		testAPIKey: {TenantID: "tenant-a", PrincipalID: testPrincipal},
	})
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	require.ErrorContains(t, err, "grants no agent app")
}

// A grant must not stay reachable through the map the caller passed in, nor
// through an Identity a previous caller received.
func TestStaticAPIKeyAuthenticatorIsolatesGrantCopies(t *testing.T) {
	grant := Identity{
		TenantID:      "tenant-a",
		PrincipalID:   testPrincipal,
		AllowedAppIDs: []string{"assistant"},
	}
	authenticator, err := NewStaticAPIKeyAuthenticator(map[string]Identity{testAPIKey: grant})
	require.NoError(t, err)

	grant.AllowedAppIDs[0] = "escalated"
	authenticated, err := authenticator.Authenticate(context.Background(), testAPIKey)
	require.NoError(t, err)
	require.Equal(t, []string{"assistant"}, authenticated.AllowedAppIDs)

	authenticated.AllowedAppIDs[0] = "escalated"
	again, err := authenticator.Authenticate(context.Background(), testAPIKey)
	require.NoError(t, err)
	require.Equal(t, []string{"assistant"}, again.AllowedAppIDs)
	require.True(t, again.AllowsApp("assistant"))
	require.False(t, again.AllowsApp("escalated"))
}

func TestIdentityValidate(t *testing.T) {
	valid := Identity{
		TenantID:      "tenant-a",
		PrincipalID:   testPrincipal,
		AllowedAppIDs: []string{"assistant"},
	}
	require.NoError(t, valid.Validate())

	invalid := map[string]Identity{
		"empty":                {},
		"no tenant":            {PrincipalID: testPrincipal, AllowedAppIDs: []string{"assistant"}},
		"no principal":         {TenantID: "tenant-a", AllowedAppIDs: []string{"assistant"}},
		"no app":               {TenantID: "tenant-a", PrincipalID: testPrincipal},
		"empty app":            {TenantID: "tenant-a", PrincipalID: testPrincipal, AllowedAppIDs: []string{""}},
		"app with separator":   {TenantID: "tenant-a", PrincipalID: testPrincipal, AllowedAppIDs: []string{"a/b"}},
		"tenant with wildcard": {TenantID: "*", PrincipalID: testPrincipal, AllowedAppIDs: []string{"assistant"}},
	}
	for name, item := range invalid {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, item.Validate(), tenant.ErrInvalidArgument)
			require.False(t, item.AllowsApp("assistant"))
		})
	}
}

func TestIdentityAllowsApp(t *testing.T) {
	granted := Identity{
		TenantID:      "tenant-a",
		PrincipalID:   testPrincipal,
		AllowedAppIDs: []string{"assistant", "reporter"},
	}
	require.True(t, granted.AllowsApp("assistant"))
	require.True(t, granted.AllowsApp("reporter"))
	require.False(t, granted.AllowsApp("billing"))
	require.False(t, granted.AllowsApp("Assistant"))
	require.False(t, granted.AllowsApp(""))
	require.False(t, granted.AllowsApp("assistant/../billing"))
}

func newTestAuthenticator(t *testing.T) *StaticAPIKeyAuthenticator {
	t.Helper()
	authenticator, err := NewStaticAPIKeyAuthenticator(map[string]Identity{
		testAPIKey: {
			TenantID:      "tenant-a",
			PrincipalID:   testPrincipal,
			AllowedAppIDs: []string{"assistant", "reporter"},
		},
		testOtherKey: {
			TenantID:      "tenant-b",
			PrincipalID:   "principal-2",
			AllowedAppIDs: []string{"assistant"},
		},
	})
	require.NoError(t, err)
	return authenticator
}
