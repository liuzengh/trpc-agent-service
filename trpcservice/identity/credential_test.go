package identity

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// untransmittableKeys are keys long enough to clear both length floors and
// impossible to present as a Bearer credential anyway.
//
// Each is a configuration that loads today: the process starts, reports the
// credential it was given, and then refuses the operator holding it. That is
// the failure being moved from runtime to startup — a 401 that no amount of
// reading the configuration explains, because the configuration is right and
// the credential is unusable.
var untransmittableKeys = map[string]string{
	"nothing but spaces":          strings.Repeat(" ", 40),
	"nothing but tabs":            strings.Repeat("\t", 40),
	"a leading space":             " identity-test-key-0123456789abcdef",
	"a trailing space":            "identity-test-key-0123456789abcdef ",
	"a leading tab":               "\tidentity-test-key-0123456789abcdef",
	"a trailing newline":          "identity-test-key-0123456789abcdef\n",
	"an embedded newline":         "identity-test-key\n0123456789abcdef01",
	"an embedded return":          "identity-test-key\r0123456789abcdef01",
	"an embedded NUL":             "identity-test-key\x00" + "0123456789abcdef01",
	"an embedded DEL":             "identity-test-key\x7f" + "0123456789abcdef01",
	"an embedded escape":          "identity-test-key\x1b" + "0123456789abcdef01",
	"a vertical tab":              "identity-test-key\v0123456789abcdef01",
	"a form feed":                 "identity-test-key\f0123456789abcdef01",
	"a leading ideographic space": "　identity-test-key-0123456789abcd",
}

// A key that cannot be carried is refused where it is configured.
func TestNewStaticAdminAPIKeyAuthenticatorRejectsAnUncarryableKey(t *testing.T) {
	for name, key := range untransmittableKeys {
		t.Run(name, func(t *testing.T) {
			authenticator, err := NewStaticAdminAPIKeyAuthenticator(
				map[string]AdminIdentity{key: {
					Role:        RolePlatformAdmin,
					PrincipalID: testPrincipal,
				}})

			require.Nil(t, authenticator)
			require.ErrorContains(t, err, "cannot be sent as a Bearer credential")
			// The role locates the entry; the key is never named, here or anywhere
			// else this package reports a configuration fault.
			require.ErrorContains(t, err, string(RolePlatformAdmin))
			require.NotContains(t, err.Error(), key)
		})
	}
}

// The data plane presents its credential the same way, so it is held to the
// same rule.
func TestNewStaticAPIKeyAuthenticatorRejectsAnUncarryableKey(t *testing.T) {
	grant := Identity{
		TenantID:      "tenant-a",
		PrincipalID:   testPrincipal,
		AllowedAppIDs: []string{"assistant"},
	}
	for name, key := range untransmittableKeys {
		t.Run(name, func(t *testing.T) {
			authenticator, err := NewStaticAPIKeyAuthenticator(map[string]Identity{key: grant})

			require.Nil(t, authenticator)
			require.ErrorContains(t, err, "cannot be sent as a Bearer credential")
			require.ErrorContains(t, err, "tenant-a")
			require.NotContains(t, err.Error(), key)
		})
	}
}

// The rule refuses only what cannot work.
//
// This is the half that keeps it honest. An API key is not required to be
// RFC 6750 token68 here, because keys already in use are not: they carry
// colons, slashes, pipes, plus signs and equals padding, and every one of them
// travels in a header and compares byte for byte on arrival. A check that
// refused them would be a check that broke working deployments to prevent
// nothing.
func TestStaticAuthenticatorsAcceptOrdinaryKeys(t *testing.T) {
	ordinary := map[string]string{
		"the published development key": "local-development-key-not-a-secret",
		"a hyphenated key":              "identity-test-key-0123456789abcdef",
		"a prefixed key":                "sk:live:0123456789abcdef01234567",
		"base64 padding":                "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo=",
		"a JWT-shaped key":              "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln",
		"pipes and slashes":             "vault/kv|admin|0123456789abcdef0123",
		"an internal space":             "admin key 0123456789abcdef0123456",
		"an internal tab":               "admin\tkey\t0123456789abcdef012345",
		"non-ASCII":                     "clé-d-administration-0123456789ab",
	}
	for name, key := range ordinary {
		t.Run(name, func(t *testing.T) {
			admin, err := NewStaticAdminAPIKeyAuthenticator(
				map[string]AdminIdentity{key: {
					Role:        RolePlatformAdmin,
					PrincipalID: testPrincipal,
				}})
			require.NoError(t, err)

			// And it still authenticates: the rule is about what may be configured,
			// and changes nothing about what a configured key does.
			granted, err := admin.AuthenticateAdmin(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, RolePlatformAdmin, granted.Role)

			chat, err := NewStaticAPIKeyAuthenticator(map[string]Identity{key: {
				TenantID:      "tenant-a",
				PrincipalID:   testPrincipal,
				AllowedAppIDs: []string{"assistant"},
			}})
			require.NoError(t, err)
			authenticated, err := chat.Authenticate(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, "tenant-a", authenticated.TenantID)
		})
	}
}
