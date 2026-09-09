package wecom

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// A binding is the trust anchor, so it is checked before anything is dialed,
// and a refusal never repeats the value it refused.
func TestBindingValidate(t *testing.T) {
	require.NoError(t, testBinding().Validate())

	for _, tc := range []struct {
		name   string
		mutate func(b *Binding)
		want   error
	}{
		{"no tenant", func(b *Binding) { b.TenantID = "" }, tenant.ErrInvalidArgument},
		{"a tenant with a space", func(b *Binding) { b.TenantID = "tenant a" },
			tenant.ErrInvalidArgument},
		{"a tenant that is a path", func(b *Binding) { b.TenantID = "../tenant-b" },
			tenant.ErrInvalidArgument},
		{"no app", func(b *Binding) { b.AgentAppID = "" }, tenant.ErrInvalidArgument},
		{"no binding", func(b *Binding) { b.BindingID = "" }, tenant.ErrInvalidArgument},
		{"no bot", func(b *Binding) { b.BotID = "" }, ErrConfig},
		{"a bot id past the bound",
			func(b *Binding) { b.BotID = strings.Repeat("b", maxExternalIDBytes+1) }, ErrConfig},
		{"no secret reference", func(b *Binding) { b.SecretRef = "" }, secretref.ErrScheme},
		{"a secret pasted in place of a reference",
			func(b *Binding) { b.SecretRef = secretMarker }, secretref.ErrScheme},
		{"a secret pasted behind the scheme",
			func(b *Binding) { b.SecretRef = "env:" + secretMarker }, secretref.ErrInvalidName},
		{"a scheme naming nothing", func(b *Binding) { b.SecretRef = "env:" },
			secretref.ErrNoName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := testBinding()
			tc.mutate(&binding)

			err := binding.Validate()

			require.ErrorIs(t, err, ErrConfig)
			require.ErrorIs(t, err, tc.want)
			requireRedacted(t, err)
		})
	}
}

// Derived identity is scoped to the binding that received the message. The
// same external user reached through another tenant, another binding or
// another app is a different principal and a different session: this is the
// property that keeps one tenant from addressing a conversation of another.
func TestDerivedIdentityIsScopedToItsBinding(t *testing.T) {
	b := testBinding()
	principal := principalID(b.TenantID, b.BindingID, userIDMarker)
	session := directSessionID(b.TenantID, b.AgentAppID, b.BindingID, principal)

	require.Equal(t, principal, principalID(b.TenantID, b.BindingID, userIDMarker),
		"the same inputs must derive the same principal on every call and every process")
	require.Equal(t, session,
		directSessionID(b.TenantID, b.AgentAppID, b.BindingID, principal))

	otherTenant := principalID("tenant-b", b.BindingID, userIDMarker)
	otherBinding := principalID(b.TenantID, "binding-b", userIDMarker)
	otherUser := principalID(b.TenantID, b.BindingID, "other-"+userIDMarker)
	require.NotEqual(t, principal, otherTenant)
	require.NotEqual(t, principal, otherBinding)
	require.NotEqual(t, principal, otherUser)

	require.NotEqual(t, session,
		directSessionID("tenant-b", b.AgentAppID, b.BindingID, principal))
	require.NotEqual(t, session,
		directSessionID(b.TenantID, "app-b", b.BindingID, principal))
	require.NotEqual(t, session,
		directSessionID(b.TenantID, b.AgentAppID, "binding-b", principal))
	require.NotEqual(t, session,
		directSessionID(b.TenantID, b.AgentAppID, b.BindingID, otherTenant))

	// A stream id is per message and scoped the same way, so two messages
	// never answer on one stream.
	stream := streamID(b.TenantID, b.BindingID, msgIDMarker)
	require.Equal(t, stream, streamID(b.TenantID, b.BindingID, msgIDMarker))
	require.NotEqual(t, stream, streamID("tenant-b", b.BindingID, msgIDMarker))
	require.NotEqual(t, stream, streamID(b.TenantID, "binding-b", msgIDMarker))
	require.NotEqual(t, stream, streamID(b.TenantID, b.BindingID, "other-"+msgIDMarker))
}

// The three id schemes are separate namespaces: one set of fields hashed for
// one purpose must not equal the hash of the same fields for another.
func TestIdentityDomainsDoNotCollide(t *testing.T) {
	const a, b, c = "tenant-a", "binding-a", "shared"
	require.NotEqual(t, digest(principalDomain, a, b, c), digest(sessionDomain, a, b, c))
	require.NotEqual(t, digest(principalDomain, a, b, c), digest(streamDomain, a, b, c))
	require.NotEqual(t, digest(sessionDomain, a, b, c), digest(streamDomain, a, b, c))

	// The prefixes keep the kinds apart on sight as well as by hash.
	require.True(t, strings.HasPrefix(principalID(a, b, c), "p-"))
	require.True(t, strings.HasPrefix(directSessionID(a, "app-a", b, c), "d-"))
	require.True(t, strings.HasPrefix(streamID(a, b, c), "s-"))
}

// digest hashes a JSON array, not a concatenation, so a field boundary cannot
// be moved. Without this, ("ab","c") and ("a","bc") hash alike and a user of
// one tenant resolves into the session of another.
func TestDigestSeparatesItsFields(t *testing.T) {
	require.NotEqual(t, digest("ab", "c"), digest("a", "bc"))
	require.NotEqual(t, digest("a", "b", ""), digest("a", "b"))
	require.NotEqual(t, digest(`a"`, "b"), digest("a", `"b`))
	require.NotEqual(t, digest("a-b"), digest("a", "b"))
}

// Every derived id has to be usable where the platform expects an id: the
// resource id validator accepts it, and a SessionID fits in the 67 ASCII
// characters the architecture allows.
func TestDerivedIdentifiersFitThePlatformLimits(t *testing.T) {
	b := testBinding()
	principal := principalID(b.TenantID, b.BindingID, userIDMarker)
	session := directSessionID(b.TenantID, b.AgentAppID, b.BindingID, principal)
	stream := streamID(b.TenantID, b.BindingID, msgIDMarker)

	for _, id := range []string{principal, session, stream} {
		// A hex digest plus a two-character prefix: 66 characters, every one
		// of them ASCII and accepted by the resource id rules.
		require.Len(t, id, 66)
		require.LessOrEqual(t, len(id), 67)
		require.NoError(t, tenant.ValidateResourceID("session_id", id))

		// None of them carries an external identifier that must not travel
		// into a session namespace, a cache key or a log.
		require.NotContains(t, id, userIDMarker)
		require.NotContains(t, id, msgIDMarker)
		require.NotContains(t, id, botIDMarker)
	}
}
