package security

import (
	"testing"

	"github.com/stretchr/testify/require"

	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// A backend storage profile names a credential the same way a revision does, so
// it goes through the same table by the same exact string. Anything looser here
// would be a second, unentitled channel to the process environment.
func TestAuthorizeSecretRefMatchesTheRevisionLookup(t *testing.T) {
	table, err := NewEntitlements(
		Grant{TenantID: "tenant-a", SecretRefs: []string{"env:TENANT_A_SESSION_DSN"}},
		Grant{TenantID: "tenant-b", SecretRefs: []string{"env:TENANT_B_SESSION_URL"}},
	)
	require.NoError(t, err)

	require.NoError(t, table.AuthorizeSecretRef("tenant-a", "env:TENANT_A_SESSION_DSN"))
	require.NoError(t, table.AuthorizeSecretRef("tenant-b", "env:TENANT_B_SESSION_URL"))

	// The whole point of the check: tenant-a may not connect to tenant-b's
	// database by naming tenant-b's variable.
	require.ErrorIs(t,
		table.AuthorizeSecretRef("tenant-a", "env:TENANT_B_SESSION_URL"), ErrNotEntitled)
	require.ErrorIs(t,
		table.AuthorizeSecretRef("tenant-b", "env:TENANT_A_SESSION_DSN"), ErrNotEntitled)

	// A tenant with no entitlements at all is refused by the same path, with
	// the same error, as a tenant that has some but not this one.
	require.ErrorIs(t,
		table.AuthorizeSecretRef("tenant-c", "env:TENANT_A_SESSION_DSN"), ErrNotEntitled)

	// One table, one answer: a reference entitled for profiles is the same
	// reference entitled for a model key, because it is the same grant.
	require.NoError(t, table.AuthorizeRevision("tenant-a", tenant.RevisionConfig{
		Model: tenant.ModelConfig{SecretRef: "env:TENANT_A_SESSION_DSN"},
	}))
	require.ErrorIs(t, table.AuthorizeRevision("tenant-a", tenant.RevisionConfig{
		Model: tenant.ModelConfig{SecretRef: "env:TENANT_B_SESSION_URL"},
	}), ErrNotEntitled)
}

// Exact string, no folding, no normalization — the same rule the revision path
// documents. A matching rule cleverer than the lookup it guards is a rule with
// a gap in it.
func TestAuthorizeSecretRefMatchesExactStringsOnly(t *testing.T) {
	table, err := NewEntitlements(
		Grant{TenantID: "tenant-a", SecretRefs: []string{"env:TENANT_A_SESSION_DSN"}},
	)
	require.NoError(t, err)

	for _, ref := range []string{
		"env:tenant_a_session_dsn",
		"env:TENANT_A_SESSION_DSN ",
		" env:TENANT_A_SESSION_DSN",
		"ENV:TENANT_A_SESSION_DSN",
		"env:TENANT_A_SESSION_DSN2",
		"TENANT_A_SESSION_DSN",
	} {
		require.ErrorIs(t, table.AuthorizeSecretRef("tenant-a", ref), ErrNotEntitled, ref)
	}
}

// An empty reference is not "nothing to check": a revision that names no secret
// is asking for nothing, but an empty reference handed to this method is a
// caller that validated too little, and answering "allowed" would turn that
// mistake into an entitlement.
func TestAuthorizeSecretRefRefusesAnEmptyReference(t *testing.T) {
	table, err := NewEntitlements(
		Grant{TenantID: "tenant-a", SecretRefs: []string{"env:TENANT_A_SESSION_DSN"}},
	)
	require.NoError(t, err)

	require.ErrorIs(t, table.AuthorizeSecretRef("tenant-a", ""), ErrNotEntitled)
	require.ErrorIs(t, table.AuthorizeSecretRef("", ""), ErrNotEntitled)

	// A revision naming nothing is still allowed, unchanged.
	require.NoError(t, table.AuthorizeRevision("tenant-a", tenant.RevisionConfig{}))
}

// Fail closed: a nil table and the deny-everything table entitle nothing, so a
// caller that forgot to build one cannot accidentally get the permissive
// answer.
func TestAuthorizeSecretRefFailsClosedWithoutATable(t *testing.T) {
	var missing *Entitlements
	require.ErrorIs(t, missing.AuthorizeSecretRef("tenant-a", "env:ANY"), ErrNotEntitled)
	require.ErrorIs(t,
		DenyCapabilities().AuthorizeSecretRef("tenant-a", "env:ANY"), ErrNotEntitled)

	// And the deny table still runs a revision that asks for nothing, which is
	// exactly the set of revisions that need no entitlement.
	require.NoError(t, missing.AuthorizeRevision("tenant-a", tenant.RevisionConfig{}))
	require.NoError(t, DenyCapabilities().AuthorizeRevision("tenant-a", tenant.RevisionConfig{}))
}

// The error says nothing about what was rejected. A caller who could tell
// "unknown variable" from "known but not yours" would have a probe for the
// process environment built out of nothing but refusals.
func TestAuthorizeSecretRefSaysNothingAboutTheReference(t *testing.T) {
	table, err := NewEntitlements(
		Grant{TenantID: "tenant-a", SecretRefs: []string{"env:TENANT_A_SESSION_DSN"}},
	)
	require.NoError(t, err)

	err = table.AuthorizeSecretRef("tenant-b", "env:TENANT_A_SESSION_DSN")
	require.ErrorIs(t, err, ErrNotEntitled)
	require.NotContains(t, err.Error(), "TENANT_A_SESSION_DSN")
	require.NotContains(t, err.Error(), "tenant-b")
}

// The reserved namespace is unreachable through this method for the same reason
// it is unreachable through a revision: the grant that would have to exist
// cannot be built. The rule is enforced where the table is built, so there is no
// second place for it to be forgotten.
func TestNoGrantCanEntitleTheReservedNamespace(t *testing.T) {
	for _, ref := range []string{
		"env:TRPC_SERVICE_ADMIN_API_KEY",
		"env:TRPC_SERVICE_SESSION_DSN",
		"env:TRPC_SERVICE_",
	} {
		_, err := NewEntitlements(Grant{TenantID: "tenant-a", SecretRefs: []string{ref}})
		require.ErrorContains(t, err, "reserved TRPC_SERVICE_ namespace", ref)
	}

	// And an unbuildable grant is not a hole: whatever table does exist refuses
	// the reference.
	table, err := NewEntitlements(Grant{TenantID: "tenant-a"})
	require.NoError(t, err)
	require.ErrorIs(t,
		table.AuthorizeSecretRef("tenant-a", "env:TRPC_SERVICE_ADMIN_API_KEY"), ErrNotEntitled)
}

// The manifest's platform-credential rule covers profile references too,
// because it is the same table: a variable holding this process's own admin or
// chat key cannot be entitled to any tenant, so no storage profile can name it.
func TestLoadedTableRefusesPlatformCredentialsForProfiles(t *testing.T) {
	manifest := `{"version":1,"credentials":[
		{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
		 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
		{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
	],"tenant_entitlements":[
		{"tenant_id":"tenant-a","allowed_secret_refs":["env:TENANT_A_SESSION_DSN"]}
	]}`
	vars := writeManifest(t, manifest, validEnv())

	cfg, err := Load(vars.getenv, tool.Builtin())
	require.NoError(t, err)

	require.NoError(t, cfg.Revisions.AuthorizeSecretRef("tenant-a", "env:TENANT_A_SESSION_DSN"))
	for _, ref := range []string{
		"env:ADMIN_KEY",
		"env:CHAT_KEY",
		"env:TRPC_SERVICE_ADMIN_API_KEY",
	} {
		require.ErrorIs(t,
			cfg.Revisions.AuthorizeSecretRef("tenant-a", ref), ErrNotEntitled, ref)
	}
}

// The demo profile entitles one policy and no secret at all, so a demo process
// can run the safe tools and cannot build a durable storage profile. That is
// the fail-closed default, and it is asserted here so a future convenience
// grant has to be a deliberate edit.
func TestDemoProfileEntitlesNoSecretRef(t *testing.T) {
	vars := env{AdminAPIKeyEnvVar: testAdminKey}
	cfg, err := Load(vars.getenv, tool.Builtin())
	require.NoError(t, err)

	require.NoError(t, cfg.Revisions.AuthorizeRevision(
		platformconfig.DemoTenantID,
		tenant.RevisionConfig{PolicyRefs: []string{tool.PolicySafeTools}},
	))
	require.ErrorIs(t, cfg.Revisions.AuthorizeSecretRef(
		platformconfig.DemoTenantID, "env:DEMO_SESSION_DSN"), ErrNotEntitled)
}
