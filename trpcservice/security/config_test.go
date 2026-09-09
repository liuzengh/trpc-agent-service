package security

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// Test credentials. They are long enough to pass both floors — 16 for chat, 32
// for admin — because a test that failed on length would be testing the wrong
// rule.
const (
	testChatKey        = "test-chat-key-0123456789abcdef01"
	testAdminKey       = "test-admin-key-0123456789abcdef01"
	testTenantAdminKey = "test-tenant-admin-key-0123456789ab"
	testModelKey       = "test-model-key-0123456789abcdef01"
)

// env is one process environment. Load takes a getenv function precisely so a
// test can hand it one of these instead of mutating the real environment, which
// matters here: several of these tests would otherwise leave a credential in the
// environment of every test that ran after them.
type env map[string]string

func (e env) getenv(name string) string { return e[name] }

// writeManifest puts body in a temp file and returns an env pointing at it.
func writeManifest(t *testing.T, body string, extra env) env {
	t.Helper()
	path := filepath.Join(t.TempDir(), "security.json")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	vars := env{ConfigFileEnvVar: path}
	for name, value := range extra {
		vars[name] = value
	}
	return vars
}

// A working manifest, used as the base for the mutations below. It has one chat
// credential, one platform admin, one tenant admin, and one entitlement.
const validManifest = `{
	"version": 1,
	"credentials": [
		{
			"purpose": "chat",
			"principal_id": "tenant-a-user",
			"key_ref": "env:CHAT_KEY",
			"tenant_id": "tenant-a",
			"allowed_app_ids": ["assistant"]
		},
		{
			"purpose": "platform_admin",
			"principal_id": "ops",
			"key_ref": "env:ADMIN_KEY"
		},
		{
			"purpose": "tenant_admin",
			"principal_id": "tenant-a-ops",
			"key_ref": "env:TENANT_ADMIN_KEY",
			"tenant_id": "tenant-a"
		}
	],
	"tenant_entitlements": [
		{
			"tenant_id": "tenant-a",
			"allowed_secret_refs": ["env:TENANT_A_MODEL_KEY"],
			"allowed_policy_refs": ["builtin.safe-tools"]
		}
	]
}`

// validEnv is the environment validManifest resolves against.
func validEnv() env {
	return env{
		"CHAT_KEY":         testChatKey,
		"ADMIN_KEY":        testAdminKey,
		"TENANT_ADMIN_KEY": testTenantAdminKey,
	}
}

// The manifest path is the whole configuration when it is set: the three values
// it produces are wired from it and from nothing else.
func TestLoadBuildsEveryTrustBoundaryFromTheManifest(t *testing.T) {
	cfg, err := Load(writeManifest(t, validManifest, validEnv()).getenv, tool.Builtin())
	require.NoError(t, err)

	chat, err := cfg.Chat.Authenticate(context.Background(), testChatKey)
	require.NoError(t, err)
	require.Equal(t, "tenant-a", chat.TenantID)
	require.Equal(t, "tenant-a-user", chat.PrincipalID)
	require.True(t, chat.AllowsApp("assistant"))
	require.False(t, chat.AllowsApp("reporter"))

	platform, err := cfg.Admin.AuthenticateAdmin(context.Background(), testAdminKey)
	require.NoError(t, err)
	require.Equal(t, identity.RolePlatformAdmin, platform.Role)
	require.Equal(t, "ops", platform.PrincipalID)
	require.Empty(t, platform.TenantID)

	bound, err := cfg.Admin.AuthenticateAdmin(context.Background(), testTenantAdminKey)
	require.NoError(t, err)
	require.Equal(t, identity.RoleTenantAdmin, bound.Role)
	require.Equal(t, "tenant-a", bound.TenantID)

	// The two purposes do not cross. This is the single most important property
	// of having two authenticators at all.
	_, err = cfg.Admin.AuthenticateAdmin(context.Background(), testChatKey)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
	_, err = cfg.Chat.Authenticate(context.Background(), testAdminKey)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
	_, err = cfg.Chat.Authenticate(context.Background(), testTenantAdminKey)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)

	entitled := tenant.RevisionConfig{
		Model:      tenant.ModelConfig{SecretRef: "env:TENANT_A_MODEL_KEY"},
		PolicyRefs: []string{tool.PolicySafeTools},
	}
	require.NoError(t, cfg.Revisions.AuthorizeRevision("tenant-a", entitled))
	// Entitlement follows the tenant, not the credential that created the
	// revision: the same config under another tenant is refused.
	require.ErrorIs(t,
		cfg.Revisions.AuthorizeRevision("tenant-b", entitled), ErrNotEntitled)

	// The startup line is safe to log.
	require.NotContains(t, cfg.Description, testChatKey)
	require.NotContains(t, cfg.Description, testAdminKey)
	require.NotContains(t, cfg.Description, testTenantAdminKey)
}

// When a manifest is in force, the demo inputs stop existing. A deployment that
// wrote a manifest did not also mean to keep an ambient second way in, and the
// published development key least of all.
func TestLoadWithAManifestIgnoresTheDemoCredentials(t *testing.T) {
	vars := writeManifest(t, validManifest, validEnv())
	vars[ChatAPIKeyEnvVar] = "an-ambient-chat-key-0123456789"
	vars[AdminAPIKeyEnvVar] = "an-ambient-admin-key-0123456789ab"

	cfg, err := Load(vars.getenv, tool.Builtin())
	require.NoError(t, err)

	_, err = cfg.Chat.Authenticate(context.Background(), vars[ChatAPIKeyEnvVar])
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
	_, err = cfg.Chat.Authenticate(context.Background(), DevelopmentChatAPIKey)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
	_, err = cfg.Admin.AuthenticateAdmin(context.Background(), vars[AdminAPIKeyEnvVar])
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}

// Every way a manifest can be wrong, and the one thing every refusal has in
// common: it names the entry or the variable, never a value.
func TestLoadRejectsAMalformedManifest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		manifest string
		env      env
		wantErr  string
	}{
		{
			name:     "no version",
			manifest: `{"credentials":[]}`,
			wantErr:  "declares version 0",
		},
		{
			name:     "a later version",
			manifest: `{"version":2,"credentials":[]}`,
			wantErr:  "supports version 1 only",
		},
		{
			name:     "unknown top-level field",
			manifest: `{"version":1,"credentials":[],"rbac":{"roles":[]}}`,
			wantErr:  "not a valid security manifest",
		},
		{
			name: "unknown credential field",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY","expires":"never"}
			]}`,
			wantErr: "not a valid security manifest",
		},
		{
			name:     "two JSON objects",
			manifest: `{"version":1,"credentials":[]}{"version":1,"credentials":[]}`,
			wantErr:  "exactly one JSON object",
		},
		{
			name:     "trailing garbage",
			manifest: `{"version":1,"credentials":[]} oops`,
			wantErr:  "exactly one JSON object",
		},
		{
			name:     "not JSON at all",
			manifest: "version = 1\n",
			wantErr:  "not a valid security manifest",
		},
		{
			name:     "no credentials",
			manifest: `{"version":1}`,
			wantErr:  "declares no credentials",
		},
		{
			name:     "empty credential list",
			manifest: `{"version":1,"credentials":[]}`,
			wantErr:  "declares no credentials",
		},
		{
			name: "unknown purpose",
			manifest: `{"version":1,"credentials":[
				{"purpose":"superuser","principal_id":"u","key_ref":"env:CHAT_KEY"}
			]}`,
			wantErr: "unsupported purpose",
		},
		{
			name: "chat without a tenant",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "allowed_app_ids":["assistant"]}
			]}`,
			wantErr: "tenant_id",
		},
		{
			name: "chat without apps",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a"}
			]}`,
			wantErr: "grants no allowed_app_ids",
		},
		{
			name: "chat repeating an app",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant","assistant"]}
			]}`,
			wantErr: "repeats an allowed_app_ids entry",
		},
		{
			name: "platform admin with a tenant",
			manifest: `{"version":1,"credentials":[
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY",
				 "tenant_id":"tenant-a"}
			]}`,
			wantErr: "must not carry tenant_id",
		},
		{
			name: "platform admin with apps",
			manifest: `{"version":1,"credentials":[
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY",
				 "allowed_app_ids":["assistant"]}
			]}`,
			wantErr: "must not carry allowed_app_ids",
		},
		{
			name: "tenant admin without a tenant",
			manifest: `{"version":1,"credentials":[
				{"purpose":"tenant_admin","principal_id":"ops","key_ref":"env:TENANT_ADMIN_KEY"}
			]}`,
			wantErr: "tenant_id",
		},
		{
			name: "tenant admin with apps",
			manifest: `{"version":1,"credentials":[
				{"purpose":"tenant_admin","principal_id":"ops","key_ref":"env:TENANT_ADMIN_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]}
			]}`,
			wantErr: "must not carry allowed_app_ids",
		},
		{
			name: "no principal",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]}
			]}`,
			wantErr: "principal_id",
		},
		{
			name: "a literal key instead of a reference",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"` + testChatKey + `",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]}
			]}`,
			wantErr: "key_ref",
		},
		{
			name: "a key behind the scheme",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:` + testChatKey + `",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]}
			]}`,
			wantErr: "key_ref",
		},
		{
			name: "repeated key_ref",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:CHAT_KEY"}
			]}`,
			env:     validEnv(),
			wantErr: "repeats the key_ref of credentials[0]",
		},
		{
			name: "missing environment variable",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:ABSENT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]}
			]}`,
			wantErr: `"ABSENT_KEY" is unset or empty`,
		},
		{
			name: "empty environment variable",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]}
			]}`,
			env:     env{"CHAT_KEY": ""},
			wantErr: `"CHAT_KEY" is unset or empty`,
		},
		{
			name: "a chat key that is too weak",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			]}`,
			env:     env{"CHAT_KEY": "short", "ADMIN_KEY": testAdminKey},
			wantErr: "build chat authenticator",
		},
		{
			name: "an admin key that is only chat-strong",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			]}`,
			env:     env{"CHAT_KEY": testChatKey, "ADMIN_KEY": "0123456789abcdef"},
			wantErr: "build admin authenticator",
		},
		{
			name: "no chat credential",
			manifest: `{"version":1,"credentials":[
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			]}`,
			env:     validEnv(),
			wantErr: "at least one chat credential is required",
		},
		{
			name: "no platform admin",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"tenant_admin","principal_id":"ops","key_ref":"env:TENANT_ADMIN_KEY",
				 "tenant_id":"tenant-a"}
			]}`,
			env:     validEnv(),
			wantErr: "at least one platform_admin credential is required",
		},
		{
			name: "an unknown policy in an entitlement",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			],"tenant_entitlements":[
				{"tenant_id":"tenant-a","allowed_policy_refs":["builtin.safe-tool"]}
			]}`,
			env:     validEnv(),
			wantErr: "unknown policy",
		},
		{
			name: "a duplicate tenant entitlement",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			],"tenant_entitlements":[
				{"tenant_id":"tenant-a"},{"tenant_id":"tenant-a"}
			]}`,
			env:     validEnv(),
			wantErr: "repeats tenant",
		},
		{
			name: "a repeated secret ref inside one entitlement",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			],"tenant_entitlements":[
				{"tenant_id":"tenant-a",
				 "allowed_secret_refs":["env:MODEL_KEY","env:MODEL_KEY"]}
			]}`,
			env:     validEnv(),
			wantErr: "repeats an allowed_secret_refs entry",
		},
		{
			name: "a repeated policy ref inside one entitlement",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			],"tenant_entitlements":[
				{"tenant_id":"tenant-a",
				 "allowed_policy_refs":["builtin.safe-tools","builtin.safe-tools"]}
			]}`,
			env:     validEnv(),
			wantErr: "repeats an allowed_policy_refs entry",
		},
		{
			name: "an entitlement without a tenant",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			],"tenant_entitlements":[{"tenant_id":""}]}`,
			env:     validEnv(),
			wantErr: "tenant_id",
		},
		{
			name: "a malformed secret ref in an entitlement",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			],"tenant_entitlements":[
				{"tenant_id":"tenant-a","allowed_secret_refs":["vault://secret/key"]}
			]}`,
			env:     validEnv(),
			wantErr: "allowed_secret_refs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vars := writeManifest(t, tc.manifest, tc.env)

			cfg, err := Load(vars.getenv, tool.Builtin())
			require.Nil(t, cfg)
			require.ErrorContains(t, err, tc.wantErr)
			requireNoSecrets(t, err.Error())
		})
	}
}

// The file itself has to be there and has to be a file. "Set but not honored"
// is the one outcome a security configuration must never have.
// A member named twice in one object is a file that says two things, and
// encoding/json quietly takes the second.
//
// Every manifest below is well-formed JSON naming only fields this build knows,
// so neither DisallowUnknownFields nor the single-object check sees anything
// wrong with it. What the decoder would drop is whichever half the author meant:
// a chat credential read as a platform admin, a key_ref read as the second of
// two variables, an entitlement list read as the shorter of the two written.
func TestLoadRejectsARepeatedMember(t *testing.T) {
	const repeated = "repeats an earlier member"

	for _, tc := range []struct {
		name     string
		manifest string
	}{
		{
			name: "the version, at the top level",
			manifest: `{"version":1,"version":2,"credentials":[
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}]}`,
		},
		{
			name: "the credential list, at the top level",
			manifest: `{"version":1,
				"credentials":[{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
					"tenant_id":"tenant-a","allowed_app_ids":["assistant"]}],
				"credentials":[
					{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}]}`,
		},
		{
			name: "the entitlement table, at the top level",
			manifest: `{"version":1,
				"credentials":[
					{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}],
				"tenant_entitlements":[{"tenant_id":"tenant-a"}],
				"tenant_entitlements":[{"tenant_id":"tenant-b"}]}`,
		},
		{
			// The one that decides what a credential is: read as written it is a
			// chat key, read by the decoder it administers the platform.
			name: "a credential's purpose",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","purpose":"platform_admin",
				 "principal_id":"ops","key_ref":"env:ADMIN_KEY"}]}`,
		},
		{
			name: "a credential's key_ref",
			manifest: `{"version":1,"credentials":[
				{"purpose":"platform_admin","principal_id":"ops",
				 "key_ref":"env:ADMIN_KEY","key_ref":"env:CHAT_KEY"}]}`,
		},
		{
			name: "a credential's allowed_app_ids",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a",
				 "allowed_app_ids":["assistant"],"allowed_app_ids":["payroll"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}]}`,
		},
		{
			name: "an entitlement's tenant_id",
			manifest: `{"version":1,
				"credentials":[
					{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}],
				"tenant_entitlements":[{"tenant_id":"tenant-a","tenant_id":"tenant-b"}]}`,
		},
		{
			name: "an entitlement's allowed_secret_refs",
			manifest: `{"version":1,
				"credentials":[
					{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}],
				"tenant_entitlements":[{"tenant_id":"tenant-a",
					"allowed_secret_refs":["env:TENANT_A_MODEL_KEY"],
					"allowed_secret_refs":[]}]}`,
		},
		{
			name: "an entitlement's allowed_policy_refs",
			manifest: `{"version":1,
				"credentials":[
					{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}],
				"tenant_entitlements":[{"tenant_id":"tenant-a",
					"allowed_policy_refs":["builtin.safe-tools"],
					"allowed_policy_refs":["builtin.safe-tools"]}]}`,
		},
		{
			// Not a literal repeat, and the same defect: the decoder matches a
			// member to a field with ASCII case folded, so both of these land in
			// TenantID and the second wins.
			name: "a credential's tenant_id in another case",
			manifest: `{"version":1,"credentials":[
				{"purpose":"tenant_admin","principal_id":"ops",
				 "key_ref":"env:TENANT_ADMIN_KEY",
				 "tenant_id":"tenant-a","TENANT_ID":"tenant-b"},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}]}`,
		},
		{
			name: "the version in another case",
			manifest: `{"version":1,"VERSION":2,"credentials":[
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}]}`,
		},
		{
			// The decoder's fold is not ASCII-only. The long s is in the same
			// unicode.SimpleFold set as "s", so both spellings land in Purpose and
			// this chat credential is read as an administrator of the platform.
			name: "a credential's purpose spelled with a long s",
			manifest: `{"version":1,"credentials":[
				{"purpose":"chat","purpo\u017Fe":"platform_admin",
				 "principal_id":"ops","key_ref":"env:ADMIN_KEY"}]}`,
		},
		{
			// The Kelvin sign folds with "k", so this names key_ref twice and the
			// credential authenticates from the second variable rather than the
			// first.
			name: "a credential's key_ref spelled with a Kelvin sign",
			manifest: `{"version":1,"credentials":[
				{"purpose":"platform_admin","principal_id":"ops",
				 "key_ref":"env:ADMIN_KEY","\u212Aey_ref":"env:CHAT_KEY"}]}`,
		},
		{
			name: "the version spelled with a long s",
			manifest: `{"version":1,"ver\u017Fion":2,"credentials":[
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}]}`,
		},
		{
			// Neither spelling is the ASCII-lower-case one, so a fold that only
			// walks A-Z has nothing to key them together by.
			name: "an entitlement's allowed_secret_refs in mixed case and a long s",
			manifest: `{"version":1,
				"credentials":[
					{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}],
				"tenant_entitlements":[{"tenant_id":"tenant-a",
					"ALLOWED_SECRET_REFS":["env:TENANT_A_MODEL_KEY"],
					"allowed_\u017Fecret_refs":[]}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vars := writeManifest(t, tc.manifest, validEnv())

			cfg, err := Load(vars.getenv, tool.Builtin())
			require.Nil(t, cfg)
			require.ErrorContains(t, err, repeated)
			requireNoSecrets(t, err.Error())
		})
	}

	// The rule is per object, not per file. Every credential in validManifest
	// names purpose, principal_id and key_ref, and sibling objects sharing member
	// names is simply what a list looks like.
	_, err := Load(writeManifest(t, validManifest, validEnv()).getenv, tool.Builtin())
	require.NoError(t, err)
}

// The scan folds member names the way the decoder does, past ASCII.
//
// encoding/json looks a member name up against the struct's fields exactly and
// then, failing that, through a fold it documents as equivalent to
// bytes.EqualFold — which sends every non-ASCII rune through
// unicode.SimpleFold. A member spelled with a rune from the same fold set as an
// ASCII letter is therefore matched to the field the ASCII spelling names, is
// not reported by DisallowUnknownFields, and wins over the spelling written
// before it. A scan that folded only A-Z would file the two spellings apart and
// pass the document.
//
// The premise is asserted first, against this build's own schema and the same
// decoder settings readManifest uses, so this stays a rule rather than a list of
// interesting strings: if a later Go moves the fold, the failure says which of
// the two halves moved.
func TestRepeatedMemberFoldingIsNotASCIIOnly(t *testing.T) {
	t.Run("the decoder collapses both spellings onto one field", func(t *testing.T) {
		decoder := json.NewDecoder(strings.NewReader(
			`{"purpose":"chat","purpo\u017Fe":"platform_admin",` +
				`"principal_id":"ops","key_ref":"env:ADMIN_KEY"}`))
		decoder.DisallowUnknownFields()
		var entry credentialEntry
		require.NoError(t, decoder.Decode(&entry),
			"DisallowUnknownFields does not see a non-ASCII fold alias")
		require.Equal(t, purposePlatformAdmin, entry.Purpose,
			"the second spelling won, which is what the scan exists to catch")
	})

	for _, tc := range []struct {
		name    string
		written string
		alias   string
	}{
		{"the long s folds with s", "purpose", "purpo\u017Fe"},
		{"the Kelvin sign folds with k", "key_ref", "\u212Aey_ref"},
		{"the long s, against an upper-case spelling", "PURPOSE", "purpo\u017Fe"},
		{"the Kelvin sign, against an upper-case spelling", "KEY_REF", "\u212Aey_ref"},
		{
			"more than one folded rune in a name",
			"allowed_secret_refs", "allowed_\u017Fecret_ref\u017F",
		},
		// A fold set with no ASCII member in it at all, which an ASCII fold
		// cannot key together even in principle.
		{"final sigma folds with sigma", "me\u03C3\u03C3age", "ME\u03C2\u03C2AGE"},
		{"the angstrom sign folds with A-with-ring", "\u00C5", "\u212B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NotEqual(t, tc.written, tc.alias,
				"the case is only meaningful if the two spellings differ")
			require.True(t, bytes.EqualFold([]byte(tc.written), []byte(tc.alias)),
				"the case is only meaningful if encoding/json folds them together")

			document := fmt.Sprintf(`{%q:1,%q:2}`, tc.written, tc.alias)
			require.ErrorContains(t, rejectRepeatedMembers([]byte(document)),
				"repeats an earlier member")
		})
	}

	// Folding is not collapsing: names that merely look alike stay distinct, so
	// the rule does not start refusing manifests that say two different things.
	require.NoError(t, rejectRepeatedMembers([]byte(
		`{"tenant_id":1,"tenantid":2,"tenant-id":3,"tenant_\u0131d":4,"tenant_id2":5}`)))
}

// The refusal locates the repeat without quoting it.
//
// This is asserted against the scanner rather than through Load because Load
// wraps it in the manifest path, and a temp path in a test is full of words that
// would make the assertion pass or fail for the wrong reason. The rule it holds
// is the scanner's own: a member name is the operator's text, this error is
// written straight to a startup log, and the two should not meet. The names
// below are ones no schema of this build has, which is the case that matters —
// the scan reads the whole file, so it is the one place a member name of
// unknown provenance could be picked up and repeated.
func TestRepeatedMemberErrorsDoNotEchoTheMember(t *testing.T) {
	for _, document := range []string{
		`{"AKIAIOSFODNN7EXAMPLE":1,"AKIAIOSFODNN7EXAMPLE":2}`,
		`{"outer":{"sk-live-0123456789":1,"sk-live-0123456789":2}}`,
		`{"outer":[{"nested":1},{"password":"a","password":"b"}]}`,
	} {
		err := rejectRepeatedMembers([]byte(document))
		require.Error(t, err)
		require.ErrorContains(t, err, "repeats an earlier member")
		for _, member := range []string{
			"AKIAIOSFODNN7EXAMPLE", "sk-live-0123456789", "password", "outer", "nested",
		} {
			require.NotContains(t, err.Error(), member)
		}
	}

	// A document with nothing repeated passes, at every depth and through every
	// shape the scanner has to walk to get there.
	require.NoError(t, rejectRepeatedMembers([]byte(`{
		"version":1,
		"scalars":[1,"two",true,null,3.5],
		"empty_object":{},
		"empty_list":[],
		"nested":[{"a":{"b":[{"c":1,"d":2}]}},{"a":{"b":[{"c":3,"d":4}]}}]
	}`)))
}

func TestLoadRejectsAnUnusableManifestFile(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		vars := env{ConfigFileEnvVar: filepath.Join(t.TempDir(), "nope.json")}
		_, err := Load(vars.getenv, tool.Builtin())
		require.ErrorIs(t, err, os.ErrNotExist)
		require.ErrorContains(t, err, ConfigFileEnvVar)
	})

	t.Run("a directory", func(t *testing.T) {
		vars := env{ConfigFileEnvVar: t.TempDir()}
		_, err := Load(vars.getenv, tool.Builtin())
		require.Error(t, err)
		require.ErrorContains(t, err, ConfigFileEnvVar)
	})

	t.Run("unreadable", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads anything")
		}
		path := filepath.Join(t.TempDir(), "security.json")
		require.NoError(t, os.WriteFile(path, []byte(validManifest), 0o000))
		vars := env{ConfigFileEnvVar: path}
		_, err := Load(vars.getenv, tool.Builtin())
		require.ErrorIs(t, err, os.ErrPermission)
	})

	t.Run("oversized", func(t *testing.T) {
		body := `{"version":1,"credentials":[],"tenant_entitlements":[]}` +
			strings.Repeat(" ", maxManifestBytes)
		vars := writeManifest(t, body, nil)
		_, err := Load(vars.getenv, tool.Builtin())
		require.ErrorContains(t, err, "larger than")
	})

	t.Run("just inside the bound", func(t *testing.T) {
		// The same file one byte under the limit is read and then refused on its
		// contents, which shows the bound is on size and not on padding.
		body := `{"version":1,"credentials":[]}` +
			strings.Repeat(" ", maxManifestBytes-len(`{"version":1,"credentials":[]}`))
		vars := writeManifest(t, body, nil)
		_, err := Load(vars.getenv, tool.Builtin())
		require.ErrorContains(t, err, "declares no credentials")
	})
}

// A path that is set to whitespace is a path an operator meant to set. Treating
// it as unset would silently drop them into the demo profile with a manifest
// they believe is in force.
func TestLoadRefusesABlankPathRatherThanFallingBack(t *testing.T) {
	// The environment is otherwise a working demo: the admin key is set, so the
	// demo profile would load cleanly. That is what makes this test worth
	// writing — the failure it guards is not an error message, it is a process
	// that starts successfully on a configuration nobody selected.
	for _, path := range []string{" ", "   ", "\t", "\n", " \t\n "} {
		cfg, err := Load(env{
			ConfigFileEnvVar:  path,
			AdminAPIKeyEnvVar: testAdminKey,
		}.getenv, tool.Builtin())

		require.Nil(t, cfg)
		require.ErrorContains(t, err, ConfigFileEnvVar)
		// Not the demo's complaint about a missing admin key, and not the demo's
		// success either: a blank path is refused on its own terms.
		require.NotContains(t, err.Error(), AdminAPIKeyEnvVar)
	}

	// Unset still means the demo, which is what keeps this a rule about blank
	// values rather than a second way to fail to start.
	cfg, err := Load(env{AdminAPIKeyEnvVar: testAdminKey}.getenv, tool.Builtin())
	require.NoError(t, err)
	require.Contains(t, cfg.Description, "demo configuration")

	// And an empty string is unset: the variable exported with no value is the
	// same as never exported, because that is what every shell produces for it.
	cfg, err = Load(env{
		ConfigFileEnvVar:  "",
		AdminAPIKeyEnvVar: testAdminKey,
	}.getenv, tool.Builtin())
	require.NoError(t, err)
	require.Contains(t, cfg.Description, "demo configuration")
}

// The path is used as written. A value with a stray space around a real path is
// not trimmed into the path it resembles: the file that was named is the file
// that is read, and if that file does not exist the process says so rather than
// loading a neighbour.
func TestLoadDoesNotTrimTheConfiguredPath(t *testing.T) {
	vars := writeManifest(t, validManifest, validEnv())
	vars[ConfigFileEnvVar] = " " + vars[ConfigFileEnvVar] + " "

	cfg, err := Load(vars.getenv, tool.Builtin())
	require.Nil(t, cfg)
	require.ErrorContains(t, err, ConfigFileEnvVar)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// Two credentials that resolve to the same string are refused even though their
// variables differ. Without this, exporting the admin key into a second variable
// read by a chat credential would make one key act as two, and which one a
// request got would depend on which authenticator saw it first.
func TestLoadRejectsTwoVariablesHoldingTheSameKey(t *testing.T) {
	manifest := `{"version":1,"credentials":[
		{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
		 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
		{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
	]}`
	vars := writeManifest(t, manifest, env{
		"CHAT_KEY":  testAdminKey,
		"ADMIN_KEY": testAdminKey,
	})

	cfg, err := Load(vars.getenv, tool.Builtin())
	require.Nil(t, cfg)
	require.ErrorContains(t, err, "resolve to the same key")
	requireNoSecrets(t, err.Error())
}

// The converse must stay possible: one principal holding two different keys is
// a rotation in progress, and a platform that refused it would be a platform
// whose credentials cannot be rotated without downtime.
func TestLoadAcceptsTwoKeysForOnePrincipal(t *testing.T) {
	const rotating = `{"version":1,"credentials":[
		{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
		 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
		{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"},
		{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY_NEXT"}
	]}`
	const nextAdminKey = "test-admin-key-next-0123456789abc"
	vars := writeManifest(t, rotating, env{
		"CHAT_KEY":       testChatKey,
		"ADMIN_KEY":      testAdminKey,
		"ADMIN_KEY_NEXT": nextAdminKey,
	})

	cfg, err := Load(vars.getenv, tool.Builtin())
	require.NoError(t, err)
	for _, key := range []string{testAdminKey, nextAdminKey} {
		granted, err := cfg.Admin.AuthenticateAdmin(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, "ops", granted.PrincipalID)
	}
}

// The separation rule, in both its forms. A tenant entitled to an environment
// variable can publish a revision whose model sends that variable's contents to
// an endpoint the tenant chose, so the set of variables a tenant may name must
// exclude this platform's own credentials and its whole namespace.
func TestLoadRefusesToEntitleATenantToAPlatformVariable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ref     string
		env     env
		wantErr string
	}{
		{
			name:    "a variable holding a credential of this manifest",
			ref:     "env:ADMIN_KEY",
			wantErr: "holds a platform credential",
		},
		{
			name:    "the chat credential of this manifest",
			ref:     "env:CHAT_KEY",
			wantErr: "holds a platform credential",
		},
		{
			name:    "the reserved namespace",
			ref:     "env:TRPC_SERVICE_ADMIN_API_KEY",
			wantErr: "reserved TRPC_SERVICE_ namespace",
		},
		{
			name: "the reserved namespace, variable absent",
			// Absence is not a defence: the variable may exist on the next
			// deployment, and the rule is about the name.
			ref:     "env:TRPC_SERVICE_ANYTHING_AT_ALL",
			wantErr: "reserved TRPC_SERVICE_ namespace",
		},
		{
			name:    "the reserved prefix alone",
			ref:     "env:TRPC_SERVICE_",
			wantErr: "reserved TRPC_SERVICE_ namespace",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := `{"version":1,"credentials":[
				{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
				 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
				{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
			],"tenant_entitlements":[
				{"tenant_id":"tenant-a","allowed_secret_refs":["` + tc.ref + `"]}
			]}`
			vars := writeManifest(t, manifest, validEnv())

			cfg, err := Load(vars.getenv, tool.Builtin())
			require.Nil(t, cfg)
			require.ErrorContains(t, err, tc.wantErr)
			requireNoSecrets(t, err.Error())
		})
	}
}

// The rule is by exact name. A near miss is a different variable and is
// allowed — which is the point of not normalizing: a matching rule cleverer
// than the lookup it guards is a rule with a gap in it.
func TestEntitlementSeparationMatchesExactNamesOnly(t *testing.T) {
	manifest := `{"version":1,"credentials":[
		{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
		 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
		{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
	],"tenant_entitlements":[
		{"tenant_id":"tenant-a","allowed_secret_refs":[
			"env:chat_key","env:ADMIN_KEY_2","env:MY_ADMIN_KEY","env:trpc_service_key"
		]}
	]}`
	vars := writeManifest(t, manifest, validEnv())

	cfg, err := Load(vars.getenv, tool.Builtin())
	require.NoError(t, err)
	// And they are entitled as written, with no folding on the lookup side
	// either.
	require.NoError(t, cfg.Revisions.AuthorizeRevision("tenant-a", tenant.RevisionConfig{
		Model: tenant.ModelConfig{SecretRef: "env:chat_key"},
	}))
	require.ErrorIs(t, cfg.Revisions.AuthorizeRevision("tenant-a", tenant.RevisionConfig{
		Model: tenant.ModelConfig{SecretRef: "env:CHAT_KEY"},
	}), ErrNotEntitled)
}

// A tenant that does not exist in the database yet may be configured: the
// manifest is read before storage is even opened, so it cannot check, and
// requiring the tenant first would make the configuration order depend on the
// deployment order.
func TestLoadAcceptsAnEntitlementForATenantThatDoesNotExistYet(t *testing.T) {
	manifest := `{"version":1,"credentials":[
		{"purpose":"chat","principal_id":"u","key_ref":"env:CHAT_KEY",
		 "tenant_id":"tenant-a","allowed_app_ids":["assistant"]},
		{"purpose":"platform_admin","principal_id":"ops","key_ref":"env:ADMIN_KEY"}
	],"tenant_entitlements":[
		{"tenant_id":"tenant-not-created-yet","allowed_policy_refs":["builtin.safe-tools"]}
	]}`
	vars := writeManifest(t, manifest, validEnv())

	cfg, err := Load(vars.getenv, tool.Builtin())
	require.NoError(t, err)
	require.NoError(t, cfg.Revisions.AuthorizeRevision(
		"tenant-not-created-yet",
		tenant.RevisionConfig{PolicyRefs: []string{tool.PolicySafeTools}},
	))
}

// The demo profile: what it grants, and the one thing it refuses to invent.
func TestLoadDemoProfile(t *testing.T) {
	t.Run("the admin key has no default", func(t *testing.T) {
		_, err := Load(env{}.getenv, tool.Builtin())
		require.ErrorContains(t, err, AdminAPIKeyEnvVar)
		require.ErrorContains(t, err, "no default")
	})

	t.Run("an empty admin key is not a key", func(t *testing.T) {
		_, err := Load(env{AdminAPIKeyEnvVar: ""}.getenv, tool.Builtin())
		require.ErrorContains(t, err, AdminAPIKeyEnvVar)
	})

	t.Run("a weak admin key is refused", func(t *testing.T) {
		_, err := Load(env{AdminAPIKeyEnvVar: "0123456789abcdef"}.getenv, tool.Builtin())
		require.ErrorContains(t, err, "build admin authenticator")
	})

	t.Run("chat falls back to the published key", func(t *testing.T) {
		cfg, err := Load(env{AdminAPIKeyEnvVar: testAdminKey}.getenv, tool.Builtin())
		require.NoError(t, err)

		granted, err := cfg.Chat.Authenticate(context.Background(), DevelopmentChatAPIKey)
		require.NoError(t, err)
		require.Equal(t, platformconfig.DemoTenantID, granted.TenantID)
		require.Equal(t, platformconfig.DemoPrincipalID, granted.PrincipalID)
		require.True(t, granted.AllowsApp(platformconfig.DemoAgentAppID))
		require.False(t, granted.AllowsApp("another-app"))

		admin, err := cfg.Admin.AuthenticateAdmin(context.Background(), testAdminKey)
		require.NoError(t, err)
		require.Equal(t, identity.RolePlatformAdmin, admin.Role)
		require.Equal(t, DemoPlatformAdminPrincipalID, admin.PrincipalID)
		require.Empty(t, admin.TenantID)
		// The demo admin is a platform admin, so it is not bound to the demo
		// tenant and the demo chat key is not an admin credential.
		_, err = cfg.Admin.AuthenticateAdmin(context.Background(), DevelopmentChatAPIKey)
		require.ErrorIs(t, err, identity.ErrUnauthenticated)

		require.Contains(t, cfg.Description, "published development key")
	})

	t.Run("a configured chat key replaces the published one", func(t *testing.T) {
		cfg, err := Load(env{
			ChatAPIKeyEnvVar:  testChatKey,
			AdminAPIKeyEnvVar: testAdminKey,
		}.getenv, tool.Builtin())
		require.NoError(t, err)

		granted, err := cfg.Chat.Authenticate(context.Background(), testChatKey)
		require.NoError(t, err)
		require.Equal(t, platformconfig.DemoTenantID, granted.TenantID)
		// Replaced, not added next to: the documented placeholder must stop
		// working the moment a real key is configured.
		_, err = cfg.Chat.Authenticate(context.Background(), DevelopmentChatAPIKey)
		require.ErrorIs(t, err, identity.ErrUnauthenticated)
		require.NotContains(t, cfg.Description, "published development key")
	})

	t.Run("the demo tenant is entitled to safe tools and to no secret", func(t *testing.T) {
		cfg, err := Load(env{AdminAPIKeyEnvVar: testAdminKey}.getenv, tool.Builtin())
		require.NoError(t, err)

		require.NoError(t, cfg.Revisions.AuthorizeRevision(
			platformconfig.DemoTenantID,
			tenant.RevisionConfig{PolicyRefs: []string{tool.PolicySafeTools}},
		))
		// No secret at all: the demo agent is deterministic, so a demo that could
		// name a credential would be a demo that could reach one.
		require.ErrorIs(t, cfg.Revisions.AuthorizeRevision(
			platformconfig.DemoTenantID,
			tenant.RevisionConfig{Model: tenant.ModelConfig{SecretRef: "env:ANY_KEY"}},
		), ErrNotEntitled)
		// And no other tenant is entitled to anything.
		require.ErrorIs(t, cfg.Revisions.AuthorizeRevision(
			"tenant-a",
			tenant.RevisionConfig{PolicyRefs: []string{tool.PolicySafeTools}},
		), ErrNotEntitled)
	})

	t.Run("the demo chat key is refused as an admin credential", func(t *testing.T) {
		// The published key is 34 characters, past the admin floor, so this is a
		// real crossing attempt rather than one the length check would stop.
		require.GreaterOrEqual(t, len(DevelopmentChatAPIKey), 32)
		cfg, err := Load(env{AdminAPIKeyEnvVar: testAdminKey}.getenv, tool.Builtin())
		require.NoError(t, err)
		_, err = cfg.Admin.AuthenticateAdmin(context.Background(), DevelopmentChatAPIKey)
		require.ErrorIs(t, err, identity.ErrUnauthenticated)
	})
}

// Load's own arguments are checked. A nil registry would make every policy ref
// unknown, which would be a refusal — but a nil getenv would make every
// credential missing, and the shape of that failure should not be left to
// whichever line dereferences it first.
func TestLoadRequiresItsInputs(t *testing.T) {
	_, err := Load(nil, tool.Builtin())
	require.ErrorContains(t, err, "environment lookup is required")

	_, err = Load(env{AdminAPIKeyEnvVar: testAdminKey}.getenv, nil)
	require.ErrorContains(t, err, "policy registry is required")
}

// requireNoSecrets is the assertion every startup error in this file makes: an
// operator's terminal, their CI log and their scrollback are all places a
// credential must never reach.
func requireNoSecrets(t *testing.T, message string) {
	t.Helper()
	for _, secret := range []string{
		testChatKey, testAdminKey, testTenantAdminKey, testModelKey,
	} {
		require.NotContains(t, message, secret, "an error carried a credential")
	}
}
