package security

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// Environment variables the loader reads.
const (
	// ConfigFileEnvVar points at a custom security manifest. When it is set the
	// manifest is the whole configuration: ChatAPIKeyEnvVar and the published
	// development key do not participate, because a deployment that wrote a
	// manifest did not also mean to keep a second, ambient way in.
	ConfigFileEnvVar = "TRPC_SERVICE_SECURITY_CONFIG_FILE"
	// ChatAPIKeyEnvVar overrides the chat credential of the demo configuration.
	ChatAPIKeyEnvVar = "TRPC_SERVICE_API_KEY"
	// AdminAPIKeyEnvVar carries the platform admin credential of the demo
	// configuration. It has no fallback: see Load.
	AdminAPIKeyEnvVar = "TRPC_SERVICE_ADMIN_API_KEY"
)

// DevelopmentChatAPIKey is a published placeholder, not a secret. It exists so
// the demo chats without configuration, and it is safe only because the process
// refuses to bind anything but a loopback address.
//
// There is deliberately no admin counterpart. A published chat key can talk to
// one demo app; a published admin key would create tenants and publish
// revisions, so the admin credential must be supplied or the process does not
// start.
const DevelopmentChatAPIKey = "local-development-key-not-a-secret"

// DemoPlatformAdminPrincipalID is who the demo admin credential authenticates
// as. It belongs to no tenant, like every platform admin.
const DemoPlatformAdminPrincipalID = "local-admin"

// PolicyRegistry is the part of the tool registry the manifest is validated
// against: whether a policy ref names something this binary actually has.
//
// It is an interface so the security package depends on the question, not on
// the registry's construction. *tool.Registry satisfies it.
type PolicyRegistry interface {
	HasPolicy(ref string) bool
}

// Config is the resolved process security configuration: the three values every
// trust boundary in the process is built from.
//
// It holds no plaintext credential. Keys are read while it is being built and
// are retained only as SHA-256 digests inside the authenticators, so nothing
// here would disclose a credential if it were logged or dumped.
//
// It is also static for the life of the process. There is no reload: the
// manifest is read once, and a Runtime already cached by the resolver keeps the
// entitlement decision it was built under. Changing the file means restarting
// the process.
type Config struct {
	// Chat authenticates data-plane traffic.
	Chat identity.Authenticator
	// Admin authenticates control-plane traffic.
	Admin identity.AdminAuthenticator
	// Revisions decides which capabilities a tenant may name: in a revision
	// config, and in a backend storage profile. It is one value rather than two
	// so both questions are answered from the same table.
	Revisions CapabilityAuthorizer
	// Description is a value-free one-line summary for the startup log. It
	// reports presence and counts, never contents.
	Description string
}

// Load resolves the process security configuration.
//
// There are two sources and they do not mix. With ConfigFileEnvVar set, the
// manifest at that path is the configuration, and a missing, unreadable,
// oversized or invalid file fails the process — a security file that was meant
// to be in force and silently was not is worse than not starting. With it
// unset, the demo configuration applies: one chat credential, one platform
// admin credential, and a demo tenant entitled to the safe-tools policy and to
// no secret at all.
//
// Unset means empty, exactly. Every other value takes the manifest path, and a
// value that is only whitespace is refused there rather than falling back:
// falling back is how an operator ends up with the demo profile in production
// believing their manifest is in force.
//
// The demo admin credential comes from AdminAPIKeyEnvVar and from nowhere else.
// An unset variable is a refusal to start rather than a generated or published
// default: a default admin key is a published admin key the moment anyone reads
// the source.
//
// Nothing here touches a database, and it is called before storage is opened,
// so a security configuration that cannot be honored is reported before a
// connection is made rather than after.
func Load(getenv func(string) string, policies PolicyRegistry) (*Config, error) {
	if getenv == nil {
		return nil, errors.New("security: environment lookup is required")
	}
	if policies == nil {
		return nil, errors.New("security: policy registry is required")
	}
	path := getenv(ConfigFileEnvVar)
	if path == "" {
		return loadDemo(getenv, policies)
	}
	// Set to whitespace is set. An operator who exported this variable meant to
	// put a manifest in force, and the one outcome that must not follow is the
	// demo profile: it would start, log a description naming the demo, and serve
	// a configuration nobody asked for while the file they wrote sat unread. The
	// value is not trimmed before use either — a path with a stray space is not
	// silently guessed into the path it resembles.
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf(
			"security: %s is set to whitespace; point it at a security manifest, "+
				"or unset it to use the demo configuration", ConfigFileEnvVar)
	}
	parsed, err := readManifest(path)
	if err != nil {
		return nil, err
	}
	credentials, err := resolveCredentials(parsed.Credentials, getenv)
	if err != nil {
		return nil, err
	}
	entitlements, err := validateEntitlements(parsed.TenantEntitlements, credentials, policies)
	if err != nil {
		return nil, err
	}
	description := fmt.Sprintf(
		"manifest %q: %d credential(s), %d tenant entitlement(s)",
		path, len(credentials), len(parsed.TenantEntitlements),
	)
	return build(credentials, entitlements, description)
}

// loadDemo builds the zero-configuration local setup. It goes through exactly
// the same validation as a manifest — the same shape rules, the same registry
// check, the same duplicate-key check — so the path an operator runs first is
// not a path with fewer rules.
func loadDemo(getenv func(string) string, policies PolicyRegistry) (*Config, error) {
	chatKey := getenv(ChatAPIKeyEnvVar)
	chatEnvName := ChatAPIKeyEnvVar
	published := false
	if chatKey == "" {
		chatKey = DevelopmentChatAPIKey
		chatEnvName = ""
		published = true
	}
	adminKey := getenv(AdminAPIKeyEnvVar)
	if adminKey == "" {
		return nil, fmt.Errorf(
			"security: %s is not set; the demo configuration has no admin credential and "+
				"no default one, so set it or point %s at a security manifest",
			AdminAPIKeyEnvVar, ConfigFileEnvVar)
	}
	credentials := []resolvedCredential{
		{
			purpose:       purposeChat,
			principalID:   platformconfig.DemoPrincipalID,
			tenantID:      platformconfig.DemoTenantID,
			allowedAppIDs: []string{platformconfig.DemoAgentAppID},
			key:           chatKey,
			envName:       chatEnvName,
		},
		{
			purpose:     purposePlatformAdmin,
			principalID: DemoPlatformAdminPrincipalID,
			key:         adminKey,
			envName:     AdminAPIKeyEnvVar,
		},
	}
	// The demo tenant may run the safe-tools policy and nothing else, and is
	// entitled to no secret reference at all: the demo agent is deterministic,
	// so a demo that could name a credential would be a demo that could reach
	// one.
	entitlements, err := validateEntitlements(
		[]entitlementEntry{{
			TenantID:          platformconfig.DemoTenantID,
			AllowedPolicyRefs: []string{tool.PolicySafeTools},
		}},
		credentials,
		policies,
	)
	if err != nil {
		return nil, err
	}
	description := "demo configuration: 1 chat credential, 1 platform admin, 1 tenant entitlement"
	if published {
		description += fmt.Sprintf(
			"; %s is not set, so chat is served with the published development key",
			ChatAPIKeyEnvVar)
	}
	return build(credentials, entitlements, description)
}

// build turns resolved credentials into the two authenticators, after the
// checks that can only be made once every key is in hand.
//
// The duplicate-value check is the one that needs saying. Credentials are
// compared by the SHA-256 of the resolved key, not by the variable they came
// from, so two differently-named variables holding the same string are refused.
// Without it, exporting the admin key into a second variable that some tenant's
// chat credential reads would quietly make one credential act as two — and
// which one a request got would depend on which authenticator saw it first. The
// converse is not restricted: one principal holding two distinct keys is a key
// rotation, and rotation has to be possible.
//
// A manifest must carry at least one chat credential and one platform admin.
// Without a platform admin nobody could create a tenant, and the process would
// start into a control plane that cannot be administered; without a chat
// credential it serves nothing.
func build(
	credentials []resolvedCredential,
	entitlements *Entitlements,
	description string,
) (*Config, error) {
	chatGrants := make(map[string]identity.Identity)
	adminGrants := make(map[string]identity.AdminIdentity)
	seenKeys := make(map[[sha256.Size]byte]int, len(credentials))
	for index, credential := range credentials {
		digest := sha256.Sum256([]byte(credential.key))
		if first, repeated := seenKeys[digest]; repeated {
			return nil, fmt.Errorf(
				"security: credentials[%d] and credentials[%d] resolve to the same key",
				first, index)
		}
		seenKeys[digest] = index
		switch credential.purpose {
		case purposeChat:
			chatGrants[credential.key] = identity.Identity{
				TenantID:      credential.tenantID,
				PrincipalID:   credential.principalID,
				AllowedAppIDs: credential.allowedAppIDs,
			}
		case purposePlatformAdmin:
			adminGrants[credential.key] = identity.AdminIdentity{
				Role:        identity.RolePlatformAdmin,
				PrincipalID: credential.principalID,
			}
		case purposeTenantAdmin:
			adminGrants[credential.key] = identity.AdminIdentity{
				Role:        identity.RoleTenantAdmin,
				PrincipalID: credential.principalID,
				TenantID:    credential.tenantID,
			}
		default:
			return nil, fmt.Errorf(
				"security: credentials[%d] has unsupported purpose %q",
				index, credential.purpose)
		}
	}
	if len(chatGrants) == 0 {
		return nil, fmt.Errorf(
			"security: at least one %s credential is required", purposeChat)
	}
	if !hasPlatformAdmin(adminGrants) {
		return nil, fmt.Errorf(
			"security: at least one %s credential is required", purposePlatformAdmin)
	}
	chat, err := identity.NewStaticAPIKeyAuthenticator(chatGrants)
	if err != nil {
		return nil, fmt.Errorf("security: build chat authenticator: %w", err)
	}
	admin, err := identity.NewStaticAdminAPIKeyAuthenticator(adminGrants)
	if err != nil {
		return nil, fmt.Errorf("security: build admin authenticator: %w", err)
	}
	return &Config{
		Chat:        chat,
		Admin:       admin,
		Revisions:   entitlements,
		Description: description,
	}, nil
}

func hasPlatformAdmin(grants map[string]identity.AdminIdentity) bool {
	for _, granted := range grants {
		if granted.Role == identity.RolePlatformAdmin {
			return true
		}
	}
	return false
}
