package security

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// manifestVersion is the only manifest version this binary understands.
//
// It is checked for equality, not for "at least". A file written for a later
// version may mean something this build would misread — a field that changed
// meaning reads as the old meaning, silently — so an unknown version is a
// refusal rather than a best effort.
const manifestVersion = 1

// maxManifestBytes bounds the file. The manifest is a handful of credentials
// and entitlements; anything larger is a wrong path or a runaway generator, and
// reading it into memory at startup is not something to do on trust.
const maxManifestBytes = 256 << 10

// platformEnvPrefix is the namespace this process reserves for its own
// configuration, including its own credentials. See validateEntitlements.
const platformEnvPrefix = "TRPC_SERVICE_"

// purpose is what a manifest credential is for. It is a closed set: a purpose
// this build does not know is not a purpose to ignore, because the entry would
// then be a credential that authenticates nothing while looking configured.
type purpose string

const (
	purposeChat          purpose = "chat"
	purposePlatformAdmin purpose = "platform_admin"
	purposeTenantAdmin   purpose = "tenant_admin"
)

// manifest is the on-disk security configuration.
//
// The encoding is strict in every direction that could hide a mistake: unknown
// fields are refused, a member named twice in one object is refused, a second
// JSON value in the file is refused, and the version must match exactly. A
// security file is the one file where "the parser ignored the part it did not
// recognise" is the worst possible behavior — the ignored part is the grant
// someone thought they had written.
type manifest struct {
	Version            int                `json:"version"`
	Credentials        []credentialEntry  `json:"credentials"`
	TenantEntitlements []entitlementEntry `json:"tenant_entitlements"`
}

// credentialEntry is one static API key: what it is for, who it authenticates
// as, and where its value is read from. The value is never in the file.
type credentialEntry struct {
	Purpose     purpose `json:"purpose"`
	PrincipalID string  `json:"principal_id"`
	KeyRef      string  `json:"key_ref"`
	// TenantID is required for chat and tenant_admin, and must be absent for
	// platform_admin.
	TenantID string `json:"tenant_id,omitempty"`
	// AllowedAppIDs is required for chat and must be absent for either admin
	// purpose: an admin credential addresses the control plane, which is scoped
	// by tenant, not by app.
	AllowedAppIDs []string `json:"allowed_app_ids,omitempty"`
}

// entitlementEntry is what one tenant's revisions may name. It is keyed by
// tenant, not by credential: entitlement is a property of the tenant whose
// revision is running, not of whoever happened to create it.
type entitlementEntry struct {
	TenantID          string   `json:"tenant_id"`
	AllowedSecretRefs []string `json:"allowed_secret_refs,omitempty"`
	AllowedPolicyRefs []string `json:"allowed_policy_refs,omitempty"`
}

// resolvedCredential is one credential after its key has been read out of the
// environment. It holds plaintext, so it exists only on the stack of a load and
// never reaches the Config that load returns.
type resolvedCredential struct {
	purpose       purpose
	principalID   string
	tenantID      string
	allowedAppIDs []string
	key           string
	// envName is the variable the key came from, or "" when the key is the
	// published demo placeholder. It is used for messages and for the
	// credential/entitlement separation check, never for lookup after this.
	envName string
}

// readManifest reads and decodes the file at path.
//
// Everything about the file is checked before its contents are trusted: that it
// exists, that it is a regular file, and that it is not larger than a security
// manifest has any reason to be. The handle is stat'ed rather than the path, so
// what is measured is what is read.
func readManifest(path string) (manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return manifest{}, fmt.Errorf(
			"security: open %s %q: %w", ConfigFileEnvVar, path, err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return manifest{}, fmt.Errorf(
			"security: read %s %q: %w", ConfigFileEnvVar, path, err)
	}
	if !info.Mode().IsRegular() {
		return manifest{}, fmt.Errorf(
			"security: %s %q is not a regular file", ConfigFileEnvVar, path)
	}
	// Limited to one byte past the bound, so a file that grew between the stat
	// and the read is caught by the length check rather than by the stat alone.
	data, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return manifest{}, fmt.Errorf(
			"security: read %s %q: %w", ConfigFileEnvVar, path, err)
	}
	if len(data) > maxManifestBytes {
		return manifest{}, fmt.Errorf(
			"security: %s %q is larger than %d bytes", ConfigFileEnvVar, path, maxManifestBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var parsed manifest
	if err := decoder.Decode(&parsed); err != nil {
		return manifest{}, fmt.Errorf(
			"security: %s %q is not a valid security manifest: %w",
			ConfigFileEnvVar, path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return manifest{}, fmt.Errorf(
			"security: %s %q must contain exactly one JSON object", ConfigFileEnvVar, path)
	}
	// After the decode, so the bytes are known to be well-formed and this reports
	// only what it is for; before the version check, so a file that states two
	// versions is not answered with a complaint about the second one.
	if err := rejectRepeatedMembers(data); err != nil {
		return manifest{}, fmt.Errorf(
			"security: %s %q is not a valid security manifest: %w",
			ConfigFileEnvVar, path, err)
	}
	if parsed.Version != manifestVersion {
		return manifest{}, fmt.Errorf(
			"security: %s %q declares version %d; this build supports version %d only",
			ConfigFileEnvVar, path, parsed.Version, manifestVersion)
	}
	return parsed, nil
}

// rejectRepeatedMembers refuses a manifest whose objects name a member twice,
// at any depth.
//
// encoding/json takes the last of a repeated member and reports nothing, so
// {"purpose":"chat","purpose":"platform_admin"} decodes as a platform admin
// with no sign that the file also said something else. That is the same failure
// DisallowUnknownFields exists to prevent — a grant the author wrote and the
// parser dropped — arriving through a member the decoder does recognise. The
// version, the credential list, an entitlement's secret refs: all of them are
// reachable this way, so the whole tree is scanned rather than the few places
// it would be most embarrassing.
//
// Names are compared with strings.EqualFold, because that is how the decoder
// matches a member to a struct field. encoding/json looks a name up exactly,
// then falls back to a folded lookup whose fold it documents as equivalent to
// bytes.EqualFold: ASCII case for ASCII, and unicode.SimpleFold for everything
// else. So "key_ref", "KEY_REF" and "\u212Aey_ref" — U+212A KELVIN SIGN, which
// unicode.SimpleFold puts in the same fold set as "k" — all land in KeyRef, and
// the last of them wins exactly as a literal repeat would. Folding only ASCII
// here would let the non-ASCII spellings through as distinct members, which is
// the same silent last-win with one more step in front of it. Two spellings of
// one field are one member here.
//
// No error names the member. A member name is the file's text rather than this
// package's, and a security manifest is a file whose contents have no business
// in a startup log; the byte offset locates the repeat without quoting it.
func rejectRepeatedMembers(data []byte) error {
	return scanJSONValue(json.NewDecoder(bytes.NewReader(data)))
}

// scanJSONValue consumes exactly one JSON value, checking every object in it.
func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isComposite := token.(json.Delim)
	if !isComposite {
		return nil
	}
	switch delimiter {
	case '{':
		if err := scanJSONObject(decoder); err != nil {
			return err
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		// A closing delimiter cannot open a value, and this runs only on bytes
		// that have already decoded.
		return fmt.Errorf("unexpected %q where a value was expected", delimiter)
	}
	// The matching '}' or ']'.
	_, err = decoder.Token()
	return err
}

// scanJSONObject consumes the members of an object whose '{' has been read.
//
// Names already seen are held as written and compared pairwise, rather than
// normalized into a map key. strings.EqualFold is the relation encoding/json
// defines its own fold against, so asking it directly keeps the two in step;
// reproducing the normalization here would mean maintaining a second copy of a
// Unicode rule whose only job is to agree with the first. The pairwise cost is
// bounded to nothing that matters: the file is capped at maxManifestBytes, this
// runs only after a decode that refused unknown fields — so every name reaching
// it is one of the handful the schema declares — and the first repeat returns.
func scanJSONObject(decoder *json.Decoder) error {
	var seen []string
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		name, isName := token.(string)
		if !isName {
			return errors.New("an object member is not named by a string")
		}
		for _, earlier := range seen {
			if strings.EqualFold(earlier, name) {
				return fmt.Errorf(
					"an object member ending at byte %d repeats an earlier member "+
						"of the same object", decoder.InputOffset())
			}
		}
		seen = append(seen, name)
		if err := scanJSONValue(decoder); err != nil {
			return err
		}
	}
	return nil
}

// resolveCredentials validates every credential entry and reads its key.
//
// No error below carries a key. They name the entry by index, the purpose, and
// where a missing value was looked for — which is what an operator needs to fix
// the file, and is all of it.
func resolveCredentials(
	entries []credentialEntry,
	getenv func(string) string,
) ([]resolvedCredential, error) {
	if len(entries) == 0 {
		return nil, errors.New("security: manifest declares no credentials")
	}
	resolved := make([]resolvedCredential, 0, len(entries))
	seenRefs := make(map[string]int, len(entries))
	for index, entry := range entries {
		if err := validateCredentialShape(index, entry); err != nil {
			return nil, err
		}
		envName, err := secretref.EnvName(entry.KeyRef)
		if err != nil {
			return nil, fmt.Errorf("security: credentials[%d] key_ref: %w", index, err)
		}
		if first, repeated := seenRefs[envName]; repeated {
			return nil, fmt.Errorf(
				"security: credentials[%d] repeats the key_ref of credentials[%d]",
				index, first)
		}
		seenRefs[envName] = index
		// An exported-but-empty variable is treated as unset: it means the
		// operator asked for a credential that is not there, and a zero-length
		// key would be refused by the authenticator anyway.
		value := getenv(envName)
		if value == "" {
			return nil, fmt.Errorf(
				"security: credentials[%d] key_ref environment variable %q is unset or empty",
				index, envName)
		}
		resolved = append(resolved, resolvedCredential{
			purpose:       entry.Purpose,
			principalID:   entry.PrincipalID,
			tenantID:      entry.TenantID,
			allowedAppIDs: append([]string(nil), entry.AllowedAppIDs...),
			key:           value,
			envName:       envName,
		})
	}
	return resolved, nil
}

// validateCredentialShape enforces the field combination each purpose requires.
// The absences are checked as strictly as the presences: a platform_admin entry
// carrying a tenant is a file whose author believed something about the grant
// that is not true, and accepting it while ignoring the field would confirm the
// belief.
func validateCredentialShape(index int, entry credentialEntry) error {
	if err := tenant.ValidateResourceID("principal_id", entry.PrincipalID); err != nil {
		return fmt.Errorf("security: credentials[%d]: %w", index, err)
	}
	switch entry.Purpose {
	case purposeChat:
		if err := tenant.ValidateResourceID("tenant_id", entry.TenantID); err != nil {
			return fmt.Errorf("security: credentials[%d]: %w", index, err)
		}
		if len(entry.AllowedAppIDs) == 0 {
			return fmt.Errorf(
				"security: credentials[%d] grants no allowed_app_ids", index)
		}
		seen := make(map[string]struct{}, len(entry.AllowedAppIDs))
		for _, appID := range entry.AllowedAppIDs {
			if err := tenant.ValidateResourceID("allowed_app_ids", appID); err != nil {
				return fmt.Errorf("security: credentials[%d]: %w", index, err)
			}
			if _, repeated := seen[appID]; repeated {
				return fmt.Errorf(
					"security: credentials[%d] repeats an allowed_app_ids entry", index)
			}
			seen[appID] = struct{}{}
		}
	case purposePlatformAdmin:
		if entry.TenantID != "" {
			return fmt.Errorf(
				"security: credentials[%d] is %s and must not carry tenant_id",
				index, purposePlatformAdmin)
		}
		if len(entry.AllowedAppIDs) != 0 {
			return fmt.Errorf(
				"security: credentials[%d] is %s and must not carry allowed_app_ids",
				index, purposePlatformAdmin)
		}
	case purposeTenantAdmin:
		if err := tenant.ValidateResourceID("tenant_id", entry.TenantID); err != nil {
			return fmt.Errorf("security: credentials[%d]: %w", index, err)
		}
		if len(entry.AllowedAppIDs) != 0 {
			return fmt.Errorf(
				"security: credentials[%d] is %s and must not carry allowed_app_ids",
				index, purposeTenantAdmin)
		}
	default:
		return fmt.Errorf(
			"security: credentials[%d] has unsupported purpose %q", index, entry.Purpose)
	}
	return nil
}

// validateEntitlements checks the entitlement table against the manifest's own
// credentials and against the running tool registry.
//
// Two rules here are not obvious and are the reason this is a separate pass.
//
// The first is separation: a tenant may not be entitled to any environment
// variable that holds one of this platform's credentials, nor to anything in
// the platform's own TRPC_SERVICE_ namespace. Without it, entitling a tenant to
// its model key would be enough to publish a revision whose secret_ref points
// at the admin key and have the runtime send it to an endpoint the tenant
// chose. The check is by exact name — no wildcards, no case folding, no
// normalization — because a matching rule that is cleverer than the lookup it
// guards is a rule with a gap in it.
//
// The second is that policy refs are validated against the registry now, at
// load, rather than at first use. A typo in a policy ref would otherwise be a
// tenant that appears entitled and is refused on every publish, and the
// difference between "not entitled" and "misspelled" is invisible by design at
// the point where it would be noticed.
func validateEntitlements(
	entries []entitlementEntry,
	credentials []resolvedCredential,
	policies PolicyRegistry,
) (*Entitlements, error) {
	credentialEnvNames := make(map[string]struct{}, len(credentials))
	for _, credential := range credentials {
		if credential.envName != "" {
			credentialEnvNames[credential.envName] = struct{}{}
		}
	}
	table := &Entitlements{byTenant: make(map[string]tenantEntitlement, len(entries))}
	for index, entry := range entries {
		label := fmt.Sprintf("tenant_entitlements[%d]", index)
		// The two rules that need what only this loader knows: which variables
		// hold this process's credentials, and which policies the binary has.
		// Everything else is Entitlements.add, so a grant built in code obeys the
		// same rules as one read from a file.
		for _, ref := range entry.AllowedSecretRefs {
			envName, err := secretref.EnvName(ref)
			if err != nil {
				// Deliberately not reported here: add says the same thing about
				// the same ref, and one message for one fault is enough.
				continue
			}
			if _, isCredential := credentialEnvNames[envName]; isCredential {
				return nil, fmt.Errorf(
					"security: %s allowed_secret_refs names %q, "+
						"which holds a platform credential", label, envName)
			}
		}
		for _, ref := range entry.AllowedPolicyRefs {
			if !policies.HasPolicy(ref) {
				return nil, fmt.Errorf(
					"security: %s allowed_policy_refs names unknown policy %q", label, ref)
			}
		}
		if err := table.add(label, Grant{
			TenantID:   entry.TenantID,
			SecretRefs: entry.AllowedSecretRefs,
			PolicyRefs: entry.AllowedPolicyRefs,
		}); err != nil {
			return nil, err
		}
	}
	return table, nil
}
