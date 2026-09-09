// Package security loads the process security configuration and answers the
// one authorization question every trust boundary in the process asks of it:
// may this tenant use the capabilities its configuration names? The
// configuration is a revision config at one boundary and a backend storage
// profile at another, and both are answered from one table — a second table
// would be a gap to entitle a credential through.
//
// The package is deliberately small and static. It has no RBAC engine, no
// schema language and no hot reload: the manifest is read once at startup,
// validated as a whole, and turned into three values — a chat authenticator, an
// admin authenticator and a CapabilityAuthorizer — that never change for the
// life of the process.
package security

import (
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrNotEntitled reports a configuration naming a capability its tenant is not
// entitled to. The configuration is a revision config or a backend storage
// profile, and the sentinel does not say which: one message covers both
// questions this package answers, because wording that told a revision's
// refusal from a profile's would be a difference a caller could measure.
//
// It is a single sentinel with a single message, and the message names neither
// the rejected reference nor anything about it. That is the whole point of the
// check: a caller who could tell "unknown policy" from "known but not yours",
// or "this env var exists" from "it does not", would have a probe for the tool
// registry and the process environment built out of nothing but refusals.
var ErrNotEntitled = errors.New(
	"security: configuration is not entitled to a referenced capability")

// RevisionAuthorizer decides whether a revision may use the capabilities its
// config names. It is consulted before the tool registry and before any secret
// is resolved, so a refusal costs nothing and reveals nothing.
//
// It is an interface with one method because every call site — the admin API on
// create and on publish, and the runtime builder — has to ask the same question
// of the same value. A second implementation is a test fixture, not a variant
// policy.
type RevisionAuthorizer interface {
	// AuthorizeRevision reports whether tenantID may run config as written. A
	// config that names no secret and no policy needs no entitlement and is
	// always allowed.
	AuthorizeRevision(tenantID string, config tenant.RevisionConfig) error
}

// SecretRefAuthorizer decides whether a tenant may name one secret reference
// outside a revision config.
//
// A revision is not the only thing that can name a credential: a backend
// storage profile names the reference holding its database DSN or its Redis
// URL, and that reference has to go through the same table, by the same exact
// string, as a model key would. Without this the profile would be an
// unentitled second channel to the process environment — one tenant naming
// another tenant's variable and getting a working connection out of it.
type SecretRefAuthorizer interface {
	// AuthorizeSecretRef reports whether tenantID may name ref. An empty
	// reference is never entitled: there is no capability to grant, and the
	// caller that asked has a reference it should have rejected earlier.
	AuthorizeSecretRef(tenantID string, ref string) error
}

// CapabilityAuthorizer answers both capability questions the control plane
// asks. It is the interface the admin API, the runtime builder and the storage
// factory are wired with, so all of them consult one table: a process that
// answered "may this revision run" from one value and "may this tenant name
// this DSN" from another could entitle a credential through the gap between
// them.
type CapabilityAuthorizer interface {
	RevisionAuthorizer
	SecretRefAuthorizer
}

// Entitlements is the process entitlement table: which secret references and
// which tool policies each tenant may name.
//
// It is a value, not a registry: it is built once from the manifest and read
// concurrently for the rest of the process, so it needs no lock and offers no
// way to add a grant after startup.
type Entitlements struct {
	byTenant map[string]tenantEntitlement
}

type tenantEntitlement struct {
	secretRefs map[string]struct{}
	policyRefs map[string]struct{}
}

// Grant is one tenant's entitlement stated in code rather than in a manifest.
//
// It is the same grant the manifest's tenant_entitlements describes, and it goes
// through the same validation, so a fixture built here cannot express something
// the file format could not.
type Grant struct {
	TenantID   string
	SecretRefs []string
	PolicyRefs []string
}

// NewEntitlements builds an entitlement table from grants.
//
// It exists for callers that hold a capability configuration without a file: the
// gated live test that needs one real model key entitled, and the unit tests
// that exercise a revision naming a secret or a policy. Those cannot use a
// manifest — there is no file, and the key comes from the test environment — but
// they must not therefore run against a different authorization path than the
// process does, which is the whole reason this returns the same *Entitlements
// the loader builds rather than a permissive stand-in.
//
// The rules that do not need manifest context are enforced here, including the
// reserved-namespace rule: no grant, however it was built, may entitle a tenant
// to a TRPC_SERVICE_ variable. The manifest loader adds the one rule that does
// need context — that no grant names a variable holding one of this process's
// own credentials — because only it knows what those are.
func NewEntitlements(grants ...Grant) (*Entitlements, error) {
	table := &Entitlements{byTenant: make(map[string]tenantEntitlement, len(grants))}
	for _, grant := range grants {
		if err := table.add("entitlement for tenant "+quoteID(grant.TenantID), grant); err != nil {
			return nil, err
		}
	}
	return table, nil
}

// add validates one grant and records it.
//
// label names the grant in every error: the manifest passes
// "tenant_entitlements[2]" so an operator can find the line, and NewEntitlements
// passes the tenant id. One implementation of the rules, two ways of pointing at
// the thing that broke them.
func (e *Entitlements) add(label string, grant Grant) error {
	if err := tenant.ValidateResourceID("tenant_id", grant.TenantID); err != nil {
		return fmt.Errorf("security: %s: %w", label, err)
	}
	if _, repeated := e.byTenant[grant.TenantID]; repeated {
		return fmt.Errorf("security: %s repeats tenant %q", label, grant.TenantID)
	}
	secretRefs := make(map[string]struct{}, len(grant.SecretRefs))
	for _, ref := range grant.SecretRefs {
		envName, err := secretref.EnvName(ref)
		if err != nil {
			return fmt.Errorf("security: %s allowed_secret_refs: %w", label, err)
		}
		if _, repeated := secretRefs[ref]; repeated {
			return fmt.Errorf("security: %s repeats an allowed_secret_refs entry", label)
		}
		if strings.HasPrefix(envName, platformEnvPrefix) {
			return fmt.Errorf(
				"security: %s allowed_secret_refs names %q, "+
					"which is inside the reserved %s namespace",
				label, envName, platformEnvPrefix)
		}
		secretRefs[ref] = struct{}{}
	}
	policyRefs := make(map[string]struct{}, len(grant.PolicyRefs))
	for _, ref := range grant.PolicyRefs {
		if _, repeated := policyRefs[ref]; repeated {
			return fmt.Errorf("security: %s repeats an allowed_policy_refs entry", label)
		}
		policyRefs[ref] = struct{}{}
	}
	e.byTenant[grant.TenantID] = tenantEntitlement{secretRefs: secretRefs, policyRefs: policyRefs}
	return nil
}

// quoteID renders an identifier for a message. It is a helper only so that an
// empty tenant id reads as "" rather than vanishing from the sentence.
func quoteID(id string) string {
	return fmt.Sprintf("%q", id)
}

// DenyCapabilities returns an authorizer that entitles no tenant to anything.
//
// It is not "authorization turned off" — it is the strictest possible answer,
// and it exists so that a caller with no capability configuration has something
// explicit to pass. A revision that names no secret_ref and no policy_refs
// still runs under it, which is exactly the set of revisions that need no
// entitlement to begin with.
//
// A backend profile that names no credential is stored under it for the same
// reason, but that decision is not made here: AuthorizeSecretRef has no empty
// case, so a profile with nothing to check is one its caller never asks about.
func DenyCapabilities() *Entitlements {
	return &Entitlements{}
}

// AuthorizeRevision implements RevisionAuthorizer.
//
// The empty case is first and is not a special case: a revision that names no
// secret and no policy is asking for nothing, so there is nothing to entitle.
// Every other revision needs its tenant to have been granted every reference it
// names, one by one, by exact string.
func (e *Entitlements) AuthorizeRevision(
	tenantID string,
	config tenant.RevisionConfig,
) error {
	if e == nil {
		// A nil table entitles nothing, and says so the same way a populated one
		// would. Failing closed here means a caller that forgot to build the
		// table cannot accidentally get the permissive answer.
		if config.Model.SecretRef == "" && len(config.PolicyRefs) == 0 {
			return nil
		}
		return ErrNotEntitled
	}
	if config.Model.SecretRef == "" && len(config.PolicyRefs) == 0 {
		return nil
	}
	if config.Model.SecretRef != "" {
		if err := e.AuthorizeSecretRef(tenantID, config.Model.SecretRef); err != nil {
			return err
		}
	}
	// Looked up after the empty case, so a tenant that has no entitlements at
	// all is refused by the same path — and with the same error — as a tenant
	// that has some but not this one.
	granted := e.byTenant[tenantID]
	for _, ref := range config.PolicyRefs {
		if _, allowed := granted.policyRefs[ref]; !allowed {
			return ErrNotEntitled
		}
	}
	return nil
}

// AuthorizeSecretRef implements SecretRefAuthorizer.
//
// It is the same lookup AuthorizeRevision makes for a model key, against the
// same per-tenant table, by the same exact string — which is the point. The
// reserved-namespace rule and the platform-credential rule are enforced when
// the table is built, so no reference that reaches this map can name one of
// this process's own credentials however it was spelled.
//
// There is no empty case here. A revision that names nothing is asking for
// nothing, but an empty reference handed to this method is a caller that
// validated too little, and answering "allowed" would turn that mistake into
// an entitlement.
func (e *Entitlements) AuthorizeSecretRef(tenantID string, ref string) error {
	if e == nil || ref == "" {
		// A nil table entitles nothing, and says so the same way a populated one
		// would, so a caller that forgot to build the table cannot accidentally
		// get the permissive answer.
		return ErrNotEntitled
	}
	if _, allowed := e.byTenant[tenantID].secretRefs[ref]; !allowed {
		return ErrNotEntitled
	}
	return nil
}
