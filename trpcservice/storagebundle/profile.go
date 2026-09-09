package storagebundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Profile is one immutable storage arrangement, named by a tenant-scoped id.
//
// The id is the version. Two Profiles with the same (TenantID, ID) must be the
// same Profile forever: a Runtime that was built against one, and a session
// pinned to the revision that named it, would otherwise be moved to different
// storage by an edit nobody re-published. Router enforces that contract on
// every resolution by comparing Fingerprint, and reports a violation rather
// than following it.
//
// A Profile carries references, never values. Resolving a reference to a
// connection string happens in a Factory, after the revision has been
// authorized, and never here.
type Profile struct {
	TenantID string `json:"tenant_id"`
	ID       string `json:"id"`
	// Session describes the conversation store. It is a sub-struct rather than
	// a flat set of fields so a Memory or Artifact spec can be added beside it
	// without reshaping what is already written down.
	Session SessionSpec `json:"session"`
}

// SessionSpec describes one upstream session backend. Exactly one of the
// backend-specific structs is set, and it is the one Backend names.
type SessionSpec struct {
	Backend  sessionbackend.Backend `json:"backend"`
	Postgres *PostgresSpec          `json:"postgres,omitempty"`
	Redis    *RedisSpec             `json:"redis,omitempty"`
}

// PostgresSpec describes a PostgreSQL session store.
type PostgresSpec struct {
	// DSNRef names the credential holding the connection string. It is a
	// secret reference ("env:VAR"), never the DSN itself.
	DSNRef      string `json:"dsn_ref"`
	Schema      string `json:"schema,omitempty"`
	TablePrefix string `json:"table_prefix,omitempty"`
}

// RedisSpec describes a Redis session store.
type RedisSpec struct {
	// URLRef names the credential holding the connection URL. It is a secret
	// reference ("env:VAR"), never the URL itself.
	URLRef    string `json:"url_ref"`
	KeyPrefix string `json:"key_prefix,omitempty"`
}

// placeholderConnection stands in for the connection string a Profile does not
// carry, so the namespacing rules can be checked by the package that owns them.
//
// sessionbackend.Config.Validate is the only definition of what a table
// prefix, a schema or a key prefix may contain — the upstream options panic on
// input they dislike, so those rules have to hold before a Factory reaches
// them. Restating them here would leave two copies to drift, so the config
// handed to Validate is exactly the one a Factory will build, with this in the
// single field a Profile replaces with a reference.
const placeholderConnection = "storagebundle://validate-shape-only"

// Validate reports whether this Profile is well formed: a tenant-scoped
// identity, a known backend, and exactly the settings that backend takes.
//
// It contacts nothing and resolves nothing. A Profile that validates can still
// name a variable that is unset, or a database that is unreachable.
func (p Profile) Validate() error {
	if err := (tenant.TenantContext{TenantID: p.TenantID}).Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidProfile, err)
	}
	if err := tenant.ValidateResourceID("backend profile id", p.ID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidProfile, err)
	}
	return p.Session.validate()
}

// Fingerprint returns the content fingerprint of a valid Profile.
//
// It is derived the way RevisionConfig.Digest is — canonical JSON, SHA-256 —
// and for the same reason: it is compared byte for byte against the value a
// Bundle was built from, so it has to be a function of the content alone.
// An invalid Profile has no fingerprint, so content that could never be built
// cannot be recorded as if it had been.
func (p Profile) Fingerprint() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("%w: encode backend profile: %v", ErrInvalidProfile, err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// SecretRefs returns every credential reference this Profile names, in no
// particular order. It returns nothing for a backend that names none.
//
// It exists so the entitlement check has one definition. Three places have to
// ask "which credentials does this profile want": the admin API when a profile
// is created, the admin API again when a revision names one, and the Factory
// before it reads a single environment variable. A switch copied into each of
// them is a switch that grows a fourth backend in two of the three.
//
// The references themselves are names, not values, so this discloses nothing a
// Profile did not already carry in the clear. It reads both sub-structs rather
// than switching on Backend: a Profile carrying settings for a backend it does
// not name is refused by Validate, and requiring the entitlement for both is
// the fail-closed answer to one that is checked here first.
func (p Profile) SecretRefs() []string {
	var refs []string
	if p.Session.Postgres != nil && p.Session.Postgres.DSNRef != "" {
		refs = append(refs, p.Session.Postgres.DSNRef)
	}
	if p.Session.Redis != nil && p.Session.Redis.URLRef != "" {
		refs = append(refs, p.Session.Redis.URLRef)
	}
	return refs
}

// clone returns a Profile that shares no memory with p.
//
// SessionSpec keeps its backend settings behind pointers, so copying a Profile
// by assignment copies the pointers and not what they point at. Every boundary
// that stores a Profile or hands one out has to break that sharing, because the
// id is the version: a caller still holding one of those pointers could change
// the content of a stored profile without publishing a new id, and Router would
// then report that profile as ErrProfileChanged — permanently, since it neither
// rebuilds nor evicts — for an edit nobody made through the source.
//
// This is a defence against sequential aliasing, not against a data race. A
// caller that writes to *p.Session.Postgres while this reads it has a race in
// its own program, and no copy taken here can make that defined.
func (p Profile) clone() Profile {
	if p.Session.Postgres != nil {
		postgres := *p.Session.Postgres
		p.Session.Postgres = &postgres
	}
	if p.Session.Redis != nil {
		redis := *p.Session.Redis
		p.Session.Redis = &redis
	}
	return p
}

func (s SessionSpec) validate() error {
	switch s.Backend {
	case sessionbackend.BackendInMemory:
		if s.Postgres != nil || s.Redis != nil {
			return fmt.Errorf(
				"%w: session backend %q takes no settings", ErrInvalidProfile, s.Backend)
		}
		return nil
	case sessionbackend.BackendPostgres:
		if s.Postgres == nil {
			return fmt.Errorf(
				"%w: session backend %q requires postgres settings",
				ErrInvalidProfile, s.Backend,
			)
		}
		if s.Redis != nil {
			return fmt.Errorf(
				"%w: session backend %q must not carry redis settings",
				ErrInvalidProfile, s.Backend,
			)
		}
		if err := validateSecretRef("postgres dsn_ref", s.Postgres.DSNRef); err != nil {
			return err
		}
		return checkShape(sessionbackend.Config{
			Backend: sessionbackend.BackendPostgres,
			Postgres: sessionbackend.PostgresConfig{
				DSN:         placeholderConnection,
				Schema:      s.Postgres.Schema,
				TablePrefix: s.Postgres.TablePrefix,
			},
		})
	case sessionbackend.BackendRedis:
		if s.Redis == nil {
			return fmt.Errorf(
				"%w: session backend %q requires redis settings",
				ErrInvalidProfile, s.Backend,
			)
		}
		if s.Postgres != nil {
			return fmt.Errorf(
				"%w: session backend %q must not carry postgres settings",
				ErrInvalidProfile, s.Backend,
			)
		}
		if err := validateSecretRef("redis url_ref", s.Redis.URLRef); err != nil {
			return err
		}
		return checkShape(sessionbackend.Config{
			Backend: sessionbackend.BackendRedis,
			Redis: sessionbackend.RedisConfig{
				URL:       placeholderConnection,
				KeyPrefix: s.Redis.KeyPrefix,
			},
		})
	case "":
		return fmt.Errorf("%w: session backend is required", ErrInvalidProfile)
	default:
		return fmt.Errorf(
			"%w: unknown session backend %q", ErrInvalidProfile, string(s.Backend))
	}
}

func checkShape(cfg sessionbackend.Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidProfile, err)
	}
	return nil
}

// validateSecretRef checks that a reference is a reference.
//
// The rejected value never reaches the error, which is secretref's rule and not
// a local preference: the likeliest way to get an invalid reference here is a
// connection string pasted in where its name belonged, and an error message is
// exactly the wrong place for it to resurface.
func validateSecretRef(field string, ref string) error {
	if ref == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidProfile, field)
	}
	if _, err := secretref.EnvName(ref); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrInvalidProfile, field, err)
	}
	return nil
}
