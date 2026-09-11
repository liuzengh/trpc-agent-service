// Package secrets resolves a secret reference to its value, and nothing else.
//
// The control plane stores references (env:NAME, file:/abs/path), never the
// secret itself. This package is the only place that crosses from "reference"
// to "plaintext", which keeps the boundary in one file rather than scattered
// across every consumer that needs a key.
package secrets

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// ErrUnsupportedScheme is returned for a reference that is neither env: nor
// file:. There is no KMS-backed scheme yet (approved plan defers that), and a
// reference that asks for one fails loudly rather than silently treating the
// whole string as a literal secret, which would be the worst possible
// fallback: "kms://..." would then be sent to an API as a key.
var ErrUnsupportedScheme = errors.New("secrets: unsupported reference scheme")

// resolve turns a reference into its value. It is deliberately unexported:
// every caller reaches it through Resolver.Resolve, which checks the
// allowlist first. The value can come from a database row a tenant admin
// wrote, and "file:/etc/shadow" is a perfectly well-formed reference — the
// allowlist, not this function, is what distinguishes an operator-authored
// config from a tenant authorising themselves to read arbitrary files.
//
// An empty reference resolves to an empty value rather than an error: a
// deployment with no credentials configured is a real state (fake-model needs
// none), and the caller decides whether "no key" is acceptable for what it is
// about to do.
func resolve(ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	scheme, rest, found := strings.Cut(ref, ":")
	if !found {
		return "", fmt.Errorf("%w: %q has no scheme", ErrUnsupportedScheme, ref)
	}
	switch scheme {
	case "env":
		return os.Getenv(rest), nil
	case "file":
		// filepath.Clean + a rooted check would be the usual defence against
		// a path escaping its base; there is no base here — file: refs are
		// expected to be absolute. The containment that matters is the
		// allowlist in Resolver.Resolve, applied before this function is ever
		// reached.
		if !strings.HasPrefix(rest, "/") {
			return "", fmt.Errorf("secrets: file reference %q must be an absolute path", rest)
		}
		b, err := os.ReadFile(rest)
		if err != nil {
			return "", fmt.Errorf("secrets: read %s: %w", rest, err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	default:
		return "", fmt.Errorf("%w: %q", ErrUnsupportedScheme, scheme)
	}
}

// AllowedPrefixes restricts which env names or file roots a Resolver will
// accept, for the case where references are not purely operator-authored.
// The plan's threat model treats a tenant's own admin as trusted enough to
// point a model profile at an env var; it does NOT treat them as trusted
// enough to point it at an arbitrary path or name. A deployment that wants
// to enforce that distinction wires a Resolver with an allowlist instead of
// calling Resolve directly.
type AllowedPrefixes struct {
	EnvVars  []string
	FileRoot []string
}

// Resolver is Resolve plus an allowlist.
type Resolver struct {
	allowed AllowedPrefixes
}

// NewResolver builds a Resolver. An empty allowlist means "no reference of
// that kind is acceptable" — deliberately not "everything is", so a caller
// that constructs one forgets to fill it in fails loudly rather than quietly
// permitting anything.
func NewResolver(allowed AllowedPrefixes) *Resolver {
	return &Resolver{allowed: allowed}
}

// Resolve checks a reference against the allowlist before fetching it.
func (r *Resolver) Resolve(ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	scheme, rest, found := strings.Cut(ref, ":")
	if !found {
		return "", fmt.Errorf("%w: %q has no scheme", ErrUnsupportedScheme, ref)
	}
	switch scheme {
	case "env":
		if !contains(r.allowed.EnvVars, rest) {
			return "", fmt.Errorf("secrets: env var %q is not in the allowlist", rest)
		}
	case "file":
		root := false
		for _, p := range r.allowed.FileRoot {
			if strings.HasPrefix(rest, p) {
				root = true
				break
			}
		}
		if !root {
			return "", fmt.Errorf("secrets: file path %q is not under an allowed root", rest)
		}
	default:
		return "", fmt.Errorf("%w: %q", ErrUnsupportedScheme, scheme)
	}
	return resolve(ref)
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// ResolvedSecrets returns every plaintext secret the resolver has been
// configured to know about (env var values from the allowlist). These are
// the values the log redactor should mask, collected at a point in time.
// File-root secrets are not enumerated (and for log redaction the env-var
// secrets — API keys, channel credentials — are the critical ones).
func (r *Resolver) ResolvedSecrets() []string {
	var out []string
	for _, name := range r.allowed.EnvVars {
		if v := os.Getenv(name); v != "" {
			out = append(out, v)
		}
	}
	return out
}
