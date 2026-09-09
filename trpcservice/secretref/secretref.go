// Package secretref parses the platform's secret reference syntax.
//
// A secret reference names a credential; it never carries one. There is exactly
// one syntax — "env:VAR_NAME" — and exactly one parser for it, here, because
// two parsers are two sets of rules: a loader that accepted a form the runtime
// rejected (or worse, the other way round) would let a reference pass one
// boundary and mean something else at the next. Everything that reads a
// reference — the security manifest, the model builder, the entitlement check —
// goes through EnvName.
//
// No error in this package repeats the reference it rejected. A reference that
// is not a reference is most likely a key that was pasted into configuration by
// mistake, and an error message is exactly the wrong place for it to resurface.
package secretref

import (
	"errors"
	"strings"
)

// EnvScheme is the only reference scheme this platform resolves. A real secret
// manager arrives later; until then a reference that is not "env:VAR_NAME" is
// rejected rather than treated as a literal key.
const EnvScheme = "env:"

var (
	// ErrScheme reports a reference that does not use the env: scheme.
	ErrScheme = errors.New(
		"secretref: reference must use the " + EnvScheme + "VAR_NAME scheme")
	// ErrNoName reports the scheme with nothing after it.
	ErrNoName = errors.New("secretref: reference names no environment variable")
	// ErrInvalidName reports a name that cannot name a real environment
	// variable. The likeliest way to get one is a key pasted in behind the
	// scheme ("env:sk-..."), which is why the value never reaches the error.
	ErrInvalidName = errors.New(
		"secretref: reference must name a valid environment variable")
)

// EnvName returns the environment variable named by ref.
//
// The match is exact: no trimming, no case folding, no normalization. A
// reference either is the name it claims to be or it is refused, so two
// references are the same reference only when they are the same string.
func EnvName(ref string) (string, error) {
	name, found := strings.CutPrefix(ref, EnvScheme)
	if !found {
		return "", ErrScheme
	}
	if name == "" {
		return "", ErrNoName
	}
	if !IsEnvName(name) {
		return "", ErrInvalidName
	}
	return name, nil
}

// IsEnvName reports whether name is a POSIX-style environment variable name: a
// letter or underscore, followed by letters, digits or underscores. The empty
// string is not a name.
func IsEnvName(name string) bool {
	if name == "" {
		return false
	}
	for i, char := range name {
		switch {
		case char == '_':
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z':
		case char >= '0' && char <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
