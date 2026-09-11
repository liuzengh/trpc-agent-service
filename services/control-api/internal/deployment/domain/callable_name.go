package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
)

const (
	ProviderCallableNameVersionV1 = "provider-callable-name-v1"
	ProviderCallableNameLengthV1  = 63
)

var (
	ErrInvalidCallableEntry   = errors.New("invalid logical callable entry")
	ErrDuplicateCallableEntry = errors.New("duplicate logical callable entry")
	ErrCallableNameCollision  = errors.New("provider callable name collision")
	callableEntryPattern      = regexp.MustCompile(`^(tools|knowledge)/[a-z][a-z0-9_-]{0,63}$`)
)

// ProviderCallableNames resolves one node's complete logical entry set under
// provider-callable-name-v1. Each output is "fn_" followed by the first 60
// lowercase hexadecimal digits of SHA-256 over the exact UTF-8 entry ID.
// Category is part of that hash input: tools/search and knowledge/search are
// distinct identities, regardless of the remote provider's tool name.
//
// The function does not mutate entries and does not depend on enumeration
// order. Empty sets return an empty map. Invalid entries, duplicates, and a
// collision within this node reject the complete set without a partial result;
// callers must never resolve conflicts through suffixes or input truncation.
// Future Worker adapters use this same frozen mapping for registration and
// reverse dispatch, while the Manifest remains authoritative for node access.
func ProviderCallableNames(entries []string) (map[string]string, error) {
	return providerCallableNamesWithDigest(entries, sha256.Sum256)
}

// The digest seam makes the truncated-hash collision branch testable without
// permitting callers to replace the public V1 hash algorithm.
func providerCallableNamesWithDigest(entries []string, digest func([]byte) [32]byte) (map[string]string, error) {
	ordered := slices.Clone(entries)
	slices.Sort(ordered)
	resolved := make(map[string]string, len(ordered))
	owners := make(map[string]string, len(ordered))
	for _, entry := range ordered {
		if !callableEntryPattern.MatchString(entry) {
			return nil, fmt.Errorf("%w: %q", ErrInvalidCallableEntry, entry)
		}
		if _, exists := resolved[entry]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateCallableEntry, entry)
		}
		sum := digest([]byte(entry))
		name := "fn_" + hex.EncodeToString(sum[:30])
		if other, collision := owners[name]; collision {
			return nil, fmt.Errorf("%w: %q and %q", ErrCallableNameCollision, other, entry)
		}
		owners[name] = entry
		resolved[entry] = name
	}
	return resolved, nil
}
