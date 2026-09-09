//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
)

// LoadKeyRingFromEnv loads the envelope-encryption key ring used by all
// long-running enterprise processes. MASTER_KEY_RING is a JSON object keyed by
// stable key IDs, for example {"v1":"...","v2":"..."}; the active key
// is selected by ACTIVE_MASTER_KEY_ID. MASTER_KEY remains a compatibility
// fallback for installations that have not started a rotation yet.
//
// The returned map contains key material only in memory. Error messages never
// include key values.
func LoadKeyRingFromEnv(minimumLength int) (string, map[string]string, error) {
	resolver, err := NewEnvSecretResolver("TRPC_SECRET_")
	if err != nil {
		return "", nil, err
	}
	return LoadKeyRingWithResolver(context.Background(), os.LookupEnv, resolver, minimumLength)
}

// LoadKeyRing is the injectable form of LoadKeyRingFromEnv, intended for
// startup tests and secret-manager adapters.
func LoadKeyRing(getenv func(string) (string, bool), minimumLength int) (string, map[string]string, error) {
	return LoadKeyRingWithResolver(context.Background(), getenv, nil, minimumLength)
}

// LoadKeyRingWithResolver loads an encryption key ring while allowing an
// operator-owned SecretResolver to supply references instead of placing key
// material directly in the process environment. MASTER_KEY_RING_REF and
// MASTER_KEY_REF are reference-valued alternatives to MASTER_KEY_RING and
// MASTER_KEY. Supplying both forms for one key is rejected to avoid an
// ambiguous source during a rolling deployment.
//
// The resolver is called only for the configured reference. Returned bytes are
// copied into the parsed key ring and then cleared before returning. Resolver
// errors are intentionally normalized so provider details and secret values
// cannot cross the startup error boundary.
func LoadKeyRingWithResolver(
	ctx context.Context,
	getenv func(string) (string, bool),
	resolver SecretResolver,
	minimumLength int,
) (string, map[string]string, error) {
	if getenv == nil {
		return "", nil, fmt.Errorf("key-ring environment lookup is required")
	}
	if minimumLength <= 0 {
		return "", nil, fmt.Errorf("key-ring minimum length must be positive")
	}

	ring := make(map[string]string)
	encoded, encodedOK := getenv("MASTER_KEY_RING")
	ringRef, ringRefOK := getenv("MASTER_KEY_RING_REF")
	if encodedOK && encoded != "" && ringRefOK && ringRef != "" {
		return "", nil, fmt.Errorf("MASTER_KEY_RING and MASTER_KEY_RING_REF are mutually exclusive")
	}
	if ringRefOK && ringRef != "" {
		resolved, err := resolveSecretReference(ctx, resolver, SecretRef(ringRef))
		if err != nil {
			return "", nil, fmt.Errorf("resolve MASTER_KEY_RING_REF: %w", err)
		}
		encoded = string(resolved)
		clearBytes(resolved)
	}
	if encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &ring); err != nil {
			return "", nil, fmt.Errorf("MASTER_KEY_RING must be a JSON object")
		}
		if ring == nil {
			return "", nil, fmt.Errorf("MASTER_KEY_RING must not be null")
		}
	}

	// During a rolling migration, retain the legacy key as v1 when it is still
	// present. This lets old rows and old pods continue to decrypt safely while
	// the new active key is rolled out everywhere.
	legacy, legacyOK := getenv("MASTER_KEY")
	legacyRef, legacyRefOK := getenv("MASTER_KEY_REF")
	if legacyOK && legacy != "" && legacyRefOK && legacyRef != "" {
		return "", nil, fmt.Errorf("MASTER_KEY and MASTER_KEY_REF are mutually exclusive")
	}
	if legacyRefOK && legacyRef != "" {
		resolved, err := resolveSecretReference(ctx, resolver, SecretRef(legacyRef))
		if err != nil {
			return "", nil, fmt.Errorf("resolve MASTER_KEY_REF: %w", err)
		}
		legacy = string(resolved)
		clearBytes(resolved)
	}
	if legacy != "" {
		if _, exists := ring[defaultEncryptionKeyID]; !exists {
			ring[defaultEncryptionKeyID] = legacy
		}
	}
	if len(ring) == 0 {
		return "", nil, fmt.Errorf("MASTER_KEY_RING or MASTER_KEY is required")
	}

	for id, material := range ring {
		if !validEncryptionKeyID(id) {
			return "", nil, fmt.Errorf("invalid encryption key id")
		}
		if len(material) < minimumLength {
			return "", nil, fmt.Errorf("encryption key material is too short")
		}
	}

	activeID := defaultEncryptionKeyID
	if configured, ok := getenv("ACTIVE_MASTER_KEY_ID"); ok && configured != "" {
		activeID = configured
	}
	if !validEncryptionKeyID(activeID) {
		return "", nil, fmt.Errorf("invalid active encryption key id")
	}
	if _, ok := ring[activeID]; !ok {
		return "", nil, fmt.Errorf("active encryption key is not present in key ring")
	}

	// Copy in deterministic order so callers cannot accidentally depend on map
	// iteration order when reporting or constructing startup diagnostics.
	ids := make([]string, 0, len(ring))
	for id := range ring {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordered := make(map[string]string, len(ring))
	for _, id := range ids {
		ordered[id] = ring[id]
	}
	return activeID, ordered, nil
}

func resolveSecretReference(ctx context.Context, resolver SecretResolver, ref SecretRef) ([]byte, error) {
	if resolver == nil {
		return nil, ErrSecretUnavailable
	}
	value, err := resolver.Resolve(ctx, ref)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if errors.Is(err, ErrInvalidSecretRef) {
			return nil, ErrInvalidSecretRef
		}
		return nil, ErrSecretUnavailable
	}
	if len(value) == 0 {
		return nil, ErrSecretUnavailable
	}
	return value, nil
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
