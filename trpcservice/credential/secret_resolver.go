package credential

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// SecretResolver resolves an opaque secret reference at runtime. Configuration
// snapshots only persist the reference, never the resolved value.
type SecretResolver interface {
	Resolve(context.Context, string) (string, error)
}

// EnvironmentSecretResolver is the local-development adapter for SecretResolver.
// It intentionally understands only env:NAME references. Production KMS/Vault
// adapters satisfy the same interface without widening configuration callers.
type EnvironmentSecretResolver struct {
	lookup func(string) string
}

// AllowlistSecretResolver prevents tenant-controlled references from widening
// the set of secrets a process is allowed to resolve.
type AllowlistSecretResolver struct {
	base    SecretResolver
	allowed map[string]struct{}
}

func NewAllowlistSecretResolver(base SecretResolver, references []string) (*AllowlistSecretResolver, error) {
	if base == nil {
		return nil, errors.New("base secret resolver is required")
	}
	allowed := make(map[string]struct{}, len(references))
	for _, reference := range references {
		reference = strings.TrimSpace(reference)
		if !strings.HasPrefix(reference, "env:") || strings.TrimSpace(strings.TrimPrefix(reference, "env:")) == "" {
			return nil, fmt.Errorf("invalid allowed secret reference %q", reference)
		}
		allowed[reference] = struct{}{}
	}
	return &AllowlistSecretResolver{base: base, allowed: allowed}, nil
}

func (r *AllowlistSecretResolver) Resolve(ctx context.Context, reference string) (string, error) {
	reference = strings.TrimSpace(reference)
	if r == nil || r.base == nil {
		return "", errors.New("secret resolver is not configured")
	}
	if _, ok := r.allowed[reference]; !ok {
		return "", fmt.Errorf("secret reference %q is not managed by the platform", reference)
	}
	return r.base.Resolve(ctx, reference)
}

// NewEnvironmentSecretResolver constructs the env: reference resolver.
func NewEnvironmentSecretResolver(lookup func(string) string) (*EnvironmentSecretResolver, error) {
	if lookup == nil {
		return nil, errors.New("environment lookup is required")
	}
	return &EnvironmentSecretResolver{lookup: lookup}, nil
}

// Resolve returns the named environment value. It never includes resolved
// values in errors so callers can safely wrap and log the returned error.
func (r *EnvironmentSecretResolver) Resolve(ctx context.Context, reference string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if r == nil || r.lookup == nil {
		return "", errors.New("secret resolver is not configured")
	}
	reference = strings.TrimSpace(reference)
	if !strings.HasPrefix(reference, "env:") {
		return "", fmt.Errorf("unsupported secret reference %q", reference)
	}
	name := strings.TrimSpace(strings.TrimPrefix(reference, "env:"))
	if name == "" {
		return "", errors.New("environment secret reference has no variable name")
	}
	if strings.ContainsAny(name, "=/\\ 	\r\n") {
		return "", fmt.Errorf("environment secret reference has invalid variable name %q", name)
	}
	value := r.lookup(name)
	if value == "" {
		return "", fmt.Errorf("environment secret %q is not set", name)
	}
	return value, nil
}
