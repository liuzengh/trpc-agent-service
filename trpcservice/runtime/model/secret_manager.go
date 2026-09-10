package modelruntime

import (
	"context"
	"fmt"
)

// SecretManagerResolver adapts a SecretManager to the runtime SecretResolver
// boundary. It keeps provider-specific clients out of execution plans.
type SecretManagerResolver struct {
	manager SecretManager
}

// NewSecretManagerResolver creates a resolver backed by one SecretManager.
func NewSecretManagerResolver(manager SecretManager) (*SecretManagerResolver, error) {
	if manager == nil {
		return nil, fmt.Errorf("%w: secret manager is required", ErrInvalid)
	}
	return &SecretManagerResolver{manager: manager}, nil
}

// Resolve returns a secret only after validating its tenant scope. Provider
// failures are reduced to ErrSecretUnavailable so diagnostic paths cannot
// disclose credential values or backend details.
func (resolver *SecretManagerResolver) Resolve(ctx context.Context, scope SecretScope) (SecretValue, error) {
	if ctx == nil {
		return SecretValue{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return SecretValue{}, err
	}
	if resolver == nil || resolver.manager == nil || scope.Validate() != nil {
		return SecretValue{}, ErrSecretUnavailable
	}
	value, err := resolver.manager.Read(ctx, scope)
	if err != nil {
		if ctx.Err() != nil {
			return SecretValue{}, ctx.Err()
		}
		return SecretValue{}, ErrSecretUnavailable
	}
	if value.Value() == "" {
		return SecretValue{}, ErrSecretUnavailable
	}
	if err := ctx.Err(); err != nil {
		return SecretValue{}, err
	}
	return value, nil
}
