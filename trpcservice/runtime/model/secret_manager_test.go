package modelruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSecretManagerResolverValidatesScopeRedactsFailuresAndHonorsCancellation(t *testing.T) {
	secret, err := NewSecretValue("manager-secret")
	if err != nil {
		t.Fatal(err)
	}
	manager := secretManagerFunc(func(_ context.Context, scope SecretScope) (SecretValue, error) {
		if scope.TenantID != registryTenant || scope.SecretRef != "secret/manager" {
			return SecretValue{}, errors.New("foreign scope")
		}
		return secret, nil
	})
	resolver, err := NewSecretManagerResolver(manager)
	if err != nil {
		t.Fatal(err)
	}
	scope := SecretScope{TenantID: registryTenant, SecretRef: "secret/manager"}
	value, err := resolver.Resolve(context.Background(), scope)
	if err != nil || value.Value() != secret.Value() {
		t.Fatalf("Resolve() = %q, %v", value.Value(), err)
	}
	_, err = resolver.Resolve(context.Background(), SecretScope{TenantID: registryTenant, SecretRef: "secret/other"})
	if !errors.Is(err, ErrSecretUnavailable) || strings.Contains(err.Error(), secret.Value()) {
		t.Fatalf("foreign Resolve() = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Resolve() = %v", err)
	}
	if _, err := NewSecretManagerResolver(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil manager = %v", err)
	}
}

func TestSecretManagerResolverMapsManagerFailureAndLateCancellation(t *testing.T) {
	scope := SecretScope{TenantID: registryTenant, SecretRef: "secret/manager"}
	manager := secretManagerFunc(func(context.Context, SecretScope) (SecretValue, error) {
		return SecretValue{}, errors.New("transport secret detail")
	})
	resolver, err := NewSecretManagerResolver(manager)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), scope); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("manager failure Resolve() = %v", err)
	}
	empty := secretManagerFunc(func(context.Context, SecretScope) (SecretValue, error) { return SecretValue{}, nil })
	resolver, err = NewSecretManagerResolver(empty)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), scope); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("empty secret Resolve() = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	late := secretManagerFunc(func(context.Context, SecretScope) (SecretValue, error) {
		cancel()
		return NewSecretValue("value")
	})
	resolver, err = NewSecretManagerResolver(late)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(ctx, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("late cancellation Resolve() = %v", err)
	}
}

func TestSecretManagerResolverNilBoundaries(t *testing.T) {
	resolver, err := NewSecretManagerResolver(secretManagerFunc(func(context.Context, SecretScope) (SecretValue, error) {
		return NewSecretValue("value")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(nil, SecretScope{TenantID: registryTenant, SecretRef: "secret/manager"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context Resolve() = %v", err)
	}
	var nilResolver *SecretManagerResolver
	if _, err := nilResolver.Resolve(context.Background(), SecretScope{TenantID: registryTenant, SecretRef: "secret/manager"}); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("nil resolver Resolve() = %v", err)
	}
}

type secretManagerFunc func(context.Context, SecretScope) (SecretValue, error)

func (function secretManagerFunc) Read(ctx context.Context, scope SecretScope) (SecretValue, error) {
	return function(ctx, scope)
}
