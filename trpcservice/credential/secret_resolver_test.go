package credential

import (
	"context"
	"strings"
	"testing"
)

func TestEnvironmentSecretResolverResolvesOnlyEnvironmentReferences(t *testing.T) {
	resolver, err := NewEnvironmentSecretResolver(func(name string) string {
		if name == "MODEL_KEY" {
			return "secret-value"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("NewEnvironmentSecretResolver() error = %v", err)
	}

	value, err := resolver.Resolve(context.Background(), "env:MODEL_KEY")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if value != "secret-value" {
		t.Fatalf("Resolve() = %q, want secret-value", value)
	}

	for _, reference := range []string{"", "MODEL_KEY", "env:", "kms:model-key"} {
		t.Run(reference, func(t *testing.T) {
			_, err := resolver.Resolve(context.Background(), reference)
			if err == nil {
				t.Fatal("Resolve() error = nil, want error")
			}
			if strings.Contains(err.Error(), "secret-value") {
				t.Fatalf("Resolve() leaked secret in error: %v", err)
			}
		})
	}
}

func TestEnvironmentSecretResolverHonorsCancellation(t *testing.T) {
	resolver, err := NewEnvironmentSecretResolver(func(string) string { return "secret" })
	if err != nil {
		t.Fatalf("NewEnvironmentSecretResolver() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(ctx, "env:MODEL_KEY"); err == nil {
		t.Fatal("Resolve() error = nil, want canceled context")
	}
}

func TestAllowlistSecretResolverRejectsUnmanagedEnvironmentReference(t *testing.T) {
	base, err := NewEnvironmentSecretResolver(func(name string) string {
		return map[string]string{"MODEL_KEY": "model-secret", "DATABASE_URL": "database-secret"}[name]
	})
	if err != nil {
		t.Fatalf("NewEnvironmentSecretResolver() error = %v", err)
	}
	resolver, err := NewAllowlistSecretResolver(base, []string{"env:MODEL_KEY"})
	if err != nil {
		t.Fatalf("NewAllowlistSecretResolver() error = %v", err)
	}
	value, err := resolver.Resolve(context.Background(), "env:MODEL_KEY")
	if err != nil || value != "model-secret" {
		t.Fatalf("Resolve(allowed) = %q, %v", value, err)
	}
	value, err = resolver.Resolve(context.Background(), "env:DATABASE_URL")
	if err == nil {
		t.Fatal("Resolve(unmanaged) error = nil, want rejection")
	}
	if value != "" || strings.Contains(err.Error(), "database-secret") {
		t.Fatalf("Resolve(unmanaged) leaked a secret: value=%q err=%v", value, err)
	}
}
