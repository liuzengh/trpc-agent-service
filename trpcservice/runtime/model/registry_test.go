package modelruntime

import (
	"context"
	"errors"
	"strings"
	"testing"

	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

const registryTenant = "t_00000000000000000000000000"

func TestSecretRegistryIsTenantScopedAndRedacted(t *testing.T) {
	registry := NewSecretRegistry()
	scope := SecretScope{TenantID: registryTenant, SecretRef: "secret/model"}
	if err := registry.RegisterValue(scope, "top-secret"); err != nil {
		t.Fatal(err)
	}
	value, err := registry.Resolve(context.Background(), scope)
	if err != nil || value.Value() != "top-secret" {
		t.Fatalf("Resolve() = %q, %v", value.Value(), err)
	}
	foreign := scope
	foreign.TenantID = "t_00000000000000000000000001"
	_, err = registry.Resolve(context.Background(), foreign)
	if !errors.Is(err, ErrSecretUnavailable) || strings.Contains(err.Error(), "top-secret") {
		t.Fatalf("foreign Resolve() = %v", err)
	}
	if value.String() != "<redacted-secret>" {
		t.Fatalf("secret String() = %q", value.String())
	}
}

func TestSecretRegistryCancellationAndClose(t *testing.T) {
	registry := NewSecretRegistry()
	scope := SecretScope{TenantID: registryTenant, SecretRef: "secret/model"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Resolve(ctx, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve cancellation = %v", err)
	}
	if err := registry.RegisterValue(scope, "secret"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(context.Background(), scope); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("closed Resolve() = %v", err)
	}
	if err := registry.RegisterValue(scope, "again"); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("closed Register() = %v", err)
	}
}

func TestSecretRegistryRegistrationAndRemovalBoundaries(t *testing.T) {
	var nilRegistry *SecretRegistry
	scope := SecretScope{TenantID: registryTenant, SecretRef: "secret/model"}
	if err := nilRegistry.RegisterValue(scope, "value"); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("nil RegisterValue() = %v", err)
	}
	if _, err := nilRegistry.Resolve(context.Background(), scope); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("nil Resolve() = %v", err)
	}
	if err := NewSecretRegistry().RegisterValue(scope, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty RegisterValue() = %v", err)
	}

	registry := NewSecretRegistry()
	invalid := scope
	invalid.SecretRef = ""
	if err := registry.RegisterValue(invalid, "value"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid RegisterValue() = %v", err)
	}
	if err := registry.RegisterValue(scope, "value"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(scope); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(context.Background(), scope); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("removed Resolve() = %v", err)
	}
	if _, err := registry.Resolve(context.Background(), invalid); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("invalid Resolve() = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(scope); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("closed Remove() = %v", err)
	}
}

func TestSecretRegistryNilAndIdempotentCloseBoundaries(t *testing.T) {
	var nilRegistry *SecretRegistry
	if err := nilRegistry.Remove(SecretScope{TenantID: registryTenant, SecretRef: "secret/model"}); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("nil Remove() = %v", err)
	}
	registry := NewSecretRegistry()
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nilRegistry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(SecretScope{TenantID: registryTenant}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid Remove() = %v", err)
	}
}

func TestModelProviderRegistryScopesAndClonesInput(t *testing.T) {
	registry := NewModelProviderRegistry()
	factory := &registryModelFactory{}
	if err := registry.Register(registryTenant, "Fake", factory); err != nil {
		t.Fatal(err)
	}
	input := ModelFactoryInput{TenantID: registryTenant, Provider: "fake", Model: "chat", Options: map[string]string{"mode": "one"}}
	secret, err := NewSecretValue("secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.New(context.Background(), input, secret); err != nil {
		t.Fatal(err)
	}
	input.Options["mode"] = "changed"
	if factory.options != "one" {
		t.Fatalf("factory observed aliased options: %q", factory.options)
	}
	foreign := input
	foreign.TenantID = "t_00000000000000000000000001"
	if _, err := registry.New(context.Background(), foreign, secret); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("foreign provider lookup = %v", err)
	}
}

func TestModelProviderRegistryHonorsCancellationAfterFactoryReturns(t *testing.T) {
	registry := NewModelProviderRegistry()
	cancelFactory := &registryModelFactory{}
	if err := registry.Register(registryTenant, "fake", cancelFactory); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancelFactory.cancel = cancel
	secret, err := NewSecretValue("secret")
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.New(ctx, ModelFactoryInput{TenantID: registryTenant, Provider: "fake", Model: "chat"}, secret)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("New() after factory cancellation = %v", err)
	}
}

func TestModelProviderRegistryFailureAndLifecycleBoundaries(t *testing.T) {
	secret, err := NewSecretValue("secret")
	if err != nil {
		t.Fatal(err)
	}
	input := ModelFactoryInput{TenantID: registryTenant, Provider: "fake", Model: "chat"}
	registry := NewModelProviderRegistry()
	if err := registry.Register(registryTenant, "", &registryModelFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty provider Register() = %v", err)
	}
	if err := registry.Register("", "fake", &registryModelFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty tenant Register() = %v", err)
	}
	if err := registry.Register(registryTenant, "fake", modelFactoryFunc(func(context.Context, ModelFactoryInput, SecretValue) (trpcmodel.Model, error) {
		return nil, errors.New("provider detail")
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.New(context.Background(), input, secret); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("factory failure New() = %v", err)
	}
	if err := registry.Register(registryTenant, "fake", modelFactoryFunc(func(context.Context, ModelFactoryInput, SecretValue) (trpcmodel.Model, error) {
		return nil, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.New(context.Background(), input, secret); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("nil model New() = %v", err)
	}
	if err := registry.Remove(registryTenant, "fake"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.New(context.Background(), input, secret); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("removed provider New() = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(registryTenant, "fake"); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("closed Remove() = %v", err)
	}
	var nilRegistry *ModelProviderRegistry
	if _, err := nilRegistry.New(context.Background(), input, secret); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("nil registry New() = %v", err)
	}
	if err := registry.Remove("", "fake"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid Remove() = %v", err)
	}
}

func TestModelProviderRegistryCancellationAndClosedRegistration(t *testing.T) {
	secret, err := NewSecretValue("secret")
	if err != nil {
		t.Fatal(err)
	}
	registry := NewModelProviderRegistry()
	cancelCtx, cancel := context.WithCancel(context.Background())
	if err := registry.Register(registryTenant, "cancel", modelFactoryFunc(func(context.Context, ModelFactoryInput, SecretValue) (trpcmodel.Model, error) {
		cancel()
		return nil, errors.New("provider detail")
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.New(cancelCtx, ModelFactoryInput{TenantID: registryTenant, Provider: "cancel", Model: "chat"}, secret); !errors.Is(err, context.Canceled) {
		t.Fatalf("factory cancellation error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(registryTenant, "fake", &registryModelFactory{}); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("closed Register() = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

type registryModelFactory struct {
	options string
	cancel  context.CancelFunc
}

type modelFactoryFunc func(context.Context, ModelFactoryInput, SecretValue) (trpcmodel.Model, error)

func (function modelFactoryFunc) New(ctx context.Context, input ModelFactoryInput, secret SecretValue) (trpcmodel.Model, error) {
	return function(ctx, input, secret)
}

func (factory *registryModelFactory) New(_ context.Context, input ModelFactoryInput, _ SecretValue) (trpcmodel.Model, error) {
	factory.options = input.Options["mode"]
	if factory.cancel != nil {
		factory.cancel()
	}
	return registryFakeModel{}, nil
}

type registryFakeModel struct{}

func (registryFakeModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "registry"} }
func (registryFakeModel) GenerateContent(context.Context, *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	return nil, errors.New("unused")
}
