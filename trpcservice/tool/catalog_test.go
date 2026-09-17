package tool

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type catalogCallable struct {
	name string
	call func(context.Context, []byte) (any, error)
}

func (c catalogCallable) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{Name: c.name, InputSchema: &agenttool.Schema{Type: "object"}}
}

func (c catalogCallable) Call(ctx context.Context, args []byte) (any, error) {
	if c.call == nil {
		return args, nil
	}
	return c.call(ctx, args)
}

type catalogSecrets struct {
	gotScope secrets.Scope
	value    []byte
}

func (s *catalogSecrets) Resolve(_ context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	s.gotScope = scope
	return secrets.SecretValue{Bytes: append([]byte(nil), s.value...), Version: ref.Version}, nil
}

func TestCatalogResolverExactTenantVersionAndImmutableRegistration(t *testing.T) {
	builds := 0
	catalog, err := NewCatalog(Registration{TenantID: "tenant-a", ID: "crm_lookup", Version: 3, Status: StatusActive,
		Build: func(_ context.Context, request BuildRequest) (agenttool.CallableTool, error) {
			builds++
			if request.TenantID != "tenant-a" || request.ID != "crm_lookup" || request.Version != 3 {
				t.Fatalf("bad build request: %#v", request)
			}
			return catalogCallable{name: "crm_lookup"}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(Registration{TenantID: "tenant-a", ID: "crm_lookup", Version: 3, Status: StatusActive,
		Build: func(context.Context, BuildRequest) (agenttool.CallableTool, error) {
			return catalogCallable{name: "crm_lookup"}, nil
		}}); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("duplicate registration: %v", err)
	}
	resolver := Resolver{Catalog: catalog}
	values, err := resolver.ResolveTools(context.Background(), "tenant-a", []profile.VersionedRef{{ID: "crm_lookup", Version: 3}})
	if err != nil || len(values) != 1 || builds != 1 {
		t.Fatalf("resolve values=%#v builds=%d err=%v", values, builds, err)
	}
	if _, err := resolver.ResolveTools(context.Background(), "tenant-b", []profile.VersionedRef{{ID: "crm_lookup", Version: 3}}); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("cross tenant lookup: %v", err)
	}
	if _, err := resolver.ResolveTools(context.Background(), "tenant-a", []profile.VersionedRef{{ID: "crm_lookup", Version: 2}}); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("version fallback: %v", err)
	}
	if _, err := resolver.ResolveTools(context.Background(), "tenant-a", []profile.VersionedRef{{ID: "crm_lookup", Version: 3}, {ID: "crm_lookup", Version: 3}}); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("duplicate ref: %v", err)
	}
}

func TestCatalogResolverRejectsInactiveAndDeclarationMismatch(t *testing.T) {
	catalog, err := NewCatalog(
		Registration{TenantID: "tenant-a", ID: "disabled", Version: 1, Status: StatusDisabled,
			Build: func(context.Context, BuildRequest) (agenttool.CallableTool, error) {
				return catalogCallable{name: "disabled"}, nil
			}},
		Registration{TenantID: "tenant-a", ID: "wrong", Version: 1, Status: StatusActive,
			Build: func(context.Context, BuildRequest) (agenttool.CallableTool, error) {
				return catalogCallable{name: "other"}, nil
			}},
	)
	if err != nil {
		t.Fatal(err)
	}
	resolver := Resolver{Catalog: catalog}
	for _, ref := range []profile.VersionedRef{{ID: "disabled", Version: 1}, {ID: "wrong", Version: 1}} {
		if _, err := resolver.ResolveTools(context.Background(), "tenant-a", []profile.VersionedRef{ref}); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
			t.Fatalf("ref=%#v err=%v", ref, err)
		}
	}
	if _, err := resolver.ResolveTools(context.Background(), "tenant-a", []profile.VersionedRef{{ID: "bad/name", Version: 1}}); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("invalid id: %v", err)
	}
}

func TestCatalogZeroValueRegisterAndSecretDependencyFailClosed(t *testing.T) {
	var catalog Catalog
	if err := catalog.Register(Registration{TenantID: "tenant-a", ID: "secured", Version: 1, Status: StatusActive,
		SecretRef: secrets.SecretRef{Ref: "secret://secured", Version: 1},
		Build: func(context.Context, BuildRequest) (agenttool.CallableTool, error) {
			return catalogCallable{name: "secured"}, nil
		}}); err != nil {
		t.Fatal(err)
	}
	if _, err := (Resolver{Catalog: &catalog}).ResolveTools(context.Background(), "tenant-a", []profile.VersionedRef{{ID: "secured", Version: 1}}); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("missing secret provider: %v", err)
	}
}

func TestBuildRequestResolveSecretUsesRuntimeTenantAndSubject(t *testing.T) {
	provider := &catalogSecrets{value: []byte("secret")}
	request := BuildRequest{TenantID: "tenant-a", ID: "crm_lookup", Version: 3, provider: provider,
		secret: secrets.SecretRef{Ref: "secret://crm", Version: 7}}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "req-1", SubjectID: "subject-a"})
	value, err := request.ResolveSecret(ctx)
	if err != nil || string(value.Bytes) != "secret" || value.Version != 7 {
		t.Fatalf("value=%#v err=%v", value, err)
	}
	if provider.gotScope.TenantID != "tenant-a" || provider.gotScope.Subject != "subject-a" || provider.gotScope.Purpose != secrets.PurposeToolCall || provider.gotScope.ResourceID != "crm_lookup" || provider.gotScope.ResourceVersion != 3 {
		t.Fatalf("scope=%#v", provider.gotScope)
	}
	clear(value.Bytes)
	wrong := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-b", RequestID: "req-2", SubjectID: "subject-b"})
	if _, err := request.ResolveSecret(wrong); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross tenant secret: %v", err)
	}
}

func TestTenantBoundCallableRejectsCrossTenantExecution(t *testing.T) {
	catalog, err := NewCatalog(Registration{TenantID: "tenant-a", ID: "safe", Version: 1, Status: StatusActive,
		Build: func(context.Context, BuildRequest) (agenttool.CallableTool, error) {
			return catalogCallable{name: "safe"}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	values, err := (Resolver{Catalog: catalog}).ResolveTools(context.Background(), "tenant-a", []profile.VersionedRef{{ID: "safe", Version: 1}})
	if err != nil {
		t.Fatal(err)
	}
	wrong := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-b", RequestID: "req-1"})
	if _, err := values[0].(agenttool.CallableTool).Call(wrong, nil); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross tenant call: %v", err)
	}
}
