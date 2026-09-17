package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

func TestProviderRequiresExactScopeAndReturnsCopy(t *testing.T) {
	provider := New()
	scope := secrets.Scope{TenantID: "tenant-a", Subject: "worker", Purpose: secrets.PurposeModelCall, ResourceID: "model", ResourceVersion: 2}
	ref := secrets.SecretRef{Ref: "kms/model", Version: 3}
	provider.Put(scope, ref, []byte("secret"))
	value, err := provider.Resolve(context.Background(), scope, ref)
	if err != nil {
		t.Fatal(err)
	}
	value.Bytes[0] = 'X'
	again, err := provider.Resolve(context.Background(), scope, ref)
	if err != nil || string(again.Bytes) != "secret" {
		t.Fatalf("defensive copy: value=%q err=%v", again.Bytes, err)
	}
	other := scope
	other.TenantID = "tenant-b"
	if _, err := provider.Resolve(context.Background(), other, ref); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("cross tenant scope: got %v", err)
	}
	if _, err := provider.Resolve(context.Background(), secrets.Scope{}, ref); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("empty scope: got %v", err)
	}
}
