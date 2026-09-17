package payloadkey_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
)

func TestResolverUsesExactTenantPurposeAndGeneration(t *testing.T) {
	provider := inmemory.New()
	ref := secrets.SecretRef{Ref: "secret://messaging/payload", Version: 4}
	scope := payloadkey.Scope("tenant-a", 4)
	key := bytes.Repeat([]byte{0x4a}, 32)
	provider.Put(scope, ref, key)
	resolver, err := payloadkey.New(provider, ref.Ref)
	if err != nil {
		t.Fatal(err)
	}
	value, err := resolver.ResolvePayloadKey(context.Background(), "tenant-a", 4)
	if err != nil || value.Version != 4 || !bytes.Equal(value.Bytes, key) {
		t.Fatalf("version=%d bytes_len=%d err=%v", value.Version, len(value.Bytes), err)
	}
	value.Bytes[0] = 0
	again, err := resolver.ResolvePayloadKey(context.Background(), "tenant-a", 4)
	if err != nil || !bytes.Equal(again.Bytes, key) {
		t.Fatalf("second resolve bytes_len=%d err=%v", len(again.Bytes), err)
	}
	for _, test := range []struct {
		tenant  string
		version int64
	}{
		{tenant: "tenant-b", version: 4},
		{tenant: "tenant-a", version: 3},
		{tenant: "tenant-a", version: 5},
	} {
		if _, err := resolver.ResolvePayloadKey(context.Background(), test.tenant, test.version); !errors.Is(err, runtime.ErrNotFound) {
			t.Fatalf("tenant=%q version=%d err=%v", test.tenant, test.version, err)
		}
	}
}

func TestResolverRejectsInvalidAES256Material(t *testing.T) {
	provider := inmemory.New()
	ref := secrets.SecretRef{Ref: "secret://messaging/payload", Version: 1}
	provider.Put(payloadkey.Scope("tenant-a", 1), ref, bytes.Repeat([]byte{1}, 16))
	resolver, err := payloadkey.New(provider, ref.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolvePayloadKey(context.Background(), "tenant-a", 1); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("err=%v", err)
	}
}
