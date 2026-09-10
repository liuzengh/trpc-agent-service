package main

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

func TestDLPAuthorizerUsesExactTenantBackendScope(t *testing.T) {
	ref := secrets.SecretRef{Ref: "secret://dlp/service", Version: 3}
	provider := &recordingSecretProvider{value: secrets.SecretValue{Bytes: []byte("scoped-token"), Version: 3}}
	authorize, err := newDLPAuthorizer(provider, 5, ref)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, "https://dlp.example.test/healthz", nil)
	if err := authorize(context.Background(), "tenant-a", request); err != nil {
		t.Fatal(err)
	}
	want := dlpSecretScope("tenant-a", 5)
	if provider.scope != want || provider.ref != ref || request.Header.Get("Authorization") != "Bearer scoped-token" ||
		request.Header.Get("X-Tenant-ID") != "tenant-a" {
		t.Fatalf("scope=%#v ref=%#v headers=%#v", provider.scope, provider.ref, request.Header)
	}
}

func TestDLPAuthorizerRejectsProviderVersionDriftAndEmptySecret(t *testing.T) {
	ref := secrets.SecretRef{Ref: "secret://dlp/service", Version: 3}
	for _, value := range []secrets.SecretValue{{Bytes: []byte("token"), Version: 4}, {Version: 3}} {
		provider := &recordingSecretProvider{value: value}
		authorize, err := newDLPAuthorizer(provider, 5, ref)
		if err != nil {
			t.Fatal(err)
		}
		request, _ := http.NewRequest(http.MethodGet, "https://dlp.example.test/healthz", nil)
		if err := authorize(context.Background(), "tenant-a", request); !errors.Is(err, runtime.ErrVersionMismatch) {
			t.Fatalf("secret rejection err=%v", err)
		}
		if request.Header.Get("Authorization") != "" {
			t.Fatal("authorization header set on rejected secret")
		}
	}
}

type recordingSecretProvider struct {
	scope secrets.Scope
	ref   secrets.SecretRef
	value secrets.SecretValue
	err   error
}

func (p *recordingSecretProvider) Resolve(_ context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	p.scope, p.ref = scope, ref
	if p.err != nil {
		return secrets.SecretValue{}, p.err
	}
	return secrets.SecretValue{Bytes: append([]byte(nil), p.value.Bytes...), Version: p.value.Version}, nil
}
