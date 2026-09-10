package vault

import (
	"context"
	"os"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type integrationToken string

func (t integrationToken) Token(context.Context) ([]byte, error) { return []byte(t), nil }

func TestProviderVaultKVv2Integration(t *testing.T) {
	endpoint, token := os.Getenv("TRPC_VAULT_TEST_ENDPOINT"), os.Getenv("TRPC_VAULT_TEST_TOKEN")
	if endpoint == "" || token == "" {
		t.Skip("TRPC_VAULT_TEST_ENDPOINT and TRPC_VAULT_TEST_TOKEN are required")
	}
	provider, err := New(Config{Endpoint: endpoint, TokenSource: integrationToken(token), AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	value, err := provider.Resolve(context.Background(), secrets.Scope{TenantID: "tenant", Subject: "integration", Purpose: secrets.PurposeModelCall, ResourceID: "model", ResourceVersion: 1}, secrets.SecretRef{Ref: "vault://secret/model", Version: 1})
	if err != nil || string(value.Bytes) != "integration-secret" || value.Version != 1 {
		t.Fatalf("value=%q version=%d err=%v", value.Bytes, value.Version, err)
	}
}
