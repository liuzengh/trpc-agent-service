package assembly

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	frameworkpostgres "trpc.group/trpc-go/trpc-agent-go/storage/postgres"
)

type rejectingDDLClient struct {
	operations int
}

func (c *rejectingDDLClient) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	c.operations++
	return nil, errors.New("unexpected runtime PostgreSQL DDL")
}

func (c *rejectingDDLClient) Query(context.Context, frameworkpostgres.HandlerFunc, string, ...any) error {
	c.operations++
	return errors.New("unexpected runtime PostgreSQL query during construction")
}

func (c *rejectingDDLClient) Transaction(context.Context, frameworkpostgres.TxFunc) error {
	c.operations++
	return errors.New("unexpected runtime PostgreSQL transaction during construction")
}

func (*rejectingDDLClient) Close() error { return nil }

func TestManagedSessionProviderDoesNotRunPostgresDDLAtRuntime(t *testing.T) {
	originalBuilder := frameworkpostgres.GetClientBuilder()
	t.Cleanup(func() { frameworkpostgres.SetClientBuilder(originalBuilder) })

	client := &rejectingDDLClient{}
	frameworkpostgres.SetClientBuilder(func(context.Context, ...frameworkpostgres.ClientBuilderOpt) (frameworkpostgres.Client, error) {
		return client, nil
	})

	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewManagedSessionProvider("postgres://unused", resolver, testBackendProfileResolver{}, noopSessionSummarizer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	_, err = provider.SessionServiceFor(context.Background(), config.TenantConfig{
		TenantID: "tenant-a",
		AppCode:  "support",
	}, platformstorage.SessionBackendRef{Driver: SessionDriverPostgres})
	if err != nil {
		t.Fatalf("construct PostgreSQL Session backend: %v", err)
	}
	if client.operations != 0 {
		t.Fatalf("runtime Session construction performed %d PostgreSQL operations, want 0", client.operations)
	}
}
