package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	frameworkpostgres "trpc.group/trpc-go/trpc-agent-go/storage/postgres"
)

type stubFrameworkPostgresClient struct {
	queryErr error
}

func (*stubFrameworkPostgresClient) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return driver.RowsAffected(1), nil
}

func (c *stubFrameworkPostgresClient) Query(context.Context, frameworkpostgres.HandlerFunc, string, ...any) error {
	return c.queryErr
}

func (*stubFrameworkPostgresClient) Transaction(context.Context, frameworkpostgres.TxFunc) error {
	return nil
}
func (*stubFrameworkPostgresClient) Close() error { return nil }

func TestObservedFrameworkPostgresClientRecordsComponentWithoutSQL(t *testing.T) {
	observer := &recordingStoreObserver{}
	wantErr := errors.New("query failed")
	client := &observedFrameworkPostgresClient{
		delegate: &stubFrameworkPostgresClient{queryErr: wantErr}, observer: observer,
		scope: frameworkPostgresScope{component: "session", tenantID: "tenant-a"},
	}

	if _, err := client.ExecContext(context.Background(), "private SQL", "private argument"); err != nil {
		t.Fatal(err)
	}
	if err := client.Query(context.Background(), nil, "another private SQL"); !errors.Is(err, wantErr) {
		t.Fatalf("Query() error = %v, want %v", err, wantErr)
	}
	if err := client.Transaction(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	wantOperations := []string{"framework.session.exec", "framework.session.query", "framework.session.transaction"}
	if len(observer.attributes) != len(wantOperations) {
		t.Fatalf("store operations = %#v", observer.attributes)
	}
	for index, operation := range wantOperations {
		got := observer.attributes[index]
		if got.TenantID != "tenant-a" || got.Backend != "postgres" || got.Operation != operation {
			t.Fatalf("store operation %d = %#v", index, got)
		}
	}
	if observer.errors[0] != nil || !errors.Is(observer.errors[1], wantErr) || observer.errors[2] != nil {
		t.Fatalf("store errors = %#v", observer.errors)
	}
}
