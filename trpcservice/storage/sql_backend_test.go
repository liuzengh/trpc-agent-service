package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestSQLBackendFormattingRedactsCredential(t *testing.T) {
	const secret = "backend-secret-value"
	backend, err := NewSQLBackend(tenant.StorageProfile{
		TenantID: "tenant", ID: "mysql", Kind: tenant.StorageKindMySQL,
		CredentialRef: "env:MYSQL_DSN", TablePrefix: "tenant",
	}, "private-user:"+secret+"@tcp(private-db.example:3306)/private_db?parseTime=true&charset=utf8mb4&loc=UTC", sessionfence.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	for _, formatted := range []string{fmt.Sprintf("%v", backend), fmt.Sprintf("%+v", backend), fmt.Sprintf("%#v", backend)} {
		if strings.Contains(formatted, secret) || strings.Contains(formatted, "private-user") || strings.Contains(formatted, "private-db.example") || strings.Contains(formatted, "private_db") {
			t.Fatalf("SQL backend formatting leaked DSN material: %s", formatted)
		}
	}
}

func TestSQLBackendConcurrentClose(t *testing.T) {
	backend, err := NewSQLBackend(tenant.StorageProfile{
		TenantID: "tenant", ID: "mysql", Kind: tenant.StorageKindMySQL,
		CredentialRef: "env:MYSQL_DSN", TablePrefix: "tenant",
	}, "user:password@tcp(127.0.0.1:1)/db?parseTime=true&charset=utf8mb4&loc=UTC", sessionfence.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- backend.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Close error = %v", err)
		}
	}
	if err := backend.Ready(context.Background()); !errors.Is(err, persistence.ErrBackendUnavailable) {
		t.Fatalf("Ready after Close error = %v", err)
	}
}

func TestSQLBackendHealthFailureResetsResourcesForReconnect(t *testing.T) {
	backend := &SQLBackend{profile: tenant.StorageProfile{Kind: tenant.StorageKindMySQL}, initialized: true, generation: 7, healthCheck: func(context.Context) error { return errors.New("connection lost") }}
	if err := backend.Ready(context.Background()); !errors.Is(err, persistence.ErrBackendUnavailable) {
		t.Fatalf("Ready error=%v", err)
	}
	if backend.initialized || backend.healthCheck != nil || backend.sessions != nil || backend.memories != nil || backend.committer != nil {
		t.Fatal("failed SQL resources were not reset")
	}
	if generation := backend.ResourceGeneration(); generation != 7 {
		t.Fatalf("failed reset advanced resource generation to %d", generation)
	}
}
