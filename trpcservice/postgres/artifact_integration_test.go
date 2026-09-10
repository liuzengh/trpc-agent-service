//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestArtifactReservationsAllocateDistinctVersionsAndDeleteExactTargets(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	id := fmt.Sprintf("artifact-reservation-%d", time.Now().UnixNano())
	scope := tenant.Scope{TenantID: id, AppID: "support"}
	config := integrationAppConfig("v1", "gpt-4.1-mini")
	config.TenantID, config.AppID = scope.TenantID, scope.AppID
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID: scope.TenantID, Name: id, Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: scope.TenantID, AppID: scope.AppID, Name: "Support",
		ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create app: %v", err)
	}
	access := platformartifact.Access{
		Scope: scope, ConfigVersion: config.Version, SessionPrincipalID: "user-1", SessionID: "session-1",
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, scope.TenantID, scope.AppID, access.SessionPrincipalID, access.SessionID); err != nil {
		t.Fatalf("create session lane: %v", err)
	}

	versions := make(chan int, 2)
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			record, err := store.ReserveArtifact(ctx, access, "report.txt", "text/plain", 1)
			if err != nil {
				errs <- err
				return
			}
			key := fmt.Sprintf("artifact/report/%d", record.Version)
			if err := store.BindArtifactObject(ctx, record, key); err != nil {
				errs <- err
				return
			}
			record.ObjectKey = key
			if err := store.PublishArtifact(ctx, record); err != nil {
				errs <- err
				return
			}
			versions <- record.Version
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("reserve artifact: %v", err)
	}
	close(versions)
	got := make([]int, 0, 2)
	for version := range versions {
		got = append(got, version)
	}
	sort.Ints(got)
	if want := []int{0, 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reserved versions = %v, want %v", got, want)
	}

	deleted, err := store.MarkArtifactsDeleted(ctx, access, "report.txt")
	if err != nil {
		t.Fatalf("mark artifacts deleted: %v", err)
	}
	if len(deleted) != 2 || deleted[0].ObjectKey == "" || deleted[1].ObjectKey == "" {
		t.Fatalf("deleted records = %#v, want two exact targets", deleted)
	}
}
