package cluster_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/cluster"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway/openclaw"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestPostgresCrossNodeStatusCancelHeartbeatBudgetAndApproval(t *testing.T) {
	dsn := os.Getenv("TRPC_AGENT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set TRPC_AGENT_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := repository.Migrate(ctx, func(ctx context.Context, script string) error {
		_, err := db.ExecContext(ctx, script)
		return err
	}, repository.DirectionUp); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantID, appID, bindingID := "cluster-"+suffix, "assistant", "binding-"+suffix
	requestID := tenantID + "/" + bindingID + "/message"
	seedClusterScope(t, ctx, db, tenantID, appID, bindingID, requestID)

	writer := &openclaw.SQLStatusStore{DB: db}
	reader := &openclaw.SQLStatusStore{DB: db}
	writer.Publish(gateway.RunEvent{TenantID: tenantID, BindingID: bindingID, RequestID: requestID, SessionID: "session", TraceID: "trace", Type: "run.accepted"})
	status, ok := reader.Get(ctx, tenantID, requestID)
	if !ok || status.Type != "run.accepted" {
		t.Fatalf("status=%+v ok=%v", status, ok)
	}
	if !reader.Cancel(tenantID, requestID) || !writer.Requested(ctx, tenantID, requestID) {
		t.Fatal("cross-node cancellation was not persisted")
	}
	writer.Publish(gateway.RunEvent{TenantID: tenantID, BindingID: bindingID, WorkerID: "worker-b", RequestID: requestID, SessionID: "session", TraceID: "trace", Type: "run.canceled", Terminal: true})
	// A delayed retry/stale-worker projection must not replace terminal truth.
	writer.Publish(gateway.RunEvent{TenantID: tenantID, BindingID: bindingID, WorkerID: "worker-a", RequestID: requestID, SessionID: "session", TraceID: "trace", Type: "run.retrying", Error: "stale"})
	status, ok = reader.Get(ctx, tenantID, requestID)
	if !ok || status.Type != "run.canceled" || status.WorkerID != "worker-b" || !status.CancelRequested {
		t.Fatalf("terminal status=%+v ok=%v", status, ok)
	}
	if reader.Cancel(tenantID, requestID) {
		t.Fatal("terminal request accepted another cancellation")
	}

	registry, err := cluster.NewNodeRegistry(ctx, db, "node-"+suffix, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.NewNodeRegistry(ctx, db, "node-"+suffix, 100*time.Millisecond); !errors.Is(err, cluster.ErrNodeIDInUse) {
		t.Fatalf("duplicate node err=%v", err)
	}
	if err := registry.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := cluster.NewNodeRegistry(ctx, db, "node-"+suffix, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("graceful node restart: %v", err)
	}
	defer restarted.Close(context.Background())

	budgetsA := &policy.SQLBudgetStore{DB: db}
	budgetsB := &policy.SQLBudgetStore{DB: db}
	toolPolicy := tenant.ToolPolicy{MonthlyCostBudgetCents: 1}
	first := policy.Request{TenantID: tenantID, RequestID: "budget-a", Policy: toolPolicy, EstimatedCostMicros: 6000}
	second := policy.Request{TenantID: tenantID, RequestID: "budget-b", Policy: toolPolicy, EstimatedCostMicros: 5000}
	if err := budgetsA.Reserve(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := budgetsB.Reserve(ctx, second); !errors.Is(err, policy.ErrBudgetExceeded) {
		t.Fatalf("over budget err=%v", err)
	}
	if err := budgetsA.Reconcile(ctx, first, 1, 4000); err != nil {
		t.Fatal(err)
	}
	if err := budgetsB.Reserve(ctx, second); err != nil {
		t.Fatalf("reserve after reconciliation: %v", err)
	}

	approvalsA := &policy.SQLApprovals{DB: db, PollInterval: 10 * time.Millisecond}
	approvalsB := &policy.SQLApprovals{DB: db, PollInterval: 10 * time.Millisecond}
	approved := make(chan bool, 1)
	go func() { approved <- approvalsB.Wait(ctx, tenantID, requestID, "dangerous-tool") }()
	if !approvalsA.Grant(tenantID, requestID, "dangerous-tool") {
		t.Fatal("approval grant failed")
	}
	select {
	case result := <-approved:
		if !result {
			t.Fatal("cross-node approval was not observed")
		}
	case <-time.After(time.Second):
		t.Fatal("cross-node approval timed out")
	}
}

func seedClusterScope(t *testing.T, ctx context.Context, db *sql.DB, tenantID, appID, bindingID, requestID string) {
	t.Helper()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO tenants (tenant_id,name,enabled,current_config_version) VALUES ($1,'Cluster test',FALSE,1)`, []any{tenantID}},
		{`INSERT INTO config_versions (tenant_id,version,config_yaml,config_sha256,status) VALUES ($1,1,'x','x','published')`, []any{tenantID}},
		{`INSERT INTO agent_apps (tenant_id,app_id,name,enabled,config_version) VALUES ($1,$2,'Agent',TRUE,1)`, []any{tenantID, appID}},
		{`INSERT INTO channel_bindings (tenant_id,app_id,binding_id,channel_type,provider_account_id,enabled,config_version) VALUES ($1,$2,$3,'http',$3,TRUE,1)`, []any{tenantID, appID, bindingID}},
		{`INSERT INTO inbox_messages (tenant_id,binding_id,external_message_id,app_id,user_id,session_id,status,attempts,payload_json,inbox_id,config_version,inbox_seq,completed_at) VALUES ($1,$2,'message',$3,'user','session','completed',1,'{}',$4,1,1,NOW())`, []any{tenantID, bindingID, appID, requestID}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}
