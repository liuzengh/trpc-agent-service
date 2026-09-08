package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

func TestPostgresTerminalFailureRetainsRedactedCause(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.SeedBootstrap(ctx, db, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	j, err := NewPostgresJournal(db)
	if err != nil {
		t.Fatal(err)
	}
	inbound := testInboundRequest(t, fmt.Sprintf("failure-%x", time.Now().UnixNano()), "synthetic input")
	inbound.Scope = runtimecontext.TutorialScope()
	accepted, err := j.Accept(ctx, inbound)
	if err != nil {
		t.Fatal(err)
	}
	var task workqueue.AgentTask
	var payload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM queue_outbox WHERE payload->>'request_id'=$1`, accepted.RequestID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &task); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkRunRunning(ctx, task.RequestID, "worker"); err != nil {
		t.Fatal(err)
	}
	if err := j.FailRun(ctx, task.RequestID, "attachment_import", errors.New("artifact lock failed password=synthetic-secret"), "worker"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := j.TerminalFailRun(ctx, task, RunResult{WorkerID: "worker", Reply: "safe failure notice"}); err != nil {
			t.Fatal(err)
		}
	}
	var status, category, message string
	if err := db.QueryRowContext(ctx, `SELECT status,error_type,error_message FROM agent_run WHERE request_id=$1`, task.RequestID).Scan(&status, &category, &message); err != nil {
		t.Fatal(err)
	}
	if status != "dead" || category != "retry_exhausted" || !strings.Contains(message, "artifact lock failed") || !strings.Contains(message, "[REDACTED]") || strings.Contains(message, "synthetic-secret") {
		t.Fatalf("diagnostic retention failed: status=%s category=%s", status, category)
	}
	var count int
	var notice string
	if err := db.QueryRowContext(ctx, `SELECT count(*),max(payload->>'text') FROM outbound_message WHERE request_id=$1`, task.RequestID).Scan(&count, &notice); err != nil || count != 1 || notice != "safe failure notice" {
		t.Fatal("diagnostic must not leak into or duplicate the IM notice", err)
	}
	items, err := j.ClaimOutbound(ctx, "diagnostic-sender", 100, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var item OutboundItem
	for _, candidate := range items {
		if candidate.RequestID == task.RequestID {
			item = candidate
		}
	}
	if item.ID == "" {
		t.Fatal("diagnostic reply not claimed")
	}
	part, owner, err := j.BeginPart(ctx, item, "diagnostic-sender", 0, 1, strings.Repeat("a", 64))
	if err != nil || !owner {
		t.Fatal("cannot begin synthetic delivery", err)
	}
	if err := j.FinishPart(ctx, part, "unknown", ""); err != nil {
		t.Fatal(err)
	}
	cause := &channels.DeliveryError{Cause: errors.New("Telegram delivery outcome unknown"), Unknown: true, Diagnostics: &channels.DeliveryDiagnostics{Kind: "timeout", Phase: "wait_response"}}
	if err := j.MarkOutboundFailed(ctx, item.ID, "diagnostic-sender", time.Now(), true, cause, item.AttemptCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status,last_error_message FROM outbound_message WHERE outbound_id=$1`, item.ID).Scan(&status, &message); err != nil {
		t.Fatal(err)
	}
	if status != "dead" || !strings.Contains(message, "kind=timeout phase=wait_response") {
		t.Fatal("safe outbound diagnostic not persisted")
	}
	parts, err := j.ListParts(ctx, item.TenantID, item.ID)
	if err != nil || len(parts) != 1 || parts[0].Status != "unknown" {
		t.Fatal("diagnostics must not turn uncertain delivery into rejection")
	}
}
