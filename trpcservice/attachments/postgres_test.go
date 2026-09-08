package attachments

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Real SQL control plane, journal, audit and advisory locks, with synthetic
// bytes only. The noModelRuntime fails the test if an upload reaches the LLM.
func TestAttachmentPostgresPipelineIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	schema := fmt.Sprintf("attachment_test_%x", time.Now().UnixNano())
	if _, err := base.ExecContext(ctx, `CREATE SCHEMA "`+schema+`"`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := base.ExecContext(cleanupCtx, `DROP SCHEMA "`+schema+`" CASCADE`); err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	seed := controlplane.DefaultBootstrapData()
	seed.ChannelBindings = append(seed.ChannelBindings, controlplane.ChannelBinding{
		ID: "attachment-binding", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ChannelType: "telegram", AccountID: "synthetic-bot", CallbackKey: "attachment-test", Status: "active", Version: 1,
		Config: json.RawMessage(`{"attachments_enabled":true}`),
	})
	if err := controlplane.SeedBootstrap(ctx, db, seed); err != nil {
		t.Fatal(err)
	}
	repo, err := controlplane.NewPostgresRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := storage.NewArtifactRouter(repo, secret.StaticStore{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	writer, err := audit.NewPostgresWriter(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(repo, artifacts, secret.StaticStore{}, writer)
	if err != nil {
		t.Fatal(err)
	}
	service.downloader = fakeDownload{data: []byte("synthetic attachment content")}
	journal, err := gateway.NewPostgresJournal(db)
	if err != nil {
		t.Fatal(err)
	}
	queue := workqueue.NewMemoryQueue(4)
	t.Cleanup(func() { _ = queue.Close() })
	scope, err := runtimecontext.NewScope("tutorial-tenant", "tutorial-app", "tutorial-revision-1", "telegram", "attachment-binding")
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := journal.Accept(ctx, gateway.InboundRequest{
		Scope: scope, ExternalMessageID: "synthetic-file", UserID: "synthetic-user", SessionID: "synthetic-session",
		ChatType: "direct", Text: "synthetic file metadata", Media: &runtimecontext.MediaReference{FileID: "synthetic-file-id", BindingVersion: 1},
	})
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
	if task.ChatType != "direct" {
		t.Fatal("SQL queue payload lost verified chat audience")
	}
	relay, err := gateway.NewOutboxRelay(journal, queue, gateway.RelayOptions{WorkerID: "relay", BatchSize: 10, ClaimLease: time.Second, PollInterval: time.Millisecond, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.RelayOnce(ctx); err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(queue, journal, noModelRuntime{t}, worker.Options{WorkerID: "worker", MaxAttempts: 3, RetryDelay: time.Millisecond, Attachments: service, Audit: writer})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := w.ProcessOne(ctx); err != nil || !processed {
		t.Fatalf("attachment worker processed=%t err=%v", processed, err)
	}
	if _, err := service.Import(ctx, task); err != nil {
		t.Fatalf("idempotent import: %v", err)
	}
	info := artifact.SessionInfo{AppName: scope.StorageScope, UserID: task.UserID, SessionID: task.SessionID}
	versions, err := artifacts.ListVersions(ctx, info, attachmentID(task))
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions=%v err=%v", versions, err)
	}
	inv := &agentcore.Invocation{Session: session.NewSession(info.AppName, info.UserID, info.SessionID), RunOptions: agentcore.RunOptions{AppName: info.AppName}}
	read, err := service.read(agentcore.NewInvocationContext(ctx, inv), ReadInput{AttachmentID: attachmentID(task)})
	if err != nil || read.Text != "synthetic attachment content" {
		t.Fatalf("read=%+v err=%v", read, err)
	}
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM agent_run WHERE request_id=$1`, accepted.RequestID).Scan(&status); err != nil || status != "completed" {
		t.Fatalf("run status=%s err=%v", status, err)
	}
	for _, decision := range []string{"attachment_imported", "attachment_replayed", "attachment_read"} {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM audit_log WHERE decision=$1`, decision).Scan(&count); err != nil || count != 1 {
			t.Fatalf("audit %s count=%d err=%v", decision, count, err)
		}
	}
}
