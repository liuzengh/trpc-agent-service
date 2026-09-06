package approval

import (
	"context"
	"database/sql"
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
)

func TestPostgresDecisionContractIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	// Never migrate or seed the user's active schema. All rows and constraints
	// live in a randomly named, test-owned schema that is removed on cleanup.
	root, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open PostgreSQL test connection")
	}
	t.Cleanup(func() { _ = root.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("approval_test_%x", time.Now().UnixNano())
	if _, err := root.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("create isolated test schema")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := root.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("remove test-owned schema %s: %v", schema, err)
		}
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("parse PostgreSQL test URL")
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal("open isolated test connection")
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.SeedBootstrap(ctx, db, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	journal, err := gateway.NewPostgresJournal(db)
	if err != nil {
		t.Fatal(err)
	}
	base := approvalFixture()
	accepted, err := journal.Accept(ctx, gateway.InboundRequest{
		Scope: runtimecontext.TutorialScope(), ExternalMessageID: base.MessageID,
		UserID: base.UserID, SessionID: base.SessionID, ChatType: "direct", Text: base.ResumeText,
	})
	if err != nil {
		t.Fatal(err)
	}
	base.RequestID = accepted.RequestID
	repo, err := NewPostgresRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	testDecisionContract(t, repo, base)
	t.Run("control_feedback", func(t *testing.T) {
		base.ToolCallID = "feedback"
		record, err := repo.Request(ctx, base)
		if err != nil {
			t.Fatal(err)
		}
		writer, err := audit.NewPostgresWriter(db)
		if err != nil {
			t.Fatal(err)
		}
		service, err := NewService(repo, journal, writer)
		if err != nil {
			t.Fatal(err)
		}
		input := gateway.ApprovalDecisionInput{
			Scope: runtimecontext.TutorialScope(), TenantID: base.TenantID, ChannelType: "http", ChannelBindingID: base.ChannelBindingID,
			UserID: base.UserID, SessionID: base.SessionID, ChatType: "direct", ExternalMessageID: "bad-feedback",
			Text: "拒绝请回复：拒绝 " + record.ApprovalID,
		}
		for i := 0; i < 2; i++ {
			if handled, err := service.HandleApprovalDecision(ctx, input); err != nil || !handled {
				t.Fatalf("format handling: %t %v", handled, err)
			}
		}
		stored, err := repo.get(ctx, record.ApprovalID, false)
		if err != nil || stored.Status != StatusPending {
			t.Fatalf("format changed state: %+v %v", stored, err)
		}
		input.ExternalMessageID, input.Text = "deny-feedback", "拒绝 "+record.ApprovalID
		for i := 0; i < 2; i++ {
			if handled, err := service.HandleApprovalDecision(ctx, input); err != nil || !handled {
				t.Fatalf("deny handling: %t %v", handled, err)
			}
		}
		stored, err = repo.get(ctx, record.ApprovalID, false)
		if err != nil || stored.Status != StatusDenied || stored.ResumedAt.IsZero() {
			t.Fatalf("deny not persisted: %+v %v", stored, err)
		}
		var replies, queued, tools int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbound_message o JOIN agent_run r USING(request_id) WHERE r.agent_name='platform-control' AND r.status='completed'`).Scan(&replies); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM queue_outbox q JOIN agent_run r ON r.request_id=q.payload->>'request_id' WHERE r.agent_name='platform-control'`).Scan(&queued); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tool_execution t JOIN agent_run r USING(request_id) WHERE r.agent_name='platform-control'`).Scan(&tools); err != nil {
			t.Fatal(err)
		}
		if replies != 2 || queued != 0 || tools != 0 {
			t.Fatalf("replies=%d queued=%d tools=%d", replies, queued, tools)
		}
	})
	t.Run("direct_reply_transaction_rollback", func(t *testing.T) {
		if _, err := db.ExecContext(ctx, `
CREATE FUNCTION fail_feedback() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test feedback failure'; END $$;
CREATE TRIGGER fail_feedback BEFORE INSERT ON outbound_message FOR EACH ROW EXECUTE FUNCTION fail_feedback();`); err != nil {
			t.Fatal(err)
		}
		request := gateway.InboundRequest{Scope: runtimecontext.TutorialScope(), ExternalMessageID: "atomic-feedback", UserID: "alice",
			SessionID: "atomic-feedback", ChatType: "direct", Text: "取消", DirectReply: "平台提示：本条消息未改变审批状态。"}
		if _, err := journal.Accept(ctx, request); err == nil {
			t.Fatal("expected outbound insert failure")
		}
		var count int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM inbound_message WHERE external_message_id='atomic-feedback'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatal("failed feedback left a partial inbound record")
		}
		if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_feedback ON outbound_message; DROP FUNCTION fail_feedback();`); err != nil {
			t.Fatal(err)
		}
		if _, err := journal.Accept(ctx, request); err != nil {
			t.Fatal(err)
		}
	})
}
