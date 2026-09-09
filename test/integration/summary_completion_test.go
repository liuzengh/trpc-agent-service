//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
)

type summaryCompletionFixture struct {
	db      *sql.DB
	ctx     context.Context
	store   *reliable.PostgresStore
	claim   *reliable.InboxMessage
	receipt summarycoord.EnqueueRequest
}

func newSummaryCompletionFixture(t *testing.T) summaryCompletionFixture {
	t.Helper()
	db := openDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	suffix := uuid.NewString()
	tenantID, appID, versionID := "summary-completion-"+suffix, "app-"+suffix, "version-"+suffix
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, query := range []string{
			`DELETE FROM summary_checkpoints WHERE tenant_id=$1`,
			`DELETE FROM summary_jobs WHERE tenant_id=$1`,
			`DELETE FROM outbox_messages WHERE tenant_id=$1`,
			`DELETE FROM inbox_messages WHERE tenant_id=$1`,
			`DELETE FROM inbox_session_sequences WHERE tenant_id=$1`,
			`DELETE FROM agent_versions WHERE agent_app_id IN (SELECT id FROM agent_apps WHERE tenant_id=$1)`,
			`DELETE FROM agent_apps WHERE tenant_id=$1`,
			`DELETE FROM tenants WHERE id=$1`,
		} {
			if _, err := db.ExecContext(cleanup, query, tenantID); err != nil {
				t.Errorf("cleanup atomic summary completion fixture: %v", err)
			}
		}
	})
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'summary completion','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_apps(id,tenant_id,name,status) VALUES($1,$2,'support','active')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_versions(id,agent_app_id,version_number,config_snapshot,config_hash,status,created_by)
		VALUES($1,$2,1,'{}',$3,'published','integration')`, versionID, appID, fmt.Sprintf("%064x", 1)); err != nil {
		t.Fatal(err)
	}
	store := reliable.NewPostgresStore(db)
	message := &reliable.InboxMessage{
		TenantID: tenantID, AgentApp: "support", ChannelType: "telegram", ChannelAccountID: "bot-1",
		ExternalMessageID: "update-1", ConversationID: "chat-1", ReplyToID: "reply-1",
		UserID: "owner-1", SessionOwnerID: "owner-1", SessionID: "session-1", PayloadHash: strings.Repeat("a", 64), Payload: []byte(`{"content":"hello"}`),
	}
	if inserted, err := store.EnqueueInbox(ctx, message); err != nil || !inserted {
		t.Fatalf("enqueue atomic completion fixture: inserted=%v err=%v", inserted, err)
	}
	claim, err := store.ClaimInbox(ctx, "summary-completion-consumer", time.Minute)
	if err != nil || claim == nil || claim.ID != message.ID {
		t.Fatalf("claim atomic completion fixture: claim=%+v err=%v", claim, err)
	}
	receipt := summarycoord.EnqueueRequest{
		Key:            summarycoord.Key{TenantID: tenantID, AgentAppID: appID, SessionOwnerID: claim.SessionOwnerID, SessionID: claim.SessionID, SessionIncarnationID: uuid.NewString()},
		AgentVersionID: versionID, TargetEventSequence: 4,
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("invalid atomic completion fixture receipt: %v", err)
	}
	return summaryCompletionFixture{db: db, ctx: ctx, store: store, claim: claim, receipt: receipt}
}

func assertSummaryCompletionRows(t *testing.T, fixture summaryCompletionFixture, wantStatus string, wantOutbox, wantJobs int) {
	t.Helper()
	var status, owner string
	var leaseVersion int64
	var leaseCleared bool
	var outboxCount, summaryCount int
	if err := fixture.db.QueryRowContext(fixture.ctx, `
		SELECT status, COALESCE(lease_owner,''), lease_version, lease_until IS NULL,
		       (SELECT COUNT(*) FROM outbox_messages WHERE tenant_id=$2),
		       (SELECT COUNT(*) FROM summary_jobs WHERE tenant_id=$2)
		FROM inbox_messages WHERE id=$1`, fixture.claim.ID, fixture.claim.TenantID).
		Scan(&status, &owner, &leaseVersion, &leaseCleared, &outboxCount, &summaryCount); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || outboxCount != wantOutbox || summaryCount != wantJobs {
		t.Fatalf("atomic completion rows: status=%s outbox=%d summaries=%d, want %s/%d/%d", status, outboxCount, summaryCount, wantStatus, wantOutbox, wantJobs)
	}
	if wantStatus == string(reliable.InboxProcessing) && (owner != fixture.claim.Lease.Owner || leaseVersion != fixture.claim.Lease.Fence || leaseCleared) {
		t.Fatalf("rollback changed the original Inbox lease: owner=%q version=%d cleared=%v", owner, leaseVersion, leaseCleared)
	}
	if wantStatus == string(reliable.InboxCompleted) && (owner != "" || !leaseCleared) {
		t.Fatalf("completed Inbox retained its lease: owner=%q cleared=%v", owner, leaseCleared)
	}
}

func TestPostgresCompleteInboxWithSummaryCommitsExactlyOneReplyAndJob(t *testing.T) {
	for _, target := range []int64{4, 0} {
		t.Run(fmt.Sprintf("target-%d", target), func(t *testing.T) {
			fixture := newSummaryCompletionFixture(t)
			fixture.receipt.TargetEventSequence = target
			reply := reliable.OutboxReply{ContentType: "text", Content: "atomic reply"}
			outbox, err := fixture.store.CompleteInboxWithSummary(fixture.ctx, fixture.claim.ID, fixture.claim.Lease, reply, fixture.receipt)
			if err != nil || outbox == nil || outbox.InboxID != fixture.claim.ID || outbox.TenantID != fixture.claim.TenantID ||
				outbox.Content != reply.Content || outbox.SessionID != fixture.claim.SessionID || outbox.ReplyToID != fixture.claim.ReplyToID {
				t.Fatalf("atomic summary completion: outbox=%+v err=%v", outbox, err)
			}
			assertSummaryCompletionRows(t, fixture, string(reliable.InboxCompleted), 1, 1)
			var jobID int64
			if err := fixture.db.QueryRowContext(fixture.ctx, `SELECT id FROM summary_jobs WHERE tenant_id=$1`, fixture.claim.TenantID).Scan(&jobID); err != nil {
				t.Fatal(err)
			}
			job, err := summarycoord.NewPostgresStore(fixture.db).Get(fixture.ctx, jobID)
			if err != nil || job.Key != fixture.receipt.Key || job.AgentVersionID != fixture.receipt.AgentVersionID || job.TargetEventSequence != target ||
				job.Status != summarycoord.StatusPending || (job.TargetResolutionLeaseVersion > 0) != (target == 0) {
				t.Fatalf("committed summary receipt changed identity or target: job=%+v err=%v", job, err)
			}
			if _, err := fixture.store.CompleteInboxWithSummary(fixture.ctx, fixture.claim.ID, fixture.claim.Lease, reply, fixture.receipt); !errors.Is(err, reliable.ErrStaleLease) {
				t.Fatalf("duplicate completion accepted the old Inbox lease: %v", err)
			}
			assertSummaryCompletionRows(t, fixture, string(reliable.InboxCompleted), 1, 1)
		})
	}
}

func TestPostgresCompleteInboxWithSummaryRollsBackScopeMismatch(t *testing.T) {
	for _, field := range []string{"tenant", "owner", "session", "app", "version"} {
		t.Run(field, func(t *testing.T) {
			fixture := newSummaryCompletionFixture(t)
			wrong := fixture.receipt
			switch field {
			case "tenant":
				wrong.TenantID = "another-tenant"
			case "owner":
				wrong.SessionOwnerID = "another-owner"
			case "session":
				wrong.SessionID = "another-session"
			case "app":
				wrong.AgentAppID = "another-app"
			case "version":
				wrong.AgentVersionID = "another-version"
			}
			reply := reliable.OutboxReply{Content: "must remain atomic"}
			if _, err := fixture.store.CompleteInboxWithSummary(fixture.ctx, fixture.claim.ID, fixture.claim.Lease, reply, wrong); !errors.Is(err, reliable.ErrSummaryCompletionConflict) {
				t.Fatalf("wrong %s receipt accepted: %v", field, err)
			}
			assertSummaryCompletionRows(t, fixture, string(reliable.InboxProcessing), 0, 0)
			if _, err := fixture.store.CompleteInboxWithSummary(fixture.ctx, fixture.claim.ID, fixture.claim.Lease, reply, fixture.receipt); err != nil {
				t.Fatalf("rolled-back completion could not use its original lease: %v", err)
			}
			assertSummaryCompletionRows(t, fixture, string(reliable.InboxCompleted), 1, 1)
		})
	}
}

func TestPostgresCompleteInboxWithSummaryRollsBackLateEnqueueConflict(t *testing.T) {
	fixture := newSummaryCompletionFixture(t)
	jobs := summarycoord.NewPostgresStore(fixture.db)
	existing, err := jobs.Enqueue(fixture.ctx, fixture.receipt)
	if err != nil {
		t.Fatal(err)
	}
	otherVersion := "version-" + uuid.NewString()
	if _, err := fixture.db.ExecContext(fixture.ctx, `
		INSERT INTO agent_versions(id,agent_app_id,version_number,config_snapshot,config_hash,status,created_by)
		VALUES($1,$2,2,'{}',$3,'published','integration')`, otherVersion, fixture.receipt.AgentAppID, fmt.Sprintf("%064x", 2)); err != nil {
		t.Fatal(err)
	}
	conflict := fixture.receipt
	conflict.AgentVersionID = otherVersion
	reply := reliable.OutboxReply{Content: "must remain atomic"}
	if _, err := fixture.store.CompleteInboxWithSummary(fixture.ctx, fixture.claim.ID, fixture.claim.Lease, reply, conflict); !errors.Is(err, reliable.ErrSummaryCompletionConflict) {
		t.Fatalf("late summary version conflict accepted: %v", err)
	}
	assertSummaryCompletionRows(t, fixture, string(reliable.InboxProcessing), 0, 1)
	preserved, err := jobs.Get(fixture.ctx, existing.Job.ID)
	if err != nil || preserved.AgentVersionID != existing.Job.AgentVersionID || preserved.TargetEventSequence != existing.Job.TargetEventSequence ||
		preserved.Status != existing.Job.Status || !preserved.UpdatedAt.Equal(existing.Job.UpdatedAt) {
		t.Fatalf("failed completion modified the existing summary job: job=%+v err=%v", preserved, err)
	}
	if _, err := fixture.store.CompleteInboxWithSummary(fixture.ctx, fixture.claim.ID, fixture.claim.Lease, reply, fixture.receipt); err != nil {
		t.Fatalf("late rollback did not preserve the original completion lease: %v", err)
	}
	assertSummaryCompletionRows(t, fixture, string(reliable.InboxCompleted), 1, 1)
}
