package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

func TestAcceptedFinalAttachmentsPostgres(t *testing.T) {
	migrationURL, runtimeURL := os.Getenv("WORKER_TEST_MIGRATION_URL"), os.Getenv("WORKER_TEST_RUNTIME_URL")
	if migrationURL == "" || runtimeURL == "" {
		t.Skip("requires disposable Worker PostgreSQL roles")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	open := func(url string) *pgxpool.Pool {
		p, e := pgxpool.New(ctx, url)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(p.Close)
		return p
	}
	migration, pool := open(migrationURL), open(runtimeURL)
	if e := migrations.ApplyForRuntime(ctx, migration, "worker_runtime"); e != nil {
		t.Fatal(e)
	}
	l := New(pool)
	attachment := domain.Attachment{Name: "报告.txt", Version: 2, MimeType: "text/plain", SizeBytes: 7, SHA256: strings.Repeat("a", 64)}
	newFinish := func(t *testing.T) domain.Finish {
		t.Helper()
		id := fmt.Sprintf("attachments_%d", time.Now().UnixNano())
		r := domain.Requested{EventID: "event_" + id, EventDigest: domain.Digest([]byte(id)), RunDigest: domain.Digest([]byte("run" + id)), RunID: "run_" + id, AdmissionID: "admission_" + id, Route: domain.Route{TenantID: "tenant_" + id, Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: "42", SenderID: "100", Text: "attachment", ReceivedAt: time.Now().UTC()}}
		p := domain.Policy{Version: "attachment-test", MaxRunAge: time.Minute, MaxReplyAge: time.Minute, MaxFutureSkew: time.Second, LeaseTTL: 10 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 2}
		if _, e := l.Accept(ctx, r, p, domain.IntakeLimits{MaxQueuedRuns: 10000, MaxRetainedRuns: 100000}); e != nil {
			t.Fatal(e)
		}
		g, e := l.Claim(ctx, domain.ClaimRequest{TenantID: r.Route.TenantID, RunID: r.RunID, WorkerID: "worker", MaxRunSeconds: 30, MaxActive: 100})
		if e != nil {
			t.Fatal(e)
		}
		return domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: domain.Candidate{Ref: "candidate_" + g.AttemptID, Digest: domain.Digest([]byte("snapshot")), Parent: g.Parent}, FinalText: "attachment final", Attachments: []domain.Attachment{attachment}}
	}
	read := func(c domain.Completion) (domain.AcceptedArtifact, error) {
		return l.AcceptedArtifact(ctx, c.FinalIntentID, c.RunID, c.CompletionID, attachment.Name, attachment.Version)
	}
	head := func(t *testing.T, f domain.Finish) domain.Head {
		t.Helper()
		var h domain.Head
		if e := pool.QueryRow(ctx, `SELECT accepted_ref,accepted_digest FROM execution_sessions WHERE tenant_id=$1 AND session_id=$2`, f.Grant.Run.Request.Route.TenantID, f.Grant.Run.SessionID).Scan(&h.Ref, &h.Digest); e != nil {
			t.Fatal(e)
		}
		return h
	}
	assertNoFinal := func(t *testing.T, f domain.Finish) {
		t.Helper()
		var count int
		if e := pool.QueryRow(ctx, `SELECT count(*) FROM execution_reply_outbox WHERE tenant_id=$1 AND run_id=$2`, f.Grant.Run.Request.Route.TenantID, f.Grant.Run.Request.RunID).Scan(&count); e != nil || count != 0 {
			t.Fatalf("outbox count=%d err=%v", count, e)
		}
	}
	t.Run("accepted_exact_version_digest_and_idempotency", func(t *testing.T) {
		f := newFinish(t)
		if _, e := l.AcceptedArtifact(ctx, domain.StableID("fin", f.Grant.Run.Request.RunID), f.Grant.Run.Request.RunID, domain.StableID("cmp", f.Grant.Run.Request.RunID), attachment.Name, attachment.Version); !errors.Is(e, domain.ErrNotFound) {
			t.Fatal("candidate implied authority", e)
		}
		c, e := l.Complete(ctx, f)
		if e != nil {
			t.Fatal(e)
		}
		if c.ResultDigest != domain.FinishDigest(f) {
			t.Fatal("result did not seal attachment")
		}
		accepted, e := read(c)
		if e != nil || accepted.Attachment != attachment || accepted.Run.Request != f.Grant.Run.Request || accepted.Run.Status != domain.Succeeded {
			t.Fatal(accepted, e)
		}
		final, e := l.Final(ctx, c.FinalIntentID)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(accepted.Final, final) {
			t.Fatal("accepted resolver proof differs from formal Final", accepted.Final, final)
		}
		event, e := codec.DecodeReplyIntent(final.Payload)
		if e != nil || len(event.Content.Attachments) != 1 || event.Content.Attachments[0].Sha256 != attachment.SHA256 {
			t.Fatal(event, e)
		}
		if h := head(t, f); h.Ref != f.Candidate.Ref || h.Digest != f.Candidate.Digest {
			t.Fatal(h)
		}
		replay, e := l.Complete(ctx, f)
		if e != nil || !reflect.DeepEqual(replay, c) {
			t.Fatal(replay, e)
		}
		for _, change := range []func(*domain.Attachment){func(a *domain.Attachment) { a.Version++ }, func(a *domain.Attachment) { a.SHA256 = strings.Repeat("b", 64) }} {
			different := f
			different.Attachments = append([]domain.Attachment(nil), f.Attachments...)
			change(&different.Attachments[0])
			if _, e = l.Complete(ctx, different); !errors.Is(e, domain.ErrConflict) {
				t.Fatal("changed accepted attachment", e)
			}
		}
		for _, args := range [][4]string{{"wrong", c.RunID, c.CompletionID, attachment.Name}, {c.FinalIntentID, "wrong", c.CompletionID, attachment.Name}, {c.FinalIntentID, c.RunID, "wrong", attachment.Name}, {c.FinalIntentID, c.RunID, c.CompletionID, "other.txt"}} {
			if _, e = l.AcceptedArtifact(ctx, args[0], args[1], args[2], args[3], attachment.Version); !errors.Is(e, domain.ErrNotFound) {
				t.Fatal(args, e)
			}
		}
		if _, e = l.AcceptedArtifact(ctx, c.FinalIntentID, c.RunID, c.CompletionID, attachment.Name, 3); !errors.Is(e, domain.ErrNotFound) {
			t.Fatal("latest fallback", e)
		}
		if e = l.MarkReplyPublished(ctx, final.IntentID, final.Digest); e != nil {
			t.Fatal(e)
		}
		if _, e = read(c); e != nil {
			t.Fatal("publication removed accepted authority", e)
		}
		var count int
		if e = pool.QueryRow(ctx, `SELECT count(*) FROM execution_reply_outbox WHERE intent_id=$1`, c.FinalIntentID).Scan(&count); e != nil || count != 1 {
			t.Fatal(count, e)
		}
	})
	for _, mode := range []string{"failed", "invalid", "duplicate", "parent_rejected", "stale_lease"} {
		t.Run(mode, func(t *testing.T) {
			f := newFinish(t)
			before := head(t, f)
			want := domain.ErrInvalid
			switch mode {
			case "failed":
				f.Status = domain.Failed
				f.Candidate = domain.Candidate{}
			case "invalid":
				f.Attachments[0].Name = "../secret"
			case "duplicate":
				f.Attachments = append(f.Attachments, f.Attachments[0])
			case "parent_rejected":
				f.Candidate.Parent = domain.Head{Ref: "other", Digest: domain.Digest([]byte("other"))}
				want = domain.ErrFenced
			case "stale_lease":
				f.Grant.LeaseEpoch++
				want = domain.ErrFenced
			}
			if _, e := l.Complete(ctx, f); !errors.Is(e, want) {
				t.Fatal(mode, e)
			}
			if h := head(t, f); h != before {
				t.Fatal("rejected changed head", h)
			}
			assertNoFinal(t, f)
			if _, e := l.FindCompletion(ctx, f.Grant.Run.Request.Route.TenantID, f.Grant.Run.Request.RunID); !errors.Is(e, domain.ErrNotFound) {
				t.Fatal("rejected completion", e)
			}
		})
	}
	for _, success := range []bool{true, false} {
		t.Run(fmt.Sprintf("memory_%t", success), func(t *testing.T) {
			f := newFinish(t)
			f.MemoryDigest = domain.Digest([]byte("memory"))
			c, e := l.Complete(ctx, f)
			if e != nil {
				t.Fatal(e)
			}
			acceptedHead := head(t, f)
			if _, e = read(c); !errors.Is(e, domain.ErrNotFound) {
				t.Fatal("pending attachment escaped", e)
			}
			var originalDigest string
			if e = pool.QueryRow(ctx, `SELECT digest FROM execution_reply_outbox WHERE intent_id=$1`, c.FinalIntentID).Scan(&originalDigest); e != nil {
				t.Fatal(e)
			}
			if e = l.FinalizeMemory(ctx, c, success); e != nil {
				t.Fatal(e)
			}
			if e = l.FinalizeMemory(ctx, c, success); e != nil {
				t.Fatal("same decision retry", e)
			}
			if e = l.FinalizeMemory(ctx, c, !success); !errors.Is(e, domain.ErrConflict) {
				t.Fatal("opposite decision", e)
			}
			final, e := l.Final(ctx, c.FinalIntentID)
			if e != nil {
				t.Fatal(e)
			}
			event, e := codec.DecodeReplyIntent(final.Payload)
			if e != nil {
				t.Fatal(e)
			}
			if success {
				if accepted, e := read(c); e != nil || accepted.Attachment != attachment {
					t.Fatal(accepted, e)
				}
				if final.Digest != originalDigest || len(event.Content.Attachments) != 1 {
					t.Fatal("success changed attachment", event)
				}
			} else {
				if _, e = read(c); !errors.Is(e, domain.ErrNotFound) {
					t.Fatal("failed Memory attachment leaked", e)
				}
				if len(event.Content.Attachments) != 0 || event.Content.Text != memoryFailureFinal || final.Digest == originalDigest {
					t.Fatal("failure did not clear and redigest", event)
				}
			}
			if head(t, f) != acceptedHead {
				t.Fatal("Memory decision rewrote Session")
			}
		})
	}
	t.Run("text_only_final_has_no_artifact_authority", func(t *testing.T) {
		f := newFinish(t)
		f.Attachments = nil
		c, e := l.Complete(ctx, f)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = read(c); !errors.Is(e, domain.ErrNotFound) {
			t.Fatal(e)
		}
	})
}
