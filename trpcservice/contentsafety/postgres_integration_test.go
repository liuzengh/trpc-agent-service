package contentsafety

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newSafetyPostgresCheckers(t *testing.T) (*Postgres, *Postgres, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "content_safety_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	newPool := func() *pgxpool.Pool {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["search_path"] = schema
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return pool
	}
	poolA, poolB := newPool(), newPool()
	t.Cleanup(func() {
		poolA.Close()
		poolB.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	tx, err := poolA.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyAll(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := NewPostgres(poolA, "node-a", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewPostgres(poolB, "node-b", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	return a, b, poolA
}

func TestPostgresIntegrationSafetyDecisionIdempotencyAndLeaseRecovery(t *testing.T) {
	a, b, pool := newSafetyPostgresCheckers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	request := Request{TenantID: "acme", AppName: "assistant", SessionID: "session", MessageKey: "message-1",
		Phase: PhaseInput, PolicyVersion: "p1", ConfigRevision: "r1", Text: "hello"}
	request.ContentHash = HashContent(request.Text)
	first, err := a.Check(ctx, request)
	if err != nil || first.Status != StatusAllowed {
		t.Fatalf("first decision = %+v, %v", first, err)
	}
	second, err := b.Check(ctx, request)
	if err != nil || second.DecisionHash != first.DecisionHash {
		t.Fatalf("idempotent decision = %+v, %v", second, err)
	}
	blocked := request
	blocked.MessageKey = "message-blocked"
	blocked.Text = "content-safety-block"
	blocked.ContentHash = HashContent(blocked.Text)
	decision, err := b.Check(ctx, blocked)
	if !errors.Is(err, ErrBlocked) || decision.Status != StatusBlocked {
		t.Fatalf("blocked decision = %+v, %v", decision, err)
	}
	leaseRequest := request
	leaseRequest.MessageKey = "message-lease"
	leaseRequest.Text = "lease recovery"
	leaseRequest.ContentHash = HashContent(leaseRequest.Text)
	if _, err := pool.Exec(ctx, `INSERT INTO content_safety_decisions
		(tenant_id, app_name, session_id, message_key, config_revision, phase, policy_version, content_hash, status, attempts, lease_owner, lease_expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending',1,'dead-node',clock_timestamp()-interval '1 second')`,
		leaseRequest.TenantID, leaseRequest.AppName, leaseRequest.SessionID, leaseRequest.MessageKey,
		leaseRequest.ConfigRevision, string(leaseRequest.Phase), leaseRequest.PolicyVersion, leaseRequest.ContentHash); err != nil {
		t.Fatal(err)
	}
	if decision, err := b.Check(ctx, leaseRequest); err != nil || decision.Status != StatusAllowed {
		t.Fatalf("expired lease was not reclaimed: %+v %v", decision, err)
	}
}

func TestPostgresIntegrationOutputCandidatesRemainIndependentlyChecked(t *testing.T) {
	a, b, _ := newSafetyPostgresCheckers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req := testRequest("first answer")
	req.Phase = PhaseOutput
	req.MessageKey = OutputCandidateKey("same-message", req.ConfigRevision, req.Text)
	first, err := a.Check(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := b.Check(ctx, req)
	if err != nil || replay.DecisionHash != first.DecisionHash {
		t.Fatalf("cross-node replay: %v", err)
	}
	req.Text = "different retry answer"
	req.ContentHash = HashContent(req.Text)
	req.MessageKey = OutputCandidateKey("same-message", req.ConfigRevision, req.Text)
	second, err := b.Check(ctx, req)
	if err != nil || second.DecisionHash == first.DecisionHash {
		t.Fatalf("new candidate: %v", err)
	}
	req.Text = "content-safety-block"
	req.ContentHash = HashContent(req.Text)
	req.MessageKey = OutputCandidateKey("same-message", req.ConfigRevision, req.Text)
	if _, err := a.Check(ctx, req); !errors.Is(err, ErrBlocked) {
		t.Fatalf("old allow authorized blocked candidate: %v", err)
	}
}
