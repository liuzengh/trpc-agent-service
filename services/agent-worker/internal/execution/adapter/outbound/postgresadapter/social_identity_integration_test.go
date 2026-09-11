package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

func TestMinimalSocialIdentityPostgres(t *testing.T) {
	mu, ru := os.Getenv("WORKER_TEST_MIGRATION_URL"), os.Getenv("WORKER_TEST_RUNTIME_URL")
	if mu == "" || ru == "" {
		t.Skip("requires dedicated Worker roles")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	open := func(url string) *pgxpool.Pool {
		p, e := pgxpool.New(ctx, url)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(p.Close)
		return p
	}
	owner, a, b := open(mu), open(ru), open(ru)
	if e := migrations.ApplyForRuntime(ctx, owner, "worker_runtime"); e != nil {
		t.Fatal(e)
	}
	policy := domain.Policy{Version: "minimal", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 5 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 3}
	for _, provider := range []string{"telegram", "wecom"} {
		t.Run(provider, func(t *testing.T) {
			prefix := fmt.Sprintf("social-%s-%d", provider, time.Now().UnixNano())
			request := func(id, sender string) domain.Requested {
				return domain.Requested{EventID: prefix + id, RunID: prefix + id + "run", AdmissionID: prefix + id + "adm", EventDigest: domain.Digest([]byte(prefix + id)), RunDigest: domain.Digest([]byte(prefix + id + sender)), Route: domain.Route{TenantID: prefix, Provider: provider, AccountID: "bot", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{SenderID: sender, ConversationID: "same-conversation", Text: id, ReceivedAt: time.Now().UTC()}}
			}
			limits := domain.IntakeLimits{MaxQueuedRuns: 1000, MaxRetainedRuns: 10000}
			one, two := New(a), New(b)
			req := request("first", "user-a")
			first, e := one.Accept(ctx, req, policy, limits)
			if e != nil {
				t.Fatal(e)
			}
			var seen time.Time
			if e = a.QueryRow(ctx, `SELECT last_seen_at FROM execution_social_identities WHERE tenant_id=$1 AND identity_id=$2`, prefix, req.SocialIdentityID()).Scan(&seen); e != nil {
				t.Fatal(e)
			}
			replay, e := two.Accept(ctx, req, policy, limits)
			if e != nil || replay != first {
				t.Fatal("receipt replay", e)
			}
			var after time.Time
			if e = b.QueryRow(ctx, `SELECT last_seen_at FROM execution_social_identities WHERE tenant_id=$1 AND identity_id=$2`, prefix, req.SocialIdentityID()).Scan(&after); e != nil || !seen.Equal(after) {
				t.Fatal("replay touched identity", e)
			}
			other, e := two.Accept(ctx, request("other", "user-b"), policy, limits)
			if e != nil || other.SessionID == first.SessionID || other.Sequence != 1 {
				t.Fatal("different sender shared history", e)
			}
			// Independent pools race new messages for one identity/session. No registry
			// policy, current-authority feed, quota grant or pending promoter exists.
			receipts := make(chan domain.Receipt, 8)
			errs := make(chan error, 8)
			var wg sync.WaitGroup
			for n := 0; n < 8; n++ {
				wg.Add(1)
				go func(n int) {
					defer wg.Done()
					l := one
					if n%2 == 1 {
						l = two
					}
					r, e := l.Accept(ctx, request(fmt.Sprint("next", n), "user-a"), policy, limits)
					receipts <- r
					errs <- e
				}(n)
			}
			wg.Wait()
			close(receipts)
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			seq := map[int64]bool{}
			for r := range receipts {
				if r.SessionID != first.SessionID || seq[r.Sequence] || r.Sequence < 2 || r.Sequence > 9 {
					t.Fatal("session sequence", r)
				}
				seq[r.Sequence] = true
			}
			var identities, sessions, runs int
			for sql, target := range map[string]*int{`SELECT count(*) FROM execution_social_identities WHERE tenant_id=$1`: &identities, `SELECT count(*) FROM execution_session_identities WHERE tenant_id=$1`: &sessions, `SELECT count(*) FROM execution_runs WHERE tenant_id=$1`: &runs} {
				if e = a.QueryRow(ctx, sql, prefix).Scan(target); e != nil {
					t.Fatal(e)
				}
			}
			if identities != 2 || sessions != 2 || runs != 10 {
				t.Fatal(identities, sessions, runs)
			}
			// Formal Claim proceeds immediately; the old governance gate is absent.
			g, e := two.Claim(ctx, domain.ClaimRequest{TenantID: prefix, RunID: req.RunID, WorkerID: "second-node", MaxRunSeconds: 60, MaxActive: 100})
			if e != nil {
				t.Fatal("formal claim", e)
			}
			if e = two.MarkExecuting(ctx, g); e != nil {
				t.Fatal(e)
			}
			if e = one.MarkExecuting(ctx, g); !errors.Is(e, domain.ErrFenced) {
				t.Fatal("repeat dispatch", e)
			}
			if e = two.FailAttempt(ctx, g, "TEST_COMPLETED", false); e != nil {
				t.Fatal(e)
			}
			// A storage capacity rejection must not create a social identity as a side effect.
			_, e = one.Accept(ctx, request("full", "never-recorded"), policy, domain.IntakeLimits{MaxQueuedRuns: 1, MaxRetainedRuns: 10000})
			if !errors.Is(e, domain.ErrCapacity) {
				t.Fatal(e)
			}
			if e = a.QueryRow(ctx, `SELECT count(*) FROM execution_social_identities WHERE tenant_id=$1`, prefix).Scan(&identities); e != nil || identities != 2 {
				t.Fatal("orphan identity", e, identities)
			}
		})
	}
	t.Log("MINIMAL_SOCIAL_IDENTITY=PASS providers=2 pools=2 identities=2_per_provider sessions=2_per_provider concurrent_same_user=8 duplicate_side_effects=0 formal_claim=PASS repeat_dispatch=DENIED capacity_orphans=0")
}
