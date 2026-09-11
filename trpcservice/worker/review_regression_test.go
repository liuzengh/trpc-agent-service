package worker

import (
	"context"
	"errors"
	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/contentsafety"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	auditlog "github.com/cyl6/trpc-agent-service/trpcservice/log"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/event"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type permissionAuditCapture struct{ entries []auditlog.Entry }

func (s *permissionAuditCapture) Write(e auditlog.Entry) error {
	s.entries = append(s.entries, e)
	return nil
}
func TestPermissionAuditDistinctEvaluationsDoNotCollide(t *testing.T) {
	sink := &permissionAuditCapture{}
	s := NewService(nil, nil, nil, nil, sink, nil, Options{})
	t.Cleanup(func() { _ = s.coordinator.Close() })
	d := governance.ToolDecision{Request: governance.RequestContext{TenantID: "t", SessionID: "s", RequestID: "r1", TurnID: "turn1", ConfigVersion: "v1"}, ToolName: "calculator", Decision: "allow", ArgumentsHash: "same-args"}
	s.observeToolDecision(context.Background(), d)
	// A repeated actual evaluation in the same run must also be preserved.
	s.observeToolDecision(context.Background(), d)
	d.Request.RequestID = "r2"
	d.Request.TurnID = "turn2"
	s.observeToolDecision(context.Background(), d)
	ids := map[string]bool{}
	for _, e := range sink.entries {
		if e.AuditID == "" || ids[e.AuditID] {
			t.Fatalf("audit collision: %s", e.AuditID)
		}
		ids[e.AuditID] = true
		if auditlog.StableAuditID(e) != e.AuditID {
			t.Fatal("persisted event identity changed on replay")
		}
	}
	if len(ids) != 3 {
		t.Fatal("missing evaluations")
	}
}

func TestStrictTurnSafetyAllowsRegeneratedCandidateAfterCommitRollback(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "allowed", true: "blocked"}[blocked], func(t *testing.T) {
			commits := 0
			commitErr := errors.New("injected rollback")
			turn := &strictTestTurn{commitFn: func(_ context.Context, b []byte) ([]byte, bool, error) {
				commits++
				if commits == 1 {
					return nil, false, commitErr
				}
				return b, false, nil
			}}
			calls := 0
			runner := &strictTestRunner{run: func(context.Context) (<-chan *event.Event, error) {
				calls++
				text := "first answer"
				if calls > 1 {
					text = "different retry answer"
					if blocked {
						text = "content-safety-block"
					}
				}
				return strictCompletionEvents(text, 1, 1), nil
			}}
			service, _, _ := newStrictTestService(t, runner, turn, nil, nil, nil, time.Second)
			safety := contentsafety.NewMemory(nil)
			service.safety = safety
			task := strictTask(false)
			if _, err := service.Process(context.Background(), task); !errors.Is(err, commitErr) {
				t.Fatalf("first: %v", err)
			}
			result, err := service.Process(context.Background(), task)
			if blocked {
				if !errors.Is(err, contentsafety.ErrBlocked) || commits != 1 {
					t.Fatalf("blocked retry: result=%+v err=%v commits=%d", result, err, commits)
				}
			} else {
				if err != nil || result.Text != "different retry answer" || commits != 2 {
					t.Fatalf("retry: result=%+v err=%v commits=%d", result, err, commits)
				}
				if _, err := service.Process(context.Background(), task); err != nil {
					t.Fatal(err)
				}
				if calls != 2 {
					t.Fatalf("canonical replay reran model %d times", calls)
				}
			}
			outputs := 0
			for _, d := range safety.Decisions() {
				if d.Phase == contentsafety.PhaseOutput {
					outputs++
				}
			}
			if outputs != 2 {
				t.Fatalf("candidate decisions=%d", outputs)
			}
		})
	}
}

func TestPostgresIntegrationPermissionAuditsAcrossTurnsAndReplay(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("open isolated test database")
	}
	defer pool.Close()
	sink, err := auditlog.NewPostgresSink(pool, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, cleanup := newTestService(nil)
	defer cleanup()
	service.audit = sink
	sessions := sessioninmemory.NewSessionService()
	defer sessions.Close()
	service.acquireRuntime = func(context.Context, config.TenantConfig) (*agentruntime.Runtime, func(), error) {
		return &agentruntime.Runtime{Runner: &approvalIntegrationRunner{}, AppNamespace: "audit-regression", Session: sessions}, func() {}, nil
	}
	tenant := testTenant("audit-review-" + uuid.NewString())
	for _, id := range []string{"first", "second", "second"} {
		if _, err := service.Process(ctx, taskFor(tenant, id, "user", "conversation", domain.ScopeDirect)); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_records WHERE tenant_id=$1 AND payload->>'tool_name'='calculator'`, tenant.TenantID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("tool audit count=%d; wanted two actual evaluations and no replay evaluation", count)
	}
}
