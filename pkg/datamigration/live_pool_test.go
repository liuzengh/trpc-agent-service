package datamigration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
)

func testPool(t *testing.T, limit int) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(limit)
	db.SetMaxIdleConns(limit)
	mock.MatchExpectationsInOrder(false)
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return db, mock
}

// Exercise the real outer execution fence -> migration gate -> nested SQL
// lookup chain. The barrier deterministically fills the lock pools before any
// resolver continues, reproducing the former circular pool wait without a
// database server or timing-sensitive SQL delays.
func TestLiveReadsProgressWhenExecutionAndMigrationPoolsAreFull(t *testing.T) {
	for _, limits := range []struct {
		name                     string
		control, execution, gate int
		concurrency, firstWave   int
	}{
		{"25 simultaneous lock holders", 3, 25, 25, 25, 25},
		{"25 connection production budget", 9, 8, 8, 50, 8},
	} {
		t.Run(limits.name, func(t *testing.T) {
			controlDB, controlMock := testPool(t, limits.control)
			executionDB, executionMock := testPool(t, limits.execution)
			gateDB, gateMock := testPool(t, limits.gate)
			route := liveTestRoute()
			route.Mirroring = false
			route.ReadProfile, route.WriteProfile = "target", "target"
			tokens := make([]fence.Token, limits.concurrency)
			now := time.Now().UTC()
			for i := range tokens {
				token := fence.Token{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: fmt.Sprint("session-", i), ExecutionID: int64(i + 1), Generation: 1, Value: "test-authority"}
				tokens[i] = token
				executionMock.ExpectExec("SELECT pg_advisory_lock").WithArgs(token.Scope()).WillReturnResult(sqlmock.NewResult(0, 1))
				executionMock.ExpectQuery(`e.execution_token, e.status, e.lease_until`).WithArgs(token.TenantID, token.AgentAppID, token.SessionID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "generation", "execution_id", "token", "execution_status", "lease_until", "heartbeat_at", "lease_valid"}).
						AddRow("RUNNING", 1, token.ExecutionID, token.Value, "RUNNING", now.Add(10*time.Minute), now, true))
				executionMock.ExpectQuery(`e.execution_token, e.lease_until`).WithArgs(token.TenantID, token.AgentAppID, token.SessionID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "generation", "execution_id", "token", "lease_until", "lease_valid"}).
						AddRow("RUNNING", 1, token.ExecutionID, token.Value, now.Add(10*time.Minute), true))
				executionMock.ExpectQuery("SELECT pg_advisory_unlock").WithArgs(token.Scope()).WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))
				expectLiveGate(gateMock, route, true)
				expectLiveUnlock(gateMock, true)
				controlMock.ExpectQuery(`SELECT config->'storage' FROM tenants`).WithArgs("tenant-a").
					WillReturnRows(sqlmock.NewRows([]string{"storage"}).AddRow([]byte(`{}`)))
			}
			allResolving := make(chan struct{})
			var entered, executed atomic.Int64
			coordinator, err := NewLiveCoordinator(LiveOptions{DB: controlDB, GateDB: gateDB, Owner: "test-owner",
				Resolve: func(ctx context.Context, tenantID string, _ Domain, _ string) (LiveBackend, func(), error) {
					if entered.Add(1) == int64(limits.firstWave) {
						close(allResolving)
					}
					select {
					case <-allResolving:
					case <-ctx.Done():
						return nil, nil, ctx.Err()
					}
					var config []byte
					if err := controlDB.QueryRowContext(ctx, `SELECT config->'storage' FROM tenants WHERE id=$1 AND status <> 'deleted'`, tenantID).Scan(&config); err != nil {
						return nil, nil, err
					}
					return &liveTestBackend{info: LiveBackendInfo{Backend: "postgres", Identity: route.TargetIdentity, Compatibility: route.Compatibility}}, func() {}, nil
				}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			authorizer := controlplane.NewPostgresSessionFence(executionDB)
			results := make(chan error, len(tokens))
			for _, token := range tokens {
				go func() {
					release, err := authorizer.Acquire(ctx, token)
					if err == nil {
						err = coordinator.WithRead(ctx, token.TenantID, DomainSession, "source", func(context.Context, string) error { executed.Add(1); return nil })
						err = errors.Join(err, release())
					}
					results <- err
				}()
			}
			for range tokens {
				if err := <-results; err != nil {
					t.Errorf("fenced migration read: %v", err)
				}
			}
			if executed.Load() != int64(len(tokens)) {
				t.Fatalf("completed %d/%d operations", executed.Load(), len(tokens))
			}
			for _, db := range []*sql.DB{executionDB, gateDB, controlDB} {
				if stats := db.Stats(); stats.InUse != 0 {
					t.Errorf("connections leaked after fenced operations: %+v", stats)
				}
			}
		})
	}
}

func TestLiveCoordinatorRequiresSeparateBoundedGatePool(t *testing.T) {
	controlDB, _ := testPool(t, 25)
	gateDB, _ := testPool(t, 8)
	options := LiveOptions{DB: controlDB, Owner: "test", Resolve: func(context.Context, string, Domain, string) (LiveBackend, func(), error) { return nil, nil, nil }}
	for _, invalid := range []*sql.DB{nil, controlDB} {
		options.GateDB = invalid
		if _, err := NewLiveCoordinator(options); !errors.Is(err, ErrMigrationCapability) {
			t.Fatalf("unsafe gate pool accepted: %v", err)
		}
	}
	gateDB.SetMaxOpenConns(0)
	options.GateDB = gateDB
	if _, err := NewLiveCoordinator(options); !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("unbounded gate pool accepted: %v", err)
	}
}

func TestLiveGateWaitCancellationDoesNotConsumeControlPool(t *testing.T) {
	controlDB, controlMock := testPool(t, 3)
	gateDB, _ := testPool(t, 1)
	coordinator, err := NewLiveCoordinator(LiveOptions{DB: controlDB, GateDB: gateDB, Owner: "test",
		Resolve: func(context.Context, string, Domain, string) (LiveBackend, func(), error) { return nil, nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	// A blocked PostgreSQL advisory waiter occupies the gate pool's connection.
	// A new waiter must cancel without consuming a metadata connection.
	holder, err := gateDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := coordinator.gate(ctx, "tenant-a", DomainSession, false); result <- err }()
	controlMock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	var one int
	if err := controlDB.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("gate waiter blocked metadata: %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("gate waiter cancellation: %v", err)
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	if gateDB.Stats().InUse != 0 || controlDB.Stats().InUse != 0 {
		t.Fatal("canceled gate waiter retained a connection")
	}
}
