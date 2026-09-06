// Package recovery_test verifies business invariants after a real dump/restore.
// It never accepts an application DSN or uses existing Compose containers.
package recovery_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

// Run legacy PostgreSQL contracts only against a database created here. Some
// framework adapter tests use fixed table prefixes and must not use live DSNs.
func TestIsolatedPostgresContracts(t *testing.T) {
	if os.Getenv("TEST_RECOVERY_DOCKER") != "1" {
		t.Skip("TEST_RECOVERY_DOCKER not enabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	_, addr := isolatedPostgres(t, ctx)
	dsn := "postgres://drill@" + addr + "/source?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open isolated contract database")
	}
	defer db.Close()
	for db.PingContext(ctx) != nil {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("isolated contract database startup timeout")
		}
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "go", "test", "-race", "-count=1",
		"./deploy/permissions", "./trpcservice/channels/wecommcp", "./trpcservice/approval",
		"./trpcservice/toolexec", "./trpcservice/background", "./trpcservice/controlplane", "./trpcservice/storage")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "TEST_POSTGRES_URL="+dsn, "TEST_PERMISSIONS_DOCKER=0", "TEST_RECOVERY_DOCKER=0",
		"TEST_S3_ENDPOINT=", "TEST_QDRANT_HOST=", "TEST_TRACE_OTLP_ENDPOINT=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated PostgreSQL contracts: %v\n%s", err, out)
	}
	t.Logf("isolated PostgreSQL contracts passed:\n%s", out)
}

func TestIsolatedBusinessBackupRestore(t *testing.T) {
	if os.Getenv("TEST_RECOVERY_DOCKER") != "1" {
		t.Skip("TEST_RECOVERY_DOCKER not enabled; requires cached postgres:16-alpine")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cid, addr := isolatedPostgres(t, ctx)
	open := func(name string) *sql.DB {
		db, err := sql.Open("pgx", "postgres://drill@"+addr+"/"+name+"?sslmode=disable")
		if err != nil {
			t.Fatal("open isolated database")
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	source := open("source")
	for source.PingContext(ctx) != nil {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("isolated database startup timeout")
		}
	}
	if err := database.Migrate(ctx, source); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.SeedBootstrap(ctx, source, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	j, _ := gateway.NewPostgresJournal(source)
	in := gateway.InboundRequest{Scope: runtimecontext.TutorialScope(), ExternalMessageID: "restore-fixture",
		UserID: "fixture-user", SessionID: "fixture-session", ChatType: "direct", Text: "synthetic text"}
	accepted, err := j.Accept(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := j.ClaimQueueOutbox(ctx, "fixture-relay", 1, time.Minute)
	if err != nil || len(outbox) != 1 {
		t.Fatal("fixture outbox claim failed")
	}
	if err := j.MarkQueueOutboxPublished(ctx, outbox[0].ID, "fixture-relay"); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkRunRunning(ctx, accepted.RequestID, "fixture-worker"); err != nil {
		t.Fatal(err)
	}
	result := gateway.RunResult{WorkerID: "fixture-worker", Reply: "synthetic reply", FencingToken: 7, EventCount: 3}
	if err := j.CompleteRun(ctx, outbox[0].Task, result); err != nil {
		t.Fatal(err)
	}
	repo, _ := controlplane.NewPostgresRepository(source)
	state, err := wecommcp.NewStore(repo)
	if err != nil {
		t.Fatal(err)
	}
	key := wecommcp.PollKey{TenantID: in.Scope.TenantID, BindingID: in.Scope.ChannelBindingID, ChatHash: "fixture-chat-hash"}
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	cp, err := state.Checkpoint(ctx, key, "fixture-config", start)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MarkSeen(ctx, key, "fixture-fingerprint"); err != nil {
		t.Fatal(err)
	}
	if err := state.Advance(ctx, key, cp, start.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"sent", "unknown", "attempting"} {
		dk := wecommcp.DeliveryKey{TenantID: key.TenantID, BindingID: key.BindingID, OutboundID: "fixture-" + status}
		if _, owner, err := state.BeginDelivery(ctx, dk, "fixture-input"); err != nil || !owner {
			t.Fatal("fixture delivery not owned")
		}
		if status != "attempting" {
			if err := state.FinishDelivery(ctx, dk, "fixture-input", status); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A real backend stall must not acknowledge intake. Only this owned test
	// container is paused, for the duration of the bounded database operation.
	docker(t, ctx, "pause", cid)
	unavailableCtx, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	unavailable := in
	unavailable.ExternalMessageID = "must-not-persist"
	_, unavailableErr := j.Accept(unavailableCtx, unavailable)
	stop()
	docker(t, ctx, "unpause", cid)
	if unavailableErr == nil {
		t.Fatal("intake acknowledged while its database was unavailable")
	}
	var unexpected int
	if err := source.QueryRowContext(ctx, `SELECT count(*) FROM inbound_message WHERE external_message_id='must-not-persist'`).Scan(&unexpected); err != nil || unexpected != 0 {
		t.Fatal("failed intake unexpectedly committed")
	}
	// Dump and restore inside the isolated container. Neither the dump nor a
	// real user message leaves the test container's disposable writable layer.
	docker(t, ctx, "exec", cid, "pg_dump", "-U", "drill", "-Fc", "-f", "/tmp/business.dump", "source")
	docker(t, ctx, "exec", cid, "createdb", "-U", "drill", "restored")
	docker(t, ctx, "exec", cid, "pg_restore", "--exit-on-error", "--single-transaction", "--no-owner", "-U", "drill", "-d", "restored", "/tmp/business.dump")
	restored := open("restored")
	if err := database.Migrate(ctx, restored); err != nil {
		t.Fatal("restored schema/checksums incompatible: ", err)
	}
	restoredJournal, _ := gateway.NewPostgresJournal(restored)
	replayed, err := restoredJournal.Accept(ctx, in)
	if err != nil || !replayed.Duplicate || replayed.RequestID != accepted.RequestID || replayed.TurnSeq != accepted.TurnSeq {
		t.Fatal("restore lost inbox deduplication or conversation ordering")
	}
	conflict := in
	conflict.Text = "changed synthetic text"
	if _, err := restoredJournal.Accept(ctx, conflict); !errors.Is(err, gateway.ErrMessageConflict) {
		t.Fatal("restore lost payload conflict protection")
	}
	if err := restoredJournal.CompleteRun(ctx, outbox[0].Task, result); err != nil {
		t.Fatal("restored run completion replay failed: ", err)
	}
	replies, err := restoredJournal.ClaimOutbound(ctx, "restored-sender", 10, time.Minute)
	if err != nil || len(replies) != 1 || replies[0].Text != result.Reply {
		t.Fatal("restore lost or duplicated the outbound reply")
	}
	rr, _ := controlplane.NewPostgresRepository(restored)
	rs, err := wecommcp.NewStore(rr)
	if err != nil {
		t.Fatal(err)
	}
	if seen, err := rs.Seen(ctx, key, "fixture-fingerprint"); err != nil || !seen {
		t.Fatal("restore lost channel deduplication")
	}
	current, err := rs.Checkpoint(ctx, key, cp.ConfigHash, start)
	if err != nil || current.Version != cp.Version+1 || !current.Through.Equal(start.Add(30*time.Second)) || !current.Floor.Equal(start) {
		t.Fatal("restore lost checkpoint version/position/floor")
	}
	if err := rs.Advance(ctx, key, cp, start.Add(time.Minute)); !errors.Is(err, wecommcp.ErrStateConflict) {
		t.Fatal("restored checkpoint accepted a stale writer")
	}
	for _, status := range []string{"sent", "unknown", "attempting"} {
		dk := wecommcp.DeliveryKey{TenantID: key.TenantID, BindingID: key.BindingID, OutboundID: "fixture-" + status}
		previous, owner, err := rs.BeginDelivery(ctx, dk, "fixture-input")
		if err != nil || owner || previous.Status != status {
			t.Fatalf("restore made %s delivery sendable again", status)
		}
		if _, _, err := rs.BeginDelivery(ctx, dk, "different-input"); !errors.Is(err, wecommcp.ErrStateConflict) {
			t.Fatal("restored send accepted conflicting payload")
		}
	}
	t.Log("business restore verified: inbox/run/outbox, ordering, checkpoint CAS, seen, sent/unknown/attempting; no model or IM calls")
}

func docker(t *testing.T, ctx context.Context, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("isolated Docker operation failed: %v: %s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func isolatedPostgres(t *testing.T, ctx context.Context) (string, string) {
	t.Helper()
	tag := fmt.Sprintf("recovery-%x", time.Now().UnixNano())
	cid := docker(t, ctx, "run", "-d", "--pull=never", "--label", "trpc-agent.recovery-test="+tag,
		"--tmpfs", "/var/lib/postgresql/data:rw", "-p", "127.0.0.1::5432", "-e", "POSTGRES_USER=drill", "-e", "POSTGRES_DB=source",
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:16-alpine")
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(cid) {
		t.Fatal("invalid owned container ID")
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		label, err := exec.CommandContext(cleanupCtx, "docker", "inspect", "--format", `{{index .Config.Labels "trpc-agent.recovery-test"}}`, cid).Output()
		if err != nil || strings.TrimSpace(string(label)) != tag {
			t.Error("cannot verify test container ownership; retained for review")
			return
		}
		if err := exec.CommandContext(cleanupCtx, "docker", "rm", "-f", cid).Run(); err != nil {
			t.Error("cannot remove owned recovery container")
		} else {
			t.Log("removed only the owned synthetic database container and dump")
		}
	})
	addr := docker(t, ctx, "port", cid, "5432/tcp")
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("isolated PostgreSQL must bind only loopback")
	}
	return cid, addr
}
