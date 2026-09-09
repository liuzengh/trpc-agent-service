package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestPostgres16SQLBackendSmoke(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("PHASE55_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("PHASE55_POSTGRES_DSN is not configured")
	}
	runSQLBackendSmoke(t, tenant.StorageKindPostgres, dsn)
}

func TestMySQL8SQLBackendSmoke(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("PHASE55_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("PHASE55_MYSQL_DSN is not configured")
	}
	runSQLBackendSmoke(t, tenant.StorageKindMySQL, dsn)
}

func runSQLBackendSmoke(t *testing.T, kind tenant.StorageKind, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("p55%c%x_", kind[0], time.Now().UnixNano())
	profile := tenant.StorageProfile{TenantID: "tenant-smoke", ID: string(kind) + "-smoke", Kind: kind, CredentialRef: "env:SMOKE_DSN", TablePrefix: prefix}
	if kind == tenant.StorageKindPostgres {
		profile.Schema = "public"
	}
	backend, err := NewSQLBackend(profile, dsn, sessionfence.Limits{MaxTurnEvents: 32, MaxTurnBytes: 128 << 10})
	if err != nil {
		t.Fatalf("construct SQL backend: %v", err)
	}
	defer backend.Close()
	if err := backend.Ready(ctx); err != nil {
		t.Fatalf("ready SQL backend: %v", err)
	}
	if len(backend.Memory().Tools()) != 0 {
		t.Fatalf("SQL Memory exposed tools: %#v", backend.Memory().Tools())
	}

	task := message.ExecutionTask{SchemaVersion: message.TaskSchemaVersion, TaskID: "task-smoke", Channel: "demo", ChannelBindingID: "binding", ExternalAccountID: "account", TenantID: profile.TenantID, AgentAppID: "agent", ConfigVersion: "v1", RunnerUserID: "user", SessionID: "session", PlatformMessageID: "message", ActorUserID: "actor", ConversationID: "conversation", ConversationType: message.ConversationDirect, Text: "hello", RequestID: "request", TraceID: "trace", ReceivedAt: time.Now().UTC(), Attempt: 1}
	task.PayloadDigest = task.CanonicalDigest()
	current := event.New("invocation", "user")
	current.Response = &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: "hello"}}}}
	current.Timestamp = time.Now().UTC()
	commit := sessionfence.TurnCommit{SessionCoord: "coord-smoke", SessionSeq: 1, AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID, Events: []event.Event{*current}, FinalState: session.StateMap{"answer": []byte(`"stored"`)}}
	route := persistence.Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: backend.Fingerprint()}
	envelope, err := persistence.NewEnvelope(task, route, commit, message.OutboundMessage{RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "answer"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Committer().Commit(ctx, envelope); err != nil {
		t.Fatalf("commit SQL turn: %v", err)
	}
	if err := backend.Committer().Commit(ctx, envelope); err != nil {
		t.Fatalf("idempotent SQL turn: %v", err)
	}
	stored, err := backend.Session().GetSession(ctx, session.Key{AppName: commit.AppName, UserID: commit.UserID, SessionID: commit.SessionID})
	if err != nil || stored == nil || string(stored.SnapshotState()["answer"]) != `"stored"` || len(stored.GetEvents()) != 1 {
		t.Fatalf("official Session read = (%#v,%v)", stored, err)
	}
	createdAt := stored.CreatedAt
	task.TaskID = "task-smoke-2"
	task.PlatformMessageID = "message-2"
	task.RequestID = "request-2"
	task.Text = "again"
	task.PayloadDigest = task.CanonicalDigest()
	secondEvent := event.New("invocation-2", "user")
	secondEvent.Response = &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: "again"}}}}
	secondEvent.Timestamp = time.Now().UTC()
	commit.SessionSeq = 2
	commit.Events = []event.Event{*secondEvent}
	commit.FinalState = session.StateMap{"answer": []byte(`"updated"`)}
	secondEnvelope, err := persistence.NewEnvelope(task, route, commit, message.OutboundMessage{RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "updated"}, envelope.PreparedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Committer().Commit(ctx, secondEnvelope); err != nil {
		t.Fatalf("commit second SQL turn: %v", err)
	}
	stored, err = backend.Session().GetSession(ctx, session.Key{AppName: commit.AppName, UserID: commit.UserID, SessionID: commit.SessionID})
	if err != nil || stored == nil || string(stored.SnapshotState()["answer"]) != `"updated"` || len(stored.GetEvents()) != 2 || !stored.CreatedAt.Equal(createdAt) {
		t.Fatalf("official Session second read = (%#v,%v), original created_at=%s", stored, err, createdAt)
	}
	if kind == tenant.StorageKindPostgres {
		if text, ok := backend.Session().GetSessionSummaryText(ctx, stored); text != "" || ok {
			t.Fatalf("PostgreSQL Summary guard returned (%q,%v)", text, ok)
		}
	}

	userKey := memory.UserKey{AppName: commit.AppName, UserID: commit.UserID}
	if err := backend.Memory().AddMemory(ctx, userKey, "favorite color is blue", []string{"preference"}); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	secondProfile := profile
	secondProfile.SkipDBInit = true
	second, err := NewSQLBackend(secondProfile, dsn, sessionfence.Limits{MaxTurnEvents: 32, MaxTurnBytes: 128 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Ready(ctx); err != nil {
		t.Fatalf("second backend ready: %v", err)
	}
	entries, err := second.Memory().ReadMemories(ctx, userKey, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory == nil {
		t.Fatalf("cross-backend memory read = (%#v,%v)", entries, err)
	}
	updated := &memory.UpdateResult{}
	if err := second.Memory().UpdateMemory(ctx, memory.Key{AppName: userKey.AppName, UserID: userKey.UserID, MemoryID: entries[0].ID}, "favorite color is green", []string{"preference"}, memory.WithUpdateResult(updated)); err != nil {
		t.Fatalf("update memory: %v", err)
	}
	found, err := backend.Memory().SearchMemories(ctx, userKey, "green")
	if err != nil || len(found) == 0 {
		t.Fatalf("search updated memory = (%#v,%v)", found, err)
	}
	memoryID := updated.MemoryID
	if memoryID == "" {
		memoryID = found[0].ID
	}
	if err := backend.Memory().DeleteMemory(ctx, memory.Key{AppName: userKey.AppName, UserID: userKey.UserID, MemoryID: memoryID}); err != nil {
		t.Fatalf("delete memory: %v", err)
	}
	if err := backend.Memory().AddMemory(ctx, userKey, "one", nil); err != nil {
		t.Fatal(err)
	}
	if err := backend.Memory().AddMemory(ctx, userKey, "two", nil); err != nil {
		t.Fatal(err)
	}
	if err := second.Memory().ClearMemories(ctx, userKey); err != nil {
		t.Fatalf("clear memories: %v", err)
	}
	if entries, err := backend.Memory().ReadMemories(ctx, userKey, 10); err != nil || len(entries) != 0 {
		t.Fatalf("memories after clear = (%#v,%v)", entries, err)
	}

	missingProfile := profile
	missingProfile.TablePrefix = fmt.Sprintf("m%c%x_", kind[0], time.Now().UnixNano())
	missingProfile.SkipDBInit = true
	missing, err := NewSQLBackend(missingProfile, dsn, sessionfence.Limits{MaxTurnEvents: 32, MaxTurnBytes: 128 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer missing.Close()
	if err := missing.Ready(ctx); !errors.Is(err, persistence.ErrSchemaIncompatible) {
		t.Fatalf("skip_db_init missing schema error = %v", err)
	}
	if missing.Session() != nil || missing.Memory() != nil || missing.Committer() != nil {
		t.Fatal("failed SQL initialization retained partial resources")
	}
	if err := missing.Ready(ctx); !errors.Is(err, persistence.ErrSchemaIncompatible) {
		t.Fatalf("failed SQL backend did not allow a fresh initialization attempt: %v", err)
	}
}
