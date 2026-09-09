package storagemigration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestImportSessionSnapshotPreservesStateEventsAndTracks(t *testing.T) {
	ctx := context.Background()
	key := session.Key{AppName: "tenant/demo/app/assistant", UserID: "user", SessionID: "session"}
	source := sessionmemory.NewSessionService()
	defer source.Close()
	created, err := source.CreateSession(ctx, key, session.StateMap{"local": []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.UpdateAppState(ctx, key.AppName, session.StateMap{session.StateAppPrefix + "policy": []byte("v1")}); err != nil {
		t.Fatal(err)
	}
	if err := source.UpdateUserState(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID}, session.StateMap{session.StateUserPrefix + "locale": []byte("zh")}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	item := event.Event{Response: &model.Response{ID: "event-1", Object: model.ObjectTypeChatCompletion, Choices: []model.Choice{{Message: model.NewUserMessage("synthetic")}}, Timestamp: now, Done: true}, InvocationID: "invocation"}
	if err := source.AppendEvent(ctx, created, &item); err != nil {
		t.Fatal(err)
	}
	trackSource := any(source).(session.TrackService)
	if err := trackSource.AppendTrackEvent(ctx, created, &session.TrackEvent{Track: "workflow", Payload: json.RawMessage(`{"step":1}`), Timestamp: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	current, err := source.GetSession(ctx, key, session.WithEventNum(100))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, wantChecksum, err := sessionSnapshot(current)
	if err != nil {
		t.Fatal(err)
	}
	target := sessionmemory.NewSessionService()
	defer target.Close()
	if err := importSessionSnapshot(ctx, target, tenant.BackendPostgres, key, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := verifySessionSnapshot(ctx, target, key, snapshot, wantChecksum); err != nil {
		t.Fatal(err)
	}
}

func TestSessionSnapshotChecksumDetectsContentChange(t *testing.T) {
	base := session.NewSession("tenant/demo/app/assistant", "user", "session", session.WithSessionState(session.StateMap{"key": []byte("a")}))
	_, first, err := sessionSnapshot(base)
	if err != nil {
		t.Fatal(err)
	}
	base.SetState("key", []byte("b"))
	_, second, err := sessionSnapshot(base)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("session content change did not change checksum")
	}
}

func TestSessionCoreChecksumAllowsSummaryBackfill(t *testing.T) {
	base := portableSession{
		State:  session.StateMap{"key": []byte("value")},
		Events: []event.Event{{Response: &model.Response{ID: "event"}}},
	}
	withSummary := base
	withSummary.Summaries = map[string]*session.Summary{"": {Summary: "synthetic", UpdatedAt: time.Unix(12, 0).UTC()}}
	if sessionCoreChecksum(base) != sessionCoreChecksum(withSummary) {
		t.Fatal("summary-only difference must be repairable by backfill")
	}
	withSummary.State = session.StateMap{"key": []byte("changed")}
	if sessionCoreChecksum(base) == sessionCoreChecksum(withSummary) {
		t.Fatal("core session difference must remain a conflict")
	}
}

func TestReconcileSessionSnapshotResumesPartialImport(t *testing.T) {
	ctx := context.Background()
	key := session.Key{AppName: "tenant/demo/app/assistant", UserID: "user", SessionID: "session"}
	target := sessionmemory.NewSessionService()
	defer target.Close()
	current, err := target.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first := event.Event{Response: &model.Response{ID: "event-1", Object: model.ObjectTypeChatCompletion, Choices: []model.Choice{{Message: model.NewUserMessage("first")}}, Timestamp: now, Done: true}, InvocationID: "invocation"}
	second := event.Event{Response: &model.Response{ID: "event-2", Object: model.ObjectTypeChatCompletion, Choices: []model.Choice{{Message: model.NewAssistantMessage("second")}}, Timestamp: now.Add(time.Second), Done: true}, InvocationID: "invocation"}
	if err := target.AppendEvent(ctx, current, &first); err != nil {
		t.Fatal(err)
	}
	snapshot := portableSession{State: session.StateMap{"ready": []byte("yes")}, Events: []event.Event{first, second}}
	if err := reconcileSessionSnapshot(ctx, target, tenant.BackendPostgres, current, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := target.GetSession(ctx, key, session.WithEventNum(10))
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.State["ready"]) != "yes" || len(loaded.Events) != 2 || loaded.Events[1].Response == nil || loaded.Events[1].Response.ID != "event-2" {
		t.Fatalf("partial import was not repaired: state=%q events=%d", loaded.State["ready"], len(loaded.Events))
	}
}
