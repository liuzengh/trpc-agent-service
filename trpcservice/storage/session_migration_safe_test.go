package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type countingSummary struct{ calls atomic.Int32 }

func (s *countingSummary) ShouldSummarize(*session.Session) bool { return true }
func (s *countingSummary) Summarize(context.Context, *session.Session) (string, error) {
	return fmt.Sprintf("summary-call-%d", s.calls.Add(1)), nil
}
func (s *countingSummary) SetPrompt(string)         {}
func (s *countingSummary) SetModel(model.Model)     {}
func (s *countingSummary) Metadata() map[string]any { return nil }
func safeSessionFixture(t *testing.T) (*controlplane.MemoryRepository, *SessionRouter, controlplane.BackendMigration, *countingSummary, session.Key, *portableSession) {
	t.Helper()
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	sum := &countingSummary{}
	startup := inmemory.NewSessionService(inmemory.WithSummarizer(sum))
	r, err := NewSessionRouter(repo, secret.StaticStore{}, startup, sum)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(); _ = repo.Close() })
	b := controlplane.BackendBinding{ID: "target-safe", TenantID: "tutorial-tenant", AppID: "tutorial-app", ResourceType: "session", BackendType: "inmemory", Config: json.RawMessage(`{}`), Version: 1, MigrationState: "migration_target"}
	if err := repo.CreateBackendBinding(storageTestContext(), b); err != nil {
		t.Fatal(err)
	}
	m := controlplane.BackendMigration{ID: "migration-safe", TenantID: b.TenantID, AppID: b.AppID, ResourceType: "session", SourceBindingID: "tutorial-session-backend", TargetBindingID: b.ID, State: controlplane.MigrationBackfill, Version: 1, Checkpoint: json.RawMessage(`{}`), Verification: json.RawMessage(`{}`)}
	if err := repo.CreateBackendMigration(storageTestContext(), m); err != nil {
		t.Fatal(err)
	}
	target, err := r.cachedService(storageTestContext(), b)
	if err != nil {
		t.Fatal(err)
	}
	return repo, r, m, sum, session.Key{AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "user", SessionID: "session"}, target.(*portableSession)
}
func TestSessionDualWriteKeepsEventIdentityAndSingleSummary(t *testing.T) {
	repo, r, m, sum, key, target := safeSessionFixture(t)
	ctx := storageTestContext()
	sess, err := r.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		evt := &event.Event{ID: fmt.Sprintf("stable-%d", i), Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage(fmt.Sprint(i))}}}}
		if err = r.AppendEvent(ctx, sess, evt); err != nil {
			t.Fatal(err)
		}
	}
	if err = r.CreateSessionSummary(ctx, sess, "", true); err != nil {
		t.Fatal(err)
	}
	if sum.calls.Load() != 1 {
		t.Fatal("secondary independently called summary model")
	}
	v, err := r.VerifySession(ctx, m.TenantID, m.ID, SessionMigrationItem{key.UserID, key.SessionID})
	if err != nil || !v.Passed {
		t.Fatalf("verify=%+v err=%v", v, err)
	}
	if _, err = repo.TransitionBackendMigration(ctx, m.TenantID, m.ID, controlplane.MigrationCutover, 1, nil, nil); err != nil {
		t.Fatal(err)
	}
	stored, _ := target.GetSession(ctx, key)
	if len(stored.Events) != 3 || stored.Events[1].ID != "stable-1" {
		t.Fatal("event identity changed")
	}
	if err = r.AppendEvent(ctx, sess, &event.Event{ID: "new", Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("new")}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.TransitionBackendMigration(ctx, m.TenantID, m.ID, controlplane.MigrationCompleted, 2, nil, nil); err == nil {
		t.Fatal("stale proof accepted after write")
	}
}

type stagingFault struct {
	session.Service
	fail    atomic.Bool
	entered chan struct{}
	resume  chan struct{}
	once    atomic.Bool
}

func (s *stagingFault) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, opts ...session.Option) error {
	if s.fail.Load() {
		return errors.New("synthetic staging failure")
	}
	if s.entered != nil && s.once.CompareAndSwap(false, true) {
		close(s.entered)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Service.AppendEvent(ctx, sess, evt, opts...)
}
func TestSessionImportStagesWithoutDestroyingTargetAndCoordinatesWrites(t *testing.T) {
	_, r, m, _, key, target := safeSessionFixture(t)
	ctx := storageTestContext()
	source, _ := r.startup.CreateSession(ctx, key, session.StateMap{"value": []byte("original")})
	for i := 0; i < 3; i++ {
		if err := r.startup.AppendEvent(ctx, source, &event.Event{ID: fmt.Sprint(i), Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage(fmt.Sprint(i))}}}}); err != nil {
			t.Fatal(err)
		}
	}
	old, _ := target.CreateSession(ctx, key, session.StateMap{"value": []byte("old-target")})
	if err := target.AppendEvent(ctx, old, &event.Event{ID: "old", Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("old")}}}}); err != nil {
		t.Fatal(err)
	}
	fault := &stagingFault{Service: target.Service}
	target.Service = fault
	fault.fail.Store(true)
	if _, err := r.BackfillSession(ctx, m.TenantID, m.ID, SessionMigrationItem{key.UserID, key.SessionID}); err == nil {
		t.Fatal("expected copy failure")
	}
	stored, _ := target.GetSession(ctx, key)
	if string(stored.State["value"]) != "old-target" {
		t.Fatal("failed staging destroyed active target")
	}
	fault.fail.Store(false)
	fault.entered = make(chan struct{})
	fault.resume = make(chan struct{})
	copied := make(chan error, 1)
	go func() {
		v, err := r.BackfillSession(ctx, m.TenantID, m.ID, SessionMigrationItem{key.UserID, key.SessionID})
		if err == nil && !v.Passed {
			err = errors.New("verification failed")
		}
		copied <- err
	}()
	<-fault.entered
	written := make(chan error, 1)
	go func() {
		written <- r.AppendEvent(ctx, source, &event.Event{ID: "after", Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("after")}}}})
	}()
	select {
	case <-written:
		t.Fatal("write overtook snapshot import")
	case <-time.After(20 * time.Millisecond):
	}
	close(fault.resume)
	if err := <-copied; err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	v, err := r.VerifySession(ctx, m.TenantID, m.ID, SessionMigrationItem{key.UserID, key.SessionID})
	if err != nil || !v.Passed || v.TargetEvents != 4 {
		t.Fatalf("verify=%+v err=%v", v, err)
	}
	// Full event verification catches same-count corruption.
	native, _ := target.nativeKey(ctx, key)
	corrupt, _ := fault.GetSession(ctx, native)
	corrupt.Events[0].Choices[0].Message.Content = "corrupted"
	if sameEvents(source, corrupt) {
		t.Fatal("content corruption not detected")
	}
}
func TestSessionStorageRejectsStaleFencingToken(t *testing.T) {
	_, r, _, _, key, _ := safeSessionFixture(t)
	ctx := storageTestContext()
	sess, _ := r.CreateSession(ctx, key, nil)
	if _, err := r.GetSession(coordination.ContextWithFencingToken(ctx, 4), key); err != nil {
		t.Fatal(err)
	}
	err := r.AppendEvent(coordination.ContextWithFencingToken(ctx, 3), sess, &event.Event{ID: "stale", Response: &model.Response{}})
	if !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatal("stale token reached storage")
	}
}

type summaryWriteFault struct {
	session.Service
	fail bool
}

func (s *summaryWriteFault) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	if _, summaryWrite := state[portableSummariesKey]; summaryWrite && s.fail {
		return errors.New("synthetic secondary summary failure")
	}
	return s.Service.UpdateSessionState(ctx, key, state)
}

func TestSummaryRetryRepairsSecondaryWithoutRegenerating(t *testing.T) {
	_, r, m, sum, key, target := safeSessionFixture(t)
	ctx := storageTestContext()
	sess, err := r.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AppendEvent(ctx, sess, &event.Event{ID: "one", Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("hello")}}}}); err != nil {
		t.Fatal(err)
	}
	fault := &summaryWriteFault{Service: target.Service, fail: true}
	target.Service = fault
	if err := r.CreateSessionSummary(ctx, sess, "", true); err == nil {
		t.Fatal("secondary failure was hidden")
	}
	fault.fail = false
	if err := r.CreateSessionSummary(ctx, sess, "", true); err != nil {
		t.Fatal(err)
	}
	if sum.calls.Load() != 1 {
		t.Fatal("repair unnecessarily called the model again")
	}
	v, err := r.VerifySession(ctx, m.TenantID, m.ID, SessionMigrationItem{key.UserID, key.SessionID})
	if err != nil || !v.Passed {
		t.Fatalf("secondary summary not repaired: %+v %v", v, err)
	}
}
