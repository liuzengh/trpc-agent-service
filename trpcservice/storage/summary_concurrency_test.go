package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type blockedSummary struct {
	countingSummary
	started, release chan struct{}
	first            atomic.Bool
}

func (s *blockedSummary) Summarize(ctx context.Context, _ *session.Session) (string, error) {
	if s.first.CompareAndSwap(false, true) {
		close(s.started)
		select {
		case <-s.release:
			return "old summary", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "new summary", nil
}

func TestSummaryGenerationDoesNotHoldStorageLocksOrOverwriteNewerSummary(t *testing.T) {
	ctx, cancel := context.WithTimeout(storageTestContext(), 5*time.Second)
	defer cancel()
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	defer func() { _ = repo.Close() }()
	sum := &blockedSummary{started: make(chan struct{}), release: make(chan struct{})}
	r, err := NewSessionRouter(repo, secret.StaticStore{}, inmemory.NewSessionService(inmemory.WithSummarizer(sum)), sum)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	key := session.Key{AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice", SessionID: "one"}
	sess, err := r.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent := func(sess *session.Session, id string) {
		t.Helper()
		if err := r.AppendEvent(ctx, sess, &event.Event{ID: id, Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage(id)}}}}); err != nil {
			t.Fatal(err)
		}
	}
	appendEvent(sess, "first")
	done := make(chan error, 1)
	go func() { done <- r.CreateSessionSummary(ctx, sess.Clone(), "", true) }()
	joined := false
	defer func() {
		if !joined {
			close(sum.release)
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}()
	select {
	case <-sum.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	limited, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	otherKey := key
	otherKey.UserID, otherKey.SessionID = "bob", "two"
	if _, err := r.CreateSession(limited, otherKey, nil); err != nil {
		t.Fatal("other session blocked", err)
	}
	if _, err := r.GetSession(limited, key); err != nil {
		t.Fatal("same session read blocked during model call", err)
	}
	if err := repo.WithResourceSync(limited, "tutorial-tenant", "tutorial-app", "session", func(context.Context, *controlplane.ResourceSync, func() error) error { return nil }); err != nil {
		t.Fatal("migration gate held during model call", err)
	}
	appendEvent(sess, "second")
	// Simulate a second node's completion. The framework intentionally has a
	// process-local summary lock, but it cannot serialize different nodes.
	newSnapshot, err := r.GetSession(limited, key)
	if err != nil {
		t.Fatal(err)
	}
	newCandidate := newSnapshot.Clone()
	last := newCandidate.Events[len(newCandidate.Events)-1]
	newCandidate.Summaries = map[string]*session.Summary{"": {Summary: "new summary", UpdatedAt: last.Timestamp, Boundary: session.NewSummaryBoundaryWithEventID("", last.Timestamp, last.ID)}}
	newCandidate.SetState(session.SummaryLastIncludedEventIDStateKey, []byte(last.ID))
	if err := r.commitSessionSummary(limited, sess, newSnapshot, newCandidate); err != nil {
		t.Fatal(err)
	}
	stored, err := r.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if text, ok := r.GetSessionSummaryText(ctx, stored); !ok || text != "new summary" {
		t.Fatalf("summary=%q exists=%t", text, ok)
	}
	cutoff, _ := stored.GetState(session.SummaryLastIncludedEventIDStateKey)
	close(sum.release)
	err = <-done
	joined = true
	if err != nil {
		t.Fatal(err)
	}
	stored, err = r.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := stored.GetState(session.SummaryLastIncludedEventIDStateKey)
	if text, _ := r.GetSessionSummaryText(ctx, stored); text != "new summary" || string(cutoff) != string(after) || len(stored.Events) != 2 {
		t.Fatal("old completion overwrote the new summary, cutoff or history")
	}
}

func TestSummarySnapshotRejectsRecreatedOrRewrittenHistory(t *testing.T) {
	snapshot := session.NewSession("t/tenant/a/app", "user", "session")
	snapshot.Events = []event.Event{{ID: "one"}}
	current := snapshot.Clone()
	current.Events = append(current.Events, event.Event{ID: "two"})
	if !summarySnapshotCurrent(snapshot, current) {
		t.Fatal("new suffix should be allowed")
	}
	for _, mutate := range []func(*session.Session){
		func(s *session.Session) { s.CreatedAt = s.CreatedAt.Add(time.Second) },
		func(s *session.Session) { s.Events[0].ID = "changed" },
		func(s *session.Session) { s.Events = nil },
		func(s *session.Session) { s.SetState(session.SummaryLastIncludedEventIDStateKey, []byte("newer")) },
	} {
		candidate := current.Clone()
		mutate(candidate)
		if summarySnapshotCurrent(snapshot, candidate) {
			t.Fatal("stale snapshot accepted")
		}
	}
}
