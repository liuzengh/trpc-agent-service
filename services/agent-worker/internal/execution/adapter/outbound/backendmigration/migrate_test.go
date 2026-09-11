package backendmigration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

type sessionFake struct {
	values map[sessionstore.Head]sessionstore.Candidate
	badPut bool
}

func (s *sessionFake) Load(_ context.Context, tenant, session string, head sessionstore.Head) (sessionstore.Candidate, error) {
	c, ok := s.values[head]
	if !ok || c.Identity.TenantID != tenant || c.Identity.SessionID != session {
		return c, sessionstore.ErrNotFound
	}
	return c, nil
}
func (s *sessionFake) Put(_ context.Context, c sessionstore.Candidate) (sessionstore.Head, error) {
	_, head, err := c.Encode(1 << 20)
	if err != nil {
		return head, err
	}
	if s.badPut {
		head.Digest = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		return head, nil
	}
	if old, ok := s.values[head]; ok {
		oldBody, _, _ := old.Encode(1 << 20)
		body, _, _ := c.Encode(1 << 20)
		if string(oldBody) != string(body) {
			return sessionstore.Head{}, sessionstore.ErrConflict
		}
		return head, nil
	}
	s.values[head] = c
	return head, nil
}

type memoryFake struct {
	values map[memorystore.Scope]memorystore.Snapshot
	mutate bool
}

func (m *memoryFake) Load(_ context.Context, scope memorystore.Scope) (memorystore.Snapshot, error) {
	return m.values[scope], nil
}
func (m *memoryFake) ImportSnapshot(_ context.Context, scope memorystore.Scope, snapshot memorystore.Snapshot) error {
	if old, ok := m.values[scope]; ok {
		oldDigest, _ := memorystore.SnapshotDigest(scope, old)
		newDigest, _ := memorystore.SnapshotDigest(scope, snapshot)
		if old.Revision != snapshot.Revision || oldDigest != newDigest {
			return memorystore.ErrConflict
		}
		return nil
	}
	if m.mutate {
		snapshot.Revision++
	}
	m.values[scope] = snapshot
	return nil
}

func migrationFixture(t *testing.T) (Plan, *sessionFake, *memoryFake) {
	t.Helper()
	candidate := sessionstore.Candidate{
		Identity:       sessionstore.Identity{TenantID: "tenant", SessionID: "session", RunID: "run", AttemptID: "attempt"},
		ContentVersion: sessionstore.ContentVersion,
		Snapshot:       json.RawMessage(`{"events":["user","assistant"]}`),
	}
	_, head, err := candidate.Encode(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	scope := memorystore.Scope{TenantID: "tenant", ID: "user"}
	entry := &memory.Entry{ID: "entry", AppName: "tenant", UserID: "user", Memory: &memory.Memory{Memory: "tea"}, CreatedAt: time.Unix(1, 0).UTC(), UpdatedAt: time.Unix(1, 0).UTC()}
	return Plan{Sessions: []SessionRoot{{TenantID: "tenant", SessionID: "session", Head: head}}, Memories: []memorystore.Scope{scope}},
		&sessionFake{values: map[sessionstore.Head]sessionstore.Candidate{head: candidate}},
		&memoryFake{values: map[memorystore.Scope]memorystore.Snapshot{scope: {Revision: 7, Entries: []*memory.Entry{entry}}}}
}

func TestMigrateCopiesVerifiesAndReplays(t *testing.T) {
	plan, sessionSource, memorySource := migrationFixture(t)
	sessionTarget := &sessionFake{values: map[sessionstore.Head]sessionstore.Candidate{}}
	memoryTarget := &memoryFake{values: map[memorystore.Scope]memorystore.Snapshot{}}
	for run := 0; run < 2; run++ {
		result, err := Migrate(context.Background(), plan, sessionSource, sessionTarget, memorySource, memoryTarget)
		if err != nil || result != (Result{SessionsCopied: 1, MemoriesCopied: 1}) {
			t.Fatal(result, err)
		}
	}
}

func TestMigrateRejectsInvalidPlanAndConflictingTarget(t *testing.T) {
	plan, sessionSource, memorySource := migrationFixture(t)
	for name, mutate := range map[string]func(*Plan){
		"empty":     func(p *Plan) { *p = Plan{} },
		"duplicate": func(p *Plan) { p.Memories = append(p.Memories, p.Memories[0]) },
		"no head":   func(p *Plan) { p.Sessions[0].Head = sessionstore.Head{} },
	} {
		t.Run(name, func(t *testing.T) {
			copy := plan
			copy.Sessions = append([]SessionRoot(nil), plan.Sessions...)
			copy.Memories = append([]memorystore.Scope(nil), plan.Memories...)
			mutate(&copy)
			if _, err := Migrate(context.Background(), copy, sessionSource, &sessionFake{}, memorySource, &memoryFake{}); !errors.Is(err, ErrInvalidPlan) {
				t.Fatal(err)
			}
		})
	}
	scope := plan.Memories[0]
	conflict := &memoryFake{values: map[memorystore.Scope]memorystore.Snapshot{scope: {Revision: 1}}}
	result, err := Migrate(context.Background(), plan, sessionSource, &sessionFake{values: map[sessionstore.Head]sessionstore.Candidate{}}, memorySource, conflict)
	if !errors.Is(err, memorystore.ErrConflict) || result.SessionsCopied != 1 || result.MemoriesCopied != 0 {
		t.Fatal(result, err)
	}
}

func TestMigrateDetectsTargetVerificationFailureAndCancellation(t *testing.T) {
	plan, sessionSource, memorySource := migrationFixture(t)
	if _, err := Migrate(context.Background(), plan, sessionSource, &sessionFake{values: map[sessionstore.Head]sessionstore.Candidate{}, badPut: true}, memorySource, &memoryFake{}); !errors.Is(err, ErrVerify) {
		t.Fatal(err)
	}
	plan.Sessions = nil
	if _, err := Migrate(context.Background(), plan, nil, nil, memorySource, &memoryFake{values: map[memorystore.Scope]memorystore.Snapshot{}, mutate: true}); !errors.Is(err, ErrVerify) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Migrate(canceled, plan, nil, nil, memorySource, &memoryFake{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
