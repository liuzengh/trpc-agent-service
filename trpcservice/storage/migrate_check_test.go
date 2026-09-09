package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// idDiff is a multiset diff: an ID the source holds twice and the target once
// is still missing once, and a duplicate on one side never cancels the other
// side's copy into silence.
func TestIDDiffMultiset(t *testing.T) {
	missing, extra := idDiff([]string{"a", "b", "a"}, []string{"a", "c"})
	if strings.Join(missing, ",") != "a,b" || strings.Join(extra, ",") != "c" {
		t.Fatalf("want missing a,b / extra c, got %v / %v", missing, extra)
	}
	if missing, extra = idDiff(nil, nil); len(missing) != 0 || len(extra) != 0 {
		t.Fatalf("two empty journals must not mismatch: %v / %v", missing, extra)
	}
}

func checkEvent(id, content string) *event.Event {
	return &event.Event{
		ID:     id,
		Author: "user",
		Response: &model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleUser, Content: content},
		}}},
	}
}

// The consistency check must compare content, not counts: two journals of the
// same length that agree on only one event are a hard mismatch.
func TestCheckConsistencyComparesContentNotCounts(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	t.Cleanup(func() { pool.Close() })
	rdb := redisOrSkip(t)
	redisSvc, err := sessionredis.NewService(sessionredis.WithRedisClientURL("redis://" + testRedisAddr))
	if err != nil {
		t.Skipf("redis unavailable (%v), skipping integration test", err)
	}
	t.Cleanup(func() { _ = redisSvc.Close() })
	pgSvc := NewPGSessionService(pool)
	ctx := context.Background()

	tenantID := fmt.Sprintf("fffffff2-0000-0000-0000-%012x", time.Now().UnixNano()%(1<<48))
	appID := fmt.Sprintf("fffffff1-0000-0000-0000-%012x", time.Now().UnixNano()%(1<<48))
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-check', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'migrate-check', 'llm', '{}', 1, 'published')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	key := session.Key{AppName: appID, UserID: "u-check", SessionID: "dm:mock:check-" + t.Name()}
	t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key) })
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM session_event WHERE session_id IN
			(SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`, key.AppName, key.SessionID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM session WHERE app_id=$1 AND session_key=$2`,
			key.AppName, key.SessionID)
	})

	// Source holds e1+e2; target holds e1+x9. Same count, one event swapped.
	rSess, err := redisSvc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"chk-e1", "chk-e2"} {
		if err := redisSvc.AppendEvent(ctx, rSess, checkEvent(id, "检查消息")); err != nil {
			t.Fatal(err)
		}
	}
	pSess, err := pgSvc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"chk-e1", "chk-x9"} {
		if err := pgSvc.AppendEvent(ctx, pSess, checkEvent(id, "检查消息")); err != nil {
			t.Fatal(err)
		}
	}

	m := NewMigrator(pool, rdb, map[string]session.Service{"redis": redisSvc, "postgres": pgSvc}, time.Millisecond)
	mig := migrationRow{TenantID: tenantID, From: "redis", To: "postgres"}

	srcIDs, err := m.journalIDs(ctx, "redis", key)
	if err != nil {
		t.Fatal(err)
	}
	dstIDs, err := m.journalIDs(ctx, "postgres", key)
	if err != nil {
		t.Fatal(err)
	}
	if len(srcIDs) != 2 || len(dstIDs) != 2 {
		t.Fatalf("premise: both journals must hold 2 events, got %d vs %d", len(srcIDs), len(dstIDs))
	}

	mismatches, err := m.checkConsistency(ctx, mig)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) != 1 {
		t.Fatalf("count-equal but content-divergent journals must mismatch once, got %v", mismatches)
	}
	if !strings.Contains(mismatches[0], "chk-e2") || !strings.Contains(mismatches[0], "chk-x9") {
		t.Fatalf("the mismatch must name the missing and the extra event, got %s", mismatches[0])
	}
}
