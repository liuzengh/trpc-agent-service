package sessionfence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestPrepareAndDiscardFenceLateWrites(t *testing.T) {
	r := miniredis.RunT(t)
	svc, err := New("redis://"+r.Addr()+"/0", "phase4-freeze")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	turn := svc.StartTurn(key, Fence{TaskID: "task", SessionCoord: SessionCoordFor("tenant", "binding", key.UserID, key.SessionID), SessionSeq: 1, LeaseEpoch: 1, LockToken: "token"})
	ctx := WithTurn(context.Background(), turn)
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID)
	if _, err := svc.Prepare(turn); err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess, &event.Event{}); !errors.Is(err, ErrSessionFenceLost) {
		t.Fatalf("AppendEvent after Prepare error=%v", err)
	}
	svc.Discard(turn)
	if err := svc.UpdateSessionState(ctx, key, session.StateMap{"late": []byte("write")}); !errors.Is(err, ErrSessionFenceLost) {
		t.Fatalf("UpdateSessionState after Discard error=%v", err)
	}
}

func TestDiscardRacesWithAppendWithoutLateSuccess(t *testing.T) {
	r := miniredis.RunT(t)
	svc, err := New("redis://"+r.Addr()+"/0", "phase4-discard-race")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	turn := svc.StartTurn(key, Fence{TaskID: "task", SessionCoord: SessionCoordFor("tenant", "binding", key.UserID, key.SessionID), SessionSeq: 1, LeaseEpoch: 1, LockToken: "token"})
	ctx := WithTurn(context.Background(), turn)
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.AppendEvent(ctx, sess, &event.Event{})
		}()
	}
	svc.Discard(turn)
	wg.Wait()
	if err := svc.AppendEvent(ctx, sess, &event.Event{}); !errors.Is(err, ErrSessionFenceLost) {
		t.Fatalf("late AppendEvent error=%v", err)
	}
}

func TestAppendEventUsesSerializedArrayByteLimit(t *testing.T) {
	r := miniredis.RunT(t)
	svc, err := New("redis://"+r.Addr()+"/0", "phase4-byte-limit")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	e := &event.Event{}
	encoded, err := json.Marshal([]event.Event{*e})
	if err != nil {
		t.Fatal(err)
	}
	svc.SetLimits(Limits{MaxTurnEvents: 1, MaxTurnBytes: len(encoded)})
	turn := svc.StartTurn(key, Fence{TaskID: "task", SessionCoord: SessionCoordFor("tenant", "binding", key.UserID, key.SessionID), SessionSeq: 1, LeaseEpoch: 1, LockToken: "token"})
	if err := svc.AppendEvent(WithTurn(context.Background(), turn), session.NewSession(key.AppName, key.UserID, key.SessionID), e); err != nil {
		t.Fatal(err)
	}
	if turn.IsOverLimit() {
		t.Fatal("exact serialized byte limit was rejected")
	}
	if _, err := svc.Prepare(turn); err != nil {
		t.Fatal(err)
	}

	svc.SetLimits(Limits{MaxTurnEvents: 1, MaxTurnBytes: len(encoded) - 1})
	turn = svc.StartTurn(key, Fence{TaskID: "task-2", SessionCoord: SessionCoordFor("tenant", "binding", key.UserID, "session-2"), SessionSeq: 1, LeaseEpoch: 1, LockToken: "token"})
	if err := svc.AppendEvent(WithTurn(context.Background(), turn), session.NewSession(key.AppName, key.UserID, key.SessionID), e); err != nil {
		t.Fatal(err)
	}
	if !turn.IsOverLimit() {
		t.Fatal("oversized serialized event array was accepted")
	}
}

func TestIndexedSessionCorruptionFailsClosed(t *testing.T) {
	r := miniredis.RunT(t)
	svc, err := New("redis://"+r.Addr()+"/0", "phase4-corrupt-index")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	ctx := context.Background()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	index := svc.userIndexKey(UserCoord(session.UserKey{AppName: key.AppName, UserID: key.UserID}))
	if err := svc.client.HSet(ctx, index, key.SessionID, "missing-coord").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetSession(ctx, key); !errors.Is(err, ErrSessionFenceLost) {
		t.Fatalf("GetSession with dangling index error=%v", err)
	}
	if err := svc.client.HSet(ctx, svc.metaCoordKey("other-coord"), "app_name", key.AppName, "user_id", key.UserID, "session_id", "other", "session_coord", "other-coord", "state_json", "{}", "created_at", time.Now().UTC().Format(time.RFC3339Nano), "updated_at", time.Now().UTC().Format(time.RFC3339Nano)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := svc.client.HSet(ctx, index, key.SessionID, "other-coord").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetSession(ctx, key); !errors.Is(err, ErrSessionFenceLost) {
		t.Fatalf("GetSession with mismatched meta error=%v", err)
	}
}

func TestListSessionsPaginationAndUnpagedLimit(t *testing.T) {
	r := miniredis.RunT(t)
	svc, err := New("redis://"+r.Addr()+"/0", "phase4-list")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	ctx := context.Background()
	seed := func(userID, sessionID string) {
		t.Helper()
		coord := SessionCoordFor("tenant", "binding", userID, sessionID)
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if err := svc.client.HSet(ctx, svc.metaCoordKey(coord),
			"app_name", "app", "user_id", userID, "session_id", sessionID,
			"session_coord", coord, "state_json", "{}", "created_at", now, "updated_at", now).Err(); err != nil {
			t.Fatal(err)
		}
		if err := svc.client.HSet(ctx, svc.userIndexKey(UserCoord(session.UserKey{AppName: "app", UserID: userID})), sessionID, coord).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		seed("paged-user", fmt.Sprintf("session-%d", i))
	}
	page, err := svc.ListSessions(ctx, session.UserKey{AppName: "app", UserID: "paged-user"}, session.WithListSessionPage(1, 2))
	if err != nil || len(page) != 2 || page[0].ID == page[1].ID {
		t.Fatalf("paged sessions=(%#v,%v)", page, err)
	}
	if _, err := svc.ListSessions(ctx, session.UserKey{AppName: "app", UserID: "paged-user"}, session.WithListSessionPage(0, DefaultMaxListSessions+1)); err == nil {
		t.Fatal("oversized explicit page was accepted")
	}

	for i := 0; i <= DefaultMaxListSessions; i++ {
		seed("large-user", fmt.Sprintf("large-%04d", i))
	}
	if _, err := svc.ListSessions(ctx, session.UserKey{AppName: "app", UserID: "large-user"}); err == nil || !strings.Contains(err.Error(), "pagination is required") {
		t.Fatalf("unpaged large ListSessions error=%v", err)
	}
}

func TestAppendEventMarksTurnOverLimitWithoutAppending(t *testing.T) {
	r := miniredis.RunT(t)
	svc, err := New("redis://"+r.Addr()+"/0", "phase4-limit-test")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	svc.SetLimits(Limits{MaxTurnEvents: 1, MaxTurnBytes: 1024})
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	turn := svc.StartTurn(key, Fence{TaskID: "task", SessionCoord: SessionCoordFor("tenant", "binding", key.UserID, key.SessionID), SessionSeq: 1, LeaseEpoch: 1, LockToken: "token"})
	ctx := WithTurn(context.Background(), turn)
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID)
	first := &event.Event{}
	if err := svc.AppendEvent(ctx, sess, first); err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess, first); err != nil {
		t.Fatal(err)
	}
	if !turn.IsOverLimit() || len(turn.Events) != 1 {
		t.Fatalf("turn over_limit=%v events=%d", turn.IsOverLimit(), len(turn.Events))
	}
	if _, err := svc.Prepare(turn); err != ErrTurnTooLarge {
		t.Fatalf("Prepare error=%v, want ErrTurnTooLarge", err)
	}
}
