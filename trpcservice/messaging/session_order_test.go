package messaging

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniserver "github.com/alicebob/miniredis/v2/server"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestStrongSessionOrderingAndPromotion(t *testing.T) {
	server := miniredis.RunT(t)
	mockStrongRedisTopology(server, "phase4-session-order")
	cfg := config.MessagingConfig{RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "strong-order-" + fmt.Sprint(time.Now().UnixNano()), LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: time.Second, MaxAttempts: 3, InboxRetention: time.Hour, ReplyWaitTimeout: time.Second, SessionFencing: "strong", SessionLockDuration: time.Second, SessionWaitBackoff: 10 * time.Millisecond, SessionWaitMaxBackoff: 100 * time.Millisecond}
	store, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := testTask("strong-1", "strong-msg-1")
	second := testTask("strong-2", "strong-msg-2")
	second.SessionID = first.SessionID
	second.PayloadDigest = second.CanonicalDigest()
	if _, _, err := store.Submit(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Submit(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	d1, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	l1, err := store.Begin(context.Background(), d1, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	d2, err := store.ReadTask(context.Background(), "worker-b", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := func() error { _, e := store.Begin(context.Background(), d2, "worker-b"); return e }(); err != ErrSessionBusy && err != ErrSessionWait {
		t.Fatalf("Begin(seq2)=%v, want session busy/wait", err)
	}
	if err := store.DeferSession(context.Background(), d2, time.Now(), "session_busy"); err != nil {
		t.Fatal(err)
	}
	inboxKey := store.inboxKey(d2.InboxID)
	if count := store.client.HGet(context.Background(), inboxKey, "wait_count").Val(); count != "1" {
		t.Fatalf("first wait_count=%q, want 1", count)
	}
	members, err := store.client.ZRangeWithScores(context.Background(), store.sessionWaitKey, 0, 0).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("session wait member=(%#v,%v)", members, err)
	}
	server.SetTime(time.UnixMilli(int64(members[0].Score) + 1))
	if promoted, err := store.PromoteSessionWait(context.Background(), 10); err != nil || promoted != 0 {
		t.Fatalf("PromoteSessionWait while lock active=(%d,%v)", promoted, err)
	}
	if count := store.client.HGet(context.Background(), inboxKey, "wait_count").Val(); count != "2" {
		t.Fatalf("second wait_count=%q, want 2", count)
	}
	commit := sessionfence.TurnCommit{SessionCoord: l1.SessionCoord, SessionSeq: l1.SessionSeq, AppName: tenant.AppName(first.TenantID, first.AgentAppID), UserID: first.RunnerUserID, SessionID: first.SessionID, UserCoord: sessionfence.UserCoord(session.UserKey{AppName: tenant.AppName(first.TenantID, first.AgentAppID), UserID: first.RunnerUserID}), Events: nil, FinalState: map[string][]byte{}}
	reply := message.OutboundMessage{Channel: first.Channel, BindingID: first.ChannelBindingID, RequestID: first.RequestID, TraceID: first.TraceID, SessionID: first.SessionID, Text: "ok"}
	if err := store.CompleteTurn(context.Background(), l1, reply, commit); err != nil {
		t.Fatal(err)
	}
	members, err = store.client.ZRangeWithScores(context.Background(), store.sessionWaitKey, 0, 0).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("deferred session wait member=(%#v,%v)", members, err)
	}
	server.SetTime(time.UnixMilli(int64(members[0].Score) + 1))
	if promoted, err := store.PromoteSessionWait(context.Background(), 10); err != nil || promoted != 1 {
		t.Fatalf("PromoteSessionWait()=(%d,%v)", promoted, err)
	}
	requeued, err := store.ReadTask(context.Background(), "worker-b", time.Millisecond)
	if err != nil || requeued.Task.TaskID != second.TaskID {
		t.Fatalf("requeued=(%#v,%v)", requeued, err)
	}
}

func mockStrongRedisTopology(server *miniredis.Miniredis, runID string) {
	server.Server().SetPreHook(func(peer *miniserver.Peer, command string, _ ...string) bool {
		switch command {
		case "ROLE":
			peer.WriteLen(3)
			peer.WriteBulk("master")
			peer.WriteInt(0)
			peer.WriteLen(0)
			return true
		case "INFO":
			peer.WriteBulk("# Server\r\nrun_id:" + runID + "\r\n")
			return true
		case "CLUSTER":
			peer.WriteError("ERR This instance has cluster support disabled")
			return true
		default:
			return false
		}
	})
}
