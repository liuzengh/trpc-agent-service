//go:build integration

package chat

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestMySQLLedgerRecordAndHistory(t *testing.T) {
	ctx := context.Background()
	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"),
		mysql.WithScripts("../../deployments/mysql/init/007_chat.sql"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := storage.OpenMySQL(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ledger := NewMySQLLedger(db)

	// one turn
	turn := Turn{
		TenantID: "t1", AgentID: "a1", SessionID: "s1", MemberID: "u1", Channel: "admin",
		UserMsgID: "um-1", UserText: "hello", ReplyMsgID: "am-1", ReplyText: "hi there",
		TurnID: "turn-1", TurnTS: 1000,
	}
	if err := ledger.RecordTurn(ctx, turn); err != nil {
		t.Fatalf("record turn: %v", err)
	}

	// session appears with the member
	sessions, err := ledger.Sessions(ctx, SessionQuery{TenantID: "t1"})
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].MemberID != "u1" || sessions[0].Channel != "admin" {
		t.Fatalf("sessions = %+v", sessions)
	}

	// two message rows, USER before ASSISTANT
	msgs, err := ledger.Messages(ctx, MessageQuery{SessionID: "s1"})
	if err != nil {
		t.Fatalf("messages: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages = %d, want 2", len(msgs))
	}
	if msgs[0].Role != RoleUser || msgs[0].Content != "hello" {
		t.Errorf("first row = %+v, want USER hello", msgs[0])
	}
	if msgs[1].Role != RoleAssistant || msgs[1].Content != "hi there" {
		t.Errorf("second row = %+v, want ASSISTANT reply", msgs[1])
	}
	if msgs[0].TurnID != "turn-1" || msgs[1].TurnID != "turn-1" {
		t.Errorf("turn grouping broken: %s / %s", msgs[0].TurnID, msgs[1].TurnID)
	}

	// redelivery of the same user message is idempotent (INSERT IGNORE)
	if err := ledger.RecordTurn(ctx, turn); err != nil {
		t.Fatalf("re-record turn: %v", err)
	}
	if msgs, _ = ledger.Messages(ctx, MessageQuery{SessionID: "s1"}); len(msgs) != 2 {
		t.Errorf("after redelivery messages = %d, want still 2", len(msgs))
	}

	// a second turn
	if err := ledger.RecordTurn(ctx, Turn{
		TenantID: "t1", AgentID: "a1", SessionID: "s1", MemberID: "u1", Channel: "admin",
		UserMsgID: "um-2", UserText: "again", ReplyMsgID: "am-2", ReplyText: "sure",
		TurnID: "turn-2", TurnTS: 2000,
	}); err != nil {
		t.Fatal(err)
	}
	all, _ := ledger.Messages(ctx, MessageQuery{SessionID: "s1"})
	if len(all) != 4 {
		t.Fatalf("after turn2 messages = %d, want 4", len(all))
	}
	// paginate: only the older turn (turn_timestamp < 2000)
	older, _ := ledger.Messages(ctx, MessageQuery{SessionID: "s1", BeforeTurn: 2000})
	if len(older) != 2 || older[0].TurnID != "turn-1" {
		t.Errorf("before-turn page = %+v, want turn-1 rows", older)
	}

	// session isolation on the member/tenant filters
	otherSessions, _ := ledger.Sessions(ctx, SessionQuery{TenantID: "t-other"})
	if len(otherSessions) != 0 {
		t.Errorf("other tenant sessions = %+v, want none", otherSessions)
	}
	// activity ordering: newest session first across members
	if err := ledger.RecordTurn(ctx, Turn{
		TenantID: "t1", AgentID: "a9", SessionID: "s2", MemberID: "u2", Channel: "wecom",
		UserMsgID: "um-9", UserText: "x", ReplyMsgID: "am-9", ReplyText: "y",
		TurnID: "turn-9", TurnTS: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	sess, _ := ledger.Sessions(ctx, SessionQuery{TenantID: "t1", MemberID: "u2"})
	if len(sess) != 1 || sess[0].SessionID != "s2" {
		t.Errorf("member filter = %+v", sess)
	}
}
