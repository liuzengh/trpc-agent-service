package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDirectReplyIsDurableWithoutAgentTask(t *testing.T) {
	journal := NewMemoryJournal()
	t.Cleanup(func() { _ = journal.Close() })
	request := testInboundRequest(t, "control-message", "拒绝请回复：拒绝 apr_example")
	request.DirectReply = "平台提示：本条消息没有更改审批状态。"
	first, err := journal.Accept(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.DirectReply = "a later deployment uses different wording"
	second, err := journal.Accept(context.Background(), request)
	if err != nil || !second.Duplicate || first.RequestID != second.RequestID {
		t.Fatalf("duplicate=%+v err=%v", second, err)
	}
	if len(journal.Tasks()) != 0 {
		t.Fatal("direct reply created an Agent task")
	}
	status, result, ok := journal.RunStatus(first.RequestID)
	if !ok || status != "completed" || result.AgentName != "platform-control" || result.EventCount != 0 {
		t.Fatalf("control run=%+v status=%s", result, status)
	}
	items, err := journal.ClaimOutbound(context.Background(), "sender", 10, time.Minute)
	if err != nil || len(items) != 1 || items[0].Text != "平台提示：本条消息没有更改审批状态。" {
		t.Fatalf("outbound=%+v err=%v", items, err)
	}
	request.DirectReply = ""
	if _, err := journal.Accept(context.Background(), request); !errors.Is(err, ErrMessageConflict) {
		t.Fatalf("control reply was replayed as Agent task: %v", err)
	}
}
