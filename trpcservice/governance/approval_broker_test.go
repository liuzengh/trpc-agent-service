package governance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisApprovalBrokerWaitsForMatchingHumanDecision(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	broker, err := NewRedisApprovalBroker(client, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	request := ApprovalRequest{
		TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 7,
		Channel: "telegram", BindingID: "bot-a", ConversationID: "chat-1", ConversationScope: "direct",
		ExternalUserID: "user-1", ProgressMessageID: "progress-1", ProviderReplyToken: "reply-token-1",
		ToolName: "request_refund", ToolDescription: "为指定订单发起退款申请",
	}
	result := make(chan bool, 1)
	errors := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		approved, requestErr := broker.Request(ctx, request)
		if requestErr != nil {
			errors <- requestErr
			return
		}
		result <- approved
	}()

	var pending []PendingApproval
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		pending, err = broker.ListPending(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(pending) != 1 || pending[0].ToolName != request.ToolName || pending[0].ToolDescription != request.ToolDescription ||
		pending[0].ProgressMessageID != request.ProgressMessageID || pending[0].ProviderReplyToken != request.ProviderReplyToken {
		t.Fatalf("pending approvals = %#v", pending)
	}
	if err := broker.MarkNotified(ctx, pending[0].Token, "provider-message-1"); err != nil {
		t.Fatal(err)
	}
	resolved, err := broker.Resolve(ctx, ApprovalResolution{
		Token: pending[0].Token, TenantID: request.TenantID, Channel: request.Channel, BindingID: request.BindingID,
		ConversationID: request.ConversationID, ExternalUserID: request.ExternalUserID, Approved: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.NotificationID != "provider-message-1" || resolved.ToolDescription != request.ToolDescription ||
		resolved.ProgressMessageID != request.ProgressMessageID || resolved.ProviderReplyToken != request.ProviderReplyToken {
		t.Fatalf("resolved approval = %#v", resolved)
	}
	select {
	case err := <-errors:
		t.Fatal(err)
	case approved := <-result:
		if !approved {
			t.Fatal("approval result = false, want true")
		}
	case <-ctx.Done():
		t.Fatal("approval request did not resume")
	}
}

func TestRedisApprovalBrokerRejectsDecisionFromDifferentActor(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	broker, _ := NewRedisApprovalBroker(client, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() {
		_, _ = broker.Request(ctx, ApprovalRequest{
			TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 1, Channel: "feishu", BindingID: "fs-a",
			ConversationID: "chat-1", ConversationScope: "group", ExternalUserID: "initiator", ToolName: "dangerous_tool",
		})
	}()
	var pending []PendingApproval
	for len(pending) == 0 && ctx.Err() == nil {
		pending, _ = broker.ListPending(ctx, 10)
		time.Sleep(5 * time.Millisecond)
	}
	if len(pending) != 1 {
		t.Fatalf("pending approvals = %#v", pending)
	}
	_, err := broker.Resolve(ctx, ApprovalResolution{
		Token: pending[0].Token, TenantID: "trailforge", Channel: "feishu", BindingID: "fs-a",
		ConversationID: "chat-1", ExternalUserID: "other-user", Approved: true,
	})
	if err != ErrApprovalRouteMismatch {
		t.Fatalf("Resolve() error = %v, want ErrApprovalRouteMismatch", err)
	}
}

func TestRedisApprovalBrokerRemovesPendingRequestWhenWaiterIsCanceled(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	broker, err := NewRedisApprovalBroker(client, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, requestErr := broker.Request(ctx, ApprovalRequest{
			TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 1, Channel: "wecom", BindingID: "wx-a",
			ConversationID: "user-1", ConversationScope: "direct", ExternalUserID: "user-1", ToolName: "dangerous_tool",
		})
		done <- requestErr
	}()

	var pending []PendingApproval
	deadline := time.Now().Add(time.Second)
	for len(pending) == 0 && time.Now().Before(deadline) {
		pending, _ = broker.ListPending(context.Background(), 10)
		time.Sleep(5 * time.Millisecond)
	}
	if len(pending) != 1 {
		t.Fatalf("pending approvals = %#v, want one", pending)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Request() error = %v, want context canceled", err)
	}
	if exists, err := client.Exists(context.Background(), approvalKeyPrefix+pending[0].Token).Result(); err != nil || exists != 0 {
		t.Fatalf("canceled approval exists = %d, %v; want removed", exists, err)
	}
}
