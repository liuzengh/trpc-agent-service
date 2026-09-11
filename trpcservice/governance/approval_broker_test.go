package governance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type approvalNotifierFunc func(context.Context, PendingApproval) (string, error)

func (f approvalNotifierFunc) NotifyPendingApproval(ctx context.Context, approval PendingApproval) (string, error) {
	return f(ctx, approval)
}

func TestRedisApprovalBrokerWaitsForMatchingHumanDecision(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewMemoryApprovalStore()
	broker, err := NewRedisApprovalBroker(client, store, 5*time.Minute)
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
	errC := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		approved, requestErr := broker.Request(ctx, request)
		if requestErr != nil {
			errC <- requestErr
			return
		}
		result <- approved
	}()

	var pending []PendingApproval
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
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
	if err := broker.MarkNotified(ctx, pending[0], "provider-message-1"); err != nil {
		t.Fatal(err)
	}
	if indexed, err := client.ZScore(ctx, approvalPendingKey, pending[0].Token).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("notified approval remained in pending index: score=%v err=%v", indexed, err)
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
	durable, err := store.Get(ctx, request.TenantID, pending[0].Token)
	if err != nil || durable.Status != ApprovalApproved || durable.NotificationID != "provider-message-1" || durable.ResolvedBy != request.ExternalUserID {
		t.Fatalf("durable approval = %#v, %v", durable, err)
	}
	select {
	case err := <-errC:
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
	broker, _ := NewRedisApprovalBroker(client, NewMemoryApprovalStore(), time.Minute)
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
	store := NewMemoryApprovalStore()
	broker, err := NewRedisApprovalBroker(client, store, time.Minute)
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
	if _, err := client.ZScore(context.Background(), approvalPendingKey, pending[0].Token).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("canceled approval remained in pending index: %v", err)
	}
	durable, err := store.Get(context.Background(), "trailforge", pending[0].Token)
	if err != nil || durable.Status != ApprovalCanceled {
		t.Fatalf("durable canceled approval = %#v, %v", durable, err)
	}
}

func TestRedisApprovalBrokerRecoversResolvedDecisionAfterRedisLoss(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewMemoryApprovalStore()
	broker, err := NewRedisApprovalBroker(client, store, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request := ApprovalRequest{
		TenantID: "tenant-a", AppCode: "support", ConfigVersion: 3,
		RequestID: "message-1", TraceID: "trace-1", Channel: "telegram", BindingID: "bot-a",
		ConversationID: "chat-1", ConversationScope: "direct", ExternalUserID: "user-1", RequesterUserID: "platform-user-1",
		ToolName: "request_refund", ToolDescription: "提交退款申请",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan bool, 1)
	errC := make(chan error, 1)
	go func() {
		approved, requestErr := broker.Request(ctx, request)
		if requestErr != nil {
			errC <- requestErr
			return
		}
		result <- approved
	}()

	var pending []PendingApproval
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		pending, err = broker.ListPending(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(pending) != 1 {
		t.Fatalf("pending approvals = %#v", pending)
	}
	if err := broker.MarkNotified(ctx, pending[0], "provider-message-1"); err != nil {
		t.Fatal(err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	resolved, err := broker.Resolve(ctx, ApprovalResolution{
		Token: pending[0].Token, TenantID: request.TenantID, Channel: request.Channel, BindingID: request.BindingID,
		ConversationID: request.ConversationID, ExternalUserID: request.ExternalUserID, Approved: true,
	})
	if err != nil || resolved.ToolName != request.ToolName || resolved.NotificationID != "provider-message-1" {
		t.Fatalf("Resolve(after Redis loss) = %#v, %v", resolved, err)
	}
	select {
	case requestErr := <-errC:
		t.Fatal(requestErr)
	case approved := <-result:
		if !approved {
			t.Fatal("approval result = false, want true")
		}
	case <-ctx.Done():
		t.Fatal("durable approval decision did not resume waiter")
	}
}

func TestRedisApprovalBrokerRebuildsPendingDispatchIndexAfterRedisLoss(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewMemoryApprovalStore()
	broker, err := NewRedisApprovalBroker(client, store, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request := ApprovalRequest{
		TenantID: "tenant-a", AppCode: "support", ConfigVersion: 4,
		RequestID: "message-pending", TraceID: "trace-pending", Channel: "wecom", BindingID: "bot-a",
		ConversationID: "conversation-1", ConversationScope: "direct", ExternalUserID: "external-user-1",
		RequesterUserID: "platform-user-1", ProgressMessageID: "progress-message-1",
		ProviderReplyToken: "ephemeral-reply-token", ToolName: "request_refund", ToolDescription: "提交退款申请",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, requestErr := broker.Request(ctx, request)
		done <- requestErr
	}()

	var durable []ApprovalRecord
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		durable, err = store.ListPending(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(durable) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(durable) != 1 {
		t.Fatalf("durable pending = %#v", durable)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	pending, err := broker.ListPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Token != durable[0].Token || pending[0].ProgressMessageID != request.ProgressMessageID {
		t.Fatalf("recovered pending = %#v", pending)
	}
	if pending[0].ProviderReplyToken != "" {
		t.Fatalf("provider reply token was durably restored: %q", pending[0].ProviderReplyToken)
	}
	if _, err := client.ZScore(ctx, approvalPendingKey, pending[0].Token).Result(); err != nil {
		t.Fatalf("recovered approval missing from Redis pending index: %v", err)
	}
	cancel()
	if requestErr := <-done; !errors.Is(requestErr, context.Canceled) {
		t.Fatalf("Request() error = %v, want context canceled", requestErr)
	}
}

func TestRedisApprovalBrokerListPendingCleansExpiredIndexEntries(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	broker, err := NewRedisApprovalBroker(client, NewMemoryApprovalStore(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	staleToken := "stale-token"
	if err := client.ZAdd(context.Background(), approvalPendingKey, redis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: staleToken}).Err(); err != nil {
		t.Fatal(err)
	}
	pending, err := broker.ListPending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending approvals = %#v, want none", pending)
	}
	if _, err := client.ZScore(context.Background(), approvalPendingKey, staleToken).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("expired pending index member remains: %v", err)
	}
}

func TestRedisApprovalBrokerNotifiesAndResolvesWebRequester(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewMemoryApprovalStore()
	notified := make(chan PendingApproval, 1)
	broker, err := NewRedisApprovalBroker(client, store, time.Minute, WithApprovalNotifier(approvalNotifierFunc(func(_ context.Context, approval PendingApproval) (string, error) {
		notified <- approval
		return "stream-card-1", nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	request := ApprovalRequest{
		TenantID: "tenant-a", AppCode: "support", ConfigVersion: 2, RequestID: "request-1", TraceID: "trace-1",
		Channel: "web", BindingID: "web-console", ConversationID: "conversation-1", ConversationScope: "direct",
		ExternalUserID: "platform-user-1", RequesterUserID: "platform-user-1",
		ToolName: "request_refund", ToolDescription: "提交退款申请",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan bool, 1)
	errC := make(chan error, 1)
	go func() {
		approved, requestErr := broker.Request(ctx, request)
		if requestErr != nil {
			errC <- requestErr
			return
		}
		result <- approved
	}()

	pending := <-notified
	if pending.RequestID != request.RequestID || pending.RequesterUserID != request.RequesterUserID || pending.Token == "" {
		t.Fatalf("notified approval = %#v", pending)
	}
	record, err := store.Get(ctx, request.TenantID, pending.Token)
	if err != nil || record.NotificationID != "stream-card-1" {
		t.Fatalf("durable notification = %#v, %v", record, err)
	}
	if _, err := broker.ResolveForRequester(ctx, request.TenantID, pending.Token, "another-user", true); !errors.Is(err, ErrApprovalRouteMismatch) {
		t.Fatalf("ResolveForRequester(other user) error = %v", err)
	}
	resolved, err := broker.ResolveForRequester(ctx, request.TenantID, pending.Token, request.RequesterUserID, true)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ToolName != request.ToolName {
		t.Fatalf("resolved approval = %#v", resolved)
	}
	if repeated, err := broker.ResolveForRequester(ctx, request.TenantID, pending.Token, request.RequesterUserID, true); err != nil || repeated.ToolName != request.ToolName {
		t.Fatalf("repeated ResolveForRequester() = %#v, %v", repeated, err)
	}
	if _, err := broker.ResolveForRequester(ctx, request.TenantID, pending.Token, request.RequesterUserID, false); !errors.Is(err, ErrApprovalResolved) {
		t.Fatalf("opposite repeated resolution error = %v", err)
	}
	select {
	case approved := <-result:
		if !approved {
			t.Fatal("Request() returned rejected after approval")
		}
	case requestErr := <-errC:
		t.Fatalf("Request() error = %v", requestErr)
	case <-ctx.Done():
		t.Fatal("Request() did not resume after web approval")
	}
}
