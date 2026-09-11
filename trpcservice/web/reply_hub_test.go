package web

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/redis/go-redis/v9"
)

func TestRedisReplyHubPublishesWebApprovalAsNonTerminalCard(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	hub, err := NewRedisReplyHub(client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	eventID, err := hub.NotifyPendingApproval(ctx, governance.PendingApproval{
		Token: "approval-1", TenantID: "tenant-a", RequestID: "request-a", Channel: "web",
		RequesterUserID: "owner-a", ToolName: "request_refund", ToolDescription: "提交退款申请",
	})
	if err != nil || eventID == "" {
		t.Fatalf("NotifyPendingApproval() = %q, %v", eventID, err)
	}
	events, cancel, err := hub.Subscribe(ctx, "tenant-a", "owner-a", "request-a", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	event := receiveWebStreamEvent(t, events)
	if event.Type != "card" || event.Card == nil || event.Card.State != "pending" || len(event.Card.Actions) != 2 {
		t.Fatalf("approval event = %#v", event)
	}
}

func TestRedisReplyHubReplaysDeltaAndDoneEvents(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	hub, err := NewRedisReplyHub(client)
	if err != nil {
		t.Fatalf("NewRedisReplyHub() error = %v", err)
	}
	ctx := context.Background()
	if err := hub.PublishDelta(ctx, "tenant-a", "owner-a", "request-a", "hel"); err != nil {
		t.Fatalf("PublishDelta() error = %v", err)
	}
	receipt, err := hub.Send(ctx, channels.ReplyTarget{
		Channel: channels.Web, TenantID: "tenant-a", WebOwnerID: "owner-a",
	}, channels.OutboundMessage{IdempotencyKey: "request-a", Text: "hello"})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if strings.TrimSpace(receipt.ExternalMessageID) == "" {
		t.Fatal("Send() returned an empty external message ID")
	}

	events, cancel, err := hub.Subscribe(ctx, "tenant-a", "owner-a", "request-a", "")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer cancel()
	first := receiveWebStreamEvent(t, events)
	second := receiveWebStreamEvent(t, events)
	if first.Type != "delta" || first.Content != "hel" {
		t.Fatalf("first event = %#v", first)
	}
	if second.Type != "done" || second.Reply != "hello" || second.ID != receipt.ExternalMessageID {
		t.Fatalf("second event = %#v, receipt = %#v", second, receipt)
	}

	resumed, stopResume, err := hub.Subscribe(ctx, "tenant-a", "owner-a", "request-a", first.ID)
	if err != nil {
		t.Fatalf("resume Subscribe() error = %v", err)
	}
	defer stopResume()
	replayed := receiveWebStreamEvent(t, resumed)
	if replayed.ID != second.ID || replayed.Type != "done" {
		t.Fatalf("resumed event = %#v, want done %q", replayed, second.ID)
	}
}

func TestRedisReplyHubReplaysCardOnlyDoneEvent(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	hub, err := NewRedisReplyHub(client)
	if err != nil {
		t.Fatal(err)
	}
	card := &channels.InteractiveCard{
		Title: "订单信息", Body: "订单已经找到。",
		Actions: []channels.CardAction{{Label: "查看订单", URL: "https://support.example.test/orders/42"}},
	}
	receipt, err := hub.Send(context.Background(), channels.ReplyTarget{
		Channel: channels.Web, TenantID: "tenant-a", WebOwnerID: "owner-a",
	}, channels.OutboundMessage{IdempotencyKey: "request-card", Card: card})
	if err != nil {
		t.Fatal(err)
	}
	events, cancel, err := hub.Subscribe(context.Background(), "tenant-a", "owner-a", "request-card", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	event := receiveWebStreamEvent(t, events)
	if event.ID != receipt.ExternalMessageID || event.Type != "done" || event.Reply != "" || event.Card == nil ||
		event.Card.Title != "订单信息" || len(event.Card.Actions) != 1 {
		t.Fatalf("card event = %#v", event)
	}
}

func TestRedisReplyHubReplaysTerminalFailureEvent(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	hub, err := NewRedisReplyHub(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := hub.PublishFailure(context.Background(), "tenant-a", "owner-a", "request-failed", "model_unavailable", "模型服务暂时不可用，请稍后重试。"); err != nil {
		t.Fatal(err)
	}
	events, cancel, err := hub.Subscribe(context.Background(), "tenant-a", "owner-a", "request-failed", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	event := receiveWebStreamEvent(t, events)
	if event.Type != "error" || event.Code != "model_unavailable" || event.Message != "模型服务暂时不可用，请稍后重试。" || event.ID == "" {
		t.Fatalf("failure event = %#v", event)
	}
}

func TestRedisReplyHubReplaysGeneratedArtifacts(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	hub, err := NewRedisReplyHub(client)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := hub.Send(context.Background(), channels.ReplyTarget{
		Channel: channels.Web, TenantID: "tenant-a", WebOwnerID: "owner-a",
	}, channels.OutboundMessage{
		IdempotencyKey: "request-artifact", Text: "文档已生成",
		Artifacts: []channels.OutboundArtifact{{Filename: "report.docx", Version: 2, Name: "维修报告", MimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, cancel, err := hub.Subscribe(context.Background(), "tenant-a", "owner-a", "request-artifact", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	event := receiveWebStreamEvent(t, events)
	if event.ID != receipt.ExternalMessageID || event.Type != "done" || len(event.Artifacts) != 1 || event.Artifacts[0].Filename != "report.docx" || event.Artifacts[0].Version != 2 {
		t.Fatalf("artifact event = %#v", event)
	}
}

func TestRedisReplyHubSkipsMalformedStreamEvents(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	hub, _ := NewRedisReplyHub(client)
	key := webReplyStreamKey("tenant-a", "owner-a", "request-a")
	if err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: key, Values: map[string]any{"type": "unknown"},
	}).Err(); err != nil {
		t.Fatalf("seed malformed stream event: %v", err)
	}
	if err := hub.PublishDelta(context.Background(), "tenant-a", "owner-a", "request-a", "valid"); err != nil {
		t.Fatalf("PublishDelta() error = %v", err)
	}
	events, cancel, err := hub.Subscribe(context.Background(), "tenant-a", "owner-a", "request-a", "")
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer cancel()
	event := receiveWebStreamEvent(t, events)
	if event.Type != "delta" || event.Content != "valid" {
		t.Fatalf("event after malformed record = %#v", event)
	}
}

func TestRedisReplyHubValidatesDependenciesAndRouting(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisReplyHub(nil); err == nil {
		t.Fatal("NewRedisReplyHub(nil) succeeded")
	}
	hub := &RedisReplyHub{}
	if err := hub.PublishDelta(context.Background(), "", "owner", "request", "delta"); err == nil {
		t.Fatal("PublishDelta() accepted a missing tenant")
	}
	if err := hub.PublishFailure(context.Background(), "tenant", "owner", "request", "model_unavailable", ""); err == nil {
		t.Fatal("PublishFailure() accepted an empty message")
	}
	if _, err := hub.Send(context.Background(), channels.ReplyTarget{Channel: channels.Telegram}, channels.OutboundMessage{Text: "reply"}); err == nil {
		t.Fatal("Send() accepted an invalid Web target")
	}
	if _, _, err := hub.Subscribe(context.Background(), "tenant", "owner", "request", "bad-id"); err == nil {
		t.Fatal("Subscribe() accepted an invalid event ID")
	}
	if _, _, err := hub.Subscribe(context.Background(), "", "owner", "request", "0-0"); err == nil {
		t.Fatal("Subscribe() accepted an empty tenant")
	}
	if webReplyStreamKey("tenant", "owner-a", "request") == webReplyStreamKey("tenant", "owner-b", "request") {
		t.Fatal("reply stream keys do not isolate owners")
	}
}

func receiveWebStreamEvent(t *testing.T, events <-chan WebStreamEvent) WebStreamEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("reply stream closed before an event arrived")
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reply stream event")
		return WebStreamEvent{}
	}
}
