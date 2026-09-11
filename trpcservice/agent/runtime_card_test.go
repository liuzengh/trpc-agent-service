package agent

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type cardPresentingRunner struct{}

func (*cardPresentingRunner) Run(ctx context.Context, _, _ string, _ model.Message, _ ...frameworkagent.RunOption) (<-chan *event.Event, error) {
	if _, err := platformtool.NewPresentCardTool().Call(ctx, []byte(`{
		"title":"订单信息",
		"body":"订单已经找到。",
		"actions":[{"label":"查看订单","url":"https://support.example.test/orders/42"}]
	}`)); err != nil {
		return nil, err
	}
	events := make(chan *event.Event, 1)
	events <- &event.Event{Response: &model.Response{Choices: []model.Choice{{Message: model.NewAssistantMessage("已为你整理订单信息。")}}}}
	close(events)
	return events, nil
}

func (*cardPresentingRunner) Close() error { return nil }

func TestRuntimePersistsPresentedCardIntoDurableReply(t *testing.T) {
	state := storage.NewMemoryStateStore()
	runtime, err := NewRuntime(
		tenant.NewMemoryRepository(),
		staticRunnerProvider{runner: &cardPresentingRunner{}},
		storage.NewMemoryIdempotencyStore(),
		state,
		time.Minute,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
	}}
	_, err = runtime.Handle(WithConfigurationSnapshot(context.Background(), snapshot), "web-console", channels.InboundMessage{
		MessageID: "message-card", Channel: channels.Web, ConversationID: "conversation-1",
		SenderID: "user-1", WebOwnerID: "user-1", Text: "显示订单卡片",
	})
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := state.FindOutboxByRequestID(context.Background(), "tenant-a", "message-card")
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Text string                    `json:"text"`
		Card *channels.InteractiveCard `json:"card"`
	}
	if err := json.Unmarshal(outbox.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Text != "已为你整理订单信息。" || payload.Card == nil || payload.Card.Title != "订单信息" || len(payload.Card.Actions) != 1 {
		t.Fatalf("durable reply = %#v", payload)
	}
}
