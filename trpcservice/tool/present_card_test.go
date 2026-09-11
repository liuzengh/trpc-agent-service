package tool

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestPresentCardToolRecordsSafeDisplayCard(t *testing.T) {
	recorder := NewPresentedCardRecorder()
	ctx := WithPresentedCardRecorder(context.Background(), recorder)
	result, err := NewPresentCardTool().Call(ctx, []byte(`{
		"title":"订单信息",
		"body":"订单已经找到。",
		"actions":[{"label":"查看订单","url":"https://support.example.test/orders/42","style":"primary"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	card := recorder.Snapshot()
	if card == nil || card.Title != "订单信息" || card.Body != "订单已经找到。" || len(card.Actions) != 1 ||
		card.Actions[0].URL != "https://support.example.test/orders/42" || card.Actions[0].ActionID != "" {
		t.Fatalf("recorded card = %#v", card)
	}
	if got, ok := result.(map[string]any); !ok || got["presented"] != true {
		t.Fatalf("result = %#v", result)
	}
}

func TestPresentCardToolRejectsUnsafeOrInteractiveModelActions(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "blank body", raw: `{"body":" "}`},
		{name: "http URL", raw: `{"body":"结果","actions":[{"label":"打开","url":"http://example.com"}]}`},
		{name: "callback field", raw: `{"body":"结果","actions":[{"label":"确认","url":"https://example.com","action_id":"approval:approve:x"}]}`},
		{name: "too many actions", raw: `{"body":"结果","actions":[{"label":"1","url":"https://example.com/1"},{"label":"2","url":"https://example.com/2"},{"label":"3","url":"https://example.com/3"},{"label":"4","url":"https://example.com/4"}]}`},
		{name: "danger style", raw: `{"body":"结果","actions":[{"label":"删除","url":"https://example.com/delete","style":"danger"}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := NewPresentedCardRecorder()
			_, err := NewPresentCardTool().Call(WithPresentedCardRecorder(context.Background(), recorder), []byte(test.raw))
			if err == nil {
				t.Fatal("Call() error = nil")
			}
			if recorder.Snapshot() != nil {
				t.Fatalf("invalid card produced side effect: %#v", recorder.Snapshot())
			}
		})
	}
}

func TestPresentedCardRecorderKeepsLatestCardAndCopiesIt(t *testing.T) {
	recorder := NewPresentedCardRecorder()
	recorder.Record(channels.InteractiveCard{Body: "first"})
	recorder.Record(channels.InteractiveCard{Body: "second", Actions: []channels.CardAction{{Label: "查看", URL: "https://example.com"}}})
	card := recorder.Snapshot()
	if card == nil || card.Body != "second" {
		t.Fatalf("snapshot = %#v", card)
	}
	card.Body = "mutated"
	card.Actions[0].Label = "mutated"
	again := recorder.Snapshot()
	if again == nil || again.Body != "second" || again.Actions[0].Label != "查看" {
		t.Fatalf("recorder exposed mutable state: %#v", again)
	}
}
