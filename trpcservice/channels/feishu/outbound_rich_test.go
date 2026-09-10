package feishu

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestEncodeCardUsesNativeSchemaAndActions(t *testing.T) {
	encoded, err := encodeCard(channels.ReplyCard{
		Title:  "Agent 回复",
		Body:   "任务已完成",
		Status: "SUCCEEDED",
		Actions: []channels.CardAction{{
			ID:    "retry",
			Label: "Retry",
			Value: "request-a",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Schema string `json:"schema"`
		Body   struct {
			Elements []struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
				Actions []struct {
					Tag   string            `json:"tag"`
					Value map[string]string `json:"value"`
				} `json:"actions"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatalf("decode card: %v", err)
	}
	if payload.Schema != "2.0" || len(payload.Body.Elements) != 3 {
		t.Fatalf("card payload = %s", encoded)
	}
	if payload.Body.Elements[0].Tag != "markdown" || payload.Body.Elements[0].Content != "任务已完成" {
		t.Fatalf("body element = %#v", payload.Body.Elements[0])
	}
	if !strings.Contains(string(encoded), "状态：SUCCEEDED") {
		t.Fatalf("status missing from card payload: %s", encoded)
	}
	actionElement := payload.Body.Elements[2]
	if actionElement.Tag != "action" || len(actionElement.Actions) != 1 ||
		actionElement.Actions[0].Tag != "button" ||
		actionElement.Actions[0].Value["action_id"] != "retry" {
		t.Fatalf("action element = %#v", actionElement)
	}
}
