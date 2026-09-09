package runtime

import (
	"context"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
)

func TestGovernancePluginRedactsRunnerEventContent(t *testing.T) {
	manager, err := plugin.NewManager(NewGovernancePlugin([]string{"customer_secret"}))
	if err != nil {
		t.Fatal(err)
	}
	item := &event.Event{Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Content: "token=visible customer_secret=visible"}, Delta: model.Message{Content: "api_key=visible"}}}}}
	redacted, err := manager.OnEvent(context.Background(), nil, item)
	if err != nil {
		t.Fatal(err)
	}
	content := redacted.Response.Choices[0].Message.Content + " " + redacted.Response.Choices[0].Delta.Content
	if strings.Contains(content, "visible") || !strings.Contains(content, "[REDACTED]") {
		t.Fatalf("event content was not redacted: %q", content)
	}
}
