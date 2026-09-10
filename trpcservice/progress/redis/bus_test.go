package redis

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/progress"
)

func TestProgressTopicScopesAndHidesTenantIdentifier(t *testing.T) {
	first := progressTopic("dev", "tenant-a")
	second := progressTopic("dev", "tenant-b")
	if first == second || first != progressTopic("dev", "tenant-a") || first == "" {
		t.Fatalf("topics first=%q second=%q", first, second)
	}
}

func TestValidEvent(t *testing.T) {
	valid := progress.Event{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", Sequence: 1, Kind: progress.MessageDelta, Content: "text"}
	if !validEvent(valid) {
		t.Fatal("valid event rejected")
	}
	valid.Kind = "message.completed"
	if validEvent(valid) {
		t.Fatal("terminal event accepted by transient transport")
	}
}
