package wecommcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestOnboardingUsesConfirmedGroupAndMarkerIdentity(t *testing.T) {
	now := time.Now().UTC()
	dir := t.TempDir()
	write := func(name string, value any) string {
		t.Helper()
		data, _ := json.Marshal(value)
		file := filepath.Join(dir, name)
		if err := os.WriteFile(file, data, 0600); err != nil {
			t.Fatal(err)
		}
		return file
	}
	sessions := write("sessions.json", privateSnapshot{Tool: sessionsTool, CapturedAt: now, EndpointHash: endpointHash(fixtureEndpoint), Payload: json.RawMessage(`{"errcode":0,"sessions":[{"chat_id":"group-1","chat_type":"group"},{"chat_id":"unrelated-group","chat_type":"group"},{"chat_id":"private","chat_type":"single"}]}`)})
	attempt := write("attempt.json", replyAttempt{EndpointHash: endpointHash(fixtureEndpoint), ChatHash: endpointHash("group-1"), Marker: TestReplyMarker, AttemptedAt: now.Add(-time.Hour)})
	messages := privateSnapshot{Tool: messagesTool, CapturedAt: now, EndpointHash: endpointHash(fixtureEndpoint), ChatHash: endpointHash("group-1"), Payload: json.RawMessage(`{"errcode":0,"messages":[{"userid":"human-1","msg_type":"text","text":{"content":"@testbot TRPC-WECOM-TEST-001"}}]}`)}
	file := write("messages.json", messages)
	base := controlplane.ChannelBinding{ID: "binding", TenantID: "tenant", AppID: "app", SecretRef: "env://MCP"}
	b, err := BuildConfirmedGroupBinding(fixtureEndpoint, sessions, file, attempt, base, now)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := ParseBinding(b)
	if err != nil || len(cfg.AllowedChatIDs) != 1 || cfg.AllowedChatIDs[0] != "group-1" || len(cfg.AllowedUserIDs) != 1 || cfg.AllowedUserIDs[0] != "human-1" || cfg.MentionPrefix != "@testbot" || !cfg.Start().Equal(now.Truncate(time.Second)) {
		t.Fatal("onboarding scope changed")
	}
	for _, mutate := range []func(*privateSnapshot){func(s *privateSnapshot) { s.ChatHash = endpointHash("unrelated-group") }, func(s *privateSnapshot) { s.EndpointHash = "other" }, func(s *privateSnapshot) { s.IsError = true }, func(s *privateSnapshot) {
		s.Payload = json.RawMessage(`{"errcode":0,"messages":[{"userid":"human-1","msg_type":"text","text":{"content":"@testbot TRPC-WECOM-TEST-001"}},{"userid":"human-2","msg_type":"text","text":{"content":"@testbot TRPC-WECOM-TEST-001"}}]}`)
	}} {
		bad := messages
		mutate(&bad)
		badFile := write("bad.json", bad)
		if _, err := BuildConfirmedGroupBinding(fixtureEndpoint, sessions, badFile, attempt, base, now); err == nil {
			t.Fatal("unconfirmed group or ambiguous human accepted")
		}
	}
	if _, err := confirmedGroup(fixtureEndpoint, sessions, attempt, now.Add(2*time.Hour)); err == nil {
		t.Fatal("stale session snapshot accepted")
	}
	adjacent := messages
	adjacent.Payload = json.RawMessage(`{"errcode":0,"messages":[{"userid":"human-1","msg_type":"text","text":{"content":"@Agent 智能助手TRPC-WECOM-TEST-001"}}]}`)
	adjacentFile := write("adjacent.json", adjacent)
	b, err = BuildConfirmedGroupBinding(fixtureEndpoint, sessions, adjacentFile, attempt, base, now)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ = ParseBinding(b)
	if cfg.MentionStyle != "prefix" || cfg.MentionPrefix != "@Agent 智能助手" {
		t.Fatal("actual UI mention format not derived")
	}
}
