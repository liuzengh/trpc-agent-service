package tenant

import (
	"strings"
	"testing"
)

func TestAgentCloneDeepCopiesStorageMigrationTarget(t *testing.T) {
	target := BackendConfig{Type: BackendPostgres, Endpoint: "postgres://target/runtime"}
	app := AgentApp{Storage: StorageProfile{Session: BackendConfig{Type: BackendPostgres, MigrationTarget: &target}}}
	cloned := app.Clone()
	cloned.Storage.Session.MigrationTarget.Endpoint = "postgres://changed/runtime"
	if app.Storage.Session.MigrationTarget.Endpoint != "postgres://target/runtime" {
		t.Fatal("clone mutated source migration target")
	}
}

func TestSecretRefStringRedactsKey(t *testing.T) {
	ref := SecretRef{Provider: SecretProviderEnv, Key: "VERY_SECRET_ENV_NAME"}
	formatted := ref.String()
	if strings.Contains(formatted, ref.Key) {
		t.Fatalf("String() = %q contains secret key", formatted)
	}
	if formatted != "<secret-ref:env>" {
		t.Fatalf("String() = %q", formatted)
	}
}

func TestAgentAppCloneIsDeep(t *testing.T) {
	temperature := 0.5
	original := AgentApp{
		Model:         ModelProfile{Temperature: &temperature},
		Tools:         ToolPolicy{Allow: []string{"calculator"}},
		MCPServers:    []MCPServer{{ID: "crm", AllowedTools: []string{"lookup"}}},
		BusinessTools: []HTTPBusinessTool{{Name: "ticket_lookup"}},
		Channels:      []ChannelBinding{{ID: "binding-a", AllowedUsers: []string{"alice"}, AllowedChats: []string{"room-a"}}},
	}
	cloned := original.Clone()
	*cloned.Model.Temperature = 1.5
	cloned.Tools.Allow[0] = "other"
	cloned.MCPServers[0].AllowedTools[0] = "other"
	cloned.BusinessTools[0].Name = "other"
	cloned.Channels[0].ID = "binding-b"
	cloned.Channels[0].AllowedUsers[0] = "mallory"
	cloned.Channels[0].AllowedChats[0] = "room-b"

	if *original.Model.Temperature != 0.5 {
		t.Fatalf("original temperature mutated: %v", *original.Model.Temperature)
	}
	if original.Tools.Allow[0] != "calculator" {
		t.Fatalf("original tools mutated: %v", original.Tools.Allow)
	}
	if original.Channels[0].ID != "binding-a" {
		t.Fatalf("original channels mutated: %v", original.Channels)
	}
	if original.Channels[0].AllowedUsers[0] != "alice" || original.Channels[0].AllowedChats[0] != "room-a" {
		t.Fatalf("original channel ACL mutated: %+v", original.Channels[0])
	}
	if original.MCPServers[0].AllowedTools[0] != "lookup" || original.BusinessTools[0].Name != "ticket_lookup" {
		t.Fatalf("original tool integrations mutated: %+v %+v", original.MCPServers, original.BusinessTools)
	}
}

func TestChannelBindingAllowsIdentity(t *testing.T) {
	tests := []struct {
		name, user, chat string
		binding          ChannelBinding
		want             bool
	}{
		{name: "empty policy preserves compatibility", user: "any", binding: ChannelBinding{}, want: true},
		{name: "allowed direct user", user: "alice", binding: ChannelBinding{AllowedUsers: []string{"alice"}}, want: true},
		{name: "denied direct user", user: "mallory", binding: ChannelBinding{AllowedUsers: []string{"alice"}}},
		{name: "chat-only policy denies direct", user: "alice", binding: ChannelBinding{AllowedChats: []string{"room-a"}}},
		{name: "allowed chat", user: "alice", chat: "room-a", binding: ChannelBinding{AllowedChats: []string{"room-a"}}, want: true},
		{name: "denied chat", user: "alice", chat: "room-b", binding: ChannelBinding{AllowedChats: []string{"room-a"}}},
		{name: "both dimensions must match", user: "alice", chat: "room-a", binding: ChannelBinding{AllowedUsers: []string{"alice"}, AllowedChats: []string{"room-a"}}, want: true},
		{name: "allowed chat denied user", user: "mallory", chat: "room-a", binding: ChannelBinding{AllowedUsers: []string{"alice"}, AllowedChats: []string{"room-a"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.binding.AllowsIdentity(test.user, test.chat); got != test.want {
				t.Fatalf("AllowsIdentity()=%v want=%v", got, test.want)
			}
		})
	}
}
