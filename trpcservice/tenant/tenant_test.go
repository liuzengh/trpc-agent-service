package tenant

import "testing"

func TestTenantValidationAndToolPolicy(t *testing.T) {
	valid := Tenant{
		ID: "tenant-a", Agent: AgentProfile{ID: "assistant", Version: "1", ToolAllowlist: []string{"get_server_time"}},
		Channels: []ChannelBinding{{ID: "feishu", Type: "feishu"}},
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	if !valid.AllowsTool("get_server_time") || valid.AllowsTool("shell") {
		t.Fatal("tool allowlist was not enforced")
	}

	cases := []Tenant{
		{},
		{ID: "bad|tenant", Agent: AgentProfile{ID: "a", Version: "1"}},
		{ID: "tenant"},
		{ID: "tenant", Agent: AgentProfile{ID: "a", Version: "1"}, Channels: []ChannelBinding{{}}},
		{ID: "tenant", Agent: AgentProfile{ID: "a", Version: "1"}, Channels: []ChannelBinding{{ID: "same", Type: "feishu"}, {ID: "same", Type: "wecom"}}},
	}
	for i, item := range cases {
		if err := item.Validate(); err == nil {
			t.Fatalf("case %d should fail", i)
		}
	}
}
