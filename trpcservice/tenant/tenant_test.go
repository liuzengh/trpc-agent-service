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

func TestRuntimeProfileRoundTripAndValidation(t *testing.T) {
	base := Tenant{
		ID: "tenant-runtime", Name: "runtime", Enabled: true,
		Agent:    AgentProfile{ID: "assistant", Version: "1", Instruction: "old", ToolAllowlist: []string{"clock"}},
		Model:    ModelProfile{Provider: "openai", BaseURL: "https://model.example", Model: "model-a", APIKeyRef: "env:KEY", Timeout: "5s"},
		Backend:  BackendProfile{Session: "redis", Memory: "postgres", Knowledge: "qdrant", Artifact: "s3", Namespace: "tenant-runtime"},
		Budget:   BudgetPolicy{MaxConcurrent: 2, MaxInputTokens: 100, MaxOutputTokens: 50, DailyCostUSD: 1, InputCostPerMillionUSD: .1, OutputCostPerMillionUSD: .2},
		Channels: []ChannelBinding{{ID: "primary", Type: "feishu", Enabled: true}},
	}
	profile := RuntimeProfileFromTenant(base)
	if profile.TenantID != base.ID || profile.PublishedVersion != "1" || profile.Agent.Instruction != "old" {
		t.Fatalf("profile mismatch: %+v", profile)
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}

	profile.Agent.Instruction = "new"
	profile.PublishedVersion = "2"
	profile.Revision = 7
	applied := profile.Apply(base)
	if applied.Agent.Version != "2" || applied.Agent.Instruction != "new" || applied.RuntimeRevision != 7 {
		t.Fatalf("applied profile mismatch: %+v", applied)
	}
	if len(applied.Channels) != 1 || applied.Channels[0].ID != "primary" {
		t.Fatal("runtime apply must preserve channel bindings")
	}

	profile.PublishedVersion = ""
	profile.Agent.Version = "3"
	if got := profile.Apply(base).Agent.Version; got != "3" {
		t.Fatalf("empty published version should retain agent version, got %q", got)
	}
	profile.TenantID = ""
	if err := profile.Validate(); err == nil {
		t.Fatal("runtime profile without tenant should fail")
	}
}

func TestTenantRejectsNegativeBudgetFields(t *testing.T) {
	valid := Tenant{ID: "tenant", Agent: AgentProfile{ID: "assistant", Version: "1"}}
	cases := []BudgetPolicy{
		{MaxConcurrent: -1},
		{MaxInputTokens: -1},
		{MaxOutputTokens: -1},
		{DailyCostUSD: -1},
		{InputCostPerMillionUSD: -1},
		{OutputCostPerMillionUSD: -1},
	}
	for i, budget := range cases {
		item := valid
		item.Budget = budget
		if err := item.Validate(); err == nil {
			t.Fatalf("negative budget case %d should fail", i)
		}
	}
}
