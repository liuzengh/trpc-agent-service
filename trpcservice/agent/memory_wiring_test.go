package agent

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
)

// TestFactoryWiresMemoryTools proves the Agent runtime consumes the framework
// memory.Service: a configured memory service contributes its memory tools to
// the built Agent, and a nil service leaves them out.
func TestFactoryWiresMemoryTools(t *testing.T) {
	const digest = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	key := profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "app", AgentAppRevision: 1, ContentDigest: digest, ConfigVersion: 2, PolicyVersion: 3}
	snapshot := profile.ExecutionProfileSnapshot{Key: key, ContentDigest: digest, AgentKind: agentapp.AgentKindLLM,
		Instruction: "answer", ModelProfileRef: profile.VersionedRef{ID: "m", Version: 1}}

	base := func() Factory {
		policies := allowPlatformTools(t, key.TenantID, key.PolicyVersion, memoryToolRefs)
		return Factory{Profiles: profilememory.NewResolver(snapshot), Models: modelResolver{value: mockmodel.New()},
			Policies: policies, Confirmations: stubConfirmations{}, ToolResults: messagingmemory.New()}
	}

	factory := base()
	factory.Memory = memoryinmemory.NewMemoryService()
	root, err := factory.Build(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !hasToolNamed(root, agentmemory.SearchToolName) {
		t.Fatalf("agent with memory service is missing %q tool; tools=%v", agentmemory.SearchToolName, toolNames(root))
	}

	factory = base()
	root, err = factory.Build(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if hasToolNamed(root, agentmemory.SearchToolName) {
		t.Fatal("agent without memory service should not expose memory tools")
	}
}

func allowPlatformTools(t *testing.T, tenantID string, version int64, refs []governance.VersionedRef) *governancememory.Store {
	t.Helper()
	rules := make([]governance.ToolRule, len(refs))
	for index, ref := range refs {
		rules[index] = governance.ToolRule{ToolID: ref.ID, Version: ref.Version}
	}
	policy := governance.PolicyV1{SchemaVersion: governance.CurrentPolicySchemaVersion, DefaultAction: governance.ActionAllow,
		Tools: rules, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled}
	digest, _, err := governance.PolicyDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	store := governancememory.New(0, 0)
	if err := store.PublishPolicy(governance.PolicySnapshot{TenantID: tenantID, Version: version, SchemaVersion: governance.CurrentPolicySchemaVersion,
		Policy: policy, ContentDigest: digest}); err != nil {
		t.Fatal(err)
	}
	return store
}

func hasToolNamed(agent agentcore.Agent, name string) bool {
	for _, value := range agent.Tools() {
		if value.Declaration() != nil && value.Declaration().Name == name {
			return true
		}
	}
	return false
}

func toolNames(agent agentcore.Agent) []string {
	names := make([]string, 0)
	for _, value := range agent.Tools() {
		if value.Declaration() != nil {
			names = append(names, value.Declaration().Name)
		}
	}
	return names
}
