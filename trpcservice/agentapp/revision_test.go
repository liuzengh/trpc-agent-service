package agentapp

import (
	"errors"
	"testing"
)

func TestContentDigestNormalizesReferenceOrder(t *testing.T) {
	base := Revision{AgentKind: "llm", SchemaVersion: 1, Instruction: "help", ModelProfileID: "model", ModelProfileVersion: 1, ToolRefs: []VersionedRef{{ID: "b", Version: 1}, {ID: "a", Version: 1}}}
	a, err := base.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	base.ToolRefs[0], base.ToolRefs[1] = base.ToolRefs[1], base.ToolRefs[0]
	b, err := base.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("digest changed with reference order: %s != %s", a, b)
	}
}

func TestContentDigestIncludesToolBindingDigest(t *testing.T) {
	base := Revision{AgentKind: AgentKindLLM, SchemaVersion: 1, Instruction: "help", ModelProfileID: "model", ModelProfileVersion: 1,
		ToolRefs: []VersionedRef{{ID: "mcp_weather", Version: 1, ContentDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}
	first, err := base.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	base.ToolRefs[0].ContentDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	second, err := base.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("tool binding digest did not affect revision digest")
	}
}

func TestRevisionPluginsAreVersionedAndIncludedInDigest(t *testing.T) {
	base := Revision{TenantID: "tenant", AgentAppID: "app", Revision: 1, DraftVersion: 1, AgentKind: AgentKindLLM, SchemaVersion: 1,
		Instruction: "help", ModelProfileID: "model", ModelProfileVersion: 1, PluginRefs: []PluginRef{{ID: PluginToolCallID, Version: 1}}}
	if err := base.ValidateDraft(); err != nil {
		t.Fatal(err)
	}
	first, err := base.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	base.PluginRefs[0].Version = 2
	if err := base.ValidateDraft(); err == nil {
		t.Fatal("unsupported plugin version accepted")
	}
	second, err := base.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("plugin selection did not affect digest")
	}
}

func TestToolSearchPluginRequiresLLMRevision(t *testing.T) {
	revision := Revision{TenantID: "tenant", AgentAppID: "app", Revision: 1, DraftVersion: 1,
		AgentKind: AgentKindChain, SchemaVersion: 1, PluginRefs: []PluginRef{{ID: PluginToolSearch, Version: 1}},
		AgentSpec: AgentSpecV1{Nodes: []AgentNodeSpecV1{{Key: "child", FailurePolicy: FailurePolicyFailFast,
			AgentRef: PublishedAgentRef{AgentAppID: "child", Revision: 1, ContentDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}}}
	if !errors.Is(revision.ValidateDraft(), ErrInvalid) {
		t.Fatalf("composite revision accepted tool_search: %v", revision.ValidateDraft())
	}
}

func TestNormalizeRevisionCanonicalizesNilObjectFields(t *testing.T) {
	revision := NormalizeRevision(Revision{})
	if revision.GenerationConfig == nil || revision.RuntimePolicy == nil || revision.FallbackModelRefs == nil {
		t.Fatalf("nullable fields were not normalized: %#v", revision)
	}
	nilDigest, err := (Revision{}).ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	emptyDigest, err := (Revision{GenerationConfig: map[string]any{}, RuntimePolicy: map[string]any{}, FallbackModelRefs: []VersionedRef{}}).ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if nilDigest != emptyDigest {
		t.Fatalf("nil and empty objects have different digests: %s != %s", nilDigest, emptyDigest)
	}
}

func TestFallbackModelsAreOrderedAndPartOfRevisionDigest(t *testing.T) {
	revision := Revision{TenantID: "tenant", AgentAppID: "app", Revision: 1, DraftVersion: 1, AgentKind: AgentKindLLM, SchemaVersion: 1,
		Instruction: "help", ModelProfileID: "primary", ModelProfileVersion: 1, FallbackModelRefs: []VersionedRef{{ID: "secondary", Version: 2}, {ID: "tertiary", Version: 1}}}
	if err := revision.ValidateDraft(); err != nil {
		t.Fatal(err)
	}
	first, err := revision.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	revision.FallbackModelRefs[0], revision.FallbackModelRefs[1] = revision.FallbackModelRefs[1], revision.FallbackModelRefs[0]
	second, err := revision.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("fallback priority order did not affect content digest")
	}
	revision.FallbackModelRefs = []VersionedRef{{ID: "primary", Version: 1}}
	if !errors.Is(revision.ValidateDraft(), ErrInvalid) {
		t.Fatalf("primary repeated as fallback should be rejected: %v", revision.ValidateDraft())
	}
}

func TestExecutionBudgetIsValidatedAndPartOfRevisionDigest(t *testing.T) {
	revision := Revision{TenantID: "tenant", AgentAppID: "app", Revision: 1, DraftVersion: 1, AgentKind: AgentKindLLM, SchemaVersion: 1, Instruction: "help", ModelProfileID: "model", ModelProfileVersion: 1,
		ExecutionBudget: ExecutionBudgetV1{MaxLLMCalls: 2, MaxToolCalls: 3, MaxParallelTools: 1, ExecutionTimeoutSeconds: 30}}
	if err := revision.ValidateDraft(); err != nil {
		t.Fatal(err)
	}
	first, err := revision.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	revision.ExecutionBudget.MaxLLMCalls++
	second, err := revision.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("execution budget must affect the published revision digest")
	}
	revision.ExecutionBudget.MaxLLMCalls = -1
	if !errors.Is(revision.ValidateDraft(), ErrInvalid) {
		t.Fatalf("negative budget accepted: %v", revision.ValidateDraft())
	}
}
