package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestCompileBuildsOnlyUsedClosureAndNodeScopedEntries(t *testing.T) {
	input := validCompileInput()
	compiled, report := Compile(input)
	if !report.Valid {
		t.Fatalf("Compile report = %#v", report)
	}
	if len(report.Diagnostics) != 1 || report.Diagnostics[0].Code != DiagnosticUnusedRequirement ||
		pointerString(report.Diagnostics[0].Name) != "debug" {
		t.Fatalf("unused diagnostic = %#v", report.Diagnostics)
	}
	if got := sortedKeys(compiled.Content.Resources.Tools); !equalStrings(got, []string{"search"}) {
		t.Fatalf("tool closure = %v", got)
	}
	if _, exists := compiled.Content.Resources.Storage["archive"]; exists {
		t.Fatal("extra storage resource entered the manifest")
	}
	if got := compiled.Content.StorageRoles; got[StorageRoleSession] != "session" || got[StorageRoleMemory] != "memory" || len(got) != 2 {
		t.Fatalf("storage roles = %#v", got)
	}
	researcher := compiled.Content.AgentPlan.Nodes["researcher"]
	if !equalStrings(researcher.ToolResources, []string{"search"}) ||
		!equalStrings(researcher.KnowledgeResources, []string{"docs"}) ||
		!equalStrings(researcher.CallableEntries, []string{"knowledge/docs", "tools/search"}) {
		t.Fatalf("researcher entries = %#v", researcher)
	}
	writer := compiled.Content.AgentPlan.Nodes["writer"]
	if len(writer.ToolResources) != 0 || len(writer.KnowledgeResources) != 0 || len(writer.CallableEntries) != 0 {
		t.Fatalf("writer inherited entries = %#v", writer)
	}
	if compiled.Content.Resources.Storage["session"].Destination.Username != "agent" ||
		compiled.Content.Resources.Storage["session"].Destination.SSLMode != "verify-full" {
		t.Fatalf("storage destination = %#v", compiled.Content.Resources.Storage["session"].Destination)
	}

	wantPurposes := map[string]bool{
		CredentialPurposeAPIKey: true, CredentialPurposeBearerToken: true,
		CredentialPurposeQdrantAPIKey: true, CredentialPurposeEmbeddingAPIKey: true,
		CredentialPurposeDSN: true,
	}
	gotPurposes := map[string]bool{}
	for _, use := range compiled.CredentialUses {
		gotPurposes[use.Purpose] = true
		if !strings.HasPrefix(use.AudienceDigest, "sha256:") || len(use.AudienceDigest) != 71 {
			t.Fatalf("invalid audience digest = %#v", use)
		}
		if use.CredentialID == credentialDebug || use.CredentialID == credentialArchive {
			t.Fatalf("unused credential entered closure: %#v", use)
		}
	}
	if len(compiled.CredentialUses) != 6 || !equalBoolMaps(gotPurposes, wantPurposes) {
		t.Fatalf("credential uses = %#v", compiled.CredentialUses)
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
		t.Fatalf("VerifyManifestContent: %v", err)
	}

	view := NewPublicManifestView(compiled.Content)
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"credential_id", "audience_digest", "crd_", `"purpose"`} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("public view contains %q: %s", forbidden, encoded)
		}
	}
	if !bytes.Contains(encoded, []byte(`"credential_present":true`)) {
		t.Fatalf("public view lost fixed credential presence: %s", encoded)
	}
}

func TestCompileMatchesResourceKeyNotRemoteToolName(t *testing.T) {
	input := validCompileInput()
	delete(input.Profile.Spec.Tools, "search")
	input.Profile.Spec.Tools["web_search"] = profiledomain.ToolResource{
		Kind:      profiledomain.ToolKindMCPStreamableHTTP,
		ServerURL: "https://tools.example.test/mcp", ToolsetName: "web",
		ToolName: "search", Auth: profiledomain.ToolAuth{Kind: profiledomain.AuthKindNone},
		Capability: profiledomain.CapabilityWebSearch,
	}
	compiled, report := Compile(input)
	if report.Valid || compiled.ContentDigest != "" {
		t.Fatalf("Compile = %#v, %#v", compiled, report)
	}
	assertDiagnostic(t, report, DiagnosticResourceMissing, DiagnosticSourceAgent,
		"/requirements/tools/search", "search", "")
}

func TestCompileRejectsDeclaredAndDerivedCapabilityMismatch(t *testing.T) {
	t.Run("declared", func(t *testing.T) {
		input := validCompileInput()
		input.Agent.Spec.Requirements.Tools["search"] = agentdomain.CapabilityRequirement{Capability: "data.lookup"}
		_, report := Compile(input)
		assertDiagnostic(t, report, DiagnosticCapabilityMismatch, DiagnosticSourceAgent,
			"/requirements/tools/search", "search", "")
	})
	t.Run("knowledge callable needs tool_call", func(t *testing.T) {
		input := validCompileInput()
		model := input.Profile.Spec.Models["primary"]
		model.Capabilities = []string{profiledomain.CapabilityChat}
		input.Profile.Spec.Models["primary"] = model
		_, report := Compile(input)
		assertDiagnostic(t, report, DiagnosticCapabilityMismatch, DiagnosticSourceAgent,
			"/nodes/researcher/model_slot", "primary", "researcher")
	})
}

func TestCompileStorageRoleRules(t *testing.T) {
	t.Run("memory absent is disabled", func(t *testing.T) {
		input := validCompileInput()
		delete(input.Profile.Spec.Storage, "memory")
		compiled, report := Compile(input)
		if !report.Valid || len(compiled.Content.StorageRoles) != 1 ||
			compiled.Content.StorageRoles[StorageRoleSession] != "session" {
			t.Fatalf("Compile = %#v, %#v", compiled.Content.StorageRoles, report)
		}
	})
	t.Run("conversation state does not replace session", func(t *testing.T) {
		input := validCompileInput()
		delete(input.Profile.Spec.Storage, "session")
		_, report := Compile(input)
		assertDiagnostic(t, report, DiagnosticStorageRoleMissing, DiagnosticSourceProfile,
			"/storage/session", "session", "")
	})
	t.Run("unsupported memory is an error", func(t *testing.T) {
		input := validCompileInput()
		memory := input.Profile.Spec.Storage["memory"]
		memory.Kind = profiledomain.StorageKind("future_state")
		input.Profile.Spec.Storage["memory"] = memory
		_, report := Compile(input)
		assertDiagnostic(t, report, DiagnosticStorageRoleUnsupported, DiagnosticSourceProfile,
			"/storage/memory", "memory", "")
	})
}

func TestCompileAllowsOptionalQdrantCredentialWithoutInventingAUse(t *testing.T) {
	input := validCompileInput()
	docs := input.Profile.Spec.Knowledge["docs"]
	docs.QdrantAPIKeyCredentialID = ""
	input.Profile.Spec.Knowledge["docs"] = docs
	compiled, report := Compile(input)
	if !report.Valid {
		t.Fatalf("Compile report = %#v", report)
	}
	if compiled.Content.Resources.Knowledge["docs"].Credential != nil {
		t.Fatal("compiler invented an optional qdrant credential")
	}
	for _, use := range compiled.CredentialUses {
		if use.Purpose == CredentialPurposeQdrantAPIKey {
			t.Fatalf("optional qdrant credential entered uses: %#v", use)
		}
	}
	view := NewPublicManifestView(compiled.Content)
	if view.Resources.Knowledge["docs"].CredentialPresent {
		t.Fatal("public view reported an absent qdrant credential")
	}
}

func TestCompilePlatformAdapterRangeAndLimits(t *testing.T) {
	t.Run("adapter", func(t *testing.T) {
		input := validCompileInput()
		delete(input.Platform.ToolAdapters, profiledomain.ToolKindMCPStreamableHTTP)
		redigestPlatform(t, &input.Platform)
		_, report := Compile(input)
		assertDiagnostic(t, report, DiagnosticAdapterUnsupported, DiagnosticSourcePlatform,
			"/tools/search/kind", "search", "")
	})
	t.Run("endpoint", func(t *testing.T) {
		// Any endpoint host compiles: the platform records the outbound set in
		// the manifest rather than pre-approving it, so configuring a new model
		// no longer requires a platform release first.
		input := validCompileInput()
		model := input.Profile.Spec.Models["primary"]
		model.BaseURL = "https://unlisted.example.invalid/v1"
		input.Profile.Spec.Models["primary"] = model
		manifest, report := Compile(input)
		if !report.Valid {
			t.Fatalf("report = %#v, want a valid compile for an unlisted endpoint host", report)
		}
		recorded := false
		for _, host := range manifest.Content.Execution.AllowedEndpointHosts {
			if host == "unlisted.example.invalid" {
				recorded = true
			}
		}
		if !recorded {
			t.Fatalf("manifest hosts = %#v, want the contacted host recorded", manifest.Content.Execution.AllowedEndpointHosts)
		}
	})
	t.Run("callable limit", func(t *testing.T) {
		for _, test := range []struct {
			limit int
			valid bool
		}{{1, false}, {2, true}, {3, true}} {
			input := validCompileInput()
			input.Platform.Limits.MaxCallableEntriesPerNode = test.limit
			redigestPlatform(t, &input.Platform)
			_, report := Compile(input)
			if report.Valid != test.valid {
				t.Fatalf("limit %d report = %#v", test.limit, report)
			}
			if !test.valid {
				assertDiagnostic(t, report, DiagnosticLimitExceeded, DiagnosticSourceAgent,
					"/nodes/researcher/callable_entries", "researcher", "researcher")
			}
		}
	})
	t.Run("generation limit", func(t *testing.T) {
		input := validCompileInput()
		tokens := input.Platform.Execution.MaxOutputTokens + 1
		node := input.Agent.Spec.Nodes["researcher"]
		node.Generation = &agentdomain.Generation{MaxOutputTokens: &tokens}
		input.Agent.Spec.Nodes["researcher"] = node
		_, report := Compile(input)
		assertDiagnostic(t, report, DiagnosticLimitExceeded, DiagnosticSourceAgent,
			"/nodes/researcher/generation/max_output_tokens", "primary", "researcher")
	})
	t.Run("manifest bytes", func(t *testing.T) {
		input := validCompileInput()
		input.Platform.Limits.MaxManifestBytes = 1
		redigestPlatform(t, &input.Platform)
		_, report := Compile(input)
		assertDiagnostic(t, report, DiagnosticManifestTooLarge, DiagnosticSourcePlatform,
			"/limits/max_manifest_bytes", "", "")
	})
}

func TestCompileCanonicalDeterminismAndSemanticChanges(t *testing.T) {
	first := validCompileInput()
	second := validCompileInput()
	model := second.Profile.Spec.Models["primary"]
	model.Capabilities = []string{profiledomain.CapabilityChat, profiledomain.CapabilityToolCall}
	second.Profile.Spec.Models = map[string]profiledomain.ModelResource{"primary": model}
	second.Agent.Spec.Nodes["researcher"] = second.Agent.Spec.Nodes["researcher"]
	second.Platform.Execution.AllowedEndpointHosts = reverse(second.Platform.Execution.AllowedEndpointHosts)
	redigestPlatform(t, &second.Platform)
	one, oneReport := Compile(first)
	two, twoReport := Compile(second)
	if !oneReport.Valid || !twoReport.Valid || !bytes.Equal(one.CanonicalContent, two.CanonicalContent) ||
		one.ContentDigest != two.ContentDigest {
		t.Fatalf("determinism failed\none=%s\ntwo=%s\nreports=%#v %#v", one.CanonicalContent, two.CanonicalContent, oneReport, twoReport)
	}

	changedChildren := validCompileInput()
	root := changedChildren.Agent.Spec.Nodes["main"]
	root.Children = []string{"writer", "researcher"}
	changedChildren.Agent.Spec.Nodes["main"] = root
	childrenCompiled, report := Compile(changedChildren)
	if !report.Valid || childrenCompiled.ContentDigest == one.ContentDigest {
		t.Fatalf("children order did not affect digest: %s", childrenCompiled.ContentDigest)
	}

	changedCredential := validCompileInput()
	primary := changedCredential.Profile.Spec.Models["primary"]
	primary.APIKeyCredentialID = credentialReplacement
	changedCredential.Profile.Spec.Models["primary"] = primary
	credentialCompiled, report := Compile(changedCredential)
	if !report.Valid || credentialCompiled.ContentDigest == one.ContentDigest {
		t.Fatalf("credential identity did not affect digest: %s", credentialCompiled.ContentDigest)
	}
}

func TestPlatformExecutionContractValidation(t *testing.T) {
	contract := DefaultPlatformExecutionContract()
	if err := contract.Validate(); err != nil {
		t.Fatalf("default Validate: %v", err)
	}
	reordered := contract
	reordered.Execution.AllowedEndpointHosts = reverse(contract.Execution.AllowedEndpointHosts)
	digest, err := reordered.CalculateDigest()
	if err != nil || digest != contract.Digest {
		t.Fatalf("host set changed digest: %q %v", digest, err)
	}
	tampered := contract
	tampered.Execution.MaxRunSeconds++
	if err := tampered.Validate(); err == nil {
		t.Fatal("tampered contract validated with stale digest")
	}
}

func TestPlatformExecutionContractAllowsUnsupportedImplementationsToBeAbsent(t *testing.T) {
	contract := DefaultPlatformExecutionContract()
	contract.ModelAdapters = nil
	contract.ToolAdapters = map[profiledomain.ToolKind]AdapterContract{}
	contract.KnowledgeAdapters = nil
	contract.StorageAdapters = map[profiledomain.StorageKind]AdapterContract{}
	redigestPlatform(t, &contract)

	if err := contract.Validate(); err != nil {
		t.Fatalf("Validate absent implementations: %v", err)
	}
}

func TestPlatformExecutionContractRejectsNonFrozenAdapterVersions(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*PlatformExecutionContract)
		wantError string
	}{
		{
			name: "model",
			mutate: func(contract *PlatformExecutionContract) {
				contract.ModelAdapters[profiledomain.ModelKindOpenAICompatible] = AdapterContract{Version: "future-v2"}
			},
			wantError: `invalid platform execution contract: model adapter "openai_compatible" version must be "openai-compatible-v1"`,
		},
		{
			name: "tool",
			mutate: func(contract *PlatformExecutionContract) {
				contract.ToolAdapters[profiledomain.ToolKindMCPStreamableHTTP] = AdapterContract{Version: "future-v2"}
			},
			wantError: `invalid platform execution contract: tool adapter "mcp_streamable_http" version must be "mcp-web-search-v1"`,
		},
		{
			name: "knowledge",
			mutate: func(contract *PlatformExecutionContract) {
				contract.KnowledgeAdapters[profiledomain.KnowledgeKindQdrantOpenAI] = KnowledgeAdapterContract{
					Version: "future-v2", CreatesCallable: true,
				}
			},
			wantError: `invalid platform execution contract: knowledge adapter "qdrant_openai" version must be "qdrant-openai-v1"`,
		},
		{
			name: "storage",
			mutate: func(contract *PlatformExecutionContract) {
				contract.StorageAdapters[profiledomain.StorageKindPostgresState] = AdapterContract{Version: "future-v2"}
			},
			wantError: `invalid platform execution contract: storage adapter "postgres_state" version must be "postgres-state-v1"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contract := DefaultPlatformExecutionContract()
			test.mutate(&contract)
			redigestPlatform(t, &contract)

			err := contract.Validate()
			if !errors.Is(err, ErrInvalidPlatformExecutionContract) || err.Error() != test.wantError {
				t.Fatalf("Validate error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestPlatformExecutionContractRequiresQdrantCallable(t *testing.T) {
	contract := DefaultPlatformExecutionContract()
	contract.KnowledgeAdapters[profiledomain.KnowledgeKindQdrantOpenAI] = KnowledgeAdapterContract{
		Version: KnowledgeAdapterQdrantOpenAIV1, CreatesCallable: false,
	}
	redigestPlatform(t, &contract)

	err := contract.Validate()
	want := `invalid platform execution contract: knowledge adapter "qdrant_openai" must create the frozen callable`
	if !errors.Is(err, ErrInvalidPlatformExecutionContract) || err.Error() != want {
		t.Fatalf("Validate error = %v, want %q", err, want)
	}
}

func TestPlatformExecutionContractAdapterErrorsAreDeterministic(t *testing.T) {
	contract := DefaultPlatformExecutionContract()
	contract.ToolAdapters[profiledomain.ToolKind("z_future")] = AdapterContract{Version: "z"}
	contract.ToolAdapters[profiledomain.ToolKind("a_future")] = AdapterContract{Version: "a"}
	redigestPlatform(t, &contract)

	want := `invalid platform execution contract: unsupported tool adapter kind "a_future"`
	for range 25 {
		err := contract.Validate()
		if !errors.Is(err, ErrInvalidPlatformExecutionContract) || err.Error() != want {
			t.Fatalf("Validate error = %v, want %q", err, want)
		}
	}
}

func TestDiagnosticsAreStableAndUseEmptyArray(t *testing.T) {
	report := NewValidationReport("compiler", "digest", []Diagnostic{
		diagnostic("z", SeverityWarning, DiagnosticSourceProfile, "/b", "z"),
		diagnostic("b", SeverityError, DiagnosticSourceAgent, "/a", "b"),
		diagnostic("a", SeverityWarning, DiagnosticSourceAgent, "/a", "a"),
	})
	if report.Valid || report.Diagnostics[0].Code != "a" || report.Diagnostics[1].Code != "b" || report.Diagnostics[2].Code != "z" {
		t.Fatalf("report = %#v", report)
	}
	empty := NewValidationReport("compiler", "digest", nil)
	encoded, err := json.Marshal(empty)
	if err != nil || !bytes.Contains(encoded, []byte(`"diagnostics":[]`)) {
		t.Fatalf("empty report = %s, %v", encoded, err)
	}
}

const (
	credentialModel       = "crd_00000000000000000000000000000001"
	credentialSearch      = "crd_00000000000000000000000000000002"
	credentialDebug       = "crd_00000000000000000000000000000003"
	credentialQdrant      = "crd_00000000000000000000000000000004"
	credentialEmbedding   = "crd_00000000000000000000000000000005"
	credentialSession     = "crd_00000000000000000000000000000006"
	credentialMemory      = "crd_00000000000000000000000000000007"
	credentialArchive     = "crd_00000000000000000000000000000008"
	credentialReplacement = "crd_00000000000000000000000000000009"
)

func validCompileInput() CompileInput {
	agent := agentdomain.Spec{
		SchemaVersion: SchemaVersionV1,
		Root:          "main",
		Requirements: agentdomain.Requirements{
			Models: map[string]agentdomain.ModelRequirement{
				"primary": {Capabilities: []string{profiledomain.CapabilityChat}},
			},
			Tools: map[string]agentdomain.CapabilityRequirement{
				"search": {Capability: profiledomain.CapabilityWebSearch},
				"debug":  {Capability: profiledomain.CapabilityWebSearch},
			},
			Knowledge: map[string]agentdomain.CapabilityRequirement{
				"docs": {Capability: profiledomain.CapabilityKnowledgeSearch},
			},
		},
		Nodes: map[string]agentdomain.Node{
			"main": {Kind: agentdomain.NodeKindSequence, Children: []string{"researcher", "writer"}},
			"researcher": {
				Kind: agentdomain.NodeKindLLM, Instruction: "research",
				ModelSlot: "primary", ToolSlots: []string{"search"},
				KnowledgeSlots: []string{"docs"},
			},
			"writer": {
				Kind: agentdomain.NodeKindLLM, Instruction: "write",
				ModelSlot: "primary", ToolSlots: []string{}, KnowledgeSlots: []string{},
			},
		},
	}
	profile := profiledomain.Spec{
		SchemaVersion:             SchemaVersionV1,
		CredentialProtocolVersion: profiledomain.CredentialProtocolVersionV1,
		Models: map[string]profiledomain.ModelResource{
			"primary": {
				Kind:  profiledomain.ModelKindOpenAICompatible,
				Model: "chat-model", BaseURL: "https://models.example.test/v1",
				APIKeyCredentialID: credentialModel,
				Capabilities:       []string{profiledomain.CapabilityToolCall, profiledomain.CapabilityChat},
			},
		},
		Tools: map[string]profiledomain.ToolResource{
			"search": {
				Kind:      profiledomain.ToolKindMCPStreamableHTTP,
				ServerURL: "https://tools.example.test/mcp", ToolsetName: "web", ToolName: "search_web",
				Auth:       profiledomain.ToolAuth{Kind: profiledomain.AuthKindBearer, CredentialID: credentialSearch},
				Capability: profiledomain.CapabilityWebSearch,
			},
			"debug": {
				Kind:      profiledomain.ToolKindMCPStreamableHTTP,
				ServerURL: "https://tools.example.test/debug", ToolsetName: "web", ToolName: "debug",
				Auth:       profiledomain.ToolAuth{Kind: profiledomain.AuthKindBearer, CredentialID: credentialDebug},
				Capability: profiledomain.CapabilityWebSearch,
			},
		},
		Knowledge: map[string]profiledomain.KnowledgeResource{
			"docs": {
				Kind: profiledomain.KnowledgeKindQdrantOpenAI,
				Host: "qdrant.example.test", Port: 6334, TLS: true, Collection: "docs",
				QdrantAPIKeyCredentialID: credentialQdrant,
				Embedding: profiledomain.EmbeddingResource{
					Model: "embed", BaseURL: "https://embedding.example.test/v1",
					APIKeyCredentialID: credentialEmbedding, Dimensions: 1536,
				},
			},
		},
		Storage: map[string]profiledomain.StorageResource{
			"session":            storageResource(credentialSession),
			"memory":             storageResource(credentialMemory),
			"archive":            storageResource(credentialArchive),
			"conversation_state": storageResource("crd_0000000000000000000000000000000a"),
		},
	}
	return CompileInput{
		TenantID: "tenant-1",
		Agent: AgentVersionSource{
			TenantID: "tenant-1", AgentID: "agent-1", VersionID: "agent-version-1",
			VersionNumber: 3, SchemaVersion: SchemaVersionV1,
			SpecDigest: "sha256:" + strings.Repeat("a", 64), Spec: agent,
		},
		Profile: ProfileRevisionSource{
			TenantID: "tenant-1", ProfileID: "profile-1", RevisionID: "profile-revision-1",
			RevisionNumber: 2, SchemaVersion: SchemaVersionV1,
			SpecDigest: "sha256:" + strings.Repeat("b", 64), Spec: profile,
		},
		Platform: DefaultPlatformExecutionContract(),
	}
}

func storageResource(credentialID string) profiledomain.StorageResource {
	return profiledomain.StorageResource{
		Kind: profiledomain.StorageKindPostgresState, DSNCredentialID: credentialID,
		Destination: profiledomain.StorageDestination{
			Host: "db.example.test", Port: 5432, Database: "agent_state",
			Username: "agent", SSLMode: "verify-full",
		},
	}
}

func redigestPlatform(t *testing.T, contract *PlatformExecutionContract) {
	t.Helper()
	digest, err := contract.CalculateDigest()
	if err != nil {
		t.Fatal(err)
	}
	contract.Digest = digest
}

func assertDiagnostic(
	t *testing.T,
	report ValidationReport,
	code string,
	source DiagnosticSource,
	path, name, nodeID string,
) {
	t.Helper()
	for _, diagnostic := range report.Diagnostics {
		if diagnostic.Code == code && diagnostic.Source == source && diagnostic.Path == path &&
			(name == "" || pointerString(diagnostic.Name) == name) &&
			(nodeID == "" || pointerString(diagnostic.NodeID) == nodeID) {
			return
		}
	}
	t.Fatalf("missing diagnostic %s %s %s %s %s in %#v", code, source, path, name, nodeID, report.Diagnostics)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalBoolMaps(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func reverse(values []string) []string {
	result := append([]string(nil), values...)
	for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result
}

func TestCompileManifestExactByteBoundaries(t *testing.T) {
	input := validCompileInput()
	probe, report := Compile(input)
	if !report.Valid {
		t.Fatalf("probe report = %#v", report)
	}
	size := len(probe.CanonicalContent)
	for _, delta := range []int{-1, 0, 1} {
		input := validCompileInput()
		input.Platform.Limits.MaxManifestBytes = size + delta
		redigestPlatform(t, &input.Platform)
		compiled, report := Compile(input)
		if report.Valid != (delta >= 0) {
			t.Fatalf("limit=%d size=%d report=%#v", size+delta, size, report)
		}
		if delta < 0 {
			assertDiagnostic(t, report, DiagnosticManifestTooLarge, DiagnosticSourcePlatform, "/limits/max_manifest_bytes", "", "")
		} else if len(compiled.CanonicalContent) != size {
			t.Fatalf("content size changed: %d != %d", len(compiled.CanonicalContent), size)
		}
	}
}

func TestPlatformContractRejectsUnfrozenExecutionBackend(t *testing.T) {
	contract := DefaultPlatformExecutionContract()
	contract.Execution.Backend = "arbitrary-worker"
	redigestPlatform(t, &contract)
	if !errors.Is(contract.Validate(), ErrInvalidPlatformExecutionContract) {
		t.Fatal("unfrozen backend was accepted")
	}
}

func TestCompileSourceIdentityLengthBoundaries(t *testing.T) {
	setters := map[string]func(*CompileInput, string){
		"tenant": func(input *CompileInput, value string) {
			input.TenantID = value
			input.Agent.TenantID = value
			input.Profile.TenantID = value
		},
		"agent":            func(input *CompileInput, value string) { input.Agent.AgentID = value },
		"agent version":    func(input *CompileInput, value string) { input.Agent.VersionID = value },
		"profile":          func(input *CompileInput, value string) { input.Profile.ProfileID = value },
		"profile revision": func(input *CompileInput, value string) { input.Profile.RevisionID = value },
	}
	for name, set := range setters {
		t.Run(name, func(t *testing.T) {
			for _, length := range []int{0, 1, 128, 129} {
				input := validCompileInput()
				set(&input, strings.Repeat("界", length))
				compiled, report := Compile(input)
				wantValid := length >= 1 && length <= 128
				if report.Valid != wantValid {
					t.Fatalf("length %d valid=%v, report=%#v", length, report.Valid, report)
				}
				if wantValid {
					if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
						t.Fatalf("valid length %d manifest: %v", length, err)
					}
				} else if compiled.ContentDigest != "" {
					t.Fatal("invalid identity produced a manifest")
				}
			}
		})
	}
}

func TestCompileRejectsInvalidLogicalCallableName(t *testing.T) {
	input := validCompileInput()
	input.Profile.Spec.Tools["Search"] = input.Profile.Spec.Tools["search"]
	delete(input.Profile.Spec.Tools, "search")
	input.Agent.Spec.Requirements.Tools["Search"] = input.Agent.Spec.Requirements.Tools["search"]
	delete(input.Agent.Spec.Requirements.Tools, "search")
	node := input.Agent.Spec.Nodes["researcher"]
	node.ToolSlots = []string{"Search"}
	input.Agent.Spec.Nodes["researcher"] = node
	compiled, report := Compile(input)
	if report.Valid || compiled.ContentDigest != "" {
		t.Fatal("invalid logical callable was published")
	}
	assertDiagnostic(t, report, DiagnosticEntrypointUnsupported, DiagnosticSourceAgent,
		"/nodes/researcher/callable_entries", "researcher", "researcher")
}

// Profile persistence support must not advertise an executable Deployment before
// the fixed-target compiler and runtime adapters have actually been connected.
func TestManagedProfileDoesNotBypassDeploymentAdapterGate(t *testing.T) {
	input := validCompileInput()
	input.Profile.Spec.Storage["session"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedSession, BackendID: "redis", BackendRevision: 1}
	_, report := Compile(input)
	if report.Valid {
		t.Fatal("managed storage compiled without a physical backend snapshot adapter")
	}
	found := false
	for _, d := range report.Diagnostics {
		if d.Code == DiagnosticBackendUnavailable {
			found = true
		}
	}
	if !found {
		t.Fatal("missing explicit unavailable fixed backend diagnostic", report.Diagnostics)
	}
}
