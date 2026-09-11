package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"

	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

// Compile deterministically binds one immutable AgentVersion to one immutable
// ProfileRevision. Expected incompatibilities are returned as diagnostics; an
// invalid report always has an empty CompiledManifest.
func Compile(input CompileInput) (CompiledManifest, ValidationReport) {
	compilerVersion := CompilerVersionV1
	diagnostics := validateCompileInput(input)
	diagnostics = append(diagnostics, ValidateManagedSnapshots(input)...)
	if input.Platform.Version == deploymentv1.WorkerV1PlatformVersion && agentUsesStorageRole(input.Agent.Spec, "memory") {
		backend, ok := input.ManagedBackends["storage/memory"]
		if !ok || !memoryRuntimePrincipal(backend) || input.Profile.Spec.Storage["memory"].Kind != profiledomain.StorageKindManagedMemory {
			diagnostics = append(diagnostics, diagnostic(DiagnosticStorageRoleUnsupported, SeverityError, DiagnosticSourcePlatform, "/storage/memory", "Worker V1 Memory requires managed PostgreSQL or Redis with memory_runtime principal"))
		}
	}
	if input.Platform.Version == deploymentv1.WorkerV1PlatformVersion && input.Profile.Spec.Storage["session"].Kind == profiledomain.StorageKindManagedSession {
		b, ok := input.ManagedBackends["storage/session"]
		if !ok || b.Kind != datav1.Redis || b.Redis == nil || b.Redis.Username != "session_runtime" {
			diagnostics = append(diagnostics, diagnostic(DiagnosticStorageRoleUnsupported, SeverityError, DiagnosticSourcePlatform, "/storage/session", "Worker V1 managed Session requires Redis with session_runtime principal"))
		}
	}
	diagnostics = append(diagnostics, dataContractDiagnostics(input.Agent.Spec, input.Platform)...)
	if len(errorDiagnostics(diagnostics)) > 0 {
		return CompiledManifest{}, NewValidationReport(
			compilerVersion, input.Platform.Digest, diagnostics,
		)
	}

	used := collectUsedRequirements(input.Agent.Spec)
	resolvedModels, resolvedTools, resolvedKnowledge := matchDeclaredRequirements(
		input.Agent.Spec, input.Profile.Spec, used, &diagnostics,
	)
	executors, resolvedExecutors := compileExecutors(input, &diagnostics)
	selectedStorage, storageRoles := selectStorageRoles(input.Profile.Spec, &diagnostics, input.Agent.Spec)

	plan := compileAgentPlan(
		input.Agent.Spec, input.Profile.Spec,
		resolvedTools, resolvedKnowledge,
		input.Platform, &diagnostics,
	)
	resources, uses, endpointHosts := compileResources(
		used, resolvedModels, resolvedTools, resolvedKnowledge, selectedStorage,
		input.Platform, input.ManagedBackends, &diagnostics,
	)
	resources.Executors = executors
	if len(endpointHosts) > 128 {
		diagnostics = append(diagnostics, diagnostic(
			DiagnosticLimitExceeded, SeverityError, DiagnosticSourcePlatform,
			"/execution/allowed_endpoint_hosts", "runtime manifest exceeds the fixed endpoint host limit",
		))
	}

	report := NewValidationReport(compilerVersion, input.Platform.Digest, diagnostics)
	if !report.Valid {
		return CompiledManifest{}, report
	}

	content := ManifestContent{
		SchemaVersion:          input.Platform.ManifestSchemaVersion,
		CompilerVersion:        input.Platform.CompilerVersion,
		RuntimeContractVersion: input.Platform.RuntimeContractVersion,
		PlatformContract: PlatformContractReference{
			Version: input.Platform.Version, Digest: input.Platform.Digest,
		},
		TenantID: input.TenantID,
		Sources: ManifestSources{
			Agent: ManifestAgentSource{
				AgentID: input.Agent.AgentID, VersionNumber: input.Agent.VersionNumber,
				VersionID: input.Agent.VersionID, SchemaVersion: input.Agent.SchemaVersion,
				Digest: input.Agent.SpecDigest,
			},
			Profile: ManifestProfileSource{
				ProfileID: input.Profile.ProfileID, RevisionNumber: input.Profile.RevisionNumber,
				RevisionID: input.Profile.RevisionID, SchemaVersion: input.Profile.SchemaVersion,
				Digest: input.Profile.SpecDigest,
			},
		},
		Runtime:   compileManifestRuntime(input.Agent.Spec),
		AgentPlan: plan, Resources: resources,
		ResolvedRequirements: ResolvedRequirements{
			Executors: resolvedExecutors,
			Models:    identityResolution(used.models, resolvedModels),
			Tools:     identityResolution(used.tools, resolvedTools),
			Knowledge: identityResolution(used.knowledge, resolvedKnowledge),
		},
		StorageRoles: storageRoles,
		Execution: ExecutionPolicy{
			Backend:              input.Platform.Execution.Backend,
			AllowedEndpointHosts: endpointHosts,
			MaxRunSeconds:        input.Platform.Execution.MaxRunSeconds,
			MaxToolCalls:         input.Platform.Execution.MaxToolCalls,
			MaxOutputTokens:      input.Platform.Execution.MaxOutputTokens,
		},
	}
	normalized, canonical, digest, err := CanonicalizeManifest(content)
	if err != nil {
		report = report.WithDiagnostics(diagnostic(
			DiagnosticInputInvalid, SeverityError, DiagnosticSourcePlatform, "",
			"runtime manifest content could not be canonicalized",
		))
		return CompiledManifest{}, report
	}
	if input.Platform.Version == deploymentv1.WorkerV1PlatformVersion {
		wire, decodeErr := deploymentv1.DecodeManifestContent(canonical)
		if decodeErr != nil {
			return CompiledManifest{}, report.WithDiagnostics(diagnostic(DiagnosticInputInvalid, SeverityError, DiagnosticSourcePlatform, "", "Worker V1 manifest codec rejected compiled content"))
		}
		if gateErr := deploymentv1.ValidateWorkerV1(wire, input.Platform.Digest); gateErr != nil {
			if errors.Is(gateErr, deploymentv1.ErrWorkerV1SessionRuntimeRole) {
				path := "/resources/storage/" + wire.StorageRoles["session"] + "/destination/username"
				return CompiledManifest{}, report.WithDiagnostics(diagnostic(DiagnosticStorageRoleUnsupported, SeverityError, DiagnosticSourcePlatform, path, gateErr.Error()))
			}
			return CompiledManifest{}, report.WithDiagnostics(diagnostic(DiagnosticEntrypointUnsupported, SeverityError, DiagnosticSourcePlatform, "/agent_plan", gateErr.Error()))
		}
	}
	if len(canonical) > input.Platform.Limits.MaxManifestBytes {
		report = report.WithDiagnostics(diagnostic(
			DiagnosticManifestTooLarge, SeverityError, DiagnosticSourcePlatform,
			"/limits/max_manifest_bytes", "runtime manifest exceeds the platform byte limit",
		))
		return CompiledManifest{}, report
	}
	sortCredentialUses(uses)
	return CompiledManifest{
		Content: normalized, CanonicalContent: canonical,
		ContentDigest: digest, CredentialUses: uses,
	}, report
}

func validateCompileInput(input CompileInput) []Diagnostic {
	var diagnostics []Diagnostic
	if err := input.Platform.Validate(); err != nil {
		diagnostics = append(diagnostics, diagnostic(
			DiagnosticInputInvalid, SeverityError, DiagnosticSourcePlatform, "",
			"platform execution contract is invalid",
		))
	}
	if !validSourceID(input.TenantID) || input.Agent.TenantID != input.TenantID ||
		input.Profile.TenantID != input.TenantID {
		diagnostics = append(diagnostics, diagnostic(
			DiagnosticInputInvalid, SeverityError, DiagnosticSourceInput, "/tenant_id",
			"source snapshots must belong to the trusted tenant",
		))
	}
	if !validSourceID(input.Agent.AgentID) || !validSourceID(input.Agent.VersionID) ||
		input.Agent.VersionNumber <= 0 || !validDigest(input.Agent.SpecDigest) {
		diagnostics = append(diagnostics, diagnostic(
			DiagnosticInputInvalid, SeverityError, DiagnosticSourceAgent, "",
			"agent version identity is incomplete",
		))
	}
	if !validSourceID(input.Profile.ProfileID) || !validSourceID(input.Profile.RevisionID) ||
		input.Profile.RevisionNumber <= 0 || !validDigest(input.Profile.SpecDigest) {
		diagnostics = append(diagnostics, diagnostic(
			DiagnosticInputInvalid, SeverityError, DiagnosticSourceProfile, "",
			"profile revision identity is incomplete",
		))
	}
	if input.Agent.SchemaVersion != SchemaVersionV1 ||
		input.Agent.Spec.SchemaVersion != SchemaVersionV1 {
		diagnostics = append(diagnostics, diagnostic(
			DiagnosticSourceSchemaUnsupported, SeverityError, DiagnosticSourceAgent,
			"/schema_version", "AgentSpec schema is unsupported by this compiler",
		))
	}
	if input.Profile.SchemaVersion != SchemaVersionV1 ||
		input.Profile.Spec.SchemaVersion != SchemaVersionV1 {
		diagnostics = append(diagnostics, diagnostic(
			DiagnosticSourceSchemaUnsupported, SeverityError, DiagnosticSourceProfile,
			"/schema_version", "RuntimeProfileSpec schema is unsupported by this compiler",
		))
	}
	if input.Profile.Spec.CredentialProtocolVersion != profiledomain.CredentialProtocolVersionV1 {
		diagnostics = append(diagnostics, diagnostic(
			DiagnosticSourceSchemaUnsupported, SeverityError, DiagnosticSourceProfile,
			"/credential_protocol_version", "profile credential protocol is unsupported by this compiler",
		))
	}
	return diagnostics
}

// Source IDs are opaque and use the same Unicode length bound as the wire
// contract; do not trim or reinterpret owner-provided identifiers.
func validSourceID(value string) bool {
	length := utf8.RuneCountInString(value)
	return length >= 1 && length <= 128
}

type usedRequirements struct {
	models    map[string]bool
	tools     map[string]bool
	knowledge map[string]bool
}

func collectUsedRequirements(spec agentdomain.Spec) usedRequirements {
	used := usedRequirements{
		models: make(map[string]bool), tools: make(map[string]bool),
		knowledge: make(map[string]bool),
	}
	for _, node := range spec.Nodes {
		if node.Kind != agentdomain.NodeKindLLM {
			continue
		}
		used.models[node.ModelSlot] = true
		for _, name := range node.ToolSlots {
			used.tools[name] = true
		}
		for _, name := range node.KnowledgeSlots {
			used.knowledge[name] = true
		}
	}
	if spec.Runtime != nil && spec.Runtime.Summary != nil && spec.Runtime.Summary.Enabled {
		used.models[spec.Runtime.Summary.ModelSlot] = true
	}
	return used
}

func matchDeclaredRequirements(
	agent agentdomain.Spec,
	profile profiledomain.Spec,
	used usedRequirements,
	diagnostics *[]Diagnostic,
) (
	map[string]profiledomain.ModelResource,
	map[string]profiledomain.ToolResource,
	map[string]profiledomain.KnowledgeResource,
) {
	models := make(map[string]profiledomain.ModelResource)
	tools := make(map[string]profiledomain.ToolResource)
	knowledge := make(map[string]profiledomain.KnowledgeResource)
	for _, name := range sortedKeys(agent.Requirements.Models) {
		path := "/requirements/models/" + escapeJSONPointer(name)
		resource, exists := profile.Models[name]
		if !exists {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticResourceMissing, SeverityError, DiagnosticSourceAgent,
				path, "models", name, "ProfileRevision is missing the same-name model resource",
			))
		} else if !containsAll(resource.ProvidedCapabilities(), agent.Requirements.Models[name].Capabilities) {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticCapabilityMismatch, SeverityError, DiagnosticSourceAgent,
				path, "models", name, "same-name model does not provide all required capabilities",
			))
		} else {
			models[name] = resource
		}
		if !used.models[name] {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticUnusedRequirement, SeverityWarning, DiagnosticSourceAgent,
				path, "models", name, "declared model requirement is unused by every node",
			))
		}
	}
	for _, name := range sortedKeys(agent.Requirements.Tools) {
		path := "/requirements/tools/" + escapeJSONPointer(name)
		resource, exists := profile.Tools[name]
		if !exists {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticResourceMissing, SeverityError, DiagnosticSourceAgent,
				path, "tools", name, "ProfileRevision is missing the same-name tool resource",
			))
		} else if !containsString(resource.ProvidedCapabilities(), agent.Requirements.Tools[name].Capability) {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticCapabilityMismatch, SeverityError, DiagnosticSourceAgent,
				path, "tools", name, "same-name tool does not provide the required capability",
			))
		} else {
			tools[name] = resource
		}
		if !used.tools[name] {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticUnusedRequirement, SeverityWarning, DiagnosticSourceAgent,
				path, "tools", name, "declared tool requirement is unused by every node",
			))
		}
	}
	for _, name := range sortedKeys(agent.Requirements.Knowledge) {
		path := "/requirements/knowledge/" + escapeJSONPointer(name)
		resource, exists := profile.Knowledge[name]
		if !exists {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticResourceMissing, SeverityError, DiagnosticSourceAgent,
				path, "knowledge", name, "ProfileRevision is missing the same-name knowledge resource",
			))
		} else if !containsString(resource.ProvidedCapabilities(), agent.Requirements.Knowledge[name].Capability) {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticCapabilityMismatch, SeverityError, DiagnosticSourceAgent,
				path, "knowledge", name, "same-name knowledge resource does not provide the required capability",
			))
		} else {
			knowledge[name] = resource
		}
		if !used.knowledge[name] {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticUnusedRequirement, SeverityWarning, DiagnosticSourceAgent,
				path, "knowledge", name, "declared knowledge requirement is unused by every node",
			))
		}
	}
	return models, tools, knowledge
}

func selectStorageRoles(
	profile profiledomain.Spec,
	diagnostics *[]Diagnostic,
	agents ...agentdomain.Spec,
) (map[string]profiledomain.StorageResource, map[string]string) {
	selected := make(map[string]profiledomain.StorageResource)
	roles := make(map[string]string)
	session, exists := profile.Storage[StorageRoleSession]
	if !exists {
		*diagnostics = append(*diagnostics, resourceDiagnostic(
			DiagnosticStorageRoleMissing, SeverityError, DiagnosticSourceProfile,
			"/storage/session", "storage", StorageRoleSession,
			"required storage.session runtime role is missing",
		))
	} else if storageSupportsRole(session, profiledomain.CapabilityStorageSession) {
		selected[StorageRoleSession] = session
		roles[StorageRoleSession] = StorageRoleSession
	} else {
		*diagnostics = append(*diagnostics, resourceDiagnostic(
			DiagnosticStorageRoleUnsupported, SeverityError, DiagnosticSourceProfile,
			"/storage/session", "storage", StorageRoleSession,
			"storage.session does not support the session runtime role",
		))
	}
	if memory, exists := profile.Storage[StorageRoleMemory]; exists &&
		(!memory.Kind.Managed() || (len(agents) > 0 && agentUsesStorageRole(agents[0], StorageRoleMemory))) {
		if storageSupportsRole(memory, profiledomain.CapabilityStorageMemory) {
			selected[StorageRoleMemory] = memory
			roles[StorageRoleMemory] = StorageRoleMemory
		} else {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticStorageRoleUnsupported, SeverityError, DiagnosticSourceProfile,
				"/storage/memory", "storage", StorageRoleMemory,
				"storage.memory does not support the memory runtime role",
			))
		}
	}
	if len(agents) > 0 {
		for _, role := range []string{StorageRoleMemory, "artifact"} {
			if !agentUsesStorageRole(agents[0], role) {
				continue
			}
			r, ok := profile.Storage[role]
			if !ok {
				*diagnostics = append(*diagnostics, resourceDiagnostic(DiagnosticStorageRoleMissing, SeverityError, DiagnosticSourceProfile, "/storage/"+role, "storage", role, "required runtime role is missing"))
				continue
			}
			if role == "artifact" {
				if r.Kind != profiledomain.StorageKindManagedArtifact {
					*diagnostics = append(*diagnostics, resourceDiagnostic(DiagnosticStorageRoleUnsupported, SeverityError, DiagnosticSourceProfile, "/storage/artifact", "storage", role, "artifact requires a managed artifact resource"))
					continue
				}
				selected[role] = r
				roles[role] = role
			}
		}
	}
	return selected, roles
}

func storageSupportsRole(resource profiledomain.StorageResource, capability string) bool {
	return (resource.Kind == profiledomain.StorageKindPostgresState || resource.Kind.Managed()) &&
		containsString(resource.ProvidedCapabilities(), capability)
}

func compileAgentPlan(
	agent agentdomain.Spec,
	profile profiledomain.Spec,
	tools map[string]profiledomain.ToolResource,
	knowledge map[string]profiledomain.KnowledgeResource,
	platform PlatformExecutionContract,
	diagnostics *[]Diagnostic,
) AgentPlan {
	plan := AgentPlan{Root: agent.Root, Nodes: make(map[string]ManifestNode, len(agent.Nodes))}
	for _, id := range sortedKeys(agent.Nodes) {
		source := agent.Nodes[id]
		node := ManifestNode{Kind: source.Kind, Name: source.Name}
		switch source.Kind {
		case agentdomain.NodeKindLLM:
			node.Instruction = source.Instruction
			node.ModelResource = source.ModelSlot
			node.ToolResources = selectedNames(source.ToolSlots, tools)
			node.KnowledgeResources = selectedNames(source.KnowledgeSlots, knowledge)
			node.CallableEntries = callableEntries(node.ToolResources, node.KnowledgeResources)
			if _, err := ProviderCallableNames(node.CallableEntries); err != nil {
				*diagnostics = append(*diagnostics, nodeResourceDiagnostic(
					DiagnosticEntrypointUnsupported, SeverityError, DiagnosticSourceAgent,
					"/nodes/"+escapeJSONPointer(id)+"/callable_entries", "nodes", id, id,
					"node callable entries do not satisfy the fixed provider naming contract",
				))
			}
			compileNodeData(&node, source, id, platform, diagnostics)
			if source.Workspace != nil {
				node.Workspace = &ManifestWorkspace{ExecutorResource: source.Workspace.ExecutorSlot, Tools: append([]string{}, source.Workspace.Tools...)}
			}
			node.Generation = cloneGeneration(source.Generation)
			if len(node.CallableEntries) > platform.Limits.MaxCallableEntriesPerNode {
				*diagnostics = append(*diagnostics, nodeResourceDiagnostic(
					DiagnosticLimitExceeded, SeverityError, DiagnosticSourceAgent,
					"/nodes/"+escapeJSONPointer(id)+"/callable_entries",
					"nodes", id, id, "node callable entry count exceeds the platform limit",
				))
			}
			if source.Generation != nil && source.Generation.MaxOutputTokens != nil &&
				*source.Generation.MaxOutputTokens > platform.Execution.MaxOutputTokens {
				*diagnostics = append(*diagnostics, nodeResourceDiagnostic(
					DiagnosticLimitExceeded, SeverityError, DiagnosticSourceAgent,
					"/nodes/"+escapeJSONPointer(id)+"/generation/max_output_tokens",
					"models", source.ModelSlot, id,
					"node max_output_tokens exceeds the platform execution limit",
				))
			}
			if node.Workspace != nil || len(node.CallableEntries) > 0 || (node.Memory != nil && len(node.Memory.Tools) > 0) || node.Artifact != nil {
				model, exists := profile.Models[source.ModelSlot]
				requirement := agent.Requirements.Models[source.ModelSlot]
				if exists && !containsString(model.ProvidedCapabilities(), profiledomain.CapabilityToolCall) &&
					!containsString(requirement.Capabilities, profiledomain.CapabilityToolCall) {
					*diagnostics = append(*diagnostics, nodeResourceDiagnostic(
						DiagnosticCapabilityMismatch, SeverityError, DiagnosticSourceAgent,
						"/nodes/"+escapeJSONPointer(id)+"/model_slot",
						"models", source.ModelSlot, id,
						"node has callable entries but its model does not provide tool_call",
					))
				}
			}
		case agentdomain.NodeKindSequence, agentdomain.NodeKindParallel:
			node.Children = append([]string{}, source.Children...)
		case agentdomain.NodeKindLoop:
			node.Body = source.Body
			node.MaxIterations = source.MaxIterations
		default:
			*diagnostics = append(*diagnostics, nodeResourceDiagnostic(
				DiagnosticSourceSchemaUnsupported, SeverityError, DiagnosticSourceAgent,
				"/nodes/"+escapeJSONPointer(id)+"/kind", "nodes", id, id,
				"AgentSpec node kind is unsupported by this compiler",
			))
		}
		plan.Nodes[id] = node
	}
	return plan
}

func compileResources(
	used usedRequirements,
	models map[string]profiledomain.ModelResource,
	tools map[string]profiledomain.ToolResource,
	knowledge map[string]profiledomain.KnowledgeResource,
	storage map[string]profiledomain.StorageResource,
	platform PlatformExecutionContract,
	backends map[string]datav1.Snapshot,
	diagnostics *[]Diagnostic,
) (ManifestResources, []CredentialUse, []string) {
	resources := ManifestResources{
		Models:    make(map[string]ManifestModelResource),
		Tools:     make(map[string]ManifestToolResource),
		Knowledge: make(map[string]ManifestKnowledgeResource),
		Storage:   make(map[string]ManifestStorageResource),
	}
	var uses []CredentialUse
	hosts := make(map[string]bool)

	for _, name := range sortedTrueKeys(used.models) {
		resource, selected := models[name]
		if !selected {
			continue
		}
		adapter, supported := platform.ModelAdapters[resource.Kind]
		if !supported {
			*diagnostics = append(*diagnostics, unsupportedAdapter("models", name, resource.Kind))
		}
		recordURLHost(resource.BaseURL, "/models/"+escapeJSONPointer(name)+"/base_url",
			"models", name, hosts, diagnostics)
		credential := credentialUse(
			resource.APIKeyCredentialID, CredentialPurposeAPIKey,
			audienceDigest(resource.Kind, resource.BaseURL),
		)
		uses = append(uses, credential)
		resources.Models[name] = ManifestModelResource{
			AdapterVersion: adapter.Version, Kind: resource.Kind, Model: resource.Model,
			BaseURL: resource.BaseURL, Credential: credential,
			Capabilities: append([]string{}, resource.Capabilities...),
		}
	}
	for _, name := range sortedTrueKeys(used.tools) {
		resource, selected := tools[name]
		if !selected {
			continue
		}
		adapter, supported := platform.ToolAdapters[resource.Kind]
		if !supported {
			*diagnostics = append(*diagnostics, unsupportedAdapter("tools", name, resource.Kind))
		}
		recordURLHost(resource.ServerURL, "/tools/"+escapeJSONPointer(name)+"/server_url",
			"tools", name, hosts, diagnostics)
		auth := ManifestToolAuth{Kind: resource.Auth.Kind}
		if resource.Auth.Kind == profiledomain.AuthKindBearer {
			credential := credentialUse(
				resource.Auth.CredentialID, CredentialPurposeBearerToken,
				audienceDigest(resource.Kind, resource.ServerURL, resource.Auth.Kind),
			)
			uses = append(uses, credential)
			auth.Credential = &credential
		}
		resources.Tools[name] = ManifestToolResource{
			AdapterVersion: adapter.Version, Kind: resource.Kind,
			ServerURL: resource.ServerURL, ToolsetName: resource.ToolsetName,
			ToolName: resource.ToolName, Auth: auth, Capability: resource.Capability,
		}
	}
	for _, name := range sortedTrueKeys(used.knowledge) {
		resource, selected := knowledge[name]
		if !selected {
			continue
		}
		adapter, supported := platform.KnowledgeAdapters[resource.Kind]
		if !supported {
			*diagnostics = append(*diagnostics, unsupportedAdapter("knowledge", name, resource.Kind))
		} else if !adapter.CreatesCallable {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				DiagnosticEntrypointUnsupported, SeverityError, DiagnosticSourcePlatform,
				"/knowledge/"+escapeJSONPointer(name), "knowledge", name,
				"knowledge adapter does not expose the frozen retrieval callable",
			))
		}
		if resource.Kind == profiledomain.KnowledgeKindManaged {
			snapshot := backends["knowledge/"+name].Clone()
			host, _ := snapshot.EndpointHost()
			hosts[host] = true
			recordURLHost(resource.Embedding.BaseURL, "/knowledge/"+escapeJSONPointer(name)+"/embedding/base_url", "knowledge", name, hosts, diagnostics)
			credential := credentialUse(resource.Embedding.APIKeyCredentialID, CredentialPurposeEmbeddingAPIKey, audienceDigest(resource.Kind, resource.Embedding.BaseURL))
			uses = append(uses, credential)
			var qdrantUse *CredentialUse
			if platform.Version == deploymentv1.WorkerV1PlatformVersion || resource.QdrantAPIKeyCredentialID != "" || resource.CredentialAudienceDigest != "" {
				digest, err := snapshot.Digest()
				u := credentialUse(resource.QdrantAPIKeyCredentialID, CredentialPurposeQdrantAPIKey, digest)
				if err != nil || resource.CredentialAudienceDigest != digest || !validCredentialUse(u, CredentialPurposeQdrantAPIKey) {
					*diagnostics = append(*diagnostics, resourceDiagnostic(DiagnosticCredentialUnavailable, SeverityError, DiagnosticSourceProfile, "/knowledge/"+escapeJSONPointer(name), "knowledge", name, "Qdrant credential missing or bound to different target"))
				} else {
					qdrantUse = &u
					uses = append(uses, u)
				}
			}
			resources.Knowledge[name] = ManifestKnowledgeResource{Credential: qdrantUse, Backend: &snapshot, AdapterVersion: adapter.Version, Kind: resource.Kind, Embedding: ManifestEmbeddingResource{Model: resource.Embedding.Model, BaseURL: resource.Embedding.BaseURL, Dimensions: resource.Embedding.Dimensions, Credential: credential}, Capability: profiledomain.CapabilityKnowledgeSearch}
			continue
		}
		recordHost(resource.Host, hosts)
		recordURLHost(resource.Embedding.BaseURL,
			"/knowledge/"+escapeJSONPointer(name)+"/embedding/base_url",
			"knowledge", name, hosts, diagnostics)
		var qdrantCredential *CredentialUse
		if resource.QdrantAPIKeyCredentialID != "" {
			credential := credentialUse(
				resource.QdrantAPIKeyCredentialID, CredentialPurposeQdrantAPIKey,
				audienceDigest(resource.Kind, resource.Host, resource.Port, resource.TLS),
			)
			uses = append(uses, credential)
			qdrantCredential = &credential
		}
		embeddingCredential := credentialUse(
			resource.Embedding.APIKeyCredentialID, CredentialPurposeEmbeddingAPIKey,
			audienceDigest(resource.Kind, resource.Embedding.BaseURL),
		)
		uses = append(uses, embeddingCredential)
		resources.Knowledge[name] = ManifestKnowledgeResource{
			AdapterVersion: adapter.Version, Kind: resource.Kind,
			Host: resource.Host, Port: resource.Port, TLS: resource.TLS,
			Collection: resource.Collection, Credential: qdrantCredential,
			Embedding: ManifestEmbeddingResource{
				Model: resource.Embedding.Model, BaseURL: resource.Embedding.BaseURL,
				Dimensions: resource.Embedding.Dimensions, Credential: embeddingCredential,
			},
			Capability: profiledomain.CapabilityKnowledgeSearch,
		}
	}
	for _, name := range sortedKeys(storage) {
		resource := storage[name]
		adapter, supported := platform.StorageAdapters[resource.Kind]
		if !supported {
			*diagnostics = append(*diagnostics, unsupportedAdapter("storage", name, resource.Kind))
		}
		if resource.Kind.Managed() {
			snapshot := backends["storage/"+name].Clone()
			host, _ := snapshot.EndpointHost()
			hosts[host] = true
			compiledResource := ManifestStorageResource{Backend: &snapshot, AdapterVersion: adapter.Version, Kind: resource.Kind}
			if managedPasswordBackend(resource.Kind, snapshot) && (resource.Kind == profiledomain.StorageKindManagedMemory || platform.Version == deploymentv1.WorkerV1PlatformVersion || resource.DSNCredentialID != "" || resource.CredentialAudienceDigest != "") {
				digest, err := snapshot.Digest()
				if err != nil || resource.CredentialAudienceDigest != digest || resource.DSNCredentialID == "" {
					*diagnostics = append(*diagnostics, resourceDiagnostic(DiagnosticCredentialUnavailable, SeverityError, DiagnosticSourceProfile, "/storage/"+escapeJSONPointer(name)+"/dsn_credential_id", "storage", name, "Storage credential is missing or bound to a different target"))
				} else {
					compiledResource.Credential = credentialUse(resource.DSNCredentialID, CredentialPurposeDSNPassword, digest)
					uses = append(uses, compiledResource.Credential)
				}
			} else if resource.Kind == profiledomain.StorageKindManagedArtifact && snapshot.Kind == datav1.S3 {
				if platform.Version == deploymentv1.WorkerV1PlatformVersion || resource.AccessKeyIDCredentialID != "" || resource.SecretAccessKeyCredentialID != "" || resource.CredentialAudienceDigest != "" {
					digest, err := snapshot.Digest()
					creds := &ArtifactCredentials{AccessKeyID: credentialUse(resource.AccessKeyIDCredentialID, "access_key_id", digest), SecretAccessKey: credentialUse(resource.SecretAccessKeyCredentialID, "secret_access_key", digest)}
					if err != nil || resource.CredentialAudienceDigest != digest || !validArtifactCredentials(creds, snapshot) {
						*diagnostics = append(*diagnostics, resourceDiagnostic(DiagnosticCredentialUnavailable, SeverityError, DiagnosticSourceProfile, "/storage/"+escapeJSONPointer(name), "storage", name, "Artifact credentials missing or bound to a different target"))
					} else {
						compiledResource.Credentials = creds
						uses = append(uses, creds.AccessKeyID, creds.SecretAccessKey)
					}
				}
			} else if resource.DSNCredentialID != "" || resource.CredentialAudienceDigest != "" || resource.AccessKeyIDCredentialID != "" || resource.SecretAccessKeyCredentialID != "" {
				*diagnostics = append(*diagnostics, resourceDiagnostic(DiagnosticCredentialUnavailable, SeverityError, DiagnosticSourceProfile, "/storage/"+escapeJSONPointer(name), "storage", name, "credential purpose is unsupported for this backend"))
			}
			if resource.Kind == profiledomain.StorageKindManagedArtifact {
				compiledResource.MetadataContract = ArtifactMetadataContract
			}
			resources.Storage[name] = compiledResource
			continue
		}
		recordHost(resource.Destination.Host, hosts)
		credential := credentialUse(
			resource.DSNCredentialID, CredentialPurposeDSN,
			audienceDigest(resource.Kind, resource.Destination),
		)
		uses = append(uses, credential)
		resources.Storage[name] = ManifestStorageResource{
			AdapterVersion: adapter.Version, Kind: resource.Kind,
			Destination: resource.Destination, Credential: credential,
		}
	}
	return resources, uses, sortedTrueKeys(hosts)
}

func unsupportedAdapter(category, name string, kind any) Diagnostic {
	return resourceDiagnostic(
		DiagnosticAdapterUnsupported, SeverityError, DiagnosticSourcePlatform,
		"/"+category+"/"+escapeJSONPointer(name)+"/kind", category, name,
		"platform has no executable adapter for resource kind "+stringValue(kind),
	)
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case profiledomain.ModelKind:
		return string(typed)
	case profiledomain.ToolKind:
		return string(typed)
	case profiledomain.KnowledgeKind:
		return string(typed)
	case profiledomain.StorageKind:
		return string(typed)
	default:
		return "unknown"
	}
}

func recordURLHost(
	value, path, category, name string,
	used map[string]bool,
	diagnostics *[]Diagnostic,
) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Hostname() == "" {
		*diagnostics = append(*diagnostics, resourceDiagnostic(
			DiagnosticExecutionRangeDenied, SeverityError, DiagnosticSourceProfile,
			path, category, name, "resource endpoint cannot be interpreted by the platform contract",
		))
		return
	}
	recordHost(parsed.Hostname(), used)
}

// recordHost notes one outbound host for the compiled manifest. Any host is
// accepted: the manifest declares the exact set a Deployment will contact, so
// the execution range stays auditable without gating on a platform allowlist.
func recordHost(host string, used map[string]bool) {
	used[strings.ToLower(host)] = true
}

func credentialUse(id, purpose, audience string) CredentialUse {
	return CredentialUse{CredentialID: id, Purpose: purpose, AudienceDigest: audience}
}

func audienceDigest(parts ...any) string {
	encoded, _ := json.Marshal(parts)
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func callableEntries(tools, knowledge []string) []string {
	entries := make([]string, 0, len(tools)+len(knowledge))
	for _, name := range tools {
		entries = append(entries, "tools/"+name)
	}
	for _, name := range knowledge {
		entries = append(entries, "knowledge/"+name)
	}
	return sortedUnique(entries)
}

func selectedNames[V any](requested []string, resolved map[string]V) []string {
	selected := make([]string, 0, len(requested))
	for _, name := range requested {
		if _, exists := resolved[name]; exists {
			selected = append(selected, name)
		}
	}
	return sortedUnique(selected)
}

func identityResolution[V any](used map[string]bool, resolved map[string]V) map[string]string {
	result := make(map[string]string)
	for _, name := range sortedTrueKeys(used) {
		if _, exists := resolved[name]; exists {
			result[name] = name
		}
	}
	return result
}

func containsAll(provided, required []string) bool {
	available := make(map[string]bool, len(provided))
	for _, capability := range provided {
		available[capability] = true
	}
	for _, capability := range required {
		if !available[capability] {
			return false
		}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func cloneGeneration(generation *agentdomain.Generation) *agentdomain.Generation {
	if generation == nil {
		return nil
	}
	clone := *generation
	if clone.Temperature != nil {
		value := *clone.Temperature
		clone.Temperature = &value
	}
	if clone.MaxOutputTokens != nil {
		value := *clone.MaxOutputTokens
		clone.MaxOutputTokens = &value
	}
	return &clone
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedTrueKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key, selected := range values {
		if selected {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func sortCredentialUses(uses []CredentialUse) {
	sort.SliceStable(uses, func(i, j int) bool {
		if uses[i].CredentialID != uses[j].CredentialID {
			return uses[i].CredentialID < uses[j].CredentialID
		}
		if uses[i].Purpose != uses[j].Purpose {
			return uses[i].Purpose < uses[j].Purpose
		}
		return uses[i].AudienceDigest < uses[j].AudienceDigest
	})
}

func errorDiagnostics(diagnostics []Diagnostic) []Diagnostic {
	errors := make([]Diagnostic, 0)
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == SeverityError {
			errors = append(errors, diagnostic)
		}
	}
	return errors
}

func escapeJSONPointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

// Only role-scoped runtime principals may receive Memory credentials.
func memoryRuntimePrincipal(s datav1.Snapshot) bool {
	return (s.Kind == datav1.PostgreSQL && s.PostgreSQL != nil && s.PostgreSQL.Username == "memory_runtime") ||
		(s.Kind == datav1.Redis && s.Redis != nil && s.Redis.Username == "memory_runtime")
}
