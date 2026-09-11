package domain

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

// validateManifestSemantics checks relationships that JSON Schema and a valid
// content digest alone do not prove. It uses only the fixed manifest, never the
// current Profile, platform configuration, or credential state.
func validateManifestSemantics(content ManifestContent) error {
	if (content.PlatformContract.Version != PlatformContractVersionV1 && content.PlatformContract.Version != deploymentv1.WorkerV1PlatformVersion) ||
		content.Execution.Backend != "worker-process-v1" || content.AgentPlan.Nodes == nil ||
		content.Resources.Models == nil || content.Resources.Tools == nil ||
		content.Resources.Knowledge == nil || content.Resources.Storage == nil ||
		content.ResolvedRequirements.Models == nil || content.ResolvedRequirements.Tools == nil ||
		content.ResolvedRequirements.Knowledge == nil || content.StorageRoles == nil ||
		len(content.Execution.AllowedEndpointHosts) < 1 || len(content.Execution.AllowedEndpointHosts) > 128 {
		return ErrInvalidManifestContent
	}
	for _, id := range []string{
		content.TenantID, content.Sources.Agent.AgentID, content.Sources.Agent.VersionID,
		content.Sources.Profile.ProfileID, content.Sources.Profile.RevisionID,
	} {
		if utf8.RuneCountInString(id) < 1 || utf8.RuneCountInString(id) > 128 {
			return ErrInvalidManifestContent
		}
	}
	profile, err := manifestProfile(content)
	if err != nil {
		return err
	}
	profileJSON, err := json.Marshal(profile)
	if err != nil {
		return ErrInvalidManifestContent
	}
	if _, report := profiledomain.ValidateForPublication(profileJSON, 1); !report.Valid {
		return ErrInvalidManifestContent
	}

	agent := agentdomain.Spec{
		SchemaVersion: SchemaVersionV1, Root: content.AgentPlan.Root,
		Requirements: agentdomain.Requirements{
			Models:    make(map[string]agentdomain.ModelRequirement),
			Tools:     make(map[string]agentdomain.CapabilityRequirement),
			Knowledge: make(map[string]agentdomain.CapabilityRequirement),
		},
		Nodes: make(map[string]agentdomain.Node, len(content.AgentPlan.Nodes)),
	}
	if content.Runtime != nil {
		if content.Runtime.Summary == nil || content.Runtime.Summary.Validate() != nil {
			return ErrInvalidManifestContent
		}
		summary := content.Runtime.Summary
		threshold := summary.EventThreshold
		agent.Runtime = &agentdomain.Runtime{Summary: &agentdomain.Summary{Enabled: true, ModelSlot: summary.ModelResource, EventThreshold: &threshold}}
	}
	for name, resource := range profile.Models {
		agent.Requirements.Models[name] = agentdomain.ModelRequirement{Capabilities: resource.Capabilities}
	}
	for name, resource := range profile.Tools {
		agent.Requirements.Tools[name] = agentdomain.CapabilityRequirement{Capability: resource.Capability}
	}
	for name := range profile.Knowledge {
		agent.Requirements.Knowledge[name] = agentdomain.CapabilityRequirement{Capability: profiledomain.CapabilityKnowledgeSearch}
	}
	for id, node := range content.AgentPlan.Nodes {
		if node.Kind == agentdomain.NodeKindLLM {
			if node.ToolResources == nil || node.KnowledgeResources == nil || node.CallableEntries == nil ||
				len(node.CallableEntries) > 128 || !sameUniqueSet(node.CallableEntries, callableEntries(node.ToolResources, node.KnowledgeResources)) {
				return ErrInvalidManifestContent
			}
			if _, err := ProviderCallableNames(node.CallableEntries); err != nil {
				return ErrInvalidManifestContent
			}
			model, exists := profile.Models[node.ModelResource]
			if !exists || ((node.Workspace != nil || len(node.CallableEntries) > 0 || (node.Memory != nil && len(node.Memory.Tools) > 0) || node.Artifact != nil) && !containsString(model.Capabilities, profiledomain.CapabilityToolCall)) {
				return ErrInvalidManifestContent
			}
			if node.Generation != nil && node.Generation.MaxOutputTokens != nil &&
				*node.Generation.MaxOutputTokens > content.Execution.MaxOutputTokens {
				return ErrInvalidManifestContent
			}
		}
		if node.Kind != agentdomain.NodeKindLLM && (node.Memory != nil || node.Artifact != nil || node.AddSessionSummary != nil) {
			return ErrInvalidManifestContent
		}
		var memory *agentdomain.Memory
		var artifact *agentdomain.Artifact
		if node.Memory != nil {
			if node.Memory.Validate() != nil || node.Memory.Resource != "memory" || content.StorageRoles["memory"] != "memory" {
				return ErrInvalidManifestContent
			}
			if validateNodeCallableNames(node.CallableEntries, node.Memory.Tools, ProviderCallableNames) != nil {
				return ErrInvalidManifestContent
			}
			memory = &agentdomain.Memory{Tools: append([]string{}, node.Memory.Tools...), PreloadLimit: node.Memory.PreloadLimit}
		}
		if node.Artifact != nil {
			if node.Artifact.Validate() != nil || node.Artifact.Resource != "artifact" || content.StorageRoles["artifact"] != "artifact" {
				return ErrInvalidManifestContent
			}
			artifact = &agentdomain.Artifact{Enabled: true}
		}
		if node.AddSessionSummary != nil && !*node.AddSessionSummary {
			return ErrInvalidManifestContent
		}
		agent.Nodes[id] = agentdomain.Node{
			Memory: memory, Artifact: artifact, AddSessionSummary: node.AddSessionSummary,
			Kind: node.Kind, Name: node.Name, Instruction: node.Instruction,
			ModelSlot: node.ModelResource, ToolSlots: node.ToolResources,
			KnowledgeSlots: node.KnowledgeResources, Generation: node.Generation,
			Children: node.Children, Body: node.Body, MaxIterations: node.MaxIterations,
		}
	}
	if validateWorkspaceManifest(content, &agent) != nil {
		return ErrInvalidManifestContent
	}
	// Reuse the owner's closed node shape, key/field limits, references,
	// single-parent tree, reachability, depth, and generation validation.
	agentJSON, err := json.Marshal(agent)
	if err != nil {
		return ErrInvalidManifestContent
	}
	if _, report := agentdomain.ValidateForPublication(agentJSON, 1); !report.Valid {
		return ErrInvalidManifestContent
	}
	used := collectUsedRequirements(agent)
	if !sameResourceClosure(used.models, profile.Models, content.ResolvedRequirements.Models) ||
		!sameResourceClosure(used.tools, profile.Tools, content.ResolvedRequirements.Tools) ||
		!sameResourceClosure(used.knowledge, profile.Knowledge, content.ResolvedRequirements.Knowledge) {
		return ErrInvalidManifestContent
	}
	if content.StorageRoles[StorageRoleSession] != StorageRoleSession ||
		len(content.StorageRoles) != len(profile.Storage) {
		return ErrInvalidManifestContent
	}
	for role, name := range content.StorageRoles {
		if (role != StorageRoleSession && role != StorageRoleMemory && role != "artifact") || role != name {
			return ErrInvalidManifestContent
		}
		if _, exists := profile.Storage[name]; !exists {
			return ErrInvalidManifestContent
		}
	}
	for role, resource := range profile.Storage {
		if resource.Kind.Managed() && role != "session" && !agentUsesStorageRole(agent, role) {
			return ErrInvalidManifestContent
		}
	}
	backends := map[string]datav1.Snapshot{}
	for name, r := range content.Resources.Storage {
		if r.Backend != nil {
			backends["storage/"+name] = r.Backend.Clone()
		}
	}
	for name, r := range content.Resources.Knowledge {
		if r.Backend != nil {
			backends["knowledge/"+name] = r.Backend.Clone()
		}
	}
	if ds := ValidateManagedSnapshots(CompileInput{TenantID: content.TenantID, Agent: AgentVersionSource{Spec: agent}, Profile: ProfileRevisionSource{Spec: profile}, Platform: PlatformExecutionContract{Execution: content.Execution}, ManagedBackends: backends}); len(ds) > 0 {
		return ErrInvalidManifestContent
	}
	return nil
}

// manifestProfile reverses only the execution resource projection. Reusing the
// Profile owner's existing resource validation avoids a second, weaker endpoint
// and field-validation protocol. Adapter and credential audience checks remain
// Deployment-owned because they are compilation output, not Profile fields.
func manifestProfile(content ManifestContent) (profiledomain.Spec, error) {
	profile := profiledomain.Spec{
		SchemaVersion: SchemaVersionV1, CredentialProtocolVersion: profiledomain.CredentialProtocolVersionV1,
		Models: make(map[string]profiledomain.ModelResource), Tools: make(map[string]profiledomain.ToolResource),
		Knowledge: make(map[string]profiledomain.KnowledgeResource), Storage: make(map[string]profiledomain.StorageResource),
	}
	hosts := make(map[string]bool)
	addURLHost := func(endpoint string) {
		if parsed, err := url.Parse(endpoint); err == nil {
			hosts[strings.ToLower(parsed.Hostname())] = true
		}
	}
	for name, resource := range content.Resources.Models {
		if resource.Kind != profiledomain.ModelKindOpenAICompatible || resource.AdapterVersion != ModelAdapterOpenAICompatibleV1 ||
			resource.Credential.AudienceDigest != audienceDigest(resource.Kind, resource.BaseURL) {
			return profiledomain.Spec{}, ErrInvalidManifestContent
		}
		profile.Models[name] = profiledomain.ModelResource{
			Kind: resource.Kind, Model: resource.Model, BaseURL: resource.BaseURL,
			APIKeyCredentialID: resource.Credential.CredentialID, Capabilities: resource.Capabilities,
		}
		addURLHost(resource.BaseURL)
	}
	for name, resource := range content.Resources.Tools {
		if resource.Kind != profiledomain.ToolKindMCPStreamableHTTP || resource.AdapterVersion != ToolAdapterMCPWebSearchV1 {
			return profiledomain.Spec{}, ErrInvalidManifestContent
		}
		auth := profiledomain.ToolAuth{Kind: resource.Auth.Kind}
		if resource.Auth.Credential != nil {
			if resource.Auth.Credential.AudienceDigest != audienceDigest(resource.Kind, resource.ServerURL, resource.Auth.Kind) {
				return profiledomain.Spec{}, ErrInvalidManifestContent
			}
			auth.CredentialID = resource.Auth.Credential.CredentialID
		}
		profile.Tools[name] = profiledomain.ToolResource{
			Kind: resource.Kind, ServerURL: resource.ServerURL, ToolsetName: resource.ToolsetName,
			ToolName: resource.ToolName, Auth: auth, Capability: resource.Capability,
		}
		addURLHost(resource.ServerURL)
	}
	for name, resource := range content.Resources.Knowledge {
		if resource.Kind == profiledomain.KnowledgeKindManaged {
			r := BackendRequest{Category: "knowledge", Name: name, Role: "knowledge", Dimensions: resource.Embedding.Dimensions}
			if resource.Backend == nil {
				return profiledomain.Spec{}, ErrInvalidManifestContent
			}
			r.BackendID = resource.Backend.BackendID
			r.Revision = resource.Backend.BackendRevision
			if !validBackendMatch(content.TenantID, r, *resource.Backend) || resource.AdapterVersion != KnowledgeAdapterManagedV1 || resource.Host != "" || resource.Port != 0 || resource.TLS || resource.Collection != "" || resource.Capability != profiledomain.CapabilityKnowledgeSearch || resource.Embedding.Credential.AudienceDigest != audienceDigest(resource.Kind, resource.Embedding.BaseURL) {
				return profiledomain.Spec{}, ErrInvalidManifestContent
			}
			profile.Knowledge[name] = profiledomain.KnowledgeResource{Kind: resource.Kind, BackendID: r.BackendID, BackendRevision: r.Revision, Embedding: profiledomain.EmbeddingResource{Model: resource.Embedding.Model, BaseURL: resource.Embedding.BaseURL, Dimensions: resource.Embedding.Dimensions, APIKeyCredentialID: resource.Embedding.Credential.CredentialID}}
			if resource.Credential != nil {
				digest, _ := resource.Backend.Digest()
				if !validCredentialUse(*resource.Credential, CredentialPurposeQdrantAPIKey) || resource.Credential.AudienceDigest != digest {
					return profiledomain.Spec{}, ErrInvalidManifestContent
				}
				p := profile.Knowledge[name]
				p.QdrantAPIKeyCredentialID = resource.Credential.CredentialID
				p.CredentialAudienceDigest = digest
				profile.Knowledge[name] = p
			}

			host, _ := resource.Backend.EndpointHost()
			hosts[host] = true
			addURLHost(resource.Embedding.BaseURL)
			continue
		}

		if resource.Kind != profiledomain.KnowledgeKindQdrantOpenAI || resource.AdapterVersion != KnowledgeAdapterQdrantOpenAIV1 ||
			resource.Capability != profiledomain.CapabilityKnowledgeSearch ||
			resource.Embedding.Credential.AudienceDigest != audienceDigest(resource.Kind, resource.Embedding.BaseURL) {
			return profiledomain.Spec{}, ErrInvalidManifestContent
		}
		credentialID := ""
		if resource.Credential != nil {
			if resource.Credential.AudienceDigest != audienceDigest(resource.Kind, resource.Host, resource.Port, resource.TLS) {
				return profiledomain.Spec{}, ErrInvalidManifestContent
			}
			credentialID = resource.Credential.CredentialID
		}
		profile.Knowledge[name] = profiledomain.KnowledgeResource{
			Kind: resource.Kind, Host: resource.Host, Port: resource.Port, TLS: resource.TLS,
			Collection: resource.Collection, QdrantAPIKeyCredentialID: credentialID,
			Embedding: profiledomain.EmbeddingResource{
				Model: resource.Embedding.Model, BaseURL: resource.Embedding.BaseURL,
				APIKeyCredentialID: resource.Embedding.Credential.CredentialID, Dimensions: resource.Embedding.Dimensions,
			},
		}
		hosts[strings.ToLower(resource.Host)] = true
		addURLHost(resource.Embedding.BaseURL)
	}
	for name, resource := range content.Resources.Storage {
		if resource.Kind.Managed() {
			if resource.Backend == nil {
				return profiledomain.Spec{}, ErrInvalidManifestContent
			}
			r := BackendRequest{Category: "storage", Name: name, Role: resource.Kind.Role(), BackendID: resource.Backend.BackendID, Revision: resource.Backend.BackendRevision}
			adapter := StorageAdapterManagedSessionV1
			if resource.Kind == profiledomain.StorageKindManagedMemory {
				adapter = StorageAdapterManagedMemoryV1
			}
			if resource.Kind == profiledomain.StorageKindManagedArtifact {
				adapter = StorageAdapterManagedArtifactV1
			}
			if (resource.Kind == profiledomain.StorageKindManagedArtifact && resource.MetadataContract != ArtifactMetadataContract) || (resource.Kind != profiledomain.StorageKindManagedArtifact && resource.MetadataContract != "") {
				return profiledomain.Spec{}, ErrInvalidManifestContent
			}
			if name != r.Role || !validBackendMatch(content.TenantID, r, *resource.Backend) || resource.AdapterVersion != adapter || resource.Destination != (profiledomain.StorageDestination{}) {
				return profiledomain.Spec{}, ErrInvalidManifestContent
			}
			profileResource := profiledomain.StorageResource{Kind: resource.Kind, BackendID: r.BackendID, BackendRevision: r.Revision}
			if managedPasswordBackend(resource.Kind, *resource.Backend) && ((resource.Kind == profiledomain.StorageKindManagedMemory && resource.Backend.Kind == datav1.PostgreSQL) || resource.Credential != (CredentialUse{})) {
				digest, err := resource.Backend.Digest()
				if err != nil || !validCredentialUse(resource.Credential, CredentialPurposeDSNPassword) || resource.Credential.AudienceDigest != digest {
					return profiledomain.Spec{}, ErrInvalidManifestContent
				}
				profileResource.DSNCredentialID = resource.Credential.CredentialID
				profileResource.CredentialAudienceDigest = digest
			} else if resource.Credential != (CredentialUse{}) {
				return profiledomain.Spec{}, ErrInvalidManifestContent
			}
			if resource.Credentials != nil {
				if resource.Kind != profiledomain.StorageKindManagedArtifact || !validArtifactCredentials(resource.Credentials, *resource.Backend) {
					return profiledomain.Spec{}, ErrInvalidManifestContent
				}
				profileResource.AccessKeyIDCredentialID = resource.Credentials.AccessKeyID.CredentialID
				profileResource.SecretAccessKeyCredentialID = resource.Credentials.SecretAccessKey.CredentialID
				profileResource.CredentialAudienceDigest = resource.Credentials.AccessKeyID.AudienceDigest
			}
			profile.Storage[name] = profileResource
			host, _ := resource.Backend.EndpointHost()
			hosts[host] = true
			continue
		}

		if resource.Kind != profiledomain.StorageKindPostgresState || resource.AdapterVersion != StorageAdapterPostgresStateV1 ||
			resource.Credential.AudienceDigest != audienceDigest(resource.Kind, resource.Destination) {
			return profiledomain.Spec{}, ErrInvalidManifestContent
		}
		profile.Storage[name] = profiledomain.StorageResource{
			Kind: resource.Kind, Destination: resource.Destination, DSNCredentialID: resource.Credential.CredentialID,
		}
		hosts[strings.ToLower(resource.Destination.Host)] = true
	}
	if !sameUniqueSet(content.Execution.AllowedEndpointHosts, sortedTrueKeys(hosts)) {
		return profiledomain.Spec{}, ErrInvalidManifestContent
	}
	return profile, nil
}

func sameResourceClosure[V any](used map[string]bool, resources map[string]V, resolved map[string]string) bool {
	if len(used) != len(resources) || len(used) != len(resolved) {
		return false
	}
	for name := range used {
		if _, exists := resources[name]; !exists || resolved[name] != name {
			return false
		}
	}
	return true
}

func sameUniqueSet(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	ordered := append([]string(nil), actual...)
	slices.Sort(ordered)
	for index, value := range ordered {
		if value != expected[index] || (index > 0 && value == ordered[index-1]) {
			return false
		}
	}
	return true
}
