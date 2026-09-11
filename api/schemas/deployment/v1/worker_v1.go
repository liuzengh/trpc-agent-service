package deploymentv1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// WorkerV1PlatformVersion is a new immutable execution contract identity. Legacy
// platform-v1 manifests remain readable but are not executable by Worker V1.
const WorkerV1PlatformVersion = "worker-v1"

// WorkerV1PlanContract pins the supported ordered single-parent execution tree.
// It participates in the release digest; it is not a runtime-configurable policy.
const WorkerV1PlanContract = "llm-sequence-parallel-loop-tree-v1"

// WorkerV1SessionRuntimeRole is the fixed append-only runtime principal from
// Database V1 provisioning. Draft profiles and historical contracts stay generic.
const WorkerV1SessionRuntimeRole = "session_runtime"

var ErrUnsupportedWorkerManifest = errors.New("unsupported Worker V1 manifest")

// ErrWorkerV1ContractMismatch reports a manifest produced by a different
// platform contract release than the consumer pinned. The manifest is not
// malformed and the consumer is not missing a capability: a consumer running
// the manifest's release can execute it. Release-pinned consumers may therefore
// wait for that release instead of failing the work permanently.
var ErrWorkerV1ContractMismatch = errors.New("Worker V1 manifest requires a different platform contract release")

var ErrWorkerV1SessionRuntimeRole = fmt.Errorf("%w: session runtime username must be %s", ErrUnsupportedWorkerManifest, WorkerV1SessionRuntimeRole)

// ValidateWorkerV1 is the static publication/consumer gate, not a mutable
// capability service. expectedPlatformDigest is release-pinned; empty skips
// identity matching for the publisher while retaining every capability check.
func ValidateWorkerV1(c ManifestContent, expectedPlatformDigest string) error {
	reject := func(reason string) error { return fmt.Errorf("%w: %s", ErrUnsupportedWorkerManifest, reason) }
	contractMismatch := func(reason string) error { return fmt.Errorf("%w: %s", ErrWorkerV1ContractMismatch, reason) }
	if c.SchemaVersion != "v1" || c.CompilerVersion != "deployment-compiler-v1" || c.RuntimeContractVersion != "worker-manifest-v1" || c.PlatformContract.Version != WorkerV1PlatformVersion {
		return reject("contract identity")
	}
	if expectedPlatformDigest != "" && c.PlatformContract.Digest != expectedPlatformDigest {
		// Same manifest format, different release pin: the consumer is older (or
		// newer) than the producer, not missing a capability.
		return contractMismatch("platform contract digest")
	}
	// Summary and explicitly declared PostgreSQL/Redis Memory are executable.
	if c.Runtime != nil && (c.Runtime.Summary == nil || c.Runtime.Summary.Validate() != nil) {
		return reject("invalid session summary configuration")
	}
	if c.Execution.Backend != "worker-process-v1" || c.Execution.MaxRunSeconds <= 0 || c.Execution.MaxOutputTokens <= 0 {
		return reject("execution policy")
	}
	leaves, err := workerV1LLMNodes(c.AgentPlan)
	if err != nil {
		return err
	}
	// Validate node authority locally, then validate the union once. A resource
	// shared by two leaves is one global dependency, not an extra for either leaf.
	selectedExecutors := map[string]bool{}
	selectedModels, selectedTools, selectedKnowledge := map[string]bool{}, map[string]bool{}, map[string]bool{}
	toolModels := map[string]bool{}
	memorySelected, artifactSelected := false, false
	for _, node := range leaves {
		if node.Workspace != nil {
			w := node.Workspace
			if w.Validate() != nil || c.Execution.MaxToolCalls < 1 {
				return reject("invalid workspace selection")
			}
			selectedExecutors[w.ExecutorResource] = true
			for _, tool := range w.Tools {
				if tool == "workspace_save_artifact" && node.Artifact == nil {
					return reject("workspace save requires node artifact")
				}
			}
		}
		if node.AddSessionSummary != nil && (!*node.AddSessionSummary || c.Runtime == nil) {
			return reject("summary consumption requires enabled runtime summary")
		}
		selectedModels[node.ModelResource] = true
		if node.Generation != nil && node.Generation.MaxOutputTokens != nil && *node.Generation.MaxOutputTokens > c.Execution.MaxOutputTokens {
			return reject("generation exceeds fixed single-output policy")
		}
		if node.Memory != nil {
			if node.Memory.Validate() != nil || c.Execution.MaxToolCalls < 1 || c.Sources.Agent.AgentID == "" || c.StorageRoles["memory"] != node.Memory.Resource {
				return reject("invalid memory configuration or binding")
			}
			memorySelected = true
		}
		if node.Artifact != nil {
			if node.Artifact.Validate() != nil || c.Execution.MaxToolCalls < 1 || c.StorageRoles["artifact"] != node.Artifact.Resource {
				return reject("invalid artifact configuration or binding")
			}
			artifactSelected = true
		}
		// Each SDK leaf binds at most one Knowledge service. Distinct leaves may
		// bind distinct fixed namespaces; neither gets the other's authority.
		if len(node.KnowledgeResources) > 1 {
			return reject("single explicit knowledge resource per llm")
		}
		localTools, callables := map[string]bool{}, []string{}
		for _, name := range node.ToolResources {
			if localTools[name] {
				return reject("duplicate tool selection")
			}
			localTools[name], selectedTools[name] = true, true
			callables = append(callables, "tools/"+name)
		}
		for _, name := range node.KnowledgeResources {
			selectedKnowledge[name] = true
			callables = append(callables, "knowledge/"+name)
		}
		slices.Sort(callables)
		actualCallables := append([]string(nil), node.CallableEntries...)
		slices.Sort(actualCallables)
		if !slices.Equal(callables, actualCallables) {
			return reject("selected callable authority")
		}
		if node.Workspace != nil || (node.Memory != nil && len(node.Memory.Tools) > 0) || node.Artifact != nil || len(callables) > 0 {
			toolModels[node.ModelResource] = true
		}
	}
	if c.Runtime != nil {
		selectedModels[c.Runtime.Summary.ModelResource] = true
	}
	if len(c.Resources.Models) != len(selectedModels) || !workerV1Bindings(c.ResolvedRequirements.Models, selectedModels) {
		return reject("model closure")
	}
	if len(c.Resources.Tools) != len(selectedTools) || !workerV1Bindings(c.ResolvedRequirements.Tools, selectedTools) {
		return reject("selected MCP tool closure")
	}
	if len(c.Resources.Knowledge) != len(selectedKnowledge) || !workerV1Bindings(c.ResolvedRequirements.Knowledge, selectedKnowledge) {
		return reject("explicit knowledge resource closure")
	}
	if len(c.Resources.Executors) > 16 || len(c.Resources.Executors) != len(selectedExecutors) || !workerV1Bindings(c.ResolvedRequirements.Executors, selectedExecutors) {
		return reject("executor closure")
	}
	for name := range selectedExecutors {
		r, ok := c.Resources.Executors[name]
		if !ok || r.Validate() != nil {
			return reject("workspace executor adapter")
		}
	}
	sessionKey, ok := c.StorageRoles["session"]
	session, exists := c.Resources.Storage[sessionKey]
	storageCount := 1
	if artifactSelected {
		storageCount++
	}
	if memorySelected {
		storageCount++
	}
	if !ok || !exists || len(c.StorageRoles) != storageCount || len(c.Resources.Storage) != storageCount {
		return reject("storage must equal explicit data capability closure")
	}
	expectedHosts := []string{}
	switch session.Kind {
	case "postgres_state":
		if session.AdapterVersion != "postgres-state-v1" || session.Credential.Purpose != "dsn" {
			return reject("session adapter")
		}
		if session.Destination.Username != WorkerV1SessionRuntimeRole {
			return ErrWorkerV1SessionRuntimeRole
		}
		if session.Credential.CredentialID == "" || session.Credential.AudienceDigest != CredentialAudienceDigest(session.Kind, session.Destination) {
			return reject("credential audience does not match fixed destination")
		}
		expectedHosts = append(expectedHosts, strings.ToLower(session.Destination.Host))
	case "managed_session":
		b := session.Backend
		if sessionKey != "session" || session.AdapterVersion != "managed-session-v1" || b == nil || b.ValidateForRole("session") != nil || b.TenantID != c.TenantID || b.Kind != "redis" {
			return reject("fixed managed Redis session backend")
		}
		if b.Redis.Username != WorkerV1SessionRuntimeRole {
			return ErrWorkerV1SessionRuntimeRole
		}
		d, err := b.Digest()
		if err != nil || session.Credential.CredentialID == "" || session.Credential.Purpose != "dsn_password" || session.Credential.AudienceDigest != d {
			return reject("managed session credential audience")
		}
		expectedHosts = append(expectedHosts, strings.ToLower(b.Redis.Host))
	default:
		return reject("session adapter")
	}
	credentials := map[string]CredentialUse{session.Credential.CredentialID: session.Credential}
	if memorySelected {
		key, ok := c.StorageRoles["memory"]
		resource, exists := c.Resources.Storage[key]
		if !ok || !exists || key == sessionKey || resource.Kind != "managed_memory" || resource.AdapterVersion != "managed-memory-v1" || resource.Backend == nil {
			return reject("memory adapter binding")
		}
		backend := resource.Backend
		d, err := backend.Digest()
		if err != nil || backend.ValidateForRole("memory") != nil || backend.TenantID != c.TenantID {
			return reject("fixed memory backend")
		}
		if (backend.Kind == "postgresql" && backend.PostgreSQL.Username != "memory_runtime") || (backend.Kind == "redis" && backend.Redis.Username != "memory_runtime") {
			return reject("fixed memory runtime identity")
		}
		u := resource.Credential
		if u.CredentialID == "" || u.Purpose != "dsn_password" || u.AudienceDigest != d {
			return reject("memory credential audience")
		}
		if prior, ok := credentials[u.CredentialID]; ok && prior != u {
			return reject("credential closure")
		}
		credentials[u.CredentialID] = u
		host, err := backend.EndpointHost()
		if err != nil {
			return reject("memory endpoint")
		}
		expectedHosts = append(expectedHosts, strings.ToLower(host))
	}
	if artifactSelected {
		key, ok := c.StorageRoles["artifact"]
		resource, exists := c.Resources.Storage[key]
		if !ok || !exists || key != "artifact" || resource.Kind != "managed_artifact" || resource.AdapterVersion != "managed-artifact-v1" || resource.MetadataContract != ArtifactMetadataContract || resource.Backend == nil || resource.Credentials == nil || resource.Credential != (CredentialUse{}) {
			return reject("artifact adapter binding")
		}
		b := resource.Backend
		d, err := b.Digest()
		if err != nil || b.ValidateForRole("artifact") != nil || b.TenantID != c.TenantID {
			return reject("fixed artifact backend")
		}
		for purpose, u := range map[string]CredentialUse{"access_key_id": resource.Credentials.AccessKeyID, "secret_access_key": resource.Credentials.SecretAccessKey} {
			if u.CredentialID == "" || u.Purpose != purpose || u.AudienceDigest != d {
				return reject("artifact credential audience")
			}
			if prior, ok := credentials[u.CredentialID]; ok && prior != u {
				return reject("credential closure")
			}
			credentials[u.CredentialID] = u
		}
		host, err := b.EndpointHost()
		if err != nil {
			return reject("artifact endpoint")
		}
		expectedHosts = append(expectedHosts, strings.ToLower(host))
	}
	for key := range selectedKnowledge {
		r, exists := c.Resources.Knowledge[key]
		if !exists || r.Kind != "managed_knowledge" || r.AdapterVersion != "managed-knowledge-v1" || r.Capability != "knowledge.search" || r.Backend == nil || r.Credential == nil || c.Execution.MaxToolCalls < 1 {
			return reject("knowledge adapter")
		}
		b := r.Backend
		d, err := b.Digest()
		if err != nil || b.TenantID != c.TenantID || b.ValidateForRole("knowledge") != nil || r.Embedding.Dimensions != b.Qdrant.Dimensions || strings.TrimSpace(r.Embedding.Model) == "" {
			return reject("fixed knowledge backend")
		}
		u := *r.Credential
		if u.CredentialID == "" || u.Purpose != "qdrant_api_key" || u.AudienceDigest != d {
			return reject("knowledge credential audience")
		}
		if prior, ok := credentials[u.CredentialID]; ok && prior != u {
			return reject("credential closure")
		}
		credentials[u.CredentialID] = u
		e := r.Embedding
		ep, err := url.Parse(e.BaseURL)
		if err != nil || (ep.Scheme != "http" && ep.Scheme != "https") || ep.Hostname() == "" || ep.User != nil || ep.RawQuery != "" || ep.ForceQuery || strings.Contains(e.BaseURL, "#") || e.Credential.CredentialID == "" || e.Credential.Purpose != "embedding_api_key" || e.Credential.AudienceDigest != CredentialAudienceDigest(r.Kind, e.BaseURL) {
			return reject("fixed embedding resource")
		}
		if prior, ok := credentials[e.Credential.CredentialID]; ok && prior != e.Credential {
			return reject("credential closure")
		}
		credentials[e.Credential.CredentialID] = e.Credential
		host, err := b.EndpointHost()
		if err != nil {
			return reject("knowledge endpoint")
		}
		expectedHosts = append(expectedHosts, strings.ToLower(host), strings.ToLower(ep.Hostname()))
	}

	for name := range selectedTools {
		r, exists := c.Resources.Tools[name]
		if !exists || r.Kind != "mcp_streamable_http" || r.AdapterVersion != "mcp-web-search-v1" || !regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`).MatchString(r.ToolName) || !regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`).MatchString(r.ToolsetName) || !regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`).MatchString(r.Capability) || c.Execution.MaxToolCalls < 1 {
			return reject("selected MCP adapter")
		}
		endpoint, err := url.Parse(r.ServerURL)
		if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || strings.Contains(r.ServerURL, "#") {
			return reject("fixed MCP endpoint")
		}
		switch r.Auth.Kind {
		case "none":
			if r.Auth.Credential != nil {
				return reject("MCP no-auth credential")
			}
		case "bearer":
			u := r.Auth.Credential
			if u == nil || u.CredentialID == "" || u.Purpose != "bearer_token" || u.AudienceDigest != CredentialAudienceDigest(r.Kind, r.ServerURL, r.Auth.Kind) {
				return reject("MCP credential audience")
			}
			if previous, ok := credentials[u.CredentialID]; ok && previous != *u {
				return reject("credential closure")
			}
			credentials[u.CredentialID] = *u
		default:
			return reject("MCP authentication kind")
		}
		expectedHosts = append(expectedHosts, strings.ToLower(endpoint.Hostname()))
	}

	for key := range selectedModels {
		model, exists := c.Resources.Models[key]
		if !exists || model.Kind != "openai_compatible" || model.AdapterVersion != "openai-compatible-v1" || model.Credential.CredentialID == "" || model.Credential.Purpose != "api_key" || !slices.Contains(model.Capabilities, "chat") {
			return reject("model adapter")
		}
		if toolModels[key] && !slices.Contains(model.Capabilities, "tool_call") {
			return reject("data tools require model tool_call capability")
		}
		endpoint, err := url.Parse(model.BaseURL)
		if err != nil || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || strings.Contains(model.BaseURL, "#") || endpoint.Hostname() == "" {
			return reject("fixed endpoint closure")
		}
		if model.Credential.AudienceDigest != CredentialAudienceDigest(model.Kind, model.BaseURL) {
			return reject("credential audience does not match fixed destination")
		}
		if prior, ok := credentials[model.Credential.CredentialID]; ok && prior != model.Credential {
			return reject("credential closure")
		}
		credentials[model.Credential.CredentialID] = model.Credential
		expectedHosts = append(expectedHosts, strings.ToLower(endpoint.Hostname()))
	}
	slices.Sort(expectedHosts)
	expectedHosts = slices.Compact(expectedHosts)
	actualHosts := append([]string(nil), c.Execution.AllowedEndpointHosts...)
	slices.Sort(actualHosts)
	if !slices.Equal(actualHosts, expectedHosts) {
		return reject("endpoint set must equal selected resource closure")
	}
	return nil
}

// workerV1LLMNodes validates a bounded rooted tree without depending on Control
// internals. Traversal follows Children in source order and never mutates it.
func workerV1LLMNodes(plan AgentPlan) ([]ManifestNode, error) {
	reject := func(reason string) error { return fmt.Errorf("%w: %s", ErrUnsupportedWorkerManifest, reason) }
	if plan.Root == "" || len(plan.Nodes) == 0 || len(plan.Nodes) > 128 {
		return nil, reject("plan requires 1..128 nodes and a root")
	}
	seen := make(map[string]bool, len(plan.Nodes))
	leaves := []ManifestNode{}
	var visit func(string, int) error
	visit = func(id string, depth int) error {
		node, ok := plan.Nodes[id]
		if id == "" || !ok || depth > 16 || seen[id] {
			return reject("plan must be reachable, acyclic, single-parent and depth <=16")
		}
		seen[id] = true
		switch node.Kind {
		case "llm":
			if len(node.Children) > 0 || node.Body != "" || node.MaxIterations != 0 {
				return reject("llm cannot contain composition fields")
			}
			leaves = append(leaves, node)
		case "sequence", "parallel":
			if len(node.Children) < 1 || len(node.Children) > 64 {
				return reject(node.Kind + " requires 1..64 unique children")
			}
			if node.Instruction != "" || node.ModelResource != "" || node.ToolResources != nil || node.KnowledgeResources != nil || node.CallableEntries != nil || node.Workspace != nil || node.Generation != nil || node.Memory != nil || node.Artifact != nil || node.AddSessionSummary != nil || node.Body != "" || node.MaxIterations != 0 {
				return reject(node.Kind + " cannot contain llm or data options")
			}
			for _, child := range node.Children {
				if err := visit(child, depth+1); err != nil {
					return err
				}
			}
		case "loop":
			if node.Body == "" || node.MaxIterations < 1 || node.MaxIterations > 32 {
				return reject("loop requires one body and explicit max_iterations in 1..32")
			}
			if node.Children != nil || node.Instruction != "" || node.ModelResource != "" || node.ToolResources != nil || node.KnowledgeResources != nil || node.CallableEntries != nil || node.Workspace != nil || node.Generation != nil || node.Memory != nil || node.Artifact != nil || node.AddSessionSummary != nil {
				return reject("loop cannot contain children, llm or data options")
			}
			if err := visit(node.Body, depth+1); err != nil {
				return err
			}
		default:
			return reject("only llm, sequence, parallel and loop are supported")
		}
		return nil
	}
	if err := visit(plan.Root, 1); err != nil {
		return nil, err
	}
	if len(seen) != len(plan.Nodes) {
		return nil, reject("all plan nodes must be reachable from root")
	}
	// A parallel branch has no single terminal author. Follow only the overall
	// Final path after validating every branch above: the user must explicitly
	// place a successor LLM (possibly through nested sequences). Never choose a
	// parallel child by map/order/timing or inject a hidden summarizer.
	for id := plan.Root; ; {
		node := plan.Nodes[id]
		switch node.Kind {
		case "llm":
			return leaves, nil
		case "sequence":
			id = node.Children[len(node.Children)-1]
		case "loop":
			id = node.Body
		case "parallel":
			return nil, reject(fmt.Sprintf("terminal parallel node %q requires an explicit successor llm in sequence", id))
		}
	}
}

// Logical binding names can differ from resource names, but no missing, extra,
// or duplicate resource authority is allowed in the fixed resolved map.
func workerV1Bindings(bindings map[string]string, selected map[string]bool) bool {
	if len(bindings) != len(selected) {
		return false
	}
	seen := make(map[string]bool, len(bindings))
	for _, resource := range bindings {
		if !selected[resource] || seen[resource] {
			return false
		}
		seen[resource] = true
	}
	return true
}

// CredentialAudienceDigest preserves Profile V1's frozen JSON-array digest
// protocol (not JCS). Struct field order is intentional for StorageDestination.
func CredentialAudienceDigest(parts ...any) string {
	raw, _ := json.Marshal(parts)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
