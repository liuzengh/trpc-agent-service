package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/gowebpki/jcs"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const (
	PlatformContractVersionV1 = "platform-v1"

	ModelAdapterOpenAICompatibleV1 = "openai-compatible-v1"
	ToolAdapterMCPWebSearchV1      = "mcp-web-search-v1"
	KnowledgeAdapterQdrantOpenAIV1 = "qdrant-openai-v1"
	StorageAdapterPostgresStateV1  = "postgres-state-v1"
)

var ErrInvalidPlatformExecutionContract = errors.New("invalid platform execution contract")

type AdapterContract struct {
	Version string `json:"version"`
}

type KnowledgeAdapterContract struct {
	Version         string `json:"version"`
	CreatesCallable bool   `json:"creates_callable"`
}

type CompileLimits struct {
	MaxCallableEntriesPerNode int `json:"max_callable_entries_per_node"`
	MaxManifestBytes          int `json:"max_manifest_bytes"`
}

type ExecutionPolicy struct {
	Backend              string   `json:"backend"`
	AllowedEndpointHosts []string `json:"allowed_endpoint_hosts"`
	MaxRunSeconds        int64    `json:"max_run_seconds"`
	MaxToolCalls         int64    `json:"max_tool_calls"`
	MaxOutputTokens      int64    `json:"max_output_tokens"`
}

// PlatformExecutionContract is a process configuration snapshot. It is not a
// user-selectable environment and is never mutated during one compilation.
type PlatformExecutionContract struct {
	RuntimeDataCapabilities []string `json:"runtime_data_capabilities,omitempty"`
	ManagedCatalogDigest    string   `json:"managed_catalog_digest,omitempty"`
	Version                 string   `json:"version"`
	Digest                  string   `json:"digest"`
	CompilerVersion         string   `json:"compiler_version"`
	ManifestSchemaVersion   string   `json:"manifest_schema_version"`
	RuntimeContractVersion  string   `json:"runtime_contract_version"`

	ModelAdapters     map[profiledomain.ModelKind]AdapterContract              `json:"model_adapters"`
	ToolAdapters      map[profiledomain.ToolKind]AdapterContract               `json:"tool_adapters"`
	KnowledgeAdapters map[profiledomain.KnowledgeKind]KnowledgeAdapterContract `json:"knowledge_adapters"`
	StorageAdapters   map[profiledomain.StorageKind]AdapterContract            `json:"storage_adapters"`

	Execution ExecutionPolicy `json:"execution"`
	Limits    CompileLimits   `json:"limits"`
}

// Clone detaches collection fields so a caller cannot mutate the process
// snapshot after module assembly.
func (c PlatformExecutionContract) Clone() PlatformExecutionContract {
	c.RuntimeDataCapabilities = append([]string(nil), c.RuntimeDataCapabilities...)
	c.ModelAdapters = cloneMap(c.ModelAdapters)
	c.ToolAdapters = cloneMap(c.ToolAdapters)
	c.KnowledgeAdapters = cloneMap(c.KnowledgeAdapters)
	c.StorageAdapters = cloneMap(c.StorageAdapters)
	c.Execution.AllowedEndpointHosts = append([]string(nil), c.Execution.AllowedEndpointHosts...)
	return c
}

// DefaultPlatformExecutionContract returns the closed V1 adapter matrix and
// conservative execution limits used by local Control API assembly. Production
// assembly may supply another validated contract with an explicit host set.
func DefaultPlatformExecutionContract() PlatformExecutionContract {
	contract := PlatformExecutionContract{
		Version:                PlatformContractVersionV1,
		CompilerVersion:        CompilerVersionV1,
		ManifestSchemaVersion:  SchemaVersionV1,
		RuntimeContractVersion: RuntimeContractVersionV1,
		ModelAdapters: map[profiledomain.ModelKind]AdapterContract{
			profiledomain.ModelKindOpenAICompatible: {Version: ModelAdapterOpenAICompatibleV1},
		},
		ToolAdapters: map[profiledomain.ToolKind]AdapterContract{
			profiledomain.ToolKindMCPStreamableHTTP: {Version: ToolAdapterMCPWebSearchV1},
		},
		KnowledgeAdapters: map[profiledomain.KnowledgeKind]KnowledgeAdapterContract{
			profiledomain.KnowledgeKindQdrantOpenAI: {
				Version: KnowledgeAdapterQdrantOpenAIV1, CreatesCallable: true,
			},
		},
		StorageAdapters: map[profiledomain.StorageKind]AdapterContract{
			profiledomain.StorageKindPostgresState: {Version: StorageAdapterPostgresStateV1},
		},
		Execution: ExecutionPolicy{
			Backend:         "worker-process-v1",
			MaxRunSeconds:   120,
			MaxToolCalls:    16,
			MaxOutputTokens: 4096,
		},
		Limits: CompileLimits{
			MaxCallableEntriesPerNode: agentdomain.MaxToolSlots + agentdomain.MaxKnowledgeSlots,
			MaxManifestBytes:          512 * 1024,
		},
	}
	digest, err := contract.CalculateDigest()
	if err != nil {
		panic(fmt.Sprintf("build default deployment platform contract: %v", err))
	}
	contract.Digest = digest
	return contract
}

// CalculateDigest computes the contract identity without including Digest
// itself. Set-shaped host lists and adapter maps are normalized first.
func (c PlatformExecutionContract) CalculateDigest() (string, error) {
	payload := struct {
		RuntimeDataCapabilities  []string                                                 `json:"runtime_data_capabilities,omitempty"`
		ManagedCatalogDigest     string                                                   `json:"managed_catalog_digest,omitempty"`
		Version                  string                                                   `json:"version"`
		CompilerVersion          string                                                   `json:"compiler_version"`
		ManifestSchemaVersion    string                                                   `json:"manifest_schema_version"`
		RuntimeContractVersion   string                                                   `json:"runtime_contract_version"`
		ModelAdapters            map[profiledomain.ModelKind]AdapterContract              `json:"model_adapters"`
		ToolAdapters             map[profiledomain.ToolKind]AdapterContract               `json:"tool_adapters"`
		KnowledgeAdapters        map[profiledomain.KnowledgeKind]KnowledgeAdapterContract `json:"knowledge_adapters"`
		StorageAdapters          map[profiledomain.StorageKind]AdapterContract            `json:"storage_adapters"`
		Execution                ExecutionPolicy                                          `json:"execution"`
		Limits                   CompileLimits                                            `json:"limits"`
		WorkerPlanContract       string                                                   `json:"worker_plan_contract,omitempty"`
		WorkerSessionRuntimeRole string                                                   `json:"worker_session_runtime_role,omitempty"`
	}{
		RuntimeDataCapabilities: sortedUnique(c.RuntimeDataCapabilities),
		ManagedCatalogDigest:    c.ManagedCatalogDigest,
		Version:                 c.Version, CompilerVersion: c.CompilerVersion,
		ManifestSchemaVersion:  c.ManifestSchemaVersion,
		RuntimeContractVersion: c.RuntimeContractVersion,
		ModelAdapters:          cloneMap(c.ModelAdapters), ToolAdapters: cloneMap(c.ToolAdapters),
		KnowledgeAdapters: cloneMap(c.KnowledgeAdapters), StorageAdapters: cloneMap(c.StorageAdapters),
		Execution: c.Execution, Limits: c.Limits,
	}
	// This is a frozen Worker V1 rule, not a configurable role framework. Omit it
	// entirely for platform-v1 so historical contract digests remain unchanged.
	if c.Version == deploymentv1.WorkerV1PlatformVersion {
		payload.WorkerSessionRuntimeRole = deploymentv1.WorkerV1SessionRuntimeRole
		payload.WorkerPlanContract = deploymentv1.WorkerV1PlanContract
	}
	payload.Execution.AllowedEndpointHosts = sortedUnique(c.Execution.AllowedEndpointHosts)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode platform execution contract: %w", err)
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		return "", fmt.Errorf("canonicalize platform execution contract: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (c PlatformExecutionContract) Validate() error {
	for _, capability := range c.RuntimeDataCapabilities {
		if capability != "memory" && capability != "artifact" && capability != "summary" && capability != "workspace" {
			return ErrInvalidPlatformExecutionContract
		}
	}
	if c.ManagedCatalogDigest != "" && !validDigest(c.ManagedCatalogDigest) {
		return ErrInvalidPlatformExecutionContract
	}
	if (c.Version != PlatformContractVersionV1 && c.Version != deploymentv1.WorkerV1PlatformVersion) || c.CompilerVersion != CompilerVersionV1 ||
		c.ManifestSchemaVersion != SchemaVersionV1 ||
		c.RuntimeContractVersion != RuntimeContractVersionV1 ||
		c.Execution.Backend != "worker-process-v1" || c.Execution.MaxRunSeconds <= 0 ||
		c.Execution.MaxToolCalls <= 0 || c.Execution.MaxOutputTokens <= 0 ||
		c.Limits.MaxCallableEntriesPerNode <= 0 || c.Limits.MaxManifestBytes <= 0 {
		return ErrInvalidPlatformExecutionContract
	}
	// AllowedEndpointHosts is the manifest's record of the hosts a Deployment
	// will contact, not a platform approval list. An empty value is valid here;
	// manifest validation independently requires a compiled manifest to declare
	// at least one host.
	seenHosts := make(map[string]struct{}, len(c.Execution.AllowedEndpointHosts))
	for _, host := range c.Execution.AllowedEndpointHosts {
		if !validContractHost(host) {
			return fmt.Errorf("%w: invalid allowed endpoint host", ErrInvalidPlatformExecutionContract)
		}
		if _, exists := seenHosts[host]; exists {
			return fmt.Errorf("%w: duplicate allowed endpoint host", ErrInvalidPlatformExecutionContract)
		}
		seenHosts[host] = struct{}{}
	}
	if err := validateAdapterMap(c.ModelAdapters, "model", func(kind profiledomain.ModelKind) (string, bool) {
		return ModelAdapterOpenAICompatibleV1, kind == profiledomain.ModelKindOpenAICompatible
	}); err != nil {
		return err
	}
	if err := validateAdapterMap(c.ToolAdapters, "tool", func(kind profiledomain.ToolKind) (string, bool) {
		return ToolAdapterMCPWebSearchV1, kind == profiledomain.ToolKindMCPStreamableHTTP
	}); err != nil {
		return err
	}
	for _, kind := range sortedAdapterKinds(c.KnowledgeAdapters) {
		adapter := c.KnowledgeAdapters[kind]
		if kind != profiledomain.KnowledgeKindQdrantOpenAI && kind != profiledomain.KnowledgeKindManaged {
			return invalidPlatformExecutionContract(
				"unsupported knowledge adapter kind %q", kind,
			)
		}
		expected := KnowledgeAdapterQdrantOpenAIV1
		if kind == profiledomain.KnowledgeKindManaged {
			expected = KnowledgeAdapterManagedV1
		}
		if adapter.Version != expected {
			return invalidPlatformExecutionContract(
				"knowledge adapter %q version must be %q", kind, expected,
			)
		}
		if !adapter.CreatesCallable {
			return invalidPlatformExecutionContract(
				"knowledge adapter %q must create the frozen callable", kind,
			)
		}
	}
	if err := validateAdapterMap(c.StorageAdapters, "storage", func(kind profiledomain.StorageKind) (string, bool) {
		if kind == profiledomain.StorageKindManagedSession {
			return StorageAdapterManagedSessionV1, true
		}
		if kind == profiledomain.StorageKindManagedArtifact {
			return StorageAdapterManagedArtifactV1, true
		}
		if kind == profiledomain.StorageKindManagedMemory {
			return StorageAdapterManagedMemoryV1, true
		}
		return StorageAdapterPostgresStateV1, kind == profiledomain.StorageKindPostgresState
	}); err != nil {
		return err
	}
	expected, err := c.CalculateDigest()
	if err != nil {
		return errors.Join(ErrInvalidPlatformExecutionContract, err)
	}
	if c.Digest != expected {
		return fmt.Errorf("%w: digest mismatch", ErrInvalidPlatformExecutionContract)
	}
	return nil
}

func validateAdapterMap[K ~string](
	adapters map[K]AdapterContract,
	category string,
	frozenVersion func(K) (string, bool),
) error {
	for _, kind := range sortedAdapterKinds(adapters) {
		expectedVersion, supported := frozenVersion(kind)
		if !supported {
			return invalidPlatformExecutionContract(
				"unsupported %s adapter kind %q", category, kind,
			)
		}
		if adapters[kind].Version != expectedVersion {
			return invalidPlatformExecutionContract(
				"%s adapter %q version must be %q", category, kind, expectedVersion,
			)
		}
	}
	return nil
}

func sortedAdapterKinds[K ~string, V any](adapters map[K]V) []K {
	kinds := make([]K, 0, len(adapters))
	for kind := range adapters {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
}

func invalidPlatformExecutionContract(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPlatformExecutionContract, fmt.Sprintf(format, args...))
}

func validContractHost(host string) bool {
	if host == "" || len(host) > 253 || host != strings.TrimSpace(host) || host != strings.ToLower(host) ||
		strings.ContainsAny(host, "/?#@") {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	if strings.Contains(host, ":") || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func sortedUnique(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	result := append([]string(nil), values...)
	sort.Strings(result)
	write := 0
	for _, value := range result {
		if write == 0 || result[write-1] != value {
			result[write] = value
			write++
		}
	}
	return result[:write]
}

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	result := make(map[K]V, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

// WorkerV1PlatformExecutionContract preserves the historical platform-v1
// constructor for immutable reads/tests while selecting the actual V1 runtime
// matrix for new publications. Changing Version participates in the digest.
func WorkerV1PlatformExecutionContract() PlatformExecutionContract {
	c := DefaultPlatformExecutionContract()
	c.Version = deploymentv1.WorkerV1PlatformVersion
	c.RuntimeDataCapabilities = []string{"summary", "memory", "artifact", "workspace"}
	c.StorageAdapters[profiledomain.StorageKindManagedArtifact] = AdapterContract{Version: StorageAdapterManagedArtifactV1}
	c.StorageAdapters[profiledomain.StorageKindManagedSession] = AdapterContract{Version: StorageAdapterManagedSessionV1}
	c.StorageAdapters[profiledomain.StorageKindManagedMemory] = AdapterContract{Version: StorageAdapterManagedMemoryV1}
	// The historical adapter ID describes the MCP transport, not a web.search restriction.
	c.ToolAdapters = map[profiledomain.ToolKind]AdapterContract{profiledomain.ToolKindMCPStreamableHTTP: {Version: ToolAdapterMCPWebSearchV1}}
	c.KnowledgeAdapters = map[profiledomain.KnowledgeKind]KnowledgeAdapterContract{profiledomain.KnowledgeKindManaged: {Version: KnowledgeAdapterManagedV1, CreatesCallable: true}}
	digest, err := c.CalculateDigest()
	if err != nil {
		panic(err)
	}
	c.Digest = digest
	return c
}
