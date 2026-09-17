package agentapp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"time"
)

var agentNodeKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

const FailurePolicyFailFast = "fail_fast"

type RevisionState string
type AgentKind string

const (
	RevisionDraft     RevisionState = "draft"
	RevisionPublished RevisionState = "published"

	AgentKindLLM      AgentKind = "llm"
	AgentKindGraph    AgentKind = "graph"
	AgentKindChain    AgentKind = "chain"
	AgentKindParallel AgentKind = "parallel"
	AgentKindCycle    AgentKind = "cycle"
)

type VersionedRef struct {
	ID            string `json:"id"`
	Version       int64  `json:"version"`
	ContentDigest string `json:"content_digest,omitempty"`
	Required      bool   `json:"required,omitempty"`
}

type SkillRef struct {
	ID            string `json:"id"`
	Version       int64  `json:"version"`
	ContentDigest string `json:"content_digest"`
}

// PluginRef selects a reviewed, Revision-scoped trpc-agent-go framework
// extension. Most entries materialize as runner plugins; a small number are
// code-owned Agent options. It is never a Go package path or tenant-supplied
// constructor option.
type PluginRef struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

const (
	PluginToolCallID    = "tool_call_id"
	PluginMessageMerger = "message_merger"
	// PluginToolSearch enables trpc-agent-go's deferred tool discovery. Its
	// configuration is code-owned: revisions only select this stable capability
	// identifier and cannot supply arbitrary search callbacks or options.
	PluginToolSearch = "tool_search"
	// PluginAwaitUserReply exposes the framework's await_user_reply tool to one
	// root LLM Revision. The corresponding Runner route consumer is service
	// owned and always uses the fenced Session contract.
	PluginAwaitUserReply = "await_user_reply"
)

type PublishedAgentRef struct {
	AgentAppID    string `json:"agent_app_id"`
	Revision      int64  `json:"revision"`
	ContentDigest string `json:"content_digest"`
}

type AgentNodeSpecV1 struct {
	Key           string            `json:"key"`
	AgentRef      PublishedAgentRef `json:"agent_ref"`
	FailurePolicy string            `json:"failure_policy"`
}

type AgentEdgeSpecV1 struct {
	From         string        `json:"from"`
	To           string        `json:"to"`
	ConditionRef *VersionedRef `json:"condition_ref,omitempty"`
	// Branch is the code-owned condition result token mapped to To. It is
	// required exactly when ConditionRef is set; allowing the condition to
	// return a node name directly would make the graph topology mutable at run
	// time and bypass the published edge list.
	Branch string `json:"branch,omitempty"`
}

type CheckpointPolicyV1 struct {
	Required  bool   `json:"required"`
	Namespace string `json:"namespace,omitempty"`
}

type AgentSpecV1 struct {
	Nodes          []AgentNodeSpecV1  `json:"nodes,omitempty"`
	Edges          []AgentEdgeSpecV1  `json:"edges,omitempty"`
	EntryNode      string             `json:"entry_node,omitempty"`
	MaxConcurrency int                `json:"max_concurrency,omitempty"`
	MaxIterations  int                `json:"max_iterations,omitempty"`
	Checkpoint     CheckpointPolicyV1 `json:"checkpoint"`
}

// ExecutionBudgetV1 is the bounded, per-execution portion of a revision's
// runtime policy. A zero value means that particular limit is not set. These
// fields deliberately remain separate from tenant billing budgets: they are
// local circuit breakers, not a reservation or settlement ledger.
type ExecutionBudgetV1 struct {
	MaxLLMCalls             int `json:"max_llm_calls,omitempty"`
	MaxToolCalls            int `json:"max_tool_calls,omitempty"`
	MaxParallelTools        int `json:"max_parallel_tools,omitempty"`
	ExecutionTimeoutSeconds int `json:"execution_timeout_seconds,omitempty"`
}

func (b ExecutionBudgetV1) Validate() error {
	if b.MaxLLMCalls < 0 || b.MaxToolCalls < 0 || b.MaxParallelTools < 0 || b.ExecutionTimeoutSeconds < 0 ||
		b.MaxLLMCalls > 10000 || b.MaxToolCalls > 10000 || b.MaxParallelTools > 1000 || b.ExecutionTimeoutSeconds > 86400 {
		return fmt.Errorf("%w: execution budget", ErrInvalid)
	}
	return nil
}

type Revision struct {
	TenantID            string
	AgentAppID          string
	Revision            int64
	State               RevisionState
	DraftVersion        int64
	AgentKind           AgentKind
	SchemaVersion       int
	AgentSpec           AgentSpecV1
	Description         string
	Instruction         string
	GlobalInstruction   string
	ModelProfileID      string
	ModelProfileVersion int64
	FallbackModelRefs   []VersionedRef
	ToolRefs            []VersionedRef
	SkillRefs           []SkillRef
	PluginRefs          []PluginRef
	KnowledgeRefs       []VersionedRef
	GenerationConfig    map[string]any
	RuntimePolicy       map[string]any
	ExecutionBudget     ExecutionBudgetV1
	ContentDigest       string
	PublishedAt         *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (r Revision) ValidateDraft() error {
	if r.TenantID == "" || r.AgentAppID == "" || r.Revision < 1 ||
		r.DraftVersion < 1 || r.SchemaVersion != 1 {
		return fmt.Errorf("%w: incomplete or unsupported revision", ErrInvalid)
	}
	if !isSupportedAgentKind(r.AgentKind) {
		return fmt.Errorf("%w: unsupported agent kind %q", ErrInvalid, r.AgentKind)
	}
	if err := r.ExecutionBudget.Validate(); err != nil {
		return err
	}
	if r.AgentKind == AgentKindLLM {
		if r.Instruction == "" || r.ModelProfileID == "" || r.ModelProfileVersion < 1 || !r.AgentSpec.empty() {
			return fmt.Errorf("%w: invalid llm revision", ErrInvalid)
		}
		seenFallbacks := map[string]struct{}{fmt.Sprintf("%s\x00%d", r.ModelProfileID, r.ModelProfileVersion): {}}
		for _, ref := range r.FallbackModelRefs {
			key := fmt.Sprintf("%s\x00%d", ref.ID, ref.Version)
			if ref.ID == "" || ref.Version < 1 || ref.Required {
				return fmt.Errorf("%w: invalid fallback model reference", ErrInvalid)
			}
			if _, exists := seenFallbacks[key]; exists {
				return fmt.Errorf("%w: duplicate fallback model reference %q", ErrInvalid, ref.ID)
			}
			seenFallbacks[key] = struct{}{}
		}
	} else {
		if r.ModelProfileID != "" || r.ModelProfileVersion != 0 || len(r.FallbackModelRefs) != 0 {
			return fmt.Errorf("%w: composite agent cannot own a model profile", ErrInvalid)
		}
		if err := r.AgentSpec.validate(r.AgentKind); err != nil {
			return err
		}
	}
	for kind, refs := range map[string][]VersionedRef{"tool": r.ToolRefs, "knowledge": r.KnowledgeRefs} {
		seen := make(map[string]struct{}, len(refs))
		for _, ref := range refs {
			if ref.ID == "" || ref.Version < 1 || (ref.ContentDigest != "" && !isDigest(ref.ContentDigest)) {
				return fmt.Errorf("%w: invalid %s reference", ErrInvalid, kind)
			}
			if _, exists := seen[ref.ID]; exists {
				return fmt.Errorf("%w: duplicate %s reference %q", ErrInvalid, kind, ref.ID)
			}
			seen[ref.ID] = struct{}{}
		}
	}
	seenSkills := make(map[string]struct{}, len(r.SkillRefs))
	for _, ref := range r.SkillRefs {
		if ref.ID == "" || ref.Version < 1 || !isDigest(ref.ContentDigest) {
			return fmt.Errorf("%w: invalid skill reference", ErrInvalid)
		}
		if _, exists := seenSkills[ref.ID]; exists {
			return fmt.Errorf("%w: duplicate skill reference %q", ErrInvalid, ref.ID)
		}
		seenSkills[ref.ID] = struct{}{}
	}
	seenPlugins := make(map[string]struct{}, len(r.PluginRefs))
	for _, ref := range r.PluginRefs {
		// The allow-list lives beside the immutable revision schema. Runtime
		// construction repeats this check before instantiating framework code.
		if (ref.ID != PluginToolCallID && ref.ID != PluginMessageMerger && ref.ID != PluginToolSearch && ref.ID != PluginAwaitUserReply) || ref.Version != 1 {
			return fmt.Errorf("%w: unsupported plugin %q", ErrInvalid, ref.ID)
		}
		// tool_search owns one LLM tool surface. A composite root has multiple
		// child surfaces, so sharing one deferred catalog would either omit a
		// child or expose tools to the wrong child. Keep this capability on a
		// single root LLM until a namespaced multi-agent contract is introduced.
		if (ref.ID == PluginToolSearch || ref.ID == PluginAwaitUserReply) && r.AgentKind != AgentKindLLM {
			return fmt.Errorf("%w: %s requires an llm revision", ErrInvalid, ref.ID)
		}
		if _, exists := seenPlugins[ref.ID]; exists {
			return fmt.Errorf("%w: duplicate plugin reference %q", ErrInvalid, ref.ID)
		}
		seenPlugins[ref.ID] = struct{}{}
	}
	return nil
}

func isSupportedAgentKind(kind AgentKind) bool {
	switch kind {
	case AgentKindLLM, AgentKindGraph, AgentKindChain, AgentKindParallel, AgentKindCycle:
		return true
	default:
		return false
	}
}

func isDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (s AgentSpecV1) empty() bool {
	return len(s.Nodes) == 0 && len(s.Edges) == 0 && s.EntryNode == "" &&
		s.MaxConcurrency == 0 && s.MaxIterations == 0 && !s.Checkpoint.Required && s.Checkpoint.Namespace == ""
}

func (s AgentSpecV1) validate(kind AgentKind) error {
	if len(s.Nodes) == 0 || len(s.Nodes) > 256 {
		return fmt.Errorf("%w: invalid agent node count", ErrInvalid)
	}
	seen := make(map[string]struct{}, len(s.Nodes))
	for _, node := range s.Nodes {
		if !agentNodeKeyPattern.MatchString(node.Key) || node.FailurePolicy != FailurePolicyFailFast ||
			node.AgentRef.AgentAppID == "" || node.AgentRef.Revision < 1 ||
			!isDigest(node.AgentRef.ContentDigest) {
			return fmt.Errorf("%w: invalid agent node", ErrInvalid)
		}
		if _, exists := seen[node.Key]; exists {
			return fmt.Errorf("%w: duplicate agent node %q", ErrInvalid, node.Key)
		}
		seen[node.Key] = struct{}{}
	}
	switch kind {
	case AgentKindGraph:
		if _, ok := seen[s.EntryNode]; !ok || s.MaxConcurrency < 1 || s.MaxConcurrency > 128 || s.MaxIterations != 0 ||
			s.Checkpoint.Required != (s.Checkpoint.Namespace != "") {
			return fmt.Errorf("%w: invalid graph limits or entry", ErrInvalid)
		}
		seenEdges := make(map[string]struct{}, len(s.Edges))
		conditionalSources := make(map[string]VersionedRef, len(s.Edges))
		conditionalBranches := make(map[string]map[string]struct{}, len(s.Edges))
		unconditionalSources := make(map[string]struct{}, len(s.Edges))
		for _, edge := range s.Edges {
			if edge.From == "" || edge.To == "" {
				return fmt.Errorf("%w: invalid graph edge", ErrInvalid)
			}
			if _, ok := seen[edge.From]; !ok {
				return fmt.Errorf("%w: graph edge source does not exist", ErrInvalid)
			}
			if _, ok := seen[edge.To]; !ok {
				return fmt.Errorf("%w: graph edge target does not exist", ErrInvalid)
			}
			key := edge.From + "\x00" + edge.To
			if _, exists := seenEdges[key]; exists {
				return fmt.Errorf("%w: duplicate graph edge", ErrInvalid)
			}
			seenEdges[key] = struct{}{}
			if edge.ConditionRef == nil {
				if edge.Branch != "" {
					return fmt.Errorf("%w: unconditional graph edge has branch", ErrInvalid)
				}
				if _, exists := conditionalSources[edge.From]; exists {
					return fmt.Errorf("%w: mixed graph routing for %q", ErrInvalid, edge.From)
				}
				unconditionalSources[edge.From] = struct{}{}
				continue
			}
			ref := *edge.ConditionRef
			if ref.ID == "" || ref.Version < 1 || ref.Required || ref.ContentDigest != "" || !agentNodeKeyPattern.MatchString(edge.Branch) {
				return fmt.Errorf("%w: invalid graph condition edge", ErrInvalid)
			}
			if _, exists := unconditionalSources[edge.From]; exists {
				return fmt.Errorf("%w: mixed graph routing for %q", ErrInvalid, edge.From)
			}
			if prior, exists := conditionalSources[edge.From]; exists && prior != ref {
				return fmt.Errorf("%w: multiple graph conditions for %q", ErrInvalid, edge.From)
			}
			conditionalSources[edge.From] = ref
			branches := conditionalBranches[edge.From]
			if branches == nil {
				branches = make(map[string]struct{})
				conditionalBranches[edge.From] = branches
			}
			if _, exists := branches[edge.Branch]; exists {
				return fmt.Errorf("%w: duplicate graph condition branch %q", ErrInvalid, edge.Branch)
			}
			branches[edge.Branch] = struct{}{}
		}
	case AgentKindChain:
		if len(s.Edges) != 0 || s.EntryNode != "" || s.MaxConcurrency != 0 || s.MaxIterations != 0 || s.Checkpoint.Required {
			return fmt.Errorf("%w: invalid chain spec", ErrInvalid)
		}
	case AgentKindParallel:
		if len(s.Edges) != 0 || s.EntryNode != "" || s.MaxConcurrency < 1 || s.MaxConcurrency > 128 ||
			s.MaxConcurrency < len(s.Nodes) || s.MaxIterations != 0 || s.Checkpoint.Required {
			return fmt.Errorf("%w: invalid parallel spec", ErrInvalid)
		}
	case AgentKindCycle:
		if len(s.Edges) != 0 || s.EntryNode != "" || s.MaxConcurrency != 0 ||
			s.MaxIterations < 1 || s.MaxIterations > 1000 || s.Checkpoint.Required {
			return fmt.Errorf("%w: invalid cycle spec", ErrInvalid)
		}
	}
	return nil
}

// NormalizeRevision returns a detached revision whose JSON object fields are
// always objects. PostgreSQL rejects JSON null for these columns, so all
// repository implementations apply the same canonical representation.
func NormalizeRevision(r Revision) Revision {
	r = cloneRevision(r)
	for index := range r.AgentSpec.Nodes {
		if r.AgentSpec.Nodes[index].FailurePolicy == "" {
			r.AgentSpec.Nodes[index].FailurePolicy = FailurePolicyFailFast
		}
	}
	if r.AgentSpec.Nodes == nil {
		r.AgentSpec.Nodes = []AgentNodeSpecV1{}
	}
	if r.AgentSpec.Edges == nil {
		r.AgentSpec.Edges = []AgentEdgeSpecV1{}
	}
	if r.GenerationConfig == nil {
		r.GenerationConfig = map[string]any{}
	}
	if r.RuntimePolicy == nil {
		r.RuntimePolicy = map[string]any{}
	}
	// PostgreSQL persists this field as a JSON array and rejects JSON null.
	// Canonicalizing an omitted fallback list also keeps revision hashes stable
	// across draft creation and reload.
	if r.FallbackModelRefs == nil {
		r.FallbackModelRefs = []VersionedRef{}
	}
	if r.PluginRefs == nil {
		r.PluginRefs = []PluginRef{}
	}
	return r
}

// ComputeContentDigest hashes the normalized behavior-affecting definition.
func (r Revision) ComputeContentDigest() (string, error) {
	r = NormalizeRevision(r)
	tools := cloneRefs(r.ToolRefs)
	skills := append([]SkillRef(nil), r.SkillRefs...)
	plugins := append([]PluginRef(nil), r.PluginRefs...)
	knowledge := cloneRefs(r.KnowledgeRefs)
	sort.Slice(tools, func(i, j int) bool {
		if tools[i].ID == tools[j].ID {
			return tools[i].Version < tools[j].Version
		}
		return tools[i].ID < tools[j].ID
	})
	sort.Slice(knowledge, func(i, j int) bool {
		if knowledge[i].ID == knowledge[j].ID {
			return knowledge[i].Version < knowledge[j].Version
		}
		return knowledge[i].ID < knowledge[j].ID
	})
	sort.Slice(skills, func(i, j int) bool {
		if skills[i].ID == skills[j].ID {
			return skills[i].Version < skills[j].Version
		}
		return skills[i].ID < skills[j].ID
	})
	sort.Slice(plugins, func(i, j int) bool {
		if plugins[i].ID == plugins[j].ID {
			return plugins[i].Version < plugins[j].Version
		}
		return plugins[i].ID < plugins[j].ID
	})
	input := struct {
		AgentKind           AgentKind
		SchemaVersion       int
		AgentSpec           AgentSpecV1
		Description         string
		Instruction         string
		GlobalInstruction   string
		ModelProfileID      string
		ModelProfileVersion int64
		FallbackModelRefs   []VersionedRef
		ToolRefs            []VersionedRef
		SkillRefs           []SkillRef
		PluginRefs          []PluginRef
		KnowledgeRefs       []VersionedRef
		GenerationConfig    map[string]any
		RuntimePolicy       map[string]any
		ExecutionBudget     ExecutionBudgetV1
	}{r.AgentKind, r.SchemaVersion, r.AgentSpec, r.Description, r.Instruction,
		r.GlobalInstruction, r.ModelProfileID, r.ModelProfileVersion, r.FallbackModelRefs,
		tools, skills, plugins, knowledge, r.GenerationConfig, r.RuntimePolicy, r.ExecutionBudget}
	b, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("digest revision: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func cloneRevision(r Revision) Revision {
	r.ToolRefs = cloneRefs(r.ToolRefs)
	r.FallbackModelRefs = cloneRefs(r.FallbackModelRefs)
	r.SkillRefs = append([]SkillRef(nil), r.SkillRefs...)
	r.PluginRefs = append([]PluginRef(nil), r.PluginRefs...)
	r.KnowledgeRefs = cloneRefs(r.KnowledgeRefs)
	r.AgentSpec = cloneAgentSpec(r.AgentSpec)
	r.GenerationConfig = cloneMap(r.GenerationConfig)
	r.RuntimePolicy = cloneMap(r.RuntimePolicy)
	if r.PublishedAt != nil {
		v := *r.PublishedAt
		r.PublishedAt = &v
	}
	return r
}

func cloneAgentSpec(in AgentSpecV1) AgentSpecV1 {
	b, _ := json.Marshal(in)
	var out AgentSpecV1
	_ = json.Unmarshal(b, &out)
	return out
}

func cloneRefs(in []VersionedRef) []VersionedRef { return append([]VersionedRef(nil), in...) }

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	b, _ := json.Marshal(in)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}
