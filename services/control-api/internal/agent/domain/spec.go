package domain

import "encoding/json"

const (
	SchemaVersionV1 = "v1"

	MaxDocumentBytes    = 512 * 1024
	MaxNodes            = 128
	MaxDepth            = 16
	MaxChildren         = 64
	MaxModelSlots       = 16
	MaxToolSlots        = 64
	MaxKnowledgeSlots   = 32
	MaxLoopIterations   = 32
	MaxInstructionBytes = 64 * 1024
)

type NodeKind string

const (
	NodeKindLLM      NodeKind = "llm"
	NodeKindSequence NodeKind = "sequence"
	NodeKindParallel NodeKind = "parallel"
	NodeKindLoop     NodeKind = "loop"
)

// Spec is the typed, validated representation of AgentSpec V1.
type Spec struct {
	SchemaVersion string          `json:"schema_version"`
	Root          string          `json:"root"`
	Requirements  Requirements    `json:"requirements"`
	Nodes         map[string]Node `json:"nodes"`
	Runtime       *Runtime        `json:"runtime,omitempty"`
}

type Requirements struct {
	Executors map[string]CapabilityRequirement `json:"executors,omitempty"`
	Models    map[string]ModelRequirement      `json:"models"`
	Tools     map[string]CapabilityRequirement `json:"tools"`
	Knowledge map[string]CapabilityRequirement `json:"knowledge"`
}

type ModelRequirement struct {
	Capabilities []string `json:"capabilities"`
}

type CapabilityRequirement struct {
	Capability string `json:"capability"`
}

type Generation struct {
	Temperature     *float64 `json:"temperature,omitempty"`
	MaxOutputTokens *int64   `json:"max_output_tokens,omitempty"`
}

// Node is a discriminated union. MarshalJSON emits only fields belonging to
// the selected kind so the canonical document cannot leak inactive options.
type Node struct {
	Workspace         *Workspace
	Kind              NodeKind
	Name              string
	Instruction       string
	ModelSlot         string
	ToolSlots         []string
	KnowledgeSlots    []string
	Generation        *Generation
	Memory            *Memory
	Artifact          *Artifact
	AddSessionSummary *bool
	Children          []string
	Body              string
	MaxIterations     int64
}

func (n *Node) UnmarshalJSON(data []byte) error {
	type wireNode struct {
		Workspace         *Workspace  `json:"workspace"`
		Kind              NodeKind    `json:"kind"`
		Name              string      `json:"name"`
		Instruction       string      `json:"instruction"`
		ModelSlot         string      `json:"model_slot"`
		ToolSlots         []string    `json:"tool_slots"`
		KnowledgeSlots    []string    `json:"knowledge_slots"`
		Generation        *Generation `json:"generation"`
		Memory            *Memory     `json:"memory"`
		Artifact          *Artifact   `json:"artifact"`
		AddSessionSummary *bool       `json:"add_session_summary"`
		Children          []string    `json:"children"`
		Body              string      `json:"body"`
		MaxIterations     int64       `json:"max_iterations"`
	}
	var wire wireNode
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*n = Node{
		Workspace: wire.Workspace, Kind: wire.Kind, Name: wire.Name, Instruction: wire.Instruction,
		ModelSlot: wire.ModelSlot, ToolSlots: wire.ToolSlots,
		KnowledgeSlots: wire.KnowledgeSlots, Generation: wire.Generation, Memory: wire.Memory, Artifact: wire.Artifact, AddSessionSummary: wire.AddSessionSummary,
		Children: wire.Children, Body: wire.Body, MaxIterations: wire.MaxIterations,
	}
	return nil
}

func (n Node) MarshalJSON() ([]byte, error) {
	switch n.Kind {
	case NodeKindLLM:
		return json.Marshal(struct {
			Workspace         *Workspace  `json:"workspace,omitempty"`
			Kind              NodeKind    `json:"kind"`
			Name              string      `json:"name,omitempty"`
			Instruction       string      `json:"instruction"`
			ModelSlot         string      `json:"model_slot"`
			ToolSlots         []string    `json:"tool_slots"`
			KnowledgeSlots    []string    `json:"knowledge_slots"`
			Generation        *Generation `json:"generation,omitempty"`
			Memory            *Memory     `json:"memory,omitempty"`
			Artifact          *Artifact   `json:"artifact,omitempty"`
			AddSessionSummary *bool       `json:"add_session_summary,omitempty"`
		}{n.Workspace, n.Kind, n.Name, n.Instruction, n.ModelSlot, n.ToolSlots, n.KnowledgeSlots, n.Generation, n.Memory, n.Artifact, n.AddSessionSummary})
	case NodeKindSequence, NodeKindParallel:
		return json.Marshal(struct {
			Kind     NodeKind `json:"kind"`
			Name     string   `json:"name,omitempty"`
			Children []string `json:"children"`
		}{n.Kind, n.Name, n.Children})
	case NodeKindLoop:
		return json.Marshal(struct {
			Kind          NodeKind `json:"kind"`
			Name          string   `json:"name,omitempty"`
			Body          string   `json:"body"`
			MaxIterations int64    `json:"max_iterations"`
		}{n.Kind, n.Name, n.Body, n.MaxIterations})
	default:
		return json.Marshal(struct {
			Kind NodeKind `json:"kind"`
		}{n.Kind})
	}
}

// CanonicalSpec is the immutable publishable representation.
type CanonicalSpec struct {
	SchemaVersion string
	Document      json.RawMessage
	Digest        string
}
