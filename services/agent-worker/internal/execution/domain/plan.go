package domain

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"slices"
	"time"
)

// Plan is the runtime-ready projection of one fully validated immutable
// Manifest. It carries only the V1 closure, never mutable Profile values.
type Plan struct {
	Executors                                                  map[string]ExecutorPlan
	Nodes                                                      map[string]NodePlan
	Knowledges                                                 map[string]KnowledgePlan
	Tools                                                      []ToolPlan
	Knowledge                                                  *KnowledgePlan
	Artifact                                                   *ArtifactPlan
	Memory                                                     *MemoryPlan
	MaxToolCalls                                               int64
	Summary                                                    *SummaryPlan
	TenantID, ManifestID, ManifestDigest, DeploymentRevisionID string
	ProfileID                                                  string
	ProfileRevision                                            int64
	NodeID, Instruction                                        string
	ModelEndpoint, ModelName                                   string
	Temperature                                                *float64
	NodeMaxOutputTokens                                        *int64
	MaxOutputTokens, MaxRunSeconds                             int64
	ModelCredential, SessionCredential                         CredentialUse
	SessionBackend                                             *datav1.Snapshot
	SessionTarget                                              StorageTarget
}

// NodePlan preserves each leaf's explicit authority inside an ordered SDK tree.
// Resources on Plan are the initialization closure, not implicit node options.
type NodePlan struct {
	Workspace                             *WorkspacePlan
	Body                                  string
	MaxIterations                         int64
	Kind                                  string
	Children                              []string
	Instruction, ModelEndpoint, ModelName string
	ModelCredential                       CredentialUse
	Temperature                           *float64
	MaxOutputTokens                       *int64
	ToolResources                         []string
	KnowledgeResource                     string
	Memory                                *NodeMemoryPlan
	Artifact, AddSessionSummary           bool
}
type NodeMemoryPlan struct {
	Tools        []string
	PreloadLimit int
}

// SummaryPlan is the fixed model dependency for Session summary generation.
// It shares the execution per-output ceiling, not an accumulated token budget.
type SummaryPlan struct {
	ModelEndpoint, ModelName string
	ModelCredential          CredentialUse
	EventThreshold           int64
	AddSessionSummary        bool
}

// MemoryPlan binds explicit node options to a fixed tenant backend and credential.
type MemoryPlan struct {
	AgentID      string
	Backend      datav1.Snapshot
	Credential   CredentialUse
	Tools        []string
	PreloadLimit int
}

type ToolPlan struct {
	Resource, ServerURL, ToolsetName, ToolName, AuthKind, Capability string
	Credential                                                       CredentialUse
}

type KnowledgePlan struct {
	Resource                          string
	Backend                           datav1.Snapshot
	Credential, EmbeddingCredential   CredentialUse
	EmbeddingModel, EmbeddingEndpoint string
	Dimensions                        int64
}

type ArtifactPlan struct {
	Backend                      datav1.Snapshot
	AccessKeyID, SecretAccessKey CredentialUse
}

type CredentialUse struct{ CredentialID, Purpose, AudienceDigest string }
type StorageTarget struct {
	Host                        string
	Port                        uint16
	Database, Username, SSLMode string
}

func (p Plan) Uses() []CredentialUse {
	uses := make([]CredentialUse, 0, 2)
	if p.ModelCredential.CredentialID != "" {
		uses = append(uses, p.ModelCredential)
	}
	if p.SessionCredential.CredentialID != "" {
		uses = append(uses, p.SessionCredential)
	}
	if p.Summary != nil && p.Summary.ModelCredential.CredentialID != "" {
		use := p.Summary.ModelCredential
		duplicate := false
		for _, existing := range uses {
			if existing == use {
				duplicate = true
				break
			}
		}
		if !duplicate {
			uses = append(uses, use)
		}
	}
	if p.Memory != nil && p.Memory.Credential.CredentialID != "" {
		use := p.Memory.Credential
		found := false
		for _, existing := range uses {
			if existing == use {
				found = true
			}
		}
		if !found {
			uses = append(uses, use)
		}
	}
	if p.Artifact != nil {
		uses = append(uses, p.Artifact.AccessKeyID, p.Artifact.SecretAccessKey)
	}
	if p.Knowledge != nil {
		uses = append(uses, p.Knowledge.Credential, p.Knowledge.EmbeddingCredential)
	}
	for _, t := range p.Tools {
		if t.AuthKind == "bearer" {
			found := false
			for _, use := range uses {
				if use == t.Credential {
					found = true
					break
				}
			}
			if !found {
				uses = append(uses, t.Credential)
			}
		}
	}

	keys := make([]string, 0, len(p.Nodes))
	for key := range p.Nodes {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if p.Nodes[key].Kind == "llm" {
			uses = append(uses, p.Nodes[key].ModelCredential)
		}
	}
	keys = keys[:0]
	for key := range p.Knowledges {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		k := p.Knowledges[key]
		uses = append(uses, k.Credential, k.EmbeddingCredential)
	}
	unique := make([]CredentialUse, 0, len(uses))
	seen := map[CredentialUse]bool{}
	for _, u := range uses {
		if !seen[u] {
			unique = append(unique, u)
			seen[u] = true
		}
	}
	return unique
}

type RuntimeResult struct {
	Attachments                            []Attachment
	MemoryDigest                           string
	MemoryTimeout                          time.Duration
	FinalText                              string
	Snapshot                               []byte
	InputTokens, OutputTokens, TotalTokens int64
	UsageKnown                             bool
}

type ModelUsage struct {
	Known                                  bool
	InputTokens, OutputTokens, TotalTokens int64
}

// WorkspacePlan is an explicit node selection, never authority from resource presence.
type WorkspacePlan struct {
	ExecutorResource string
	Tools            []string
}
type ExecutorPlan struct{ Kind, AdapterVersion string }
