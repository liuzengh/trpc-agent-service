package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode"
)

// RevisionState distinguishes mutable draft content from immutable published
// content.
type RevisionState string

const (
	// RevisionStateDraft identifies a mutable revision.
	RevisionStateDraft RevisionState = "draft"
	// RevisionStatePublished identifies immutable executable content.
	RevisionStatePublished RevisionState = "published"
)

// Kind identifies the tRPC-Agent-Go Agent configuration schema.
type Kind string

const (
	// KindLLM identifies a single LLMAgent definition.
	KindLLM Kind = "llm"
	// KindChain identifies a sequential composition of LLMAgent steps.
	KindChain Kind = "chain"
)

const (
	// SchemaVersionV1 is the initial LLM and Chain configuration schema.
	SchemaVersionV1     = 1
	maxInstructionRunes = 65536
	maxReferenceRunes   = 256
	maxChainSteps       = 32
)

// GenerationConfig is the provider-neutral subset materialized by the first
// Agent App schema. Nil fields preserve provider defaults.
type GenerationConfig struct {
	Temperature     *float64 `json:"temperature,omitempty"`
	TopP            *float64 `json:"top_p,omitempty"`
	MaxOutputTokens *int     `json:"max_output_tokens,omitempty"`
}

// ChainStep is one tenant-authored LLMAgent step in a sequential Chain.
// Every step uses the parent Revision's Model Profile and Tool allowlist.
type ChainStep struct {
	Name              string `json:"name"`
	Instruction       string `json:"instruction"`
	GlobalInstruction string `json:"global_instruction,omitempty"`
}

// ChainConfiguration describes the ordered child Agents of a Chain Revision.
// The list is immutable after publication and bounded to keep execution and
// budget estimates predictable.
type ChainConfiguration struct {
	Steps []ChainStep `json:"steps"`
}

// Clone returns a defensive copy of the Chain configuration.
func (configuration *ChainConfiguration) Clone() *ChainConfiguration {
	if configuration == nil {
		return nil
	}
	clone := *configuration
	clone.Steps = append([]ChainStep(nil), configuration.Steps...)
	return &clone
}

// RuntimePolicy contains bounded execution controls captured by a published
// revision. It does not carry contexts, timers, or runtime clients.
type RuntimePolicy struct {
	MaxLLMCalls             int  `json:"max_llm_calls"`
	MaxToolCalls            int  `json:"max_tool_calls"`
	EnableParallelTools     bool `json:"enable_parallel_tools"`
	MaxParallelTools        int  `json:"max_parallel_tools"`
	ExecutionTimeoutSeconds int  `json:"execution_timeout_seconds"`
}

// DefaultRuntimePolicy returns the materialized schema-v1 execution defaults.
func DefaultRuntimePolicy() RuntimePolicy {
	return RuntimePolicy{
		MaxLLMCalls:             16,
		MaxToolCalls:            64,
		MaxParallelTools:        1,
		ExecutionTimeoutSeconds: 300,
	}
}

// ToolAuthorization is one deny-by-default tool allowlist entry.
type ToolAuthorization struct {
	ToolID   string `json:"tool_id"`
	Required bool   `json:"required"`
}

// DraftConfiguration is the complete executable content of one revision.
type DraftConfiguration struct {
	Description       string
	Instruction       string
	GlobalInstruction string
	ModelProfileID    string
	Generation        GenerationConfig
	Runtime           RuntimePolicy
	Tools             []ToolAuthorization
	Chain             *ChainConfiguration
}

// Revision is one tenant-scoped version of an Agent App definition.
// Published revisions are immutable and content-addressed.
type Revision struct {
	TenantID          string
	AppID             string
	Revision          int64
	State             RevisionState
	DraftVersion      int64
	Kind              Kind
	SchemaVersion     int
	Description       string
	Instruction       string
	GlobalInstruction string
	ModelProfileID    string
	Generation        GenerationConfig
	Runtime           RuntimePolicy
	Tools             []ToolAuthorization
	Chain             *ChainConfiguration
	ContentDigest     string
	PublishedAt       *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// CreateRevisionInput contains trusted identity and caller-selected content for
// a new draft. A Repository allocates the revision number inside the App scope.
type CreateRevisionInput struct {
	TenantID      string
	AppID         string
	Revision      int64
	Kind          Kind
	SchemaVersion int
	Configuration DraftConfiguration
}

// NewRevision validates and constructs a new draft revision.
func NewRevision(input CreateRevisionInput) (*Revision, error) {
	if err := validateTenantID(input.TenantID); err != nil {
		return nil, err
	}
	if err := validateAppID(input.AppID); err != nil {
		return nil, err
	}
	if input.Revision < 1 {
		return nil, fmt.Errorf("%w: revision must be positive", ErrInvalid)
	}
	kind := input.Kind
	if kind == "" {
		kind = KindLLM
	}
	schemaVersion := input.SchemaVersion
	if schemaVersion == 0 {
		schemaVersion = SchemaVersionV1
	}
	configuration, err := normalizeDraftConfiguration(input.Configuration)
	if err != nil {
		return nil, err
	}
	if err := validateRevisionDefinition(kind, schemaVersion, configuration); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	revision := &Revision{
		TenantID:          input.TenantID,
		AppID:             input.AppID,
		Revision:          input.Revision,
		State:             RevisionStateDraft,
		DraftVersion:      1,
		Kind:              kind,
		SchemaVersion:     schemaVersion,
		Description:       configuration.Description,
		Instruction:       configuration.Instruction,
		GlobalInstruction: configuration.GlobalInstruction,
		ModelProfileID:    configuration.ModelProfileID,
		Generation:        cloneGenerationConfig(configuration.Generation),
		Runtime:           configuration.Runtime,
		Tools:             cloneTools(configuration.Tools),
		Chain:             configuration.Chain.Clone(),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	return revision, nil
}

// Clone returns a deep copy suitable for repository and execution boundaries.
func (r Revision) Clone() Revision {
	clone := r
	clone.Generation = cloneGenerationConfig(r.Generation)
	clone.Tools = cloneTools(r.Tools)
	clone.Chain = r.Chain.Clone()
	clone.PublishedAt = cloneTime(r.PublishedAt)
	return clone
}

// Configuration returns a deep copy of the revision's executable definition.
func (r Revision) Configuration() DraftConfiguration {
	return DraftConfiguration{
		Description:       r.Description,
		Instruction:       r.Instruction,
		GlobalInstruction: r.GlobalInstruction,
		ModelProfileID:    r.ModelProfileID,
		Generation:        cloneGenerationConfig(r.Generation),
		Runtime:           r.Runtime,
		Tools:             cloneTools(r.Tools),
		Chain:             r.Chain.Clone(),
	}
}

// Validate checks identity, schema, configuration, publication state, digest,
// version, and timestamp invariants.
func (r Revision) Validate() error {
	if err := r.validateIdentity(); err != nil {
		return err
	}
	_, err := r.validateConfiguration()
	if err != nil {
		return err
	}
	if err := r.validateTimestamps(); err != nil {
		return err
	}
	return r.validatePublication()
}

func (r Revision) validateIdentity() error {
	if err := validateTenantID(r.TenantID); err != nil {
		return err
	}
	if err := validateAppID(r.AppID); err != nil {
		return err
	}
	if r.Revision < 1 || r.DraftVersion < 1 {
		return fmt.Errorf("%w: revision and draft version must be positive", ErrInvalid)
	}
	return nil
}

func (r Revision) validateConfiguration() (DraftConfiguration, error) {
	configuration := r.Configuration()
	normalized, err := normalizeDraftConfiguration(configuration)
	if err != nil {
		return DraftConfiguration{}, err
	}
	if !sameDraftConfiguration(configuration, normalized) {
		return DraftConfiguration{}, fmt.Errorf("%w: revision configuration must be normalized", ErrInvalid)
	}
	if err := validateRevisionDefinition(r.Kind, r.SchemaVersion, configuration); err != nil {
		return DraftConfiguration{}, err
	}
	return configuration, nil
}

func (r Revision) validateTimestamps() error {
	if r.CreatedAt.IsZero() || r.UpdatedAt.IsZero() || r.UpdatedAt.Before(r.CreatedAt) {
		return fmt.Errorf("%w: revision timestamps must be initialized and ordered", ErrInvalid)
	}
	return nil
}

func (r Revision) validatePublication() error {
	switch r.State {
	case RevisionStateDraft:
		if r.ContentDigest != "" || r.PublishedAt != nil {
			return fmt.Errorf("%w: draft revision cannot contain publication metadata", ErrInvalid)
		}
	case RevisionStatePublished:
		if r.PublishedAt == nil || r.PublishedAt.IsZero() || r.PublishedAt.Before(r.CreatedAt) || !r.PublishedAt.Equal(r.UpdatedAt) {
			return fmt.Errorf("%w: published revision requires matching publication and update times", ErrInvalid)
		}
		digest, err := r.ComputeContentDigest()
		if err != nil {
			return err
		}
		if r.ContentDigest != digest {
			return fmt.Errorf("%w: published revision content digest mismatch", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown revision state %q", ErrInvalid, r.State)
	}
	return nil
}

// ComputeContentDigest returns the deterministic SHA-256 digest of executable
// content. Publication metadata and draft bookkeeping are excluded.
func (r Revision) ComputeContentDigest() (string, error) {
	configuration, err := normalizeDraftConfiguration(r.Configuration())
	if err != nil {
		return "", err
	}
	if err := validateRevisionDefinition(r.Kind, r.SchemaVersion, configuration); err != nil {
		return "", err
	}
	payload := struct {
		Kind              Kind                `json:"kind"`
		SchemaVersion     int                 `json:"schema_version"`
		Description       string              `json:"description"`
		Instruction       string              `json:"instruction"`
		GlobalInstruction string              `json:"global_instruction"`
		ModelProfileID    string              `json:"model_profile_id"`
		Generation        GenerationConfig    `json:"generation"`
		Runtime           RuntimePolicy       `json:"runtime"`
		Tools             []ToolAuthorization `json:"tools"`
		Chain             *ChainConfiguration `json:"chain,omitempty"`
	}{
		Kind:              r.Kind,
		SchemaVersion:     r.SchemaVersion,
		Description:       configuration.Description,
		Instruction:       configuration.Instruction,
		GlobalInstruction: configuration.GlobalInstruction,
		ModelProfileID:    configuration.ModelProfileID,
		Generation:        cloneGenerationConfig(configuration.Generation),
		Runtime:           configuration.Runtime,
		Tools:             cloneTools(configuration.Tools),
		Chain:             configuration.Chain.Clone(),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode agent app revision digest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// Publish returns an immutable published copy. The source draft is unchanged.
func (r Revision) Publish(at time.Time) (Revision, error) {
	if r.State != RevisionStateDraft {
		return Revision{}, ErrImmutableRevision
	}
	if err := r.Validate(); err != nil {
		return Revision{}, err
	}
	if at.IsZero() || at.Before(r.UpdatedAt) {
		return Revision{}, fmt.Errorf("%w: publication time must follow the latest draft update", ErrInvalid)
	}
	published := r.Clone()
	digest, err := published.ComputeContentDigest()
	if err != nil {
		return Revision{}, err
	}
	at = at.UTC()
	published.State = RevisionStatePublished
	published.ContentDigest = digest
	published.PublishedAt = &at
	published.UpdatedAt = at
	if err := published.Validate(); err != nil {
		return Revision{}, err
	}
	return published, nil
}

func normalizeDraftConfiguration(configuration DraftConfiguration) (DraftConfiguration, error) {
	normalized := configuration
	normalized.Description = strings.TrimSpace(configuration.Description)
	normalized.ModelProfileID = strings.TrimSpace(configuration.ModelProfileID)
	normalized.Generation = normalizeGenerationConfig(configuration.Generation)
	if normalized.Runtime == (RuntimePolicy{}) {
		normalized.Runtime = DefaultRuntimePolicy()
	}
	tools, err := normalizeTools(configuration.Tools)
	if err != nil {
		return DraftConfiguration{}, err
	}
	normalized.Tools = tools
	chain, err := normalizeChainConfiguration(configuration.Chain)
	if err != nil {
		return DraftConfiguration{}, err
	}
	normalized.Chain = chain
	return normalized, nil
}

func validateRevisionDefinition(kind Kind, schemaVersion int, configuration DraftConfiguration) error {
	if schemaVersion != SchemaVersionV1 {
		return fmt.Errorf("%w: unsupported agent kind %q or schema version %d", ErrInvalid, kind, schemaVersion)
	}
	switch kind {
	case KindLLM:
		if configuration.Chain != nil {
			return fmt.Errorf("%w: LLM revision cannot contain chain configuration", ErrInvalid)
		}
	case KindChain:
		if err := validateChainConfiguration(configuration.Chain); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unsupported agent kind %q or schema version %d", ErrInvalid, kind, schemaVersion)
	}
	if len([]rune(configuration.Description)) > 2000 {
		return fmt.Errorf("%w: revision description must contain at most 2000 characters", ErrInvalid)
	}
	if n := len([]rune(strings.TrimSpace(configuration.Instruction))); n < 1 || n > maxInstructionRunes {
		return fmt.Errorf("%w: instruction must contain 1-%d characters", ErrInvalid, maxInstructionRunes)
	}
	if len([]rune(configuration.GlobalInstruction)) > maxInstructionRunes {
		return fmt.Errorf("%w: global instruction must contain at most %d characters", ErrInvalid, maxInstructionRunes)
	}
	if n := len([]rune(configuration.ModelProfileID)); n < 1 || n > maxReferenceRunes {
		return fmt.Errorf("%w: model profile id must contain 1-%d characters", ErrInvalid, maxReferenceRunes)
	}
	if err := validateGenerationConfig(configuration.Generation); err != nil {
		return err
	}
	if err := validateRuntimePolicy(configuration.Runtime); err != nil {
		return err
	}
	if _, err := normalizeTools(configuration.Tools); err != nil {
		return err
	}
	return nil
}

func normalizeChainConfiguration(configuration *ChainConfiguration) (*ChainConfiguration, error) {
	if configuration == nil {
		return nil, nil
	}
	normalized := configuration.Clone()
	for index := range normalized.Steps {
		step := &normalized.Steps[index]
		step.Name = strings.TrimSpace(step.Name)
		step.Instruction = strings.TrimSpace(step.Instruction)
		step.GlobalInstruction = strings.TrimSpace(step.GlobalInstruction)
		if len([]rune(step.Name)) > maxReferenceRunes {
			return nil, fmt.Errorf("%w: chain step name must contain at most %d characters", ErrInvalid, maxReferenceRunes)
		}
		if len([]rune(step.Instruction)) > maxInstructionRunes {
			return nil, fmt.Errorf("%w: chain step instruction must contain at most %d characters", ErrInvalid, maxInstructionRunes)
		}
		if len([]rune(step.GlobalInstruction)) > maxInstructionRunes {
			return nil, fmt.Errorf("%w: chain step global instruction must contain at most %d characters", ErrInvalid, maxInstructionRunes)
		}
	}
	return normalized, nil
}

func validateChainConfiguration(configuration *ChainConfiguration) error {
	if configuration == nil {
		return fmt.Errorf("%w: chain configuration is required for chain revisions", ErrInvalid)
	}
	if len(configuration.Steps) < 2 || len(configuration.Steps) > maxChainSteps {
		return fmt.Errorf("%w: chain must contain 2-%d steps", ErrInvalid, maxChainSteps)
	}
	seen := make(map[string]struct{}, len(configuration.Steps))
	for index, step := range configuration.Steps {
		if n := len([]rune(step.Name)); n < 1 || n > maxReferenceRunes {
			return fmt.Errorf("%w: chain step %d name must contain 1-%d characters", ErrInvalid, index, maxReferenceRunes)
		}
		if strings.IndexFunc(step.Name, unicode.IsControl) >= 0 {
			return fmt.Errorf("%w: chain step %q name contains a control character", ErrInvalid, step.Name)
		}
		if _, exists := seen[step.Name]; exists {
			return fmt.Errorf("%w: duplicate chain step %q", ErrInvalid, step.Name)
		}
		seen[step.Name] = struct{}{}
		if n := len([]rune(step.Instruction)); n < 1 || n > maxInstructionRunes {
			return fmt.Errorf("%w: chain step %q instruction must contain 1-%d characters", ErrInvalid, step.Name, maxInstructionRunes)
		}
		if strings.IndexFunc(step.Instruction, unicode.IsControl) >= 0 {
			return fmt.Errorf("%w: chain step %q instruction contains a control character", ErrInvalid, step.Name)
		}
		if len([]rune(step.GlobalInstruction)) > maxInstructionRunes {
			return fmt.Errorf("%w: chain step %q global instruction must contain at most %d characters", ErrInvalid, step.Name, maxInstructionRunes)
		}
		if strings.IndexFunc(step.GlobalInstruction, unicode.IsControl) >= 0 {
			return fmt.Errorf("%w: chain step %q global instruction contains a control character", ErrInvalid, step.Name)
		}
	}
	return nil
}

func validateGenerationConfig(configuration GenerationConfig) error {
	if configuration.Temperature != nil && (math.IsNaN(*configuration.Temperature) || math.IsInf(*configuration.Temperature, 0) || *configuration.Temperature < 0 || *configuration.Temperature > 2) {
		return fmt.Errorf("%w: temperature must be finite and between 0 and 2", ErrInvalid)
	}
	if configuration.TopP != nil && (math.IsNaN(*configuration.TopP) || math.IsInf(*configuration.TopP, 0) || *configuration.TopP <= 0 || *configuration.TopP > 1) {
		return fmt.Errorf("%w: top p must be finite and in (0,1]", ErrInvalid)
	}
	if configuration.MaxOutputTokens != nil && (*configuration.MaxOutputTokens < 1 || *configuration.MaxOutputTokens > 1_000_000) {
		return fmt.Errorf("%w: max output tokens must be between 1 and 1000000", ErrInvalid)
	}
	return nil
}

func validateRuntimePolicy(policy RuntimePolicy) error {
	if policy.MaxLLMCalls < 1 || policy.MaxLLMCalls > 100 {
		return fmt.Errorf("%w: max LLM calls must be between 1 and 100", ErrInvalid)
	}
	if policy.MaxToolCalls < 0 || policy.MaxToolCalls > 1000 {
		return fmt.Errorf("%w: max tool calls must be between 0 and 1000", ErrInvalid)
	}
	if policy.MaxParallelTools < 1 || policy.MaxParallelTools > 64 {
		return fmt.Errorf("%w: max parallel tools must be between 1 and 64", ErrInvalid)
	}
	if !policy.EnableParallelTools && policy.MaxParallelTools != 1 {
		return fmt.Errorf("%w: serial tool execution requires max parallel tools of 1", ErrInvalid)
	}
	if policy.ExecutionTimeoutSeconds < 1 || policy.ExecutionTimeoutSeconds > 3600 {
		return fmt.Errorf("%w: execution timeout must be between 1 and 3600 seconds", ErrInvalid)
	}
	return nil
}

func normalizeTools(tools []ToolAuthorization) ([]ToolAuthorization, error) {
	if len(tools) == 0 {
		return []ToolAuthorization{}, nil
	}
	normalized := cloneTools(tools)
	seen := make(map[string]struct{}, len(normalized))
	for i := range normalized {
		normalized[i].ToolID = strings.TrimSpace(normalized[i].ToolID)
		if n := len([]rune(normalized[i].ToolID)); n < 1 || n > maxReferenceRunes {
			return nil, fmt.Errorf("%w: tool id must contain 1-%d characters", ErrInvalid, maxReferenceRunes)
		}
		if _, exists := seen[normalized[i].ToolID]; exists {
			return nil, fmt.Errorf("%w: duplicate tool authorization %q", ErrInvalid, normalized[i].ToolID)
		}
		seen[normalized[i].ToolID] = struct{}{}
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ToolID < normalized[j].ToolID })
	return normalized, nil
}

func sameDraftConfiguration(left, right DraftConfiguration) bool {
	if left.Description != right.Description || left.Instruction != right.Instruction || left.GlobalInstruction != right.GlobalInstruction || left.ModelProfileID != right.ModelProfileID || left.Runtime != right.Runtime {
		return false
	}
	if !sameGenerationConfig(left.Generation, right.Generation) || len(left.Tools) != len(right.Tools) {
		return false
	}
	if !sameChainConfiguration(left.Chain, right.Chain) {
		return false
	}
	for i := range left.Tools {
		if left.Tools[i] != right.Tools[i] {
			return false
		}
	}
	return true
}

func sameChainConfiguration(left, right *ChainConfiguration) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if len(left.Steps) != len(right.Steps) {
		return false
	}
	for index := range left.Steps {
		if left.Steps[index] != right.Steps[index] {
			return false
		}
	}
	return true
}

func sameGenerationConfig(left, right GenerationConfig) bool {
	return sameFloat64(left.Temperature, right.Temperature) && sameFloat64(left.TopP, right.TopP) && sameInt(left.MaxOutputTokens, right.MaxOutputTokens)
}

func sameFloat64(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameInt(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func cloneGenerationConfig(configuration GenerationConfig) GenerationConfig {
	clone := configuration
	if configuration.Temperature != nil {
		value := *configuration.Temperature
		clone.Temperature = &value
	}
	if configuration.TopP != nil {
		value := *configuration.TopP
		clone.TopP = &value
	}
	if configuration.MaxOutputTokens != nil {
		value := *configuration.MaxOutputTokens
		clone.MaxOutputTokens = &value
	}
	return clone
}

func normalizeGenerationConfig(configuration GenerationConfig) GenerationConfig {
	normalized := cloneGenerationConfig(configuration)
	if normalized.Temperature != nil && *normalized.Temperature == 0 {
		zero := 0.0
		normalized.Temperature = &zero
	}
	if normalized.TopP != nil && *normalized.TopP == 0 {
		zero := 0.0
		normalized.TopP = &zero
	}
	return normalized
}

func cloneTools(tools []ToolAuthorization) []ToolAuthorization {
	if tools == nil {
		return nil
	}
	clone := make([]ToolAuthorization, len(tools))
	copy(clone, tools)
	return clone
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
