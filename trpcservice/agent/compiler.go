package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"golang.org/x/sync/singleflight"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

var environmentNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

// Compiler turns an immutable control-plane revision into a runnable Agent.
type Compiler interface {
	Compile(ctx context.Context, scope runtimecontext.Scope) (agentcore.Agent, error)
}

// StaticCompiler preserves the dependency-free tutorial and focused tests.
type StaticCompiler struct {
	agent agentcore.Agent
}

// NewStaticCompiler returns the same immutable Agent for every valid scope.
func NewStaticCompiler(agentInstance agentcore.Agent) (*StaticCompiler, error) {
	if agentInstance == nil {
		return nil, fmt.Errorf("static compiler agent is required")
	}
	return &StaticCompiler{agent: agentInstance}, nil
}

func (c *StaticCompiler) Compile(
	_ context.Context,
	scope runtimecontext.Scope,
) (agentcore.Agent, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return c.agent, nil
}

// RevisionCompiler compiles and caches published Agent revisions.
type RevisionCompiler struct {
	repository    controlplane.Repository
	startupModel  model.Model
	defaultStream bool
	mu            sync.RWMutex
	cache         map[string]agentcore.Agent
	group         singleflight.Group
}

// NewRevisionCompiler creates a compiler. Revisions with model source
// "startup_env" reuse startupModel; other revisions build their own adapter.
func NewRevisionCompiler(
	repository controlplane.Repository,
	startupModel model.Model,
	defaultStream bool,
) (*RevisionCompiler, error) {
	if repository == nil {
		return nil, fmt.Errorf("revision compiler repository is required")
	}
	if startupModel == nil {
		return nil, fmt.Errorf("revision compiler startup model is required")
	}
	return &RevisionCompiler{
		repository:    repository,
		startupModel:  startupModel,
		defaultStream: defaultStream,
		cache:         make(map[string]agentcore.Agent),
	}, nil
}

// Compile validates revision scope, then returns a cached immutable Agent.
func (c *RevisionCompiler) Compile(
	ctx context.Context,
	scope runtimecontext.Scope,
) (agentcore.Agent, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	revision, err := c.repository.GetRevision(ctx, scope.TenantID, scope.RevisionID)
	if err != nil {
		return nil, fmt.Errorf("load Agent revision: %w", err)
	}
	if revision.AppID != scope.AppID || revision.TenantID != scope.TenantID {
		return nil, fmt.Errorf("Agent revision scope mismatch")
	}
	cacheKey := scope.TenantID + "\x00" + revision.ID + "\x00" + revision.Checksum
	c.mu.RLock()
	cached := c.cache[cacheKey]
	c.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}
	value, err, _ := c.group.Do(cacheKey, func() (any, error) {
		c.mu.RLock()
		existing := c.cache[cacheKey]
		c.mu.RUnlock()
		if existing != nil {
			return existing, nil
		}
		compiled, compileErr := c.compileRevision(revision)
		if compileErr != nil {
			return nil, compileErr
		}
		c.mu.Lock()
		c.cache[cacheKey] = compiled
		c.mu.Unlock()
		return compiled, nil
	})
	if err != nil {
		return nil, err
	}
	compiled, ok := value.(agentcore.Agent)
	if !ok || compiled == nil {
		return nil, fmt.Errorf("compiled revision returned invalid Agent")
	}
	return compiled, nil
}

// Invalidate removes every cached checksum variant of one revision.
func (c *RevisionCompiler) Invalidate(tenantID string, revisionID string) {
	prefix := tenantID + "\x00" + revisionID + "\x00"
	c.mu.Lock()
	for key := range c.cache {
		if strings.HasPrefix(key, prefix) {
			delete(c.cache, key)
		}
	}
	c.mu.Unlock()
}

type revisionAgentConfig struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Instruction string `json:"instruction"`
	Stream      *bool  `json:"stream,omitempty"`
}

type revisionModelConfig struct {
	Source    string `json:"source"`
	Provider  string `json:"provider"`
	Name      string `json:"name"`
	BaseURL   string `json:"base_url"`
	APIKeyEnv string `json:"api_key_env"`
}

func (c *RevisionCompiler) compileRevision(
	revision controlplane.AgentRevision,
) (agentcore.Agent, error) {
	if revision.AgentType != "llm" {
		return nil, fmt.Errorf("unsupported Agent type %q", revision.AgentType)
	}
	var agentConfig revisionAgentConfig
	if err := decodeStrictJSON(revision.AgentConfig, &agentConfig); err != nil {
		return nil, fmt.Errorf("decode Agent config: %w", err)
	}
	agentConfig.Name = strings.TrimSpace(agentConfig.Name)
	agentConfig.Description = strings.TrimSpace(agentConfig.Description)
	agentConfig.Instruction = strings.TrimSpace(agentConfig.Instruction)
	if agentConfig.Name == "" || agentConfig.Instruction == "" {
		return nil, fmt.Errorf("Agent revision requires name and instruction")
	}
	selectedModel, err := c.buildRevisionModel(revision.ModelConfig)
	if err != nil {
		return nil, err
	}
	stream := c.defaultStream
	if agentConfig.Stream != nil {
		stream = *agentConfig.Stream
	}
	return llmagent.New(
		agentConfig.Name,
		llmagent.WithModel(selectedModel),
		llmagent.WithDescription(agentConfig.Description),
		llmagent.WithInstruction(agentConfig.Instruction),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: stream}),
	), nil
}

func (c *RevisionCompiler) buildRevisionModel(raw json.RawMessage) (model.Model, error) {
	var modelConfig revisionModelConfig
	if err := decodeStrictJSON(raw, &modelConfig); err != nil {
		return nil, fmt.Errorf("decode revision model config: %w", err)
	}
	modelConfig.Source = strings.ToLower(strings.TrimSpace(modelConfig.Source))
	if modelConfig.Source == "" || modelConfig.Source == "startup_env" {
		return c.startupModel, nil
	}
	if modelConfig.Source != "revision" {
		return nil, fmt.Errorf("unsupported revision model source %q", modelConfig.Source)
	}
	if !environmentNamePattern.MatchString(modelConfig.APIKeyEnv) {
		return nil, fmt.Errorf("revision model api_key_env is invalid")
	}
	apiKey, ok := os.LookupEnv(modelConfig.APIKeyEnv)
	if !ok || strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("revision model secret environment %q is unavailable", modelConfig.APIKeyEnv)
	}
	return BuildModel(config.ModelConfig{
		Provider: strings.ToLower(strings.TrimSpace(modelConfig.Provider)),
		Name:     strings.TrimSpace(modelConfig.Name),
		BaseURL:  strings.TrimSpace(modelConfig.BaseURL),
		APIKey:   apiKey,
	})
}

func decodeStrictJSON(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("configuration must contain one JSON value")
	}
	return nil
}

var _ Compiler = (*StaticCompiler)(nil)
var _ Compiler = (*RevisionCompiler)(nil)
