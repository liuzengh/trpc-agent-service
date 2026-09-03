// Package agent hosts tenant-specific agents built on tRPC-Agent-Go
// (llmagent, graph, chain/parallel/cycle) and runner.Runner.
package agent

import (
	"fmt"
	"sort"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	appName      = "trpc-agent-service"
	agentName    = "assistant"
	defaultModel = "gpt-4o-mini"
)

// NewRunner builds one tenant's walking-skeleton runner: an LLMAgent backed
// by the tenant's OpenAI-compatible model, streaming output, in-memory
// session backend. The session service is intentionally in-memory here and
// will be replaced by tenant-selected shared backends (redis/mysql/postgres)
// via the Storage Adapter later.
func NewRunner(t *tenant.Context) (runner.Runner, error) {
	if t.Model.APIKey == "" {
		return nil, fmt.Errorf("tenant %s: model api key is required", t.ID)
	}

	modelName := t.Model.Name
	if modelName == "" {
		modelName = defaultModel
	}
	modelOpts := []openai.Option{openai.WithAPIKey(t.Model.APIKey)}
	if t.Model.BaseURL != "" {
		modelOpts = append(modelOpts, openai.WithBaseURL(t.Model.BaseURL))
	}
	llm := openai.New(modelName, modelOpts...)

	a := llmagent.New(agentName,
		llmagent.WithModel(llm),
		llmagent.WithInstruction("You are a helpful assistant."),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: true}),
	)

	return runner.NewRunner(appName, a,
		runner.WithSessionService(inmemory.NewSessionService()),
	), nil
}

// Registry holds one Runner per tenant. Tenant isolation at this stage is
// by separate Runner instances; per-tenant tool whitelists and guardrails
// will hang off the same lookup later. The mutex guards hot swaps driven by
// the Admin API; lookups only take a read lock.
type Registry struct {
	mu      sync.RWMutex
	runners map[string]runner.Runner
	def     string
}

// NewRegistry builds a Runner for every tenant in the loaded config.
func NewRegistry(cfg *config.Config) (*Registry, error) {
	r := &Registry{
		runners: make(map[string]runner.Runner, len(cfg.Tenants)),
		def:     cfg.DefaultTenant,
	}
	for id, t := range cfg.Tenants {
		rr, err := NewRunner(t)
		if err != nil {
			return nil, err
		}
		r.runners[id] = rr
	}
	return r, nil
}

// Apply rebuilds the runner set for a new config (Admin API hot reload).
// New runners are fully built before the swap, so a build failure keeps the
// old set serving.
func (r *Registry) Apply(cfg *config.Config) error {
	runners := make(map[string]runner.Runner, len(cfg.Tenants))
	for id, t := range cfg.Tenants {
		rr, err := NewRunner(t)
		if err != nil {
			return err
		}
		runners[id] = rr
	}
	r.mu.Lock()
	r.runners = runners
	r.def = cfg.DefaultTenant
	r.mu.Unlock()
	return nil
}

// Default returns the default tenant id.
func (r *Registry) Default() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.def
}

// IDs returns the sorted tenant ids.
func (r *Registry) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.runners))
	for id := range r.runners {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Runner returns the Runner of tenant id, or false if unknown.
func (r *Registry) Runner(id string) (runner.Runner, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rr, ok := r.runners[id]
	return rr, ok
}
