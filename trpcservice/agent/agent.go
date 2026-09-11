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
	"trpc.group/trpc-go/trpc-agent-go/session"
	frameworkskill "trpc.group/trpc-go/trpc-agent-go/skill"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

const (
	appName      = "trpc-agent-service"
	defaultModel = "gpt-4o-mini"

	// DefaultInstruction is what an agent says about itself when nothing more
	// specific was published. It is a named constant, not an inline string, so
	// the legacy path (no instruction configured) and the reliable path (a
	// revision that happens to store exactly this text) agree on what "no
	// opinion" means.
	DefaultInstruction = "You are a helpful assistant."
)

// AgentName is the single first-phase agent identity; audit records and
// traces label model calls with it.
const AgentName = "assistant"

// NewRunner builds one tenant's runner: an LLMAgent backed by the tenant's
// OpenAI-compatible model, streaming output, over the shared session service
// (memory or redis, selected platform-wide via trpcservice/storage). All
// tenants share one session.Service instance; isolation comes from the
// {tenant}:{channel}:{user} session ids.
//
// maxLLMCalls is the per-message ceiling on model calls. It is a parameter
// rather than an llmagent default because the framework treats a
// non-positive value as "no limit", and a non-positive value reaching here
// would silently reopen the unbounded-call hole the cap exists to close
// during fault testing, so it is corrected instead of
// trusted.
//
// tools carries tools the caller has already assembled — in the reliable
// path, the revision's pinned tools wrapped by the tool governor. They are
// appended to the legacy allowlist selection rather than replacing it so the
// two paths keep sharing this one constructor; the reliable runner factory
// never populates the legacy allowlist, so there is nothing to double-add.
// opts are the remaining llmagent options a caller assembles (today: the
// skill mounting from SkillOptions), kept as options so the two paths cannot
// drift on how skills are wired.
func NewRunner(t *tenant.Context, sess session.Service, maxLLMCalls int, tools []frameworktool.Tool, opts ...llmagent.Option) (runner.Runner, error) {
	if t.Model.APIKey == "" {
		return nil, fmt.Errorf("tenant %s: model api key is required", t.ID)
	}
	if maxLLMCalls <= 0 {
		maxLLMCalls = config.DefaultMaxLLMCalls
	}

	modelName := t.Model.Name
	if modelName == "" {
		modelName = defaultModel
	}
	instruction := t.Instruction
	if instruction == "" {
		instruction = DefaultInstruction
	}
	modelOpts := []openai.Option{openai.WithAPIKey(t.Model.APIKey)}
	if t.Model.BaseURL != "" {
		modelOpts = append(modelOpts, openai.WithBaseURL(t.Model.BaseURL))
	}
	llm := openai.New(modelName, modelOpts...)

	allTools := append(platformtool.Select(t.Tools.Allowed), tools...)
	base := []llmagent.Option{
		llmagent.WithModel(llm),
		llmagent.WithInstruction(instruction),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: true}),
		llmagent.WithMaxLLMCalls(maxLLMCalls),
		llmagent.WithTools(allTools),
	}
	base = append(base, opts...)
	a := llmagent.New(AgentName, base...)

	return runner.NewRunner(appName, a,
		runner.WithSessionService(sess),
	), nil
}

// SkillOptions returns the llmagent options that mount one tenant's skill
// library, or nil when the deployment or the tenant has none. It is the
// legacy path's entry point; the reliable runner factory resolves the same
// options from a revision pin through SkillOptionsFor.
func SkillOptions(root, tenantID string) ([]llmagent.Option, error) {
	if root == "" {
		return nil, nil
	}
	repo, ok, err := platformskill.For(root, tenantID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return SkillOptionsFor(repo), nil
}

// SkillOptionsFor mounts one already-resolved repository.
func SkillOptionsFor(repo *frameworkskill.FSRepository) []llmagent.Option {
	return []llmagent.Option{
		llmagent.WithSkills(repo),
		// Explicit knowledge-only profile: this is the documented opt-out
		// from the framework's convenience executor fallback. Code execution
		// is a separate, explicitly configured capability (G5); adding skills
		// must never silently add a local executor.
		llmagent.WithSkillToolProfile(llmagent.SkillToolProfileKnowledgeOnly),
		llmagent.WithMaxLoadedSkills(8),
		llmagent.WithMaxOverviewSkills(64),
	}
}

// Registry holds one Runner per tenant over a shared session service.
// Tenant isolation at this stage is by separate Runner instances;
// per-tenant tool whitelists and guardrails will hang off the same lookup
// later. The mutex guards hot swaps driven by the Admin API; lookups only
// take a read lock.
type Registry struct {
	mu      sync.RWMutex
	runners map[string]runner.Runner
	def     string
	// sessionFor resolves one tenant's session service. It is re-queried on
	// every Apply, so a reload picks up re-routed backends (see storage.Router
	// for the legacy path's per-tenant backend selection).
	sessionFor func(tenantID string) session.Service
}

// NewRegistry builds a Runner for every tenant in the loaded config, all
// sharing sess. The backend choice is fixed at startup; switching backends
// means a config change plus restart (first-phase limitation, see
// docs/多后端适配方案.md).
func NewRegistry(cfg *config.Config, sess session.Service) (*Registry, error) {
	return NewRegistryWith(cfg, func(string) session.Service { return sess })
}

// NewRegistryWith is NewRegistry over a per-tenant session-service provider,
// which is how the legacy gateway path honours backend_profiles: each tenant
// may land on its own backend instead of the platform default.
func NewRegistryWith(cfg *config.Config, sessionFor func(tenantID string) session.Service) (*Registry, error) {
	r := &Registry{
		runners:    make(map[string]runner.Runner, len(cfg.Tenants)),
		def:        cfg.DefaultTenant,
		sessionFor: sessionFor,
	}
	for id, t := range cfg.Tenants {
		opts, err := SkillOptions(cfg.Skills.Root, id)
		if err != nil {
			return nil, fmt.Errorf("tenant %s: skills: %w", id, err)
		}
		rr, err := NewRunner(t, sessionFor(id), cfg.Agent.MaxLLMCalls, nil, opts...)
		if err != nil {
			return nil, err
		}
		r.runners[id] = rr
	}
	return r, nil
}

// Apply rebuilds the runner set for a new config (Admin API hot reload),
// reusing each tenant's session provider. New runners are fully built before
// the swap, so a build failure keeps the old set serving.
func (r *Registry) Apply(cfg *config.Config) error {
	runners := make(map[string]runner.Runner, len(cfg.Tenants))
	for id, t := range cfg.Tenants {
		opts, err := SkillOptions(cfg.Skills.Root, id)
		if err != nil {
			return fmt.Errorf("tenant %s: skills: %w", id, err)
		}
		rr, err := NewRunner(t, r.sessionFor(id), cfg.Agent.MaxLLMCalls, nil, opts...)
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
