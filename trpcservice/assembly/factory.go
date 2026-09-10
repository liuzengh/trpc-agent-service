// Package assembly builds tenant-scoped tRPC-Agent-Go runners.
package assembly

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	agentknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/model"
	approvalreview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// ToolProvider returns immutable tool wrappers for one configuration version.
// Request data belongs in the Runner call context, not in the returned tools.
type ToolProvider interface {
	Surface(context.Context, config.TenantConfig) (ToolSurface, error)
}

// KnowledgeProvider resolves the native trpc-agent-go knowledge surface for
// one immutable tenant configuration.
type KnowledgeProvider interface {
	Knowledge(context.Context, config.TenantConfig, model.Model) (agentknowledge.Knowledge, error)
}

// ArtifactProvider resolves the framework artifact service for one immutable
// tenant Runner configuration.
type ArtifactProvider interface {
	ArtifactService(context.Context, config.TenantConfig) (agentartifact.Service, error)
}

const maxCachedRunnerVersionsPerApplication = 2

// Factory creates and caches runners whose configuration snapshots are safe to
// share. Runtime callers acquire a lease so idle historical versions can be
// evicted without closing a Runner that still has an in-flight invocation.
type Factory struct {
	model            model.Model
	models           ModelProvider
	tools            ToolProvider
	sessions         SessionProvider
	knowledge        KnowledgeProvider
	memory           MemoryProvider
	artifacts        ArtifactProvider
	callbacks        *agenttool.Callbacks
	approvalReviewer approvalreview.Reviewer
	documentInputs   bool

	mu      sync.Mutex
	entries map[string]*runnerCacheEntry
	clock   uint64
}

type FactoryOption func(*Factory)

func WithApprovalReviewer(reviewer approvalreview.Reviewer) FactoryOption {
	return func(factory *Factory) {
		if reviewer != nil {
			factory.approvalReviewer = reviewer
		}
	}
}

func WithDocumentInputSupport(enabled bool) FactoryOption {
	return func(factory *Factory) {
		factory.documentInputs = enabled
	}
}

type runnerResources struct {
	memory    func() error
	knowledge agentknowledge.Knowledge
	toolSets  []agenttool.ToolSet
}

type runnerCacheEntry struct {
	runner    runner.Runner
	resources runnerResources
	appName   string
	version   uint64
	refs      int
	pinned    bool
	lastUsed  uint64
}

func (r runnerResources) close(key string) error {
	var errs []error
	if r.memory != nil {
		if err := r.memory(); err != nil {
			errs = append(errs, fmt.Errorf("close memory service %q: %w", key, err))
		}
	}
	if closer, ok := r.knowledge.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close knowledge base %q: %w", key, err))
		}
	}
	for _, toolSet := range r.toolSets {
		if toolSet == nil {
			continue
		}
		if err := toolSet.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close tool set %q: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

// NewFactory constructs a factory around one injected model implementation.
func NewFactory(modelInstance model.Model) *Factory {
	return &Factory{
		model: modelInstance, entries: make(map[string]*runnerCacheEntry),
	}
}

func NewFactoryWithModelProvider(models ModelProvider, tools ToolProvider, sessions SessionProvider, knowledge KnowledgeProvider, memoryProvider MemoryProvider, artifacts ArtifactProvider, callbacks *agenttool.Callbacks, options ...FactoryOption) *Factory {
	factory := &Factory{
		models: models, tools: tools, sessions: sessions, knowledge: knowledge,
		memory: memoryProvider, artifacts: artifacts, callbacks: callbacks, entries: make(map[string]*runnerCacheEntry),
	}
	for _, option := range options {
		if option != nil {
			option(factory)
		}
	}
	return factory
}

// Get returns a pinned Runner for callers that manage the Runner lifetime at
// the Factory level. Runtime execution should use Acquire so old versions can
// be reclaimed when they become idle.
func (f *Factory) Get(ctx context.Context, tenantConfig config.TenantConfig) (runner.Runner, error) {
	entry, err := f.getOrCreate(ctx, tenantConfig, true)
	if err != nil {
		return nil, err
	}
	return entry.runner, nil
}

// Acquire returns a Runner plus a release function for one in-flight Runtime
// invocation. At most two idle configuration versions are retained per
// application so stable/canary operation remains warm without unbounded growth.
func (f *Factory) Acquire(ctx context.Context, tenantConfig config.TenantConfig) (runner.Runner, func(), error) {
	entry, err := f.getOrCreate(ctx, tenantConfig, false)
	if err != nil {
		return nil, nil, err
	}
	key := runnerCacheKey(tenantConfig)
	var once sync.Once
	release := func() {
		once.Do(func() { f.release(key, entry) })
	}
	return entry.runner, release, nil
}

func (f *Factory) getOrCreate(ctx context.Context, tenantConfig config.TenantConfig, pin bool) (*runnerCacheEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.model == nil && f.models == nil {
		return nil, fmt.Errorf("runner factory model is required")
	}
	if err := validateRunnableConfig(tenantConfig); err != nil {
		return nil, err
	}

	key := runnerCacheKey(tenantConfig)
	f.mu.Lock()
	if existing := f.entries[key]; existing != nil {
		f.markUsedLocked(existing, pin)
		f.mu.Unlock()
		return existing, nil
	}
	f.mu.Unlock()

	created, resources, err := f.buildRunner(ctx, tenantConfig, key)
	if err != nil {
		return nil, err
	}
	candidate := &runnerCacheEntry{
		runner: created, resources: resources, appName: tenantConfig.AppName(), version: tenantConfig.ConfigVersion,
	}

	f.mu.Lock()
	if existing := f.entries[key]; existing != nil {
		f.markUsedLocked(existing, pin)
		f.mu.Unlock()
		if err := closeRunnerEntry(key, candidate); err != nil {
			slog.Warn("close duplicate tenant runner", "runner", key, "error", err)
		}
		return existing, nil
	}
	f.markUsedLocked(candidate, pin)
	f.entries[key] = candidate
	evicted := f.evictIdleLocked(candidate.appName)
	f.mu.Unlock()
	closeEvictedRunners(evicted)
	return candidate, nil
}

func (f *Factory) markUsedLocked(entry *runnerCacheEntry, pin bool) {
	f.clock++
	entry.lastUsed = f.clock
	if pin {
		entry.pinned = true
		return
	}
	entry.refs++
}

func (f *Factory) release(key string, expected *runnerCacheEntry) {
	f.mu.Lock()
	entry := f.entries[key]
	if entry == nil || entry != expected {
		f.mu.Unlock()
		return
	}
	if entry.refs > 0 {
		entry.refs--
	}
	f.clock++
	entry.lastUsed = f.clock
	evicted := f.evictIdleLocked(entry.appName)
	f.mu.Unlock()
	closeEvictedRunners(evicted)
}

type evictedRunner struct {
	key   string
	entry *runnerCacheEntry
}

func (f *Factory) evictIdleLocked(appName string) []evictedRunner {
	count := 0
	for _, entry := range f.entries {
		if entry != nil && entry.appName == appName {
			count++
		}
	}
	var evicted []evictedRunner
	for count > maxCachedRunnerVersionsPerApplication {
		var oldestKey string
		var oldest *runnerCacheEntry
		for key, entry := range f.entries {
			if entry == nil || entry.appName != appName || entry.pinned || entry.refs != 0 {
				continue
			}
			if oldest == nil || entry.lastUsed < oldest.lastUsed {
				oldestKey, oldest = key, entry
			}
		}
		if oldest == nil {
			break
		}
		delete(f.entries, oldestKey)
		evicted = append(evicted, evictedRunner{key: oldestKey, entry: oldest})
		count--
	}
	return evicted
}

func closeEvictedRunners(entries []evictedRunner) {
	for _, evicted := range entries {
		if err := closeRunnerEntry(evicted.key, evicted.entry); err != nil {
			slog.Warn("close evicted tenant runner", "runner", evicted.key, "error", err)
		}
	}
}

func closeRunnerEntry(key string, entry *runnerCacheEntry) error {
	if entry == nil {
		return nil
	}
	var errs []error
	if entry.runner != nil {
		if err := entry.runner.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close runner %q: %w", key, err))
		}
	}
	if err := entry.resources.close(key); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (f *Factory) buildRunner(ctx context.Context, tenantConfig config.TenantConfig, key string) (runner.Runner, runnerResources, error) {

	configuredModel := f.model
	if f.models != nil {
		var err error
		configuredModel, err = f.models.Model(ctx, tenantConfig)
		if err != nil {
			return nil, runnerResources{}, fmt.Errorf("build tenant model: %w", err)
		}
	}

	var sessionService session.Service
	if f.sessions != nil {
		var err error
		sessionService, err = f.sessions.Session(ctx, tenantConfig)
		if err != nil {
			return nil, runnerResources{}, fmt.Errorf("build tenant Session service: %w", err)
		}
	}

	var configuredSurface ToolSurface
	if f.tools != nil {
		var err error
		configuredSurface, err = f.tools.Surface(ctx, tenantConfig)
		if err != nil {
			return nil, runnerResources{}, fmt.Errorf("build governed tools: %w", err)
		}
	}
	resources := runnerResources{toolSets: configuredSurface.ToolSets}

	var knowledge agentknowledge.Knowledge
	if f.knowledge != nil {
		var err error
		knowledge, err = f.knowledge.Knowledge(ctx, tenantConfig, configuredModel)
		if err != nil {
			_ = resources.close(key)
			return nil, runnerResources{}, fmt.Errorf("build tenant knowledge: %w", err)
		}
		resources.knowledge = knowledge
	}

	var memoryBackend MemoryBackend
	if f.memory != nil {
		var err error
		memoryBackend, err = f.memory.MemoryBackend(ctx, tenantConfig, configuredModel)
		if err != nil {
			_ = resources.close(key)
			return nil, runnerResources{}, fmt.Errorf("build tenant memory: %w", err)
		}
		resources.memory = memoryBackend.Release
	}

	var artifactService agentartifact.Service
	if f.artifacts != nil {
		var err error
		artifactService, err = f.artifacts.ArtifactService(ctx, tenantConfig)
		if err != nil {
			_ = resources.close(key)
			return nil, runnerResources{}, fmt.Errorf("build tenant artifact service: %w", err)
		}
	}

	tools := append([]agenttool.Tool(nil), configuredSurface.Tools...)
	if len(memoryBackend.Tools) > 0 {
		tools = append(tools, memoryBackend.Tools...)
	}
	if artifactService != nil {
		tools = append(tools, platformtool.NewSaveArtifactTool())
	}
	agentOptions := []llmagent.Option{
		llmagent.WithModel(configuredModel),
		llmagent.WithTools(tools),
		llmagent.WithAddCurrentTime(true),
		llmagent.WithTimezone("UTC"),
		llmagent.WithAddSessionSummary(true),
		llmagent.WithMessageBranchFilterMode(llmagent.BranchFilterModeAll),
		llmagent.WithEnableContextCompaction(true),
	}
	if len(configuredSurface.ToolSets) > 0 {
		agentOptions = append(agentOptions, llmagent.WithToolSets(configuredSurface.ToolSets))
	}
	if instruction := f.runtimeInstruction(tenantConfig); instruction != "" {
		agentOptions = append(agentOptions, llmagent.WithInstruction(instruction))
	}
	if f.callbacks != nil {
		agentOptions = append(agentOptions, llmagent.WithToolCallbacks(f.callbacks))
	}
	if knowledge != nil {
		agentOptions = append(agentOptions, llmagent.WithKnowledge(knowledge))
	}
	if memoryBackend.Service != nil {
		agentOptions = append(agentOptions,
			llmagent.WithPreloadMemory(preloadMemoryBudget),
			llmagent.WithPreloadMemoryInjectionMode(llmagent.PreloadMemoryInjectionUser),
		)
	}
	genConfig := model.GenerationConfig{Stream: true}
	if tenantConfig.Model.Generation != nil {
		genConfig = *tenantConfig.Model.Generation
		genConfig.Stream = true
	}
	agentOptions = append(agentOptions, llmagent.WithGenerationConfig(genConfig))
	agent := llmagent.New("assistant", agentOptions...)
	var options []runner.Option
	if sessionService != nil {
		options = append(options, runner.WithSessionService(sessionService))
	}
	if memoryBackend.Service != nil {
		options = append(options, runner.WithMemoryService(memoryBackend.Service))
	}
	if memoryBackend.Ingestor != nil {
		options = append(options, runner.WithSessionIngestor(memoryBackend.Ingestor))
	}
	if artifactService != nil {
		options = append(options, runner.WithArtifactService(artifactService))
	}
	guard, err := tenantGuardrailPlugin(tenantConfig, f.approvalReviewer)
	if err != nil {
		_ = resources.close(key)
		return nil, runnerResources{}, fmt.Errorf("build tenant guardrail: %w", err)
	}
	if guard != nil {
		options = append(options, runner.WithPlugins(guard))
	}
	created := runner.NewRunner(tenantConfig.AppName(), agent, options...)
	return created, resources, nil
}

func (f *Factory) runtimeInstruction(tenantConfig config.TenantConfig) string {
	instruction := strings.TrimSpace(tenantConfig.Instruction)
	capabilities, known := config.ModelInputCapabilities{}, false
	if provider, ok := f.models.(ModelInputCapabilityProvider); ok {
		capabilities, known = provider.GuaranteedInputCapabilities(tenantConfig.Model)
	}

	accepted := []string{"纯文本，以及 txt、md 和常见代码/配置类文本附件"}
	if f.documentInputs {
		accepted = append(accepted, "PDF、Word（doc/docx）、Excel（xlsx）、PPTX、HTML、CSV 等常见文档附件")
	}
	if known && capabilities.Image {
		accepted = append(accepted, "图片附件；收到照片时可直接查看并分析图片内容")
	}
	if known && capabilities.Audio {
		accepted = append(accepted, "MP3、WAV 音频附件")
	}
	if known && capabilities.File {
		accepted = append(accepted, "当前模型明确支持的其他文件附件")
	}

	platform := "附件能力由平台决定，不要根据模型名称猜测，也不要自行缩小支持范围。当前可处理：" + strings.Join(accepted, "；") + "。未列出的格式不要声称支持；压缩包等归档文件不支持。"
	if instruction == "" {
		return platform
	}
	return instruction + "\n\n" + platform
}

// Close releases framework services owned by cached tenant Runners.
func (f *Factory) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	entries := f.entries
	f.entries = make(map[string]*runnerCacheEntry)
	f.mu.Unlock()
	errs := make([]error, 0, len(entries))
	for key, entry := range entries {
		if err := closeRunnerEntry(key, entry); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func runnerCacheKey(tenantConfig config.TenantConfig) string {
	return fmt.Sprintf("%s@%d", tenantConfig.AppName(), tenantConfig.ConfigVersion)
}

func validateRunnableConfig(tenantConfig config.TenantConfig) error {
	if strings.TrimSpace(tenantConfig.TenantID) == "" || strings.ContainsAny(tenantConfig.TenantID, "/\\") {
		return fmt.Errorf("runner configuration has an invalid tenant_id")
	}
	if strings.TrimSpace(tenantConfig.AppCode) == "" || strings.ContainsAny(tenantConfig.AppCode, "/\\") {
		return fmt.Errorf("runner configuration has an invalid app_code")
	}
	if tenantConfig.Status != config.AgentActive {
		return fmt.Errorf("runner configuration is not active")
	}
	if tenantConfig.ConfigVersion == 0 {
		return fmt.Errorf("runner configuration version must be positive")
	}
	return nil
}
