package trpcagent

import (
	"fmt"
	"reflect"

	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// CapabilityConfig describes explicit, already-resolved node capabilities. This
// assembly foundation does not open the Manifest gate or create any backend.
// Session summary storage and generation must be supported by the caller's
// Session service before AddSessionSummary is enabled.
type CapabilityConfig struct {
	MemoryTools []string
	// MemoryPreloadLimit follows SDK semantics: -1 loads all, 0 disables preload,
	// positive values bound the adaptive preload entry count. No private cap.
	MemoryPreloadLimit int
	AddSessionSummary  bool
	Artifact           bool
	Knowledge          bool
}

// CapabilityServices contains borrowed, final SDK services. Memory must bind the
// trusted tenant/subject/agent scope and implement Attempt semantics; SDK session
// identity is not the Memory identity. Its Tools must call that same final service,
// never a raw backend underneath an Attempt wrapper. The caller owns lifecycle,
// authorization, persistence, and any automatic-extraction policy.
type CapabilityServices struct {
	ArtifactTools []tool.Tool
	Memory        memory.Service
	Artifact      artifact.Service
	Knowledge     knowledge.Knowledge
}

// CapabilityOptions composes with the caller's model/session options. Memory
// tools append to existing ordinary tools; callers must keep their names disjoint
// and must not overwrite the capability options later with broader services.
type CapabilityOptions struct {
	Agent  []llmagent.Option
	Runner []runner.Option
}

// BuildCapabilityOptions validates the complete selection before exposing any
// options. A service merely being available never enables its capability. Empty
// MemoryTools and zero preload do not even call Memory.Tools().
func BuildCapabilityOptions(cfg CapabilityConfig, services CapabilityServices) (CapabilityOptions, error) {
	fail := func(message string) (CapabilityOptions, error) {
		return CapabilityOptions{}, fmt.Errorf("SDK capability assembly: %s", message)
	}
	if cfg.MemoryPreloadLimit < -1 {
		return fail("memory preload limit must be -1, zero, or positive")
	}
	requested := make(map[string]bool, len(cfg.MemoryTools))
	for _, name := range cfg.MemoryTools {
		if !memoryToolName(name) {
			return fail("unsupported memory tool")
		}
		if requested[name] {
			return fail("duplicate memory tool selection")
		}
		requested[name] = true
	}
	memoryEnabled := len(requested) > 0 || cfg.MemoryPreloadLimit != 0
	if memoryEnabled && nilCapabilityService(services.Memory) {
		return fail("memory service required")
	}
	if cfg.Artifact && nilCapabilityService(services.Artifact) {
		return fail("artifact service required")
	}
	if cfg.Knowledge && nilCapabilityService(services.Knowledge) {
		return fail("knowledge service required")
	}

	var selected []tool.Tool
	if len(requested) > 0 {
		available := make(map[string]tool.Tool)
		for _, candidate := range services.Memory.Tools() {
			if nilCapabilityService(candidate) {
				return fail("nil memory tool")
			}
			declaration := candidate.Declaration()
			if declaration == nil || !memoryToolName(declaration.Name) {
				return fail("unsupported memory tool declaration")
			}
			if _, duplicate := available[declaration.Name]; duplicate {
				return fail("duplicate memory tool declaration")
			}
			available[declaration.Name] = candidate
		}
		for _, name := range cfg.MemoryTools {
			candidate, exists := available[name]
			if !exists {
				return fail("selected memory tool unavailable")
			}
			selected = append(selected, candidate)
		}
	}
	if cfg.Artifact {
		selected = append(selected, services.ArtifactTools...)
	}
	var ms memory.Service
	var as artifact.Service
	var ks knowledge.Knowledge
	if memoryEnabled {
		ms = services.Memory
	}
	if cfg.Artifact {
		as = services.Artifact
	}
	if cfg.Knowledge {
		ks = services.Knowledge
	}
	result := CapabilityOptions{
		Agent: []llmagent.Option{
			llmagent.WithPreloadMemory(cfg.MemoryPreloadLimit),
			llmagent.WithAddSessionSummary(cfg.AddSessionSummary),
			llmagent.WithKnowledge(ks),
		},
		Runner: []runner.Option{runner.WithMemoryService(ms), runner.WithArtifactService(as)},
	}
	if len(selected) > 0 {
		result.Agent = append(result.Agent, func(opts *llmagent.Options) {
			// Copy so reused options and caller-owned slices do not share append storage.
			combined := make([]tool.Tool, 0, len(opts.Tools)+len(selected))
			combined = append(combined, opts.Tools...)
			opts.Tools = append(combined, selected...)
		})
	}
	return result, nil
}

func memoryToolName(name string) bool {
	switch name {
	case memory.AddToolName, memory.UpdateToolName, memory.DeleteToolName,
		memory.ClearToolName, memory.SearchToolName, memory.LoadToolName:
		return true
	default:
		return false
	}
}

// SDK services and tools are interfaces, so a typed nil pointer must also fail
// validation instead of becoming a deferred panic in the Runner.
func nilCapabilityService(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
