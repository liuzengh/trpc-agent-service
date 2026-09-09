// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

// Entry holds execution metadata not represented by the framework Tool.
type Entry struct {
	Tool      agenttool.Tool
	Dangerous bool
}

// Registry is the platform-wide tool source filtered later by tenant policy.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

// NewRegistry constructs an empty registry.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]Entry)}
}

// Register adds one uniquely named tool.
func (r *Registry) Register(entry Entry) error {
	if entry.Tool == nil || entry.Tool.Declaration() == nil {
		return errors.New("tool and declaration are required")
	}
	name := entry.Tool.Declaration().Name
	if name == "" {
		return errors.New("tool name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[name]; exists {
		return fmt.Errorf("tool %q is already registered", name)
	}
	r.entries[name] = entry
	return nil
}

// Tools implements worker.ToolProvider. Tenant filtering is intentionally done
// in Worker after this full list is returned.
func (r *Registry) Tools(context.Context, tenant.Snapshot) ([]agenttool.Tool, error) {
	r.mu.RLock()
	names := make([]string, 0, len(r.entries))
	for name := range r.entries {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]agenttool.Tool, 0, len(names))
	for _, name := range names {
		result = append(result, r.entries[name].Tool)
	}
	r.mu.RUnlock()
	return result, nil
}

// IsDangerous reports whether a registered tool requires confirmation.
func (r *Registry) IsDangerous(name string) bool {
	r.mu.RLock()
	entry, ok := r.entries[name]
	r.mu.RUnlock()
	return ok && entry.Dangerous
}

// Lookup returns a registered tool by name.
func (r *Registry) Lookup(name string) (agenttool.Tool, bool) {
	r.mu.RLock()
	entry, ok := r.entries[name]
	r.mu.RUnlock()
	return entry.Tool, ok
}
