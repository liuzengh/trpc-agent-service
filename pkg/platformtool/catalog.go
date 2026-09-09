// Package platformtool owns the platform-approved tool catalog. Tenant Agent
// configuration selects names from this catalog; arbitrary package paths or
// executable commands can never be supplied through tenant JSON.
package platformtool

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// managedMemoryToolNames are framework-provided memory capabilities. They are
// resolved from the tenant-selected memory.Service at Worker construction,
// not registered as process-global tools, because each tenant has a different
// backend and namespace.
var managedMemoryToolNames = map[string]struct{}{
	"memory_add":    {},
	"memory_update": {},
	"memory_search": {},
	"memory_load":   {},
	"memory_delete": {},
	"memory_clear":  {},
}

const KnowledgeSearchToolName = "knowledge_search"

// IsManagedMemoryTool reports whether name is a known tRPC memory capability.
func IsManagedMemoryTool(name string) bool {
	_, ok := managedMemoryToolNames[name]
	return ok
}

// IsManagedKnowledgeTool reports the framework-injected Knowledge search
// capability. It is selected explicitly in AgentConfig.Tools but constructed
// from the tenant-scoped vector profile rather than the static tool catalog.
func IsManagedKnowledgeTool(name string) bool { return name == KnowledgeSearchToolName }

// IsFrameworkManagedTool identifies capabilities that Admin must validate
// against a runtime service instead of the process-global static catalog.
func IsFrameworkManagedTool(name string) bool {
	return IsManagedMemoryTool(name) || IsManagedKnowledgeTool(name)
}

// Catalog resolves immutable, operator-registered tools by exact name.
type Catalog struct {
	mu    sync.RWMutex
	tools map[string]tool.Tool
}

// NewCatalog creates an empty fail-closed catalog.
func NewCatalog() *Catalog {
	return &Catalog{tools: make(map[string]tool.Tool)}
}

// NewBuiltinCatalog returns the small non-destructive default capability set.
// Production integrations register business and MCP tools explicitly during
// composition, never from untrusted tenant configuration.
func NewBuiltinCatalog() *Catalog {
	catalog := NewCatalog()
	if err := catalog.Register(&currentTimeTool{}); err != nil {
		panic(err)
	}
	return catalog
}

// Register adds a tool by its declaration name and rejects ambiguous entries.
func (c *Catalog) Register(value tool.Tool) error {
	declaration, ok := safeDeclaration(value)
	if !ok {
		return fmt.Errorf("tool declaration name is required")
	}
	name := declaration.Name
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tools[name]; exists {
		return fmt.Errorf("tool %q is already registered", name)
	}
	c.tools[name] = value
	return nil
}

func safeDeclaration(value tool.Tool) (declaration *tool.Declaration, ok bool) {
	if isNilTool(value) {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			declaration, ok = nil, false
		}
	}()
	declaration = value.Declaration()
	return declaration, declaration != nil && declaration.Name != ""
}

func isNilTool(value tool.Tool) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Resolve returns tools in requested order. Unknown and duplicate names are
// configuration errors rather than silently reducing the Agent's capability.
func (c *Catalog) Resolve(names []string) ([]tool.Tool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]tool.Tool, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("tool %q is configured more than once", name)
		}
		value, exists := c.tools[name]
		if !exists {
			return nil, fmt.Errorf("tool %q is not registered by the platform", name)
		}
		seen[name] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

type currentTimeTool struct{}

func (*currentTimeTool) Declaration() *tool.Declaration {
	return &tool.Declaration{
		Name:        "current_time",
		Description: "Return the current time in an IANA timezone; defaults to UTC.",
	}
}

func (*currentTimeTool) Call(_ context.Context, arguments []byte) (any, error) {
	request := struct {
		Timezone string `json:"timezone"`
	}{Timezone: "UTC"}
	if len(arguments) > 0 && string(arguments) != "{}" {
		if err := json.Unmarshal(arguments, &request); err != nil {
			return nil, fmt.Errorf("decode current_time arguments: %w", err)
		}
	}
	location, err := time.LoadLocation(request.Timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid IANA timezone %q", request.Timezone)
	}
	now := time.Now().In(location)
	return map[string]string{
		"timezone": request.Timezone,
		"time":     now.Format(time.RFC3339),
	}, nil
}
