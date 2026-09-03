// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	memorytool "trpc.group/trpc-go/trpc-agent-go/memory/tool"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

// Catalog is the process-wide set of code-defined tools. Tenant revisions can
// only narrow this catalog; they cannot inject arbitrary executable code.
type Catalog struct {
	mu    sync.RWMutex
	tools map[string]agenttool.Tool
}

func NewCatalog(tools ...agenttool.Tool) (*Catalog, error) {
	catalog := &Catalog{tools: make(map[string]agenttool.Tool)}
	for _, item := range tools {
		if item == nil || item.Declaration() == nil || item.Declaration().Name == "" {
			return nil, fmt.Errorf("tool declaration and name are required")
		}
		name := item.Declaration().Name
		if _, exists := catalog.tools[name]; exists {
			return nil, fmt.Errorf("tool %q is duplicated", name)
		}
		catalog.tools[name] = item
	}
	return catalog, nil
}

func (c *Catalog) Resolve(names []string) ([]agenttool.Tool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]agenttool.Tool, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, exists := seen[name]; exists {
			continue
		}
		item := c.tools[name]
		if item == nil {
			return nil, fmt.Errorf("tool %q is not registered", name)
		}
		seen[name] = struct{}{}
		result = append(result, item)
	}
	return result, nil
}

func (c *Catalog) Names() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]string, 0, len(c.tools))
	for name := range c.tools {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

type EchoInput struct {
	Message string `json:"message" jsonschema:"description=Text to echo back"`
}

type EchoOutput struct {
	Message string `json:"message"`
}

type TimeInput struct{}
type TimeOutput struct {
	UTC string `json:"utc"`
}

type DangerousDemoInput struct {
	Action string `json:"action" jsonschema:"description=Demonstration action requiring approval"`
}
type DangerousDemoOutput struct {
	Accepted bool   `json:"accepted"`
	Action   string `json:"action"`
}

func DefaultCatalog() *Catalog {
	catalog, err := NewCatalog(
		function.NewFunctionTool(
			func(_ context.Context, input EchoInput) (EchoOutput, error) {
				return EchoOutput(input), nil
			},
			function.WithName("echo"),
			function.WithDescription("Echo text back to the caller"),
		),
		function.NewFunctionTool(
			func(_ context.Context, _ TimeInput) (TimeOutput, error) {
				return TimeOutput{UTC: time.Now().UTC().Format(time.RFC3339)}, nil
			},
			function.WithName("current_time"),
			function.WithDescription("Return the current UTC time"),
		),
		function.NewFunctionTool(
			func(_ context.Context, input DangerousDemoInput) (DangerousDemoOutput, error) {
				return DangerousDemoOutput{Accepted: true, Action: input.Action}, nil
			},
			function.WithName("dangerous_demo"),
			function.WithDescription("Demonstrate an operation that requires explicit approval"),
		),
		memorytool.NewAddTool(),
		memorytool.NewUpdateTool(),
		memorytool.NewDeleteTool(),
		memorytool.NewClearTool(),
		memorytool.NewSearchTool(),
		memorytool.NewLoadTool(),
	)
	if err != nil {
		panic(err)
	}
	return catalog
}
