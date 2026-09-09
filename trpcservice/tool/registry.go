package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const DangerousEchoName = "phase6.dangerous_echo"

type Registry struct {
	mu    sync.RWMutex
	tools map[string]frameworktool.CallableTool
}

func NewRegistry() *Registry {
	r := &Registry{tools: make(map[string]frameworktool.CallableTool)}
	r.Register(&dangerousEchoTool{})
	return r
}

func (r *Registry) Register(value frameworktool.CallableTool) error {
	if r == nil || value == nil || value.Declaration() == nil || value.Declaration().Name == "" {
		return errors.New("tool and declaration are required")
	}
	name := value.Declaration().Name
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tools[name]; exists {
		return fmt.Errorf("tool %q is already registered", name)
	}
	r.tools[name] = value
	return nil
}

func (r *Registry) Get(name string) (frameworktool.CallableTool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.tools[name]
	return value, ok
}

func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) NonDangerousNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		if name != DangerousEchoName {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (r *Registry) Tools(allowed map[string]struct{}) []frameworktool.Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]frameworktool.Tool, 0, len(allowed))
	for name := range allowed {
		if value, ok := r.tools[name]; ok {
			result = append(result, value)
		}
	}
	return result
}

type dangerousEchoTool struct{}

func (*dangerousEchoTool) Declaration() *frameworktool.Declaration {
	return &frameworktool.Declaration{
		Name:        DangerousEchoName,
		Description: "Echoes a caller supplied value; requires tenant confirmation.",
		InputSchema: &frameworktool.Schema{Type: "object", Required: []string{"value"}, Properties: map[string]*frameworktool.Schema{"value": {Type: "string"}}},
	}
}

func (*dangerousEchoTool) Call(_ context.Context, args []byte) (any, error) {
	var input struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return nil, errors.New("invalid dangerous_echo arguments")
	}
	return input.Value, nil
}
