package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type executionState struct {
	mu         sync.Mutex
	toolEvents []RunnerEvent
	err        error
}

func (s *executionState) recordError(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	} else {
		s.err = errors.Join(s.err, err)
	}
}

func (s *executionState) addToolEvents(events []RunnerEvent) {
	if s == nil || len(events) == 0 {
		return
	}
	s.mu.Lock()
	s.toolEvents = append(s.toolEvents, events...)
	s.mu.Unlock()
}

func (s *executionState) snapshot() (error, []RunnerEvent) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err, append([]RunnerEvent(nil), s.toolEvents...)
}

type frameworkToolBridge struct {
	spec   ToolSpec
	invoke func(context.Context, map[string]any) (ToolResult, error)
}

func (t *frameworkToolBridge) Declaration() *frameworktool.Declaration {
	return &frameworktool.Declaration{
		Name:        t.spec.Name,
		Description: t.spec.Description,
		InputSchema: schemaFromValue(t.spec.InputSchema),
	}
}

func (t *frameworkToolBridge) Call(ctx context.Context, raw []byte) (any, error) {
	arguments := map[string]any{}
	if len(strings.TrimSpace(string(raw))) > 0 {
		if err := json.Unmarshal(raw, &arguments); err != nil {
			return nil, fmt.Errorf("%w: invalid tool arguments: %w", ErrInvalidInput, err)
		}
	}
	if t.invoke == nil {
		return nil, fmt.Errorf("%w: tool bridge is not configured", ErrToolFailure)
	}
	result, err := t.invoke(ctx, arguments)
	if err != nil {
		return nil, err
	}
	if result.IsError {
		return result.Content, fmt.Errorf("%w: tool returned an error result", ErrToolFailure)
	}
	return result.Content, nil
}

func newFrameworkTools(spec AgentSpec, invoker ToolInvoker, tc tenantContext, state *executionState) []frameworktool.Tool {
	if invoker == nil {
		return nil
	}
	tools := make([]frameworktool.Tool, 0, len(spec.Tools))
	for _, toolSpec := range spec.Tools {
		toolSpec := toolSpec
		if strings.TrimSpace(toolSpec.Name) == "" {
			continue
		}
		tools = append(tools, &frameworkToolBridge{
			spec: toolSpec,
			invoke: func(ctx context.Context, arguments map[string]any) (ToolResult, error) {
				result, events, err := invokeTool(ctx, invoker, tc, spec, toolSpec.Name, arguments)
				state.addToolEvents(events)
				if err != nil {
					state.recordError(err)
				}
				return result, err
			},
		})
	}
	return tools
}

func schemaFromValue(value map[string]any) *frameworktool.Schema {
	schema := &frameworktool.Schema{Type: "object", AdditionalProperties: true}
	if len(value) == 0 {
		return schema
	}
	if rawType, ok := value["type"].(string); ok && rawType != "" {
		schema.Type = rawType
	}
	if description, ok := value["description"].(string); ok {
		schema.Description = description
	}
	if required, ok := value["required"].([]any); ok {
		for _, item := range required {
			if name, ok := item.(string); ok {
				schema.Required = append(schema.Required, name)
			}
		}
	}
	if properties, ok := value["properties"].(map[string]any); ok {
		schema.Properties = make(map[string]*frameworktool.Schema, len(properties))
		for name, property := range properties {
			if propertyMap, ok := property.(map[string]any); ok {
				schema.Properties[name] = schemaFromValue(propertyMap)
			}
		}
	}
	return schema
}

type modelToolDeclaration struct{ declaration *frameworktool.Declaration }

func (t modelToolDeclaration) Declaration() *frameworktool.Declaration { return t.declaration }
