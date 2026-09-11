package trpcagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var ErrCallable = errors.New("callable binding invalid")
var ErrKnowledgeCallable = ErrCallable
var knowledgeEntryPattern = regexp.MustCompile(`^knowledge/[a-z][a-z0-9_-]{0,63}$`)
var callableEntryPattern = regexp.MustCompile(`^(knowledge|tools)/[a-z][a-z0-9_-]{0,63}$`)

const sdkKnowledgeName = "knowledge_search"

type knowledgeCallableModel struct {
	inner     model.Model
	providers map[string]string
}

// WrapKnowledgeModel keeps the actual WithKnowledge SDK tool unchanged. Only
// the model boundary translates its name to provider-callable-name-v1.
// resource is the complete immutable logical entry, e.g. "knowledge/docs".
func WrapKnowledgeModel(inner model.Model, resource string) (model.Model, error) {
	if inner == nil || !knowledgeEntryPattern.MatchString(resource) {
		return nil, ErrKnowledgeCallable
	}
	return WrapCallableModel(inner, map[string]string{sdkKnowledgeName: resource})
}

// WrapCallableModel maps only explicit logical Manifest entries at the provider
// boundary. SDK execution and persisted history retain stable internal names.
func WrapCallableModel(inner model.Model, bindings map[string]string) (model.Model, error) {
	if inner == nil || len(bindings) == 0 {
		return nil, ErrCallable
	}
	mapped := map[string]string{}
	seen := map[string]bool{}
	for alias, entry := range bindings {
		if alias == "" || !callableEntryPattern.MatchString(entry) || seen[entry] {
			return nil, ErrCallable
		}
		seen[entry] = true
		sum := sha256.Sum256([]byte(entry))
		mapped[alias] = "fn_" + hex.EncodeToString(sum[:])[:60]
	}
	return &knowledgeCallableModel{inner: inner, providers: mapped}, nil
}

func (m *knowledgeCallableModel) Info() model.Info { return m.inner.Info() }

type providerDeclaration struct {
	tool.Tool
	name string
}

func (t providerDeclaration) Declaration() *tool.Declaration {
	d := *t.Tool.Declaration()
	d.Name = t.name
	return &d
}
func (m *knowledgeCallableModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	if req == nil {
		return nil, ErrKnowledgeCallable
	}
	copy := *req
	copy.Tools = make(map[string]tool.Tool, len(req.Tools))
	allowed := map[string]string{}
	for name, t := range req.Tools {
		if t == nil || t.Declaration() == nil || t.Declaration().Name != name {
			return nil, ErrKnowledgeCallable
		}
		provider := name
		if fixed, ok := m.providers[name]; ok {
			provider = fixed
		}
		if _, collision := allowed[provider]; collision {
			return nil, ErrCallable
		}
		copy.Tools[provider] = providerDeclaration{Tool: t, name: provider}
		allowed[provider] = name
	}
	for alias, provider := range m.providers {
		if allowed[provider] != alias {
			return nil, ErrCallable
		}
	}
	copy.Messages = append([]model.Message(nil), req.Messages...)
	for i := range copy.Messages {
		message := &copy.Messages[i]
		if message.ToolName != "" {
			if _, ok := req.Tools[message.ToolName]; !ok {
				return nil, ErrKnowledgeCallable
			}
		}
		if provider, ok := m.providers[message.ToolName]; ok {
			message.ToolName = provider
		}
		message.ToolCalls = append([]model.ToolCall(nil), message.ToolCalls...)
		for j := range message.ToolCalls {
			if _, ok := req.Tools[message.ToolCalls[j].Function.Name]; !ok {
				return nil, ErrKnowledgeCallable
			}
			if provider, ok := m.providers[message.ToolCalls[j].Function.Name]; ok {
				message.ToolCalls[j].Function.Name = provider
			}
		}
	}
	streamCtx, cancel := context.WithCancel(ctx)
	input, err := m.inner.GenerateContent(streamCtx, &copy)
	if err != nil {
		cancel()
		return nil, err
	}
	if input == nil {
		cancel()
		return nil, ErrKnowledgeCallable
	}
	output := make(chan *model.Response)
	go func() {
		defer close(output)
		defer cancel()
		// Names are deltas, while the SDK also yields a final assembled Message.
		fragments := map[[2]int]string{}
		emit := func(r *model.Response) bool {
			select {
			case output <- r:
				return true
			case <-ctx.Done():
				return false
			}
		}
		reject := func() {
			emit(&model.Response{Error: &model.ResponseError{Type: "callable_binding", Message: ErrKnowledgeCallable.Error()}, Done: true})
		}
		complete := func() bool {
			for _, name := range fragments {
				if _, ok := allowed[name]; !ok {
					return false
				}
			}
			return true
		}
		for {
			var response *model.Response
			var ok bool
			select {
			case response, ok = <-input:
			case <-ctx.Done():
				return
			}
			if !ok {
				if !complete() {
					reject()
				}
				return
			}
			if response == nil {
				reject()
				return
			}
			result := *response
			result.Choices = append([]model.Choice(nil), response.Choices...)
			for i := range result.Choices {
				c := &result.Choices[i]
				c.Message.ToolCalls = append([]model.ToolCall(nil), c.Message.ToolCalls...)
				for j := range c.Message.ToolCalls {
					call := &c.Message.ToolCalls[j]
					name, ok := allowed[call.Function.Name]
					if !ok {
						reject()
						return
					}
					call.Function.Name = name
				}
				c.Delta.ToolCalls = append([]model.ToolCall(nil), c.Delta.ToolCalls...)
				for j := range c.Delta.ToolCalls {
					call := &c.Delta.ToolCalls[j]
					index := j
					if call.Index != nil {
						index = *call.Index
					}
					if index < 0 {
						reject()
						return
					}
					key := [2]int{c.Index, index}
					name := fragments[key] + call.Function.Name
					fragments[key] = name
					prefix := false
					for candidate := range allowed {
						if strings.HasPrefix(candidate, name) {
							prefix = true
							break
						}
					}
					if !prefix {
						reject()
						return
					}
					// Emit one complete internal name only after the provider name resolves.
					old := fragments[key][:len(fragments[key])-len(call.Function.Name)]
					call.Function.Name = ""
					if internal, ok := allowed[name]; ok && old != name {
						call.Function.Name = internal
					}
				}
			}
			if result.Done && !complete() {
				reject()
				return
			}
			if !emit(&result) {
				return
			}
		}
	}()
	return output, nil
}

var _ model.Model = (*knowledgeCallableModel)(nil)
