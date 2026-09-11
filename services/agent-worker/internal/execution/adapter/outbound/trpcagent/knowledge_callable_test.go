package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	knowledgetool "trpc.group/trpc-go/trpc-agent-go/knowledge/tool"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const docsCallable = "fn_bf0861e4910faf57aabed6367d2225410370643eed80dc6fbdb5832850e0"

type callableKnowledge struct{ calls atomic.Int32 }

func (k *callableKnowledge) Search(_ context.Context, r *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	k.calls.Add(1)
	if r.Query != "orchid" {
		return nil, errors.New("wrong query")
	}
	d := &document.Document{ID: "doc", Content: "orchid is violet"}
	return &knowledge.SearchResult{Text: d.Content, Document: d, Documents: []*knowledge.Result{{Document: d, Score: 1}}}, nil
}
func TestKnowledgeCallableRealSDKRunnerSSE(t *testing.T) {
	for _, fragmented := range []bool{false, true} {
		t.Run(fmt.Sprint(fragmented), func(t *testing.T) {
			f, server := newMemoryHTTPFixture(t, []string{docsCallable}, memoryHTTPRound{tool: docsCallable, args: `{"query":"orchid"}`, fragmented: fragmented}, memoryHTTPRound{final: "knowledge complete", before: func(req map[string]any) error {
				raw, _ := json.Marshal(req["messages"])
				if strings.Contains(string(raw), sdkKnowledgeName) || !strings.Contains(string(raw), docsCallable) || !strings.Contains(string(raw), "orchid is violet") {
					return errors.New("provider history was not translated")
				}
				return nil
			}})
			transport := http.DefaultTransport.(*http.Transport).Clone()
			defer transport.CloseIdleConnections()
			primary := newFixedModel(Model{Endpoint: server.URL + "/v1", Name: "fixture-model", APIKey: "fixture-token"}, 20000, &modelTransport{base: transport})
			wrapped, err := WrapKnowledgeModel(primary, "knowledge/docs")
			if err != nil {
				t.Fatal(err)
			}
			kb := &callableKnowledge{}
			a := llmagent.New("agent", llmagent.WithModel(wrapped), llmagent.WithKnowledge(kb), llmagent.WithGenerationConfig(model.GenerationConfig{Stream: true}), llmagent.WithEnableCodeExecutionResponseProcessor(false), llmagent.WithCodeExecutor(nil))
			r := runner.NewRunner("app", a)
			defer r.Close()
			events, err := r.Run(context.Background(), "user", "session", model.NewUserMessage("query orchid"), agent.WithDetachedCancel(false))
			if err != nil {
				t.Fatal(err)
			}
			final := false
			for e := range events {
				if e.Error != nil {
					t.Fatal(e.Error)
				}
				for _, c := range e.Choices {
					if c.Message.Content == "knowledge complete" {
						final = true
					}
				}
			}
			if !final || kb.calls.Load() != 1 {
				t.Fatalf("final=%t knowledge_calls=%d", final, kb.calls.Load())
			}
			f.check(t, 2)
		})
	}
}

type callableModelStub struct {
	responses []*model.Response
	request   *model.Request
}

func (m *callableModelStub) Info() model.Info { return model.Info{Name: "stub"} }
func (m *callableModelStub) GenerateContent(_ context.Context, r *model.Request) (<-chan *model.Response, error) {
	m.request = r
	ch := make(chan *model.Response, len(m.responses))
	for _, x := range m.responses {
		ch <- x
	}
	close(ch)
	return ch, nil
}

type callableNamedTool string

func (t callableNamedTool) Declaration() *tool.Declaration { return &tool.Declaration{Name: string(t)} }
func TestKnowledgeCallableBindingAndFragments(t *testing.T) {
	for _, bad := range []string{"docs", "knowledge/Docs", "knowledge/docs\n", "tools/docs"} {
		if _, err := WrapKnowledgeModel(&callableModelStub{}, bad); !errors.Is(err, ErrKnowledgeCallable) {
			t.Fatal(bad, err)
		}
	}
	idx := 0
	for _, name := range []string{sdkKnowledgeName, "fn_unknown", "memory_clear", docsCallable[:8]} {
		stub := &callableModelStub{responses: []*model.Response{{Choices: []model.Choice{{Delta: model.Message{ToolCalls: []model.ToolCall{{Index: &idx, Function: model.FunctionDefinitionParam{Name: name}}}}}}}}}
		wrapped, _ := WrapKnowledgeModel(stub, "knowledge/docs")
		ch, err := wrapped.GenerateContent(context.Background(), &model.Request{Tools: map[string]tool.Tool{sdkKnowledgeName: knowledgetool.NewKnowledgeSearchTool(&callableKnowledge{})}})
		if err != nil {
			t.Fatal(err)
		}
		rejected := false
		for r := range ch {
			if r.Error != nil {
				rejected = true
			}
		}
		if !rejected {
			t.Fatal("unbound/incomplete name accepted", name)
		}
	}
	// Other enabled SDK names are unchanged and caller state is not rewritten.
	original := &model.Request{Tools: map[string]tool.Tool{sdkKnowledgeName: knowledgetool.NewKnowledgeSearchTool(&callableKnowledge{}), "memory_add": callableNamedTool("memory_add"), "artifact_load": callableNamedTool("artifact_load")}, Messages: []model.Message{{ToolName: sdkKnowledgeName, ToolCalls: []model.ToolCall{{Function: model.FunctionDefinitionParam{Name: sdkKnowledgeName}}}}}}
	response := &model.Response{Done: true, Choices: []model.Choice{{Message: model.Message{ToolCalls: []model.ToolCall{{Function: model.FunctionDefinitionParam{Name: "memory_add"}}, {Function: model.FunctionDefinitionParam{Name: "artifact_load"}}}}}}}
	stub := &callableModelStub{responses: []*model.Response{response}}
	wrapped, _ := WrapKnowledgeModel(stub, "knowledge/docs")
	ch, err := wrapped.GenerateContent(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	for r := range ch {
		if r.Error != nil || r.Choices[0].Message.ToolCalls[0].Function.Name != "memory_add" || r.Choices[0].Message.ToolCalls[1].Function.Name != "artifact_load" {
			t.Fatal("other name changed")
		}
	}
	if original.Messages[0].ToolName != sdkKnowledgeName || original.Messages[0].ToolCalls[0].Function.Name != sdkKnowledgeName || original.Tools[sdkKnowledgeName].Declaration().Name != sdkKnowledgeName {
		t.Fatal("caller mutation")
	}
	if stub.request.Tools[docsCallable].Declaration().Name != docsCallable {
		t.Fatal("frozen golden mapping mismatch")
	}
}
