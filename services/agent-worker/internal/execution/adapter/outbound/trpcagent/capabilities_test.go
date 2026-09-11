package trpcagent

import (
	"context"
	"strconv"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memorytool "trpc.group/trpc-go/trpc-agent-go/memory/tool"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type capabilityMemory struct {
	memory.Service // Unused methods panic, exposing accidental backend calls.
	tools          []tool.Tool
	toolsCalls     int
	add            func(memory.UserKey, string, []string) error
}

func (s *capabilityMemory) AddMemory(_ context.Context, key memory.UserKey, value string, topics []string, _ ...memory.AddOption) error {
	return s.add(key, value, topics)
}
func (s *capabilityMemory) Tools() []tool.Tool                                           { s.toolsCalls++; return s.tools }
func (s *capabilityMemory) EnqueueAutoMemoryJob(context.Context, *session.Session) error { return nil }
func (s *capabilityMemory) Close() error                                                 { return nil }

type capabilityArtifact struct{ artifact.Service }
type capabilityKnowledge struct{ knowledge.Knowledge }
type capabilityTool struct {
	declaration *tool.Declaration
	calls       int
}

func (t *capabilityTool) Declaration() *tool.Declaration { return t.declaration }
func (t *capabilityTool) Call(context.Context, []byte) (any, error) {
	t.calls++
	return "final wrapper", nil
}
func namedCapabilityTool(name string) *capabilityTool {
	return &capabilityTool{declaration: &tool.Declaration{Name: name}}
}
func applyCapabilityAgent(opts []llmagent.Option) llmagent.Options {
	var result llmagent.Options
	for _, apply := range opts {
		apply(&result)
	}
	return result
}

func TestCapabilityAssemblyDefaultDoesNotExposeAvailableServices(t *testing.T) {
	ms := &capabilityMemory{tools: []tool.Tool{namedCapabilityTool(memory.AddToolName)}}
	services := CapabilityServices{Memory: ms, Artifact: &capabilityArtifact{}, Knowledge: &capabilityKnowledge{}}
	opts, err := BuildCapabilityOptions(CapabilityConfig{}, services)
	if err != nil {
		t.Fatal(err)
	}
	a := applyCapabilityAgent(opts.Agent)
	if len(a.Tools) != 0 || a.Knowledge != nil || a.PreloadMemory != 0 || a.AddSessionSummary || ms.toolsCalls != 0 {
		t.Fatalf("default enabled a capability: %+v; Tools calls=%d", a, ms.toolsCalls)
	}
	observeCapabilityRunner(t, opts.Runner, nil, nil)
}

func TestCapabilityAssemblyUsesFinalServiceToolsAndExplicitOrder(t *testing.T) {
	names := []string{memory.AddToolName, memory.UpdateToolName, memory.DeleteToolName, memory.ClearToolName, memory.SearchToolName, memory.LoadToolName}
	ms := &capabilityMemory{}
	for _, name := range names {
		ms.tools = append(ms.tools, namedCapabilityTool(name))
	}
	as, ks := &capabilityArtifact{}, &capabilityKnowledge{}
	cfg := CapabilityConfig{MemoryTools: []string{memory.LoadToolName, memory.AddToolName}, MemoryPreloadLimit: -1, AddSessionSummary: true, Artifact: true, Knowledge: true}
	opts, err := BuildCapabilityOptions(cfg, CapabilityServices{Memory: ms, Artifact: as, Knowledge: ks})
	if err != nil {
		t.Fatal(err)
	}
	cfg.MemoryTools[0] = memory.DeleteToolName // Options have captured the validated selection.
	ordinary := namedCapabilityTool("ordinary")
	a := llmagent.Options{Tools: []tool.Tool{ordinary}}
	for _, apply := range opts.Agent {
		apply(&a)
	}
	if len(a.Tools) != 3 || a.Tools[0] != ordinary || a.Tools[1] != ms.tools[5] || a.Tools[2] != ms.tools[0] {
		t.Fatalf("wrong tool instances/order: %v", a.Tools)
	}
	if a.PreloadMemory != -1 || !a.AddSessionSummary || a.Knowledge != ks || ms.toolsCalls != 1 {
		t.Fatal("explicit options lost")
	}
	got, err := a.Tools[2].(tool.CallableTool).Call(context.Background(), []byte(`{}`))
	if err != nil || got != "final wrapper" || ms.tools[0].(*capabilityTool).calls != 1 {
		t.Fatal("tool bypassed final service")
	}
	observeCapabilityRunner(t, opts.Runner, ms, as)
}

func TestCapabilityAssemblyAllSixTools(t *testing.T) {
	names := []string{memory.AddToolName, memory.UpdateToolName, memory.DeleteToolName, memory.ClearToolName, memory.SearchToolName, memory.LoadToolName}
	ms := &capabilityMemory{}
	for _, name := range names {
		ms.tools = append(ms.tools, namedCapabilityTool(name))
	}
	opts, err := BuildCapabilityOptions(CapabilityConfig{MemoryTools: names}, CapabilityServices{Memory: ms})
	if err != nil {
		t.Fatal(err)
	}
	a := applyCapabilityAgent(opts.Agent)
	for i, name := range names {
		if a.Tools[i].Declaration().Name != name {
			t.Fatal("tool mismatch")
		}
	}
	if a.PreloadMemory != 0 {
		t.Fatal("tools implicitly enabled preload")
	}
}

func TestCapabilityAssemblyPreloadWithoutTools(t *testing.T) {
	for _, limit := range []int{-1, 1, 1000000} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			ms := &capabilityMemory{}
			opts, err := BuildCapabilityOptions(CapabilityConfig{MemoryPreloadLimit: limit}, CapabilityServices{Memory: ms})
			if err != nil {
				t.Fatal(err)
			}
			a := applyCapabilityAgent(opts.Agent)
			if a.PreloadMemory != limit || len(a.Tools) != 0 || ms.toolsCalls != 0 {
				t.Fatal("preload changed or tools implicitly enabled")
			}
			observeCapabilityRunner(t, opts.Runner, ms, nil)
		})
	}
}

func TestCapabilityAssemblyRejectsInvalidSelection(t *testing.T) {
	good := func() *capabilityMemory {
		return &capabilityMemory{tools: []tool.Tool{namedCapabilityTool(memory.AddToolName)}}
	}
	var nilMemory *capabilityMemory
	var nilTool *capabilityTool
	cases := []struct {
		name     string
		cfg      CapabilityConfig
		services CapabilityServices
	}{
		{"negative preload", CapabilityConfig{MemoryPreloadLimit: -2}, CapabilityServices{}},
		{"unknown selection", CapabilityConfig{MemoryTools: []string{"other"}}, CapabilityServices{Memory: good()}},
		{"duplicate selection", CapabilityConfig{MemoryTools: []string{memory.AddToolName, memory.AddToolName}}, CapabilityServices{Memory: good()}},
		{"missing memory", CapabilityConfig{MemoryPreloadLimit: 1}, CapabilityServices{}},
		{"typed nil memory", CapabilityConfig{MemoryPreloadLimit: 1}, CapabilityServices{Memory: nilMemory}},
		{"missing artifact", CapabilityConfig{Artifact: true}, CapabilityServices{}},
		{"missing knowledge", CapabilityConfig{Knowledge: true}, CapabilityServices{}},
		{"unavailable tool", CapabilityConfig{MemoryTools: []string{memory.LoadToolName}}, CapabilityServices{Memory: good()}},
	}
	for _, tc := range []struct {
		name  string
		tools []tool.Tool
	}{
		{"nil tool", []tool.Tool{nil}}, {"typed nil tool", []tool.Tool{nilTool}},
		{"nil declaration", []tool.Tool{&capabilityTool{}}},
		{"unknown declaration", []tool.Tool{namedCapabilityTool("other")}},
		{"duplicate declaration", []tool.Tool{namedCapabilityTool(memory.AddToolName), namedCapabilityTool(memory.AddToolName)}},
	} {
		cases = append(cases, struct {
			name     string
			cfg      CapabilityConfig
			services CapabilityServices
		}{tc.name, CapabilityConfig{MemoryTools: []string{memory.AddToolName}}, CapabilityServices{Memory: &capabilityMemory{tools: tc.tools}}})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := BuildCapabilityOptions(tc.cfg, tc.services)
			if err == nil || opts.Agent != nil || opts.Runner != nil {
				t.Fatalf("expected atomic rejection, got %+v %v", opts, err)
			}
		})
	}
}

type capabilityObserver struct {
	seen chan *agent.Invocation
	call func(context.Context, *agent.Invocation) error
}

func (a *capabilityObserver) Tools() []tool.Tool              { return nil }
func (a *capabilityObserver) SubAgents() []agent.Agent        { return nil }
func (a *capabilityObserver) FindSubAgent(string) agent.Agent { return nil }
func (a *capabilityObserver) Info() agent.Info                { return agent.Info{Name: "capability-observer"} }
func (a *capabilityObserver) Run(ctx context.Context, inv *agent.Invocation) (<-chan *event.Event, error) {
	a.seen <- inv
	if a.call != nil {
		if err := a.call(ctx, inv); err != nil {
			return nil, err
		}
	}
	events := make(chan *event.Event)
	close(events)
	return events, nil
}
func observeCapabilityRunner(t *testing.T, options []runner.Option, wantMemory memory.Service, wantArtifact artifact.Service) {
	t.Helper()
	a := &capabilityObserver{seen: make(chan *agent.Invocation, 1)}
	r := runner.NewRunner("capability-test", a, options...)
	defer r.Close()
	events, err := r.Run(context.Background(), "user", "session", model.NewUserMessage("test"))
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	select {
	case inv := <-a.seen:
		if inv.MemoryService != wantMemory || inv.ArtifactService != wantArtifact {
			t.Fatal("Runner received incorrect capability service")
		}
	default:
		t.Fatal("SDK did not execute observing agent")
	}
}

// This exercises the real SDK tool rather than a same-named test tool. The
// observed key is the SDK session identity, not yet Worker's future trusted
// Memory scope; mapping/Attempt durability are responsibilities of the wrapper.
func TestCapabilityAssemblySDKAddUsesRunnerFinalMemoryService(t *testing.T) {
	type addedMemory struct {
		key    memory.UserKey
		value  string
		topics []string
	}
	writes := make(chan addedMemory, 1)
	ms := &capabilityMemory{tools: []tool.Tool{memorytool.NewAddTool()}}
	ms.add = func(key memory.UserKey, value string, topics []string) error {
		writes <- addedMemory{key, value, topics}
		return nil
	}
	opts, err := BuildCapabilityOptions(CapabilityConfig{MemoryTools: []string{memory.AddToolName}}, CapabilityServices{Memory: ms})
	if err != nil {
		t.Fatal(err)
	}
	configured := applyCapabilityAgent(opts.Agent)
	sdkTool := configured.Tools[0].(tool.CallableTool)
	for _, field := range []string{"app_name", "user_id", "subject_id", "tenant_id"} {
		if _, exists := sdkTool.Declaration().InputSchema.Properties[field]; exists {
			t.Fatalf("scope field exposed: %s", field)
		}
	}
	results := make(chan error, 1)
	observer := &capabilityObserver{seen: make(chan *agent.Invocation, 1)}
	observer.call = func(ctx context.Context, inv *agent.Invocation) error {
		_, callErr := sdkTool.Call(agent.NewInvocationContext(ctx, inv), []byte(`{"memory":"remembers tea","topics":["drink"],"app_name":"attacker-app","user_id":"attacker-user","subject_id":"attacker-subject","tenant_id":"attacker-tenant"}`))
		results <- callErr
		return callErr
	}
	r := runner.NewRunner("trusted-app", observer, opts.Runner...)
	defer r.Close()
	events, err := r.Run(context.Background(), "trusted-user", "session", model.NewUserMessage("remember tea"))
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	select {
	case err := <-results:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("SDK tool not called")
	}
	select {
	case got := <-writes:
		if got.key != (memory.UserKey{AppName: "trusted-app", UserID: "trusted-user"}) || got.value != "remembers tea" || len(got.topics) != 1 || got.topics[0] != "drink" {
			t.Fatalf("SDK tool did not use runner identity/final service: %+v", got)
		}
	default:
		t.Fatal("final service AddMemory not called")
	}
	if inv := <-observer.seen; inv.MemoryService != ms {
		t.Fatal("runner used another memory service")
	}
}
