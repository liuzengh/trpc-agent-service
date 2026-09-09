// Package runtime manages immutable, tenant-scoped tRPC-Agent-Go runtimes.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	servicemetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/modelprovider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/chainagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/cycleagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/graphagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/parallelagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	knowledgetool "trpc.group/trpc-go/trpc-agent-go/knowledge/tool"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// ErrDrainTimeout indicates that a canceled Runner did not close its stream.
var ErrDrainTimeout = errors.New("runtime: event stream drain timeout")

// RunInput contains only trusted routing fields; callers cannot supply RunOptions.
type RunInput struct {
	RequestID, UserID, SessionID string
	Text                         string
	Attachments                  []gateway.Attachment
	Observer                     func(*event.Event)
	ToolFilter                   tool.FilterFunc
	ToolExecutionFilter          tool.FilterFunc
	ToolPermissionPolicy         tool.PermissionPolicy
}

// RunResult contains the complete event sequence emitted before closure.
type RunResult struct{ Events []*event.Event }

type ToolResume struct {
	Input                RunInput
	ToolName, ToolCallID string
	Arguments            []byte
}

// ToolResumer completes a caller-managed dangerous tool and resumes the same Runner session.
type ToolResumer interface {
	ResumeTool(context.Context, ToolResume) (RunResult, error)
}

// Bundle owns one immutable tenant/app/config-version runtime.
type Bundle struct {
	tenantID, appID, appName string
	version                  tenant.ConfigVersion
	toolNames                []string
	toolPolicy               tenant.ToolPolicy
	tools                    map[string]tool.CallableTool
	runner                   runner.Runner
	services                 *storage.Services
	externalToolsClose       func() error
	drainTimeout             time.Duration
	closeOnce                sync.Once
	closeErr                 error
	multimodal               bool
}

// NewTestBundle assembles the deterministic fixture used by unit tests and the
// standalone quickstart. Service entrypoints inject persistent services.
func NewTestBundle(snapshot config.RuntimeSnapshot, plugins ...plugin.Plugin) (*Bundle, error) {
	app := snapshot.App()
	services, err := storage.NewTestServices(app.Storage)
	if err != nil {
		return nil, err
	}
	bundle, err := NewBundleWithServices(snapshot, services, plugins...)
	if err != nil {
		_ = services.Close()
		return nil, err
	}
	return bundle, nil
}

// NewBundleWithServices assembles a Bundle with tenant-routed services owned by it.
func NewBundleWithServices(snapshot config.RuntimeSnapshot, services *storage.Services, plugins ...plugin.Plugin) (*Bundle, error) {
	return NewBundleWithServicesAndTools(snapshot, services, nil, nil, plugins...)
}

// NewBundleWithServicesAndTools assembles a Bundle with version-pinned MCP
// and business tools. closeTools becomes part of the Bundle drain lifecycle.
func NewBundleWithServicesAndTools(snapshot config.RuntimeSnapshot, services *storage.Services, externalTools map[string]tool.Tool, closeTools func() error, plugins ...plugin.Plugin) (*Bundle, error) {
	servicelog.InstallUpstreamRedaction()
	pluginNames := make(map[string]struct{}, len(plugins))
	for _, candidate := range plugins {
		if candidate == nil || candidate.Name() == "" {
			return nil, errors.New("runtime: plugin and plugin name are required")
		}
		if _, exists := pluginNames[candidate.Name()]; exists {
			return nil, fmt.Errorf("runtime: duplicate plugin %q", candidate.Name())
		}
		pluginNames[candidate.Name()] = struct{}{}
	}
	app := snapshot.App()
	var runtimeModel model.Model
	if app.Model.Provider == "mock" {
		runtimeModel = serviceagent.MockModel{}
	} else {
		resolved, err := modelprovider.NewScoped(app.Model, snapshot.TenantID(), snapshot.AppID())
		if err != nil {
			return nil, err
		}
		runtimeModel = resolved
	}
	appName, err := tenant.CanonicalAppName(snapshot.TenantID(), snapshot.AppID())
	if err != nil {
		return nil, err
	}
	extras := make(map[string]tool.Tool, len(externalTools)+1)
	for name, candidate := range externalTools {
		if name == "" || candidate == nil {
			return nil, errors.New("runtime: external tool name and implementation are required")
		}
		extras[name] = candidate
	}
	if services != nil && services.Knowledge != nil {
		maxResults := app.Knowledge.MaxResults
		if maxResults == 0 {
			maxResults = 10
		}
		if _, exists := extras["knowledge_search"]; exists {
			return nil, errors.New("runtime: external tool conflicts with knowledge_search")
		}
		extras["knowledge_search"] = knowledgetool.NewKnowledgeSearchTool(services.Knowledge, knowledgetool.WithMaxResults(maxResults), knowledgetool.WithMinScore(app.Knowledge.MinScore))
	}
	tools, toolNames, err := serviceagent.ToolsWithExtras(app.Tools.Allow, app.Tools.Deny, extras)
	if err != nil {
		return nil, err
	}
	tools = servicetool.WrapAll(tools)
	callableTools := make(map[string]tool.CallableTool, len(tools))
	for _, candidate := range tools {
		if callable, ok := candidate.(tool.CallableTool); ok && candidate.Declaration() != nil {
			callableTools[candidate.Declaration().Name] = callable
		}
	}
	if services == nil || services.Session == nil || services.Memory == nil || services.Artifact == nil {
		return nil, errors.New("runtime: session, memory, and artifact services are required")
	}
	generation := model.GenerationConfig{Stream: true, Temperature: app.Model.Temperature}
	if app.Model.MaxTokens > 0 {
		maxTokens := app.Model.MaxTokens
		generation.MaxTokens = &maxTokens
	}
	rootAgent, err := buildAgent(app, runtimeModel, tools, generation)
	if err != nil {
		return nil, err
	}
	run := runner.NewRunner(appName, rootAgent, runner.WithSessionService(services.Session), runner.WithMemoryService(services.Memory), runner.WithArtifactService(services.Artifact), runner.WithPlugins(plugins...))
	return &Bundle{tenantID: snapshot.TenantID(), appID: snapshot.AppID(), appName: appName, version: snapshot.Version(), toolNames: append([]string(nil), toolNames...), toolPolicy: app.Tools, tools: callableTools, runner: run, services: services, externalToolsClose: closeTools, drainTimeout: time.Second, multimodal: app.Model.Multimodal}, nil
}

func buildAgent(app tenant.AgentApp, runtimeModel model.Model, tools []tool.Tool, generation model.GenerationConfig) (trpcagent.Agent, error) {
	leaf := func(name, instruction string) trpcagent.Agent {
		return llmagent.New(name, llmagent.WithModel(runtimeModel), llmagent.WithInstruction(instruction), llmagent.WithTools(tools), llmagent.WithGenerationConfig(generation))
	}
	kind := app.Workflow.Type
	if kind == "" || kind == tenant.WorkflowLLM {
		return leaf(app.Name, app.Config.Instruction), nil
	}
	subAgents := make([]trpcagent.Agent, 0, len(app.Workflow.Nodes))
	for _, node := range app.Workflow.Nodes {
		subAgents = append(subAgents, leaf(node.ID, node.Instruction))
	}
	switch kind {
	case tenant.WorkflowChain:
		return chainagent.New(app.Name, chainagent.WithSubAgents(subAgents)), nil
	case tenant.WorkflowParallel:
		parallel := parallelagent.New(app.Name+"-branches", parallelagent.WithSubAgents(subAgents))
		aggregator := app.Workflow.Aggregator
		if aggregator == nil {
			return nil, errors.New("runtime: parallel workflow aggregator is required")
		}
		return chainagent.New(app.Name, chainagent.WithSubAgents([]trpcagent.Agent{parallel, leaf(aggregator.ID, aggregator.Instruction)})), nil
	case tenant.WorkflowCycle:
		return cycleagent.New(app.Name, cycleagent.WithSubAgents(subAgents), cycleagent.WithMaxIterations(app.Workflow.MaxIterations)), nil
	case tenant.WorkflowGraph:
		builder := graph.NewStateGraph(graph.MessagesStateSchema())
		for _, node := range app.Workflow.Nodes {
			builder.AddAgentNode(node.ID)
		}
		for _, edge := range app.Workflow.Edges {
			builder.AddEdge(edge.From, edge.To)
		}
		compiled, err := builder.SetEntryPoint(app.Workflow.Entry).SetFinishPoint(app.Workflow.Finish).Compile()
		if err != nil {
			return nil, fmt.Errorf("runtime: compile agent workflow: %w", err)
		}
		options := []graphagent.Option{graphagent.WithSubAgents(subAgents)}
		if app.Workflow.MaxConcurrency > 0 {
			options = append(options, graphagent.WithMaxConcurrency(app.Workflow.MaxConcurrency))
		}
		root, err := graphagent.New(app.Name, compiled, options...)
		if err != nil {
			return nil, fmt.Errorf("runtime: build graph agent: %w", err)
		}
		return root, nil
	default:
		return nil, fmt.Errorf("runtime: unsupported workflow type %q", kind)
	}
}

// Scope returns the immutable bundle identity.
func (bundle *Bundle) Scope() (string, string, tenant.ConfigVersion) {
	return bundle.tenantID, bundle.appID, bundle.version
}

// AppName returns the trusted tRPC-Agent-Go isolation key.
func (bundle *Bundle) AppName() string { return bundle.appName }

// ToolNames returns the configured tool surface.
func (bundle *Bundle) ToolNames() []string { return append([]string(nil), bundle.toolNames...) }

// Run executes and owns the event stream until it closes or bounded draining expires.
func (bundle *Bundle) Run(ctx context.Context, input RunInput) (RunResult, error) {
	if ctx == nil {
		return RunResult{}, errors.New("runtime: nil context")
	}
	if bundle == nil || bundle.runner == nil {
		return RunResult{}, errors.New("runtime: bundle is not initialized")
	}
	if input.RequestID == "" || input.UserID == "" || input.SessionID == "" || input.Text == "" {
		return RunResult{}, errors.New("runtime: request, user, session IDs, and text are required")
	}
	message, err := userMessage(input, bundle.multimodal)
	if err != nil {
		return RunResult{}, err
	}
	return bundle.runMessage(ctx, input, message)
}

func userMessage(input RunInput, multimodal bool) (model.Message, error) {
	message := model.NewUserMessage(input.Text)
	for _, attachment := range input.Attachments {
		switch attachment.Kind {
		case "image":
			if !multimodal {
				return model.Message{}, errors.New("runtime: configured model does not support image input")
			}
			if len(attachment.Data) == 0 || attachment.MIME == "" {
				return model.Message{}, errors.New("runtime: invalid image attachment")
			}
			format := attachment.MIME
			if index := strings.IndexByte(format, '/'); index >= 0 {
				format = format[index+1:]
			}
			message.AddImageData(append([]byte(nil), attachment.Data...), "auto", format)
		case "file":
			if strings.TrimSpace(attachment.ExtractedText) == "" {
				return model.Message{}, errors.New("runtime: document has no extracted text")
			}
			message.Content += "\n\n[Document " + safeDocumentName(attachment.Name) + "]\n" + attachment.ExtractedText
		default:
			return model.Message{}, errors.New("runtime: unsupported attachment kind")
		}
	}
	return message, nil
}

func safeDocumentName(name string) string {
	name = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == ']' || r == '[' {
			return -1
		}
		return r
	}, strings.TrimSpace(name))
	if name == "" {
		return "attachment"
	}
	return name
}

// ResumeTool executes an approved external tool through its guarded handler and resumes the model with a Tool result.
func (bundle *Bundle) ResumeTool(ctx context.Context, resume ToolResume) (RunResult, error) {
	callable := bundle.tools[resume.ToolName]
	if callable == nil || resume.ToolCallID == "" {
		return RunResult{}, errors.New("runtime: resumable tool and call ID are required")
	}
	result, err := callable.Call(ctx, resume.Arguments)
	if err != nil {
		return RunResult{}, err
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return RunResult{}, err
	}
	return bundle.runMessage(ctx, resume.Input, model.NewToolMessage(resume.ToolCallID, resume.ToolName, string(payload)))
}

func (bundle *Bundle) runMessage(ctx context.Context, input RunInput, message model.Message) (result RunResult, runErr error) {
	modelStarted := time.Now()
	if telemetry, ok := servicemetrics.FromContext(ctx); ok {
		modelCtx, modelSpan := telemetry.Telemetry.Start(ctx, "model.stream", telemetry.Fields)
		ctx = modelCtx
		defer func() {
			modelSpan.End()
			status := "success"
			if runErr != nil {
				status = "failed"
			}
			telemetry.Telemetry.Request(modelCtx, servicemetrics.Labels{TenantID: telemetry.Fields.TenantID, AppID: telemetry.Fields.AppID, Channel: telemetry.Fields.Channel, Operation: "model", Status: status}, time.Since(modelStarted), 0, 0)
		}()
	}
	options := []trpcagent.RunOption{trpcagent.WithRequestID(input.RequestID), trpcagent.WithAppName(bundle.appName)}
	if input.ToolFilter != nil {
		options = append(options, trpcagent.WithToolFilter(input.ToolFilter))
	}
	if input.ToolExecutionFilter != nil {
		options = append(options, trpcagent.WithToolExecutionFilter(input.ToolExecutionFilter))
	}
	if input.ToolPermissionPolicy != nil {
		options = append(options, trpcagent.WithToolPermissionPolicy(input.ToolPermissionPolicy))
	}
	stream, err := bundle.runner.Run(ctx, input.UserID, input.SessionID, message, options...)
	if err != nil {
		return RunResult{}, err
	}
	var canceled bool
	var observedFirstToken bool
	var drain <-chan time.Time
	for {
		select {
		case item, ok := <-stream:
			if !ok {
				if canceled {
					return result, ctx.Err()
				}
				return result, nil
			}
			if item != nil {
				if !observedFirstToken {
					if telemetry, ok := servicemetrics.FromContext(ctx); ok {
						telemetry.Telemetry.ModelFirstToken(ctx, servicemetrics.Labels{TenantID: telemetry.Fields.TenantID, AppID: telemetry.Fields.AppID, Channel: telemetry.Fields.Channel, Operation: "model", Status: "success"}, time.Since(modelStarted))
					}
					observedFirstToken = true
				}
				result.Events = append(result.Events, item)
				if input.Observer != nil {
					input.Observer(item)
				}
			}
		case <-ctx.Done():
			if !canceled {
				canceled = true
				if managed, ok := bundle.runner.(runner.ManagedRunner); ok {
					managed.Cancel(input.RequestID)
				}
				timer := time.NewTimer(bundle.drainTimeout)
				defer timer.Stop()
				drain = timer.C
			}
		case <-drain:
			return result, errors.Join(ctx.Err(), ErrDrainTimeout)
		}
	}
}

// Close idempotently releases Runner and externally injected storage services.
func (bundle *Bundle) Close() error {
	if bundle == nil {
		return nil
	}
	bundle.closeOnce.Do(func() {
		var runnerErr error
		if bundle.runner != nil {
			runnerErr = bundle.runner.Close()
		}
		var toolErr, servicesErr error
		if bundle.externalToolsClose != nil {
			toolErr = bundle.externalToolsClose()
		}
		if bundle.services != nil {
			servicesErr = bundle.services.Close()
		}
		bundle.closeErr = errors.Join(runnerErr, toolErr, servicesErr)
	})
	return bundle.closeErr
}
