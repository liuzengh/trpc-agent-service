// Package trpcagent assembles fixed Worker nodes into one pinned SDK Runner.
// Execution retains credential authorization, candidate durability and Completion.
package trpcagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	replycodec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	replywire "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	openaiapi "github.com/openai/openai-go"
	openaioption "github.com/openai/openai-go/option"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const SDKVersion = "v1.11.2"

var ErrModel = errors.New("model execution failed")
var ErrRetryableModel = errors.New("model dependency temporarily unavailable")
var ErrFinal = errors.New("model returned no valid complete text final")
var ErrDrain = errors.New("SDK cancellation drain timeout")

type Model struct {
	Endpoint    string
	Name        string
	APIKey      string
	Temperature *float64
	// Nil uses the published per-response MaxOutputTokens cap, never a private quota.
	MaxOutputTokens *int64
}

// SummaryConfig contains only the fixed, authorized manifest selection.
type SummaryConfig struct {
	Model             Model
	EventThreshold    int64
	AddSessionSummary bool
}
type MemoryConfig struct {
	BoundKey     memory.UserKey
	Entries      []*memory.Entry
	BaseRevision uint64
	Tools        []string
	PreloadLimit int
}
type MemorySelection struct {
	Tools        []string
	PreloadLimit int
}
type WorkspaceConfig struct{ ExecTool, SaveArtifactTool tool.CallableTool }
type NodeConfig struct {
	WorkspaceTools    []string
	Body              string
	MaxIterations     int64
	Kind              string
	Children          []string
	Instruction       string
	Model             Model
	Tools             []MCPToolConfig
	Knowledge         *KnowledgeConfig
	Memory            *MemorySelection
	Artifact          bool
	AddSessionSummary bool
}
type Request struct {
	Workspace                             *WorkspaceConfig
	Nodes                                 map[string]NodeConfig
	Tools                                 []MCPToolConfig
	Knowledge                             *KnowledgeConfig
	Artifact                              *ArtifactConfig
	Memory                                *MemoryConfig
	MaxToolCalls                          int64
	Summary                               *SummaryConfig
	TenantID, SessionID, RunID, AttemptID string
	NodeID, Instruction, InputText        string
	Model                                 Model
	MaxOutputTokens                       int64
	AcceptedSnapshot                      []byte
}
type Usage struct{ InputTokens, OutputTokens, TotalTokens int }
type Result struct {
	Attachments []domain.Attachment
	Memory      *MemoryCandidate
	FinalText   string
	Snapshot    []byte
	Usage       Usage
	UsageKnown  bool
}

type Executor struct {
	Tracer        trace.Tracer
	Approvals     ApprovalStore
	CapacityBytes int
	DrainTimeout  time.Duration
	beforeAppend  func(*event.Event) error
}

func (e Executor) Execute(ctx context.Context, req Request) (result Result, err error) {
	if e.CapacityBytes <= 0 || e.DrainTimeout <= 0 {
		return result, errors.New("positive snapshot capacity and drain timeout required")
	}
	if req.TenantID == "" || req.SessionID == "" || req.RunID == "" || req.AttemptID == "" || req.NodeID == "" || strings.TrimSpace(req.InputText) == "" {
		return result, errors.New("invalid execution request")
	}
	nodes, terminal, validationErr := executionNodes(req)
	if validationErr != nil {
		return result, validationErr
	}
	if req.Summary != nil {
		sm := req.Summary.Model
		ep, parseErr := url.Parse(sm.Endpoint)
		if parseErr != nil || (ep.Scheme != "http" && ep.Scheme != "https") || ep.Host == "" || ep.User != nil || ep.RawQuery != "" || ep.ForceQuery || strings.Contains(sm.Endpoint, "#") || strings.TrimSpace(sm.Name) == "" || sm.MaxOutputTokens != nil || sm.Temperature != nil || req.Summary.EventThreshold <= 0 || req.Summary.EventThreshold > 9007199254740991 || int64(int(req.Summary.EventThreshold)) != req.Summary.EventThreshold {
			return result, errors.New("invalid fixed summary configuration")
		}
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if e.Tracer != nil {
		var span trace.Span
		ctx, span = e.Tracer.Start(ctx, "worker.runner.run", trace.WithAttributes(
			attribute.String("app.run.id", req.RunID), attribute.String("app.attempt.id", req.AttemptID), attribute.String("app.session.id", req.SessionID)))
		defer func() {
			if err != nil {
				kind := "failed"
				if errors.Is(err, context.Canceled) {
					kind = "cancelled"
				} else if errors.Is(err, context.DeadlineExceeded) {
					kind = "deadline"
				}
				span.SetStatus(codes.Error, "")
				span.SetAttributes(attribute.String("error.type", kind))
			}
			span.End()
		}()
	}
	var summaryModel *summaryUsageModel
	var local *overlay
	if req.Summary == nil {
		// Disabling summary generation does not invalidate the same Session.
		// Retain previously accepted summaries, but do not consume or update them.
		local, err = newOverlayState(req.TenantID, req.SessionID, req.AcceptedSnapshot, e.CapacityBytes, true)
	} else {
		summaryTransport := http.DefaultTransport.(*http.Transport).Clone()
		defer summaryTransport.CloseIdleConnections()
		summaryState := &modelTransport{base: summaryTransport}
		summaryModel = &summaryUsageModel{Model: newFixedModel(req.Summary.Model, req.MaxOutputTokens, summaryState), transport: summaryState}
		summarizer := summary.NewSummarizer(summaryModel, summary.WithEventThreshold(int(req.Summary.EventThreshold)))
		local, err = newSummaryOverlay(req.TenantID, req.SessionID, req.AcceptedSnapshot, e.CapacityBytes, summarizer)
	}
	if err != nil {
		return result, err
	}
	local.beforeAppend = e.beforeAppend
	local.tracer = e.Tracer
	defer func() {
		appends, bytes := local.stats()
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int64("app.session.overlay.appends", appends), attribute.Int64("app.session.overlay.bytes", bytes))
	}()
	assembly, err := e.assemble(ctx, req, nodes, local)
	if err != nil {
		return Result{}, err
	}
	defer assembly.close()
	r := runner.NewRunner(local.key.AppName, assembly.root, assembly.runnerOptions...)
	defer func() {
		if closeErr := r.Close(); err == nil && closeErr != nil {
			result = Result{}
			err = fmt.Errorf("runner close: %w", closeErr)
		}
	}()
	runCtx, cancel := context.WithCancel(ctx)
	assembly.mcp.cancel = cancel
	defer func() {
		cancel()
		if !assembly.wait(e.DrainTimeout) {
			result = Result{}
			err = errors.Join(err, ErrDrain)
		}
	}()
	events, err := r.Run(runCtx, local.key.UserID, local.key.SessionID, model.NewUserMessage(req.InputText), agent.WithDetachedCancel(false))
	if err != nil {
		return Result{}, fmt.Errorf("%w: SDK run initialization", ErrModel)
	}
	var observed error
	var terminalInvocationID string
	for {
		select {
		case <-ctx.Done():
			cancel()
			if !drain(events, e.DrainTimeout) {
				return Result{}, errors.Join(ctx.Err(), ErrDrain)
			}
			return Result{}, ctx.Err()
		case evt, ok := <-events:
			if failure := assembly.failure(); failure != nil {
				observed = failure
				cancel()
			}
			if failure := assembly.mcp.failure(); failure != nil {
				observed = failure
				cancel()
			}
			if !ok {
				if ctx.Err() != nil {
					return Result{}, ctx.Err()
				}
				if observed != nil {
					return Result{}, observed
				}
				if err = local.Err(); err != nil {
					if summaryModel != nil && summaryModel.err() != nil {
						return Result{}, summaryModel.err()
					}
					return Result{}, err
				}
				if failure := assembly.failure(); failure != nil {
					return Result{}, failure
				}
				if !validFinalText(result.FinalText) {
					return Result{}, ErrFinal
				}
				result.Snapshot, err = local.Snapshot()
				if err != nil {
					return Result{}, err
				}
				if summaryModel != nil {
					usage := summaryModel.usage()
					result.Usage.InputTokens += usage.InputTokens
					result.Usage.OutputTokens += usage.OutputTokens
					result.Usage.TotalTokens += usage.TotalTokens
				}
				if assembly.memory != nil {
					candidate, sealErr := assembly.memory.Seal(ctx)
					if sealErr != nil {
						return Result{}, sealErr
					}
					result.Memory = &candidate
				}
				if assembly.workspace != nil {
					result.Attachments = assembly.workspace.Attachments()
				}
				return result, nil
			}
			// The same terminal leaf executes again in a Loop. A new SDK
			// invocation starts a new final candidate, even if it returns empty.
			if evt != nil && evt.Author == terminal && evt.InvocationID != terminalInvocationID {
				terminalInvocationID = evt.InvocationID
				result.FinalText = ""
			}
			if evt == nil || evt.Response == nil {
				continue
			}
			if evt.Error != nil && observed == nil {
				observed = ErrModel
				if state := assembly.models[evt.Author]; state != nil && state.retryable() {
					observed = ErrRetryableModel
				}
				cancel()
			}
			toolState := assembly.tools[evt.Author]
			for _, choice := range evt.Choices {
				if !evt.IsPartial && len(choice.Message.ToolCalls) > 0 {
					result.FinalText = ""
					for _, call := range choice.Message.ToolCalls {
						if toolState == nil || !toolState.allowed[call.Function.Name] {
							observed = ErrMemoryTool
							cancel()
						}
					}
				}
				if toolState == nil && (len(choice.Message.ToolCalls) > 0 || len(choice.Delta.ToolCalls) > 0) {
					observed = ErrFinal
					cancel()
					continue
				}
				if evt.Author == terminal && !evt.IsPartial && choice.Message.Role == model.RoleAssistant && len(choice.Message.ToolCalls) == 0 && len(choice.Message.ContentParts) == 0 && choice.Message.Content != "" {
					result.FinalText = choice.Message.Content
				}
			}
			if !evt.IsPartial && evt.Usage != nil {
				result.UsageKnown = true
				result.Usage.InputTokens += evt.Usage.PromptTokens
				result.Usage.OutputTokens += evt.Usage.CompletionTokens
				result.Usage.TotalTokens += evt.Usage.TotalTokens
			}
			if observed != nil {
				if !drain(events, e.DrainTimeout) {
					return Result{}, errors.Join(observed, ErrDrain)
				}
				return Result{}, observed
			}
		}
	}
}
func drain(events <-chan *event.Event, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

// Keep transport status separate from provider error text. Retry classification
// never parses, returns or logs an arbitrary model response body.
type modelTransport struct {
	base           *http.Transport
	code           atomic.Int32
	networkFailure atomic.Bool
}

func (t *modelTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(r)
	if err != nil {
		t.networkFailure.Store(true)
	}
	if response != nil {
		t.code.Store(int32(response.StatusCode))
	}
	return response, err
}
func (t *modelTransport) retryable() bool {
	code := t.code.Load()
	return t.networkFailure.Load() || code == 408 || code == 429 || code >= 500
}

// Validate against the existing shared Final codec before writing a candidate.
// This is a reply-protocol boundary, not a new model-token cap or truncation.
func validFinalText(text string) bool {
	if len(text) > replycodec.MaxFinalTextBytes || !utf8.ValidString(text) {
		return false
	}
	_, err := replycodec.EncodeReplyIntent(replywire.ReplyIntent{
		SchemaVersion: 1, IntentID: "validation", AdmissionID: "validation", RunID: "validation",
		Execution: replywire.ReplyExecution{AttemptID: "validation", Generation: 1, CompletionID: "validation"},
		Sequence:  1, Kind: "final", Content: replywire.FinalTextContent{Type: "text", Text: text}, Deadline: "2000-01-01T00:00:00Z",
	})
	return err == nil
}

func newFixedModel(spec Model, maxTokens int64, httpState *modelTransport) model.Model {
	clientOptions := []openaioption.RequestOption{openaioption.WithMaxRetries(0), openaioption.WithAPIKey(spec.APIKey), openaioption.WithOrganization(""), openaioption.WithProject(""), openaioption.WithHTTPClient(&http.Client{Transport: httpState})}
	if spec.APIKey == "" {
		clientOptions = append(clientOptions, openaioption.WithHeaderDel("Authorization"))
	}
	return openai.New(spec.Name, openai.WithAPIKey(spec.APIKey), openai.WithVariant(openai.VariantOpenAI), openai.WithOptimizeForCache(false), openai.WithBaseURL(spec.Endpoint), openai.WithEnableTokenTailoring(false), openai.WithOpenAIOptions(clientOptions...),
		openai.WithChatRequestCallback(func(_ context.Context, request *openaiapi.ChatCompletionNewParams) {
			// SDK v1.11.2 clamps known model names even with tailoring disabled.
			// Its public typed callback runs after conversion and before send.
			// Preserve the validated Manifest value; the provider may reject it,
			// but neither a private cap nor a lower-limit retry is our contract.
			// Worker V1 sets no extra fields that could override this parameter.
			request.MaxCompletionTokens = openaiapi.Int(maxTokens)
		}))

}
