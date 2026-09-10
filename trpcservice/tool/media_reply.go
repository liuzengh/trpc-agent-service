package tool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

const (
	// SendTestImageID is the revision allowlist ID for the controlled media
	// reply smoke-test tool.
	SendTestImageID = "send_test_image"

	testImageName     = "trpc-agent-test.png"
	testImageFallback = "[image attachment: trpc-agent-test.png]"
	testImagePayload  = "Here is the requested test image."
)

var (
	// ErrUnavailable reports a media tool that cannot use the current durable
	// execution boundary. Its detail is deliberately stable and safe for a model.
	ErrUnavailable = errors.New("media reply tool is unavailable")
	// ErrRequiredUnavailable reports an explicitly required revision tool that
	// is not installed in this service process.
	ErrRequiredUnavailable = errors.New("required tool is unavailable")
)

// ExecutionContext contains the server-owned state available while a tool is
// executing. It is deliberately absent from model-visible tool results.
type ExecutionContext struct {
	TenantID    string
	EventID     string
	RequestID   string
	TraceID     string
	Attachments runtimestorage.AttachmentStore
	Replies     *ReplyCollector
	Audit       audit.Recorder
}

type executionContextKey struct{}

// WithExecutionContext attaches one durable execution boundary to ctx.
func WithExecutionContext(ctx context.Context, execution ExecutionContext) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, executionContextKey{}, execution)
}

func executionContextFromContext(ctx context.Context) (ExecutionContext, error) {
	if ctx == nil {
		return ExecutionContext{}, ErrUnavailable
	}
	execution, ok := ctx.Value(executionContextKey{}).(ExecutionContext)
	if !ok || runtimestorage.ValidateTenant(execution.TenantID) != nil || execution.EventID == "" || execution.Attachments == nil || execution.Replies == nil {
		return ExecutionContext{}, ErrUnavailable
	}
	return execution, nil
}

// ReplyIntent is a protocol-neutral media reply selected by a tool. It never
// includes provider URLs, credentials, temporary media IDs, or object keys.
type ReplyIntent struct {
	Kind       runtimestorage.ReplyKind
	Attachment attachment.Reference
	Payload    string
	Fallback   string
}

// ReplyCollector collects media reply intents for one Runner execution. It is
// concurrency-safe because a revision may enable parallel tool calls.
type ReplyCollector struct {
	mu            sync.Mutex
	intents       []ReplyIntent
	seen          map[string]struct{}
	auditRecorder *audit.Recorder
}

// NewReplyCollector returns an empty collector for one execution.
func NewReplyCollector() *ReplyCollector {
	return &ReplyCollector{seen: make(map[string]struct{})}
}

// Add validates and records one media intent. Exact duplicate intents are
// ignored so a retried or parallel tool call cannot duplicate delivery.
func (collector *ReplyCollector) Add(intent ReplyIntent) error {
	if collector == nil {
		return ErrUnavailable
	}
	normalized, err := runtimestorage.NormalizeReplyOutbox(runtimestorage.ReplyOutbox{
		Kind:       intent.Kind,
		Attachment: intent.Attachment,
		Fallback:   intent.Fallback,
	})
	if err != nil {
		return err
	}
	intent.Kind = normalized.Kind
	intent.Attachment = normalized.Attachment
	intent.Fallback = normalized.Fallback
	key := string(intent.Kind) + "\x00" + intent.Attachment.ID
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.seen[key]; ok {
		return nil
	}
	collector.seen[key] = struct{}{}
	collector.intents = append(collector.intents, intent)
	return nil
}

// Intents returns a stable snapshot of the collected media replies.
func (collector *ReplyCollector) Intents() []ReplyIntent {
	if collector == nil {
		return nil
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	return append([]ReplyIntent(nil), collector.intents...)
}

func (collector *ReplyCollector) stableAuditRecorder(recorder audit.Recorder) audit.Recorder {
	if collector == nil {
		return recorder
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.auditRecorder == nil {
		fixed := recorder.WithFixedTime()
		collector.auditRecorder = &fixed
	}
	return *collector.auditRecorder
}

// Factory constructs one stateless, context-bound platform tool.
type Factory interface {
	ID() string
	New() trpctool.Tool
}

// Registry resolves a published revision's deny-by-default authorization list
// to the platform tools installed by this service process.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry creates an immutable tool registry from the supplied factories.
func NewRegistry(factories ...Factory) (*Registry, error) {
	registry := &Registry{factories: make(map[string]Factory, len(factories))}
	for _, factory := range factories {
		if factory == nil || strings.TrimSpace(factory.ID()) == "" {
			return nil, ErrUnavailable
		}
		id := strings.TrimSpace(factory.ID())
		if _, ok := registry.factories[id]; ok {
			return nil, ErrUnavailable
		}
		registry.factories[id] = factory
	}
	return registry, nil
}

// DefaultRegistry contains the built-in platform tools. Future special-agent
// services can add a Factory without widening the Runner or channel contracts.
func DefaultRegistry() *Registry {
	registry, err := NewRegistry(sendTestImageFactory{})
	if err != nil {
		panic(err)
	}
	return registry
}

// Resolve returns only installed tools explicitly authorized by the published
// revision. Unknown optional tools remain unavailable; unknown required tools
// fail closed during Runner construction.
func (registry *Registry) Resolve(authorizations []appmodel.ToolAuthorization) ([]trpctool.Tool, error) {
	if registry == nil || len(authorizations) == 0 {
		return nil, nil
	}
	tools := make([]trpctool.Tool, 0, len(authorizations))
	for _, authorization := range authorizations {
		id := strings.TrimSpace(authorization.ToolID)
		factory, ok := registry.factories[id]
		if !ok {
			if authorization.Required {
				return nil, fmt.Errorf("%w: %s", ErrRequiredUnavailable, id)
			}
			continue
		}
		tool := factory.New()
		if tool == nil || tool.Declaration() == nil || tool.Declaration().Name != id {
			return nil, ErrUnavailable
		}
		tools = append(tools, tool)
	}
	return tools, nil
}

type sendTestImageFactory struct{}

func (sendTestImageFactory) ID() string { return SendTestImageID }

func (sendTestImageFactory) New() trpctool.Tool {
	return function.NewFunctionTool(
		func(ctx context.Context, _ struct{}) (sendTestImageResult, error) {
			return sendTestImage(ctx)
		},
		function.WithName(SendTestImageID),
		function.WithDescription("Queue a controlled test image as a native media reply when the user asks to receive a test image."),
		function.WithSkipSummarization(true),
		function.WithConcurrencySafe(true),
	)
}

type sendTestImageResult struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func sendTestImage(ctx context.Context) (sendTestImageResult, error) {
	execution, err := executionContextFromContext(ctx)
	if err != nil {
		return sendTestImageResult{}, err
	}
	recorder := execution.Replies.stableAuditRecorder(execution.Audit)
	policy := Policy{
		Recorder: recorder,
		Allowed:  map[string]Decision{SendTestImageID: Allow},
	}
	if _, err := policy.Decide(ctx, execution.RequestID, execution.TraceID, SendTestImageID); err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	reference, err := execution.Attachments.PutAttachment(ctx, execution.TenantID, attachment.Upload{
		ID:         testImageAttachmentID(execution.EventID),
		Kind:       attachment.KindImage,
		MIMEType:   "image/png",
		Name:       testImageName,
		Size:       int64(len(testImagePNG)),
		Provider:   "tool",
		ProviderID: SendTestImageID,
	}, bytes.NewReader(testImagePNG))
	if err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	if err := execution.Attachments.BindAttachments(ctx, execution.TenantID, execution.EventID, []attachment.Reference{reference}); err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	if err := execution.Replies.Add(ReplyIntent{Kind: runtimestorage.ReplyKindImage, Attachment: reference, Payload: testImagePayload, Fallback: testImageFallback}); err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	if err := recorder.ToolExecuted(ctx, execution.RequestID, execution.TraceID, SendTestImageID); err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	return sendTestImageResult{Status: "queued", Message: "The requested test image is queued for native delivery."}, nil
}

func testImageAttachmentID(eventID string) string {
	sum := sha256.Sum256([]byte(eventID + "\x00" + SendTestImageID))
	return "tool_" + hex.EncodeToString(sum[:16])
}

func redactedToolError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrUnavailable
}

// A fixed, valid 1x1 PNG keeps the first end-to-end slice deterministic and
// avoids introducing an image-generation provider into this transport issue.
var testImagePNG = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
	0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
	0x0d, 0x49, 0x44, 0x41, 0x54, 0x08, 0xd7, 0x63, 0xf8, 0xcf, 0xc0, 0xf0,
	0x1f, 0x00, 0x05, 0x00, 0x01, 0xff, 0x89, 0x99, 0x3d, 0x1d, 0x00, 0x00,
	0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
}
