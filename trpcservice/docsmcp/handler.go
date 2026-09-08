package docsmcp

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

const ToolName = "search_project_docs"

type Handler struct {
	index        *Index
	token        string
	protocol     http.Handler
	slots        chan struct{}
	protocolGate chan struct{}
}

func NewHandler(index *Index, token string) (*Handler, error) {
	if index == nil || len(index.documents) == 0 || len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("documentation index and strong server token required")
	}
	h := &Handler{index: index, token: token, slots: make(chan struct{}, 8), protocolGate: make(chan struct{}, 1)}
	// WithoutSession avoids the pinned SDK's unclosable session-manager
	// cleanup goroutine. JSON POSTs are authenticated independently; no SSE.
	server := mcp.NewServer("trpc-project-docs", "1", mcp.WithServerPath("/mcp"), mcp.WithoutSession(), mcp.WithPostSSEEnabled(false), mcp.WithGetSSEEnabled(false), mcp.WithServerLogger(silentLogger{}))
	tool := mcp.NewTool(ToolName, mcp.WithDescription("Keyword search over curated public project documentation. Use short keywords such as Redis Session. Returns relative document paths, line numbers and bounded excerpts. Cannot read arbitrary files, credentials, messages or URLs."), mcp.WithString("query", mcp.Required(), mcp.Description("One to eight short keywords, at most 256 characters")), mcp.WithNumber("limit", mcp.Description("Maximum matches, integer 1 to 5; default 3")), mcp.WithToolAnnotations(&mcp.ToolAnnotations{ReadOnlyHint: mcp.BoolPtr(true), DestructiveHint: mcp.BoolPtr(false), IdempotentHint: mcp.BoolPtr(true), OpenWorldHint: mcp.BoolPtr(false)}))
	server.RegisterTool(tool, h.search)
	h.protocol = server.Handler()
	return h, nil
}

func (h *Handler) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if h == nil || h.index == nil || len(h.index.documents) == 0 {
		return errors.New("docs MCP unavailable")
	}
	return nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path != "/mcp" {
		http.NotFound(w, r)
		return
	}
	if r.URL.RawQuery != "" || r.Header.Get("Origin") != "" {
		http.Error(w, "request origin or query rejected", http.StatusForbidden)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+h.token)) != 1 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" || r.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON required", http.StatusUnsupportedMediaType)
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		http.Error(w, "busy", http.StatusTooManyRequests)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.HeaderCarrier(r.Header))
	ctx, span := otel.Tracer("trpc-agent-service/docs-mcp").Start(ctx, "mcp.docs.request", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(attribute.String("http.route", "/mcp"), attribute.String("http.request.method", r.Method))
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	raw, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	var envelope struct {
		Method string `json:"method"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	switch envelope.Method {
	case "initialize", "notifications/initialized", "tools/list", "tools/call", "ping":
	default:
		http.Error(w, "method unavailable", http.StatusBadRequest)
		return
	}
	r = r.WithContext(ctx)
	r.Body = io.NopCloser(bytes.NewReader(raw))
	// The pinned SDK mutates lifecycle capability maps during initialize.
	// Serialize its small JSON requests, with a cancellable bounded queue,
	// rather than exposing that mutable state to concurrent initializations.
	select {
	case h.protocolGate <- struct{}{}:
		defer func() { <-h.protocolGate }()
	case <-ctx.Done():
		http.Error(w, "request expired", http.StatusServiceUnavailable)
		return
	}
	h.protocol.ServeHTTP(w, r)
}

func (h *Handler) search(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ctx, span := otel.Tracer("trpc-agent-service/docs-mcp").Start(ctx, "mcp.docs.search")
	defer span.End()
	span.SetAttributes(attribute.String("gen_ai.tool.name", ToolName))
	invalid := func() (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{mcp.NewTextContent("Invalid bounded keyword search request.")}}, nil
	}
	if req == nil {
		return invalid()
	}
	for key := range req.Params.Arguments {
		if key != "query" && key != "limit" {
			return invalid()
		}
	}
	query, ok := req.Params.Arguments["query"].(string)
	if !ok {
		return invalid()
	}
	limit := 3
	if value, exists := req.Params.Arguments["limit"]; exists {
		number, ok := value.(float64)
		if !ok || math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || number < 1 || number > 5 {
			return invalid()
		}
		limit = int(number)
	}
	result, err := h.index.Search(ctx, query, limit)
	if err != nil {
		return invalid()
	}
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > 32<<10 {
		return nil, errors.New("documentation result unavailable")
	}
	return &mcp.CallToolResult{Content: []mcp.Content{mcp.NewTextContent(string(raw))}, StructuredContent: result}, nil
}

// Do not let dependency logs expose request arguments, bearer credentials or
// provider bodies. Platform tracing exports metadata only.
type silentLogger struct{}

func (silentLogger) Debug(...interface{})          {}
func (silentLogger) Debugf(string, ...interface{}) {}
func (silentLogger) Info(...interface{})           {}
func (silentLogger) Infof(string, ...interface{})  {}
func (silentLogger) Warn(...interface{})           {}
func (silentLogger) Warnf(string, ...interface{})  {}
func (silentLogger) Error(...interface{})          {}
func (silentLogger) Errorf(string, ...interface{}) {}
func (silentLogger) Fatal(...interface{})          {}
func (silentLogger) Fatalf(string, ...interface{}) {}
