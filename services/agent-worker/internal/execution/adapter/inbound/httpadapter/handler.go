// Package httpadapter exposes two deliberately separate proof queries. Verified
// mTLS caller identity selects the authorized endpoint, not an HTTP auth header.
package httpadapter

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrAttemptDenied = errors.New("active Attempt authorization denied")
	ErrFinalMismatch = errors.New("committed Final mismatches request")
	ErrNotYet        = errors.New("committed evidence not yet available")
	ErrUnavailable   = errors.New("execution proof temporarily unavailable")
)

type AttemptVerifier interface {
	VerifyAttempt(context.Context, proof.AttemptRequest) (proof.AttemptResponse, error)
}
type FinalVerifier interface {
	VerifyFinal(context.Context, proof.FinalRequest) (proof.FinalResponse, error)
}
type ManagementReader interface {
	List(context.Context, string, int, int) (managementv1.RunPage, error)
	Get(context.Context, string, string) (managementv1.RunDetail, error)
	Audit(context.Context, string, int, int) (managementv1.AuditPage, error)
}
type BackendMigrator interface {
	Execute(context.Context, proof.BackendMigrationRequest) (proof.BackendMigrationResponse, error)
}
type ApprovalManager interface {
	List(context.Context, string, int, int) (approvalv1.Page, error)
	Decide(context.Context, string, string, string, string, string, string) (approvalv1.DecisionResponse, error)
}
type Options struct {
	ReplyArtifacts                       ReplyArtifactReader
	Knowledge                            KnowledgeImporter
	Artifacts                            ArtifactOperator
	Management                           ManagementReader
	BackendMigrations                    BackendMigrator
	Approvals                            ApprovalManager
	Tracer                               trace.Tracer
	ControlPrincipals, GatewayPrincipals []string
	Timeout                              time.Duration
	// MaxConcurrent bounds each independent class: proof reads and data operations.
	MaxConcurrent int
}
type Handler struct {
	downloadSlots     chan struct{}
	finalCallers      map[string]bool
	replyArtifacts    ReplyArtifactReader
	knowledgeImporter KnowledgeImporter
	artifacts         ArtifactOperator
	management        ManagementReader
	usageManagement   interface {
		Usage(context.Context, string) (governancev1.UsageSummary, error)
	}
	backendMigrations BackendMigrator
	approvals         ApprovalManager
	tracer            trace.Tracer
	attempts          AttemptVerifier
	finals            FinalVerifier
	control, gateway  map[string]bool
	timeout           time.Duration
	slots             chan struct{}
	mux               *http.ServeMux
}

func New(attempts AttemptVerifier, finals FinalVerifier, o Options) (*Handler, error) {
	if attempts == nil || finals == nil {
		return nil, errors.New("both Execution proof queries are required")
	}
	if o.Timeout == 0 {
		o.Timeout = 5 * time.Second
	}
	if o.MaxConcurrent == 0 {
		o.MaxConcurrent = 64
	}
	if o.Timeout < time.Millisecond || o.Timeout > time.Minute || o.MaxConcurrent < 1 || o.MaxConcurrent > 1024 {
		return nil, errors.New("invalid Execution proof operational bounds")
	}
	control, err := principals(o.ControlPrincipals)
	if err != nil {
		return nil, err
	}
	gateway, err := principals(o.GatewayPrincipals)
	if err != nil {
		return nil, err
	}
	for identity := range control {
		if gateway[identity] {
			return nil, errors.New("Control and Gateway proof identities must be distinct")
		}
	}
	finalCallers := map[string]bool{}
	for id := range control {
		finalCallers[id] = true
	}
	for id := range gateway {
		finalCallers[id] = true
	}
	h := &Handler{finalCallers: finalCallers, replyArtifacts: o.ReplyArtifacts, knowledgeImporter: o.Knowledge, artifacts: o.Artifacts, management: o.Management, backendMigrations: o.BackendMigrations, approvals: o.Approvals, tracer: o.Tracer, attempts: attempts, finals: finals, control: control, gateway: gateway, timeout: o.Timeout, slots: make(chan struct{}, o.MaxConcurrent), downloadSlots: make(chan struct{}, o.MaxConcurrent), mux: http.NewServeMux()}
	h.usageManagement, _ = o.Management.(interface {
		Usage(context.Context, string) (governancev1.UsageSummary, error)
	})
	h.mux.HandleFunc("POST "+proof.AttemptVerifyPath, h.attempt)
	h.mux.HandleFunc("POST "+proof.FinalVerifyPath, h.final)
	if o.ReplyArtifacts != nil {
		h.mux.HandleFunc("POST "+proof.ReplyArtifactPath, h.replyArtifact)
	}
	if o.Artifacts != nil {
		h.mux.HandleFunc("POST "+proof.ArtifactPath, h.artifact)
	}
	if o.Knowledge != nil {
		h.mux.HandleFunc("POST "+proof.KnowledgePath, h.knowledge)
	}
	if o.Management != nil {
		h.mux.HandleFunc("GET /internal/v1/management/tenants/{tenant_id}/runs", h.listRuns)
		h.mux.HandleFunc("GET /internal/v1/management/tenants/{tenant_id}/runs/{run_id}", h.getRun)
		h.mux.HandleFunc("GET /internal/v1/management/tenants/{tenant_id}/audit-events", h.listAudit)
		if h.usageManagement != nil {
			h.mux.HandleFunc("GET /internal/v1/management/tenants/{tenant_id}/usage-summary", h.usageSummary)
		}
	}
	if o.BackendMigrations != nil {
		h.mux.HandleFunc("POST "+proof.BackendMigrationPath, h.backendMigration)
	}
	if o.Approvals != nil {
		h.mux.HandleFunc("GET /internal/v1/approvals/tenants/{tenant_id}/operations", h.listApprovals)
		h.mux.HandleFunc("POST /internal/v1/approvals/tenants/{tenant_id}/operations/{operation_id}/decision", h.decideApproval)
	}
	return h, nil
}
func principals(values []string) (map[string]bool, error) {
	if len(values) == 0 || len(values) > 64 {
		return nil, errors.New("explicit Execution proof caller allowlists are required")
	}
	out := map[string]bool{}
	for _, v := range values {
		u, e := url.Parse(v)
		if e != nil || len(v) > 256 || u.Scheme == "" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.String() != v || out[v] {
			return nil, errors.New("invalid Execution proof caller URI")
		}
		out[v] = true
	}
	return out, nil
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	h.mux.ServeHTTP(w, r)
}
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, allowed map[string]bool) bool {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		respondError(w, http.StatusUnauthorized, "MTLS_REQUIRED")
		return false
	}
	leaf := r.TLS.VerifiedChains[0][0]
	if leaf == nil || len(leaf.URIs) != 1 || leaf.URIs[0] == nil || !allowed[leaf.URIs[0].String()] {
		respondError(w, http.StatusForbidden, "CALLER_DENIED")
		return false
	}
	return true
}
func (h *Handler) read(w http.ResponseWriter, r *http.Request, limit int) ([]byte, bool) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "INVALID_PROOF_REQUEST")
		return nil, false
	}
	if r.ContentLength > int64(limit) {
		respondError(w, http.StatusRequestEntityTooLarge, "PROOF_TOO_LARGE")
		return nil, false
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(limit)))
	if err != nil {
		var over *http.MaxBytesError
		if errors.As(err, &over) {
			respondError(w, http.StatusRequestEntityTooLarge, "PROOF_TOO_LARGE")
		} else {
			respondError(w, http.StatusBadRequest, "INVALID_PROOF_REQUEST")
		}
		return nil, false
	}
	return raw, true
}
func (h *Handler) bounded(w http.ResponseWriter, r *http.Request) (context.Context, context.CancelFunc, bool) {
	// Data requests may synchronously cause Control to verify a committed Final.
	// Reserve a separately bounded proof class; a full download class must never
	// occupy the slot needed by that callback. Neither class is unbounded.
	slots := h.downloadSlots
	if r.URL.Path == proof.AttemptVerifyPath || r.URL.Path == proof.FinalVerifyPath {
		slots = h.slots
	}
	select {
	case slots <- struct{}{}:
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		return ctx, func() { cancel(); <-slots }, true
	default:
		respondError(w, http.StatusServiceUnavailable, "PROOF_UNAVAILABLE")
		return nil, nil, false
	}
}
func (h *Handler) attempt(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.control) {
		return
	}
	ctx, cancel, ok := h.bounded(w, r)
	if !ok {
		return
	}
	defer cancel()
	raw, ok := h.read(w, r, proof.MaxAttemptProofBytes)
	if !ok {
		return
	}
	defer clear(raw)
	request, err := proof.DecodeAttemptRequest(raw)
	if err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_PROOF_REQUEST")
		return
	}
	response, err := h.attempts.VerifyAttempt(ctx, request)
	if err != nil {
		proofError(w, err)
		return
	}
	if ctx.Err() != nil {
		proofError(w, ErrUnavailable)
		return
	}
	body, err := proof.EncodeAttemptResponse(response)
	if err != nil {
		proofError(w, ErrUnavailable)
		return
	}
	if response.ManifestID != request.ManifestID || response.ManifestDigest != request.ManifestDigest || response.WorkerID != request.WorkloadIdentity {
		proofError(w, ErrUnavailable)
		return
	}
	_, _ = w.Write(body)
}
func (h *Handler) final(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, h.finalCallers) {
		return
	}
	ctx, cancel, ok := h.bounded(w, r)
	if !ok {
		return
	}
	defer cancel()
	raw, ok := h.read(w, r, proof.MaxFinalProofBytes)
	if !ok {
		return
	}
	request, err := proof.DecodeFinalRequest(raw)
	if err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_PROOF_REQUEST")
		return
	}
	// Only the already-authorized Control/Gateway proof endpoint inherits internal W3C.
	ctx = tracecontext.FromHeaders(r.Header).Restore(ctx)
	ctx, span := telemetrytrace.Start(h.tracer, ctx, "worker.reply.verify", trace.WithSpanKind(trace.SpanKindServer))
	response, err := h.finals.VerifyFinal(ctx, request)
	defer func() { telemetrytrace.End(span, err) }()
	if err != nil {
		proofError(w, err)
		return
	}
	if ctx.Err() != nil {
		err = ctx.Err()
		proofError(w, ErrUnavailable)
		return
	}
	body, err := proof.EncodeFinalResponse(response)
	if err != nil {
		proofError(w, ErrUnavailable)
		return
	}
	if response.FinalRequest != request {
		err = ErrUnavailable
		proofError(w, ErrUnavailable)
		return
	}
	_, _ = w.Write(body)
}
func proofError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrAttemptDenied):
		respondError(w, http.StatusForbidden, "ATTEMPT_DENIED")
	case errors.Is(err, ErrFinalMismatch):
		respondError(w, http.StatusConflict, "FINAL_MISMATCH")
	case errors.Is(err, ErrNotYet):
		respondError(w, http.StatusNotFound, "PROOF_NOT_YET")
	default:
		respondError(w, http.StatusServiceUnavailable, "PROOF_UNAVAILABLE")
	}
}
func respondError(w http.ResponseWriter, status int, code string) {
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"code":"`+code+`"}`)
}
