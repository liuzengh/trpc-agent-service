// Package internalhttp is the Channel owner's mTLS-only HTTP surface. A separate
// process listener uses this handler; it is not registered on the Session router.
package internalhttp

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"time"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

type Runtime interface {
	ReadSnapshot(context.Context, application.WorkloadPrincipal) (domain.Snapshot, error)
	ResolveCredentials(context.Context, application.WorkloadPrincipal, string, string, application.ResolveRequest) (application.ResolveResponse, error)
	ReportObservations(context.Context, application.WorkloadPrincipal, application.ObservationsRequest) error
}
type principalKey struct{}
type Handler struct {
	service    Runtime
	preflight  PreflightRuntime
	principals map[string]application.WorkloadPrincipal
	mux        *http.ServeMux
}

func NewHandler(service Runtime, principals []application.WorkloadPrincipal, preflights ...PreflightRuntime) (*Handler, error) {
	if service == nil || len(principals) == 0 || len(principals) > 32 || len(preflights) > 1 {
		return nil, application.ErrWorkloadDenied
	}
	h := &Handler{service: service, principals: map[string]application.WorkloadPrincipal{}, mux: http.NewServeMux()}
	if len(preflights) == 1 {
		h.preflight = preflights[0]
	}
	instances := map[string]bool{}
	for _, p := range principals {
		u, err := url.Parse(p.PrincipalID)
		if err != nil || u.Scheme != "spiffe" || u.Host == "" || u.Path == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil || p.Audience != application.WorkloadAudience || !domain.ValidID(p.InstanceID) || !domain.ValidID(p.ScopeID) || h.principals[p.PrincipalID].PrincipalID != "" || instances[p.InstanceID] {
			return nil, application.ErrWorkloadDenied
		}
		kinds := map[string]bool{}
		for _, kind := range p.Consumers {
			if kinds[kind] || !slices.Contains([]string{"wecom_connection", "telegram_webhook", "telegram_delivery", "telegram_registration", "telegram_receiver", "telegram_preflight", "wecom_preflight"}, kind) {
				return nil, application.ErrWorkloadDenied
			}
			kinds[kind] = true
		}
		p.Consumers = slices.Clone(p.Consumers)
		h.principals[p.PrincipalID] = p
		instances[p.InstanceID] = true
	}
	h.mux.HandleFunc("GET /internal/v1/channel-accounts/snapshot", h.snapshot)
	h.mux.HandleFunc("POST /internal/v1/tenants/{tenant_id}/channel-accounts/{account_id}/credentials:resolve", h.resolve)
	h.mux.HandleFunc("POST /internal/v1/channel-account-observations", h.observations)
	if h.preflight != nil {
		h.mux.HandleFunc("POST /internal/v1/channel-preflights:claim", h.preflightClaim)
		h.mux.HandleFunc("POST /internal/v1/channel-preflights/{preflight_id}/credentials:resolve", h.preflightResolve)
		h.mux.HandleFunc("POST /internal/v1/channel-preflights/{preflight_action}", h.preflightComplete)
	}
	return h, nil
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || len(r.TLS.VerifiedChains) == 0 {
		failure(w, 401, "CHANNEL_WORKLOAD_UNAUTHENTICATED")
		return
	}
	cert := r.TLS.PeerCertificates[0]
	if !clientCertificate(cert) || len(cert.URIs) != 1 {
		failure(w, 403, "CHANNEL_WORKLOAD_DENIED")
		return
	}
	p, ok := h.principals[cert.URIs[0].String()]
	if !ok {
		failure(w, 403, "CHANNEL_WORKLOAD_DENIED")
		return
	}
	if r.URL.RawQuery != "" {
		failure(w, 400, "CHANNEL_INPUT_INVALID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, principalKey{}, p)
	h.mux.ServeHTTP(w, r.WithContext(ctx))
}
func clientCertificate(cert *x509.Certificate) bool {
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageClientAuth {
			return true
		}
	}
	return false
}
func principal(r *http.Request) application.WorkloadPrincipal {
	return r.Context().Value(principalKey{}).(application.WorkloadPrincipal)
}
func (h *Handler) snapshot(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		failure(w, 400, "CHANNEL_INPUT_INVALID")
		return
	}
	value, err := h.service.ReadSnapshot(r.Context(), principal(r))
	if err != nil {
		handleError(w, err)
		return
	}
	writeJSON(w, 200, value, domain.MaxSnapshotBytes)
}
func decode[T any](w http.ResponseWriter, r *http.Request, schema string, limit int64) (T, bool) {
	var input T
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || r.Header.Get("Content-Encoding") != "" {
		failure(w, 400, "CHANNEL_INPUT_INVALID")
		return input, false
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	defer clear(raw)
	if err != nil {
		failure(w, 400, "CHANNEL_INPUT_INVALID")
		return input, false
	}
	if int64(len(raw)) > limit {
		failure(w, 413, "CHANNEL_LIMIT_EXCEEDED")
		return input, false
	}
	if err = channelv1.Decode(schema, raw, &input); err != nil {
		failure(w, 400, "CHANNEL_INPUT_INVALID")
		return input, false
	}
	return input, true
}
func (h *Handler) resolve(w http.ResponseWriter, r *http.Request) {
	tenant, account := r.PathValue("tenant_id"), r.PathValue("account_id")
	if !domain.ValidID(tenant) || !domain.ValidID(account) {
		failure(w, 400, "CHANNEL_INPUT_INVALID")
		return
	}
	input, ok := decode[application.ResolveRequest](w, r, "credentials-resolve-request.schema.json", 16*1024)
	if !ok {
		return
	}
	result, err := h.service.ResolveCredentials(r.Context(), principal(r), tenant, account, input)
	if err != nil {
		handleError(w, err)
		return
	}
	writeJSON(w, 200, result, 64*1024)
}
func (h *Handler) observations(w http.ResponseWriter, r *http.Request) {
	input, ok := decode[application.ObservationsRequest](w, r, "observations.schema.json", 128*1024)
	if !ok {
		return
	}
	if err := h.service.ReportObservations(r.Context(), principal(r), input); err != nil {
		handleError(w, err)
		return
	}
	w.WriteHeader(204)
}
func writeJSON(w http.ResponseWriter, status int, value any, max int) {
	raw, err := json.Marshal(value)
	defer clear(raw)
	if err != nil || len(raw) > max {
		failure(w, 500, "CHANNEL_SOURCE_INTEGRITY")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}
func failure(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": "channel internal request was not completed"}})
}
func handleError(w http.ResponseWriter, err error) {
	status, code := 503, "CHANNEL_DEPENDENCY_UNAVAILABLE"
	switch {
	case errors.Is(err, application.ErrWorkloadDenied):
		status, code = 403, "CHANNEL_WORKLOAD_DENIED"
	case errors.Is(err, application.ErrAccountNotFound):
		status, code = 404, "CHANNEL_ACCOUNT_NOT_FOUND"
	case errors.Is(err, application.ErrEpochMismatch):
		status, code = 409, "CHANNEL_SOURCE_EPOCH_MISMATCH"
	}
	var d *domain.Error
	if errors.As(err, &d) {
		code = d.Code
		switch code {
		case domain.InputInvalid:
			status = 400
		case domain.SourceIntegrity, domain.TargetIntegrity:
			status = 500
		case "CHANNEL_LIMIT_EXCEEDED":
			status = 413
		default:
			status = 409
		}
	}
	failure(w, status, code)
}
