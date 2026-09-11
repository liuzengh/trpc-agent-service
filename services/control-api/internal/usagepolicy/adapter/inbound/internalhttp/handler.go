package internalhttp

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	channelapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

type Source interface {
	Runtime(context.Context, string) (governancev1.Policy, error)
}
type Handler struct {
	source     Source
	principals map[string]bool
	mux        *http.ServeMux
}

func New(source Source, principals []channelapp.WorkloadPrincipal) (*Handler, error) {
	if source == nil || len(principals) == 0 {
		return nil, channelapp.ErrWorkloadDenied
	}
	h := &Handler{source: source, principals: map[string]bool{}, mux: http.NewServeMux()}
	for _, p := range principals {
		u, e := url.Parse(p.PrincipalID)
		if e != nil || u.Scheme != "spiffe" || u.Host == "" || !domain.ValidID(p.InstanceID) || h.principals[p.PrincipalID] {
			return nil, channelapp.ErrWorkloadDenied
		}
		h.principals[p.PrincipalID] = true
	}
	h.mux.HandleFunc("GET /internal/v1/tenants/{tenant_id}/usage-policy", h.get)
	return h, nil
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		failure(w, 401)
		return
	}
	c := r.TLS.PeerCertificates[0]
	if !clientCertificate(c) || len(c.URIs) != 1 || !h.principals[c.URIs[0].String()] {
		failure(w, 403)
		return
	}
	if r.URL.RawQuery != "" || r.ContentLength != 0 {
		failure(w, 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	h.mux.ServeHTTP(w, r.WithContext(ctx))
}
func clientCertificate(c *x509.Certificate) bool {
	for _, u := range c.ExtKeyUsage {
		if u == x509.ExtKeyUsageClientAuth {
			return true
		}
	}
	return false
}
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !domain.ValidID(tenant) {
		failure(w, 400)
		return
	}
	p, e := h.source.Runtime(r.Context(), tenant)
	if e != nil {
		failure(w, 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
}
func failure(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "USAGE_POLICY_UNAVAILABLE"}})
}
