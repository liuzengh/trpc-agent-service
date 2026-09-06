package web

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

// APIAccess is immutable after construction. Its zero value disables the API.
type APIAccess struct{ principals []apiPrincipal }
type apiPrincipal struct {
	digest   [sha256.Size]byte
	tenantID string
	bindings map[string]bool
	users    map[string]bool
}

func NewAPIAccess(cfg config.HTTPAPIConfig) (*APIAccess, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	access := &APIAccess{}
	if !cfg.Enabled {
		return access, nil
	}
	for _, p := range cfg.Principals {
		principal := apiPrincipal{digest: sha256.Sum256([]byte(p.Token)), tenantID: p.TenantID,
			bindings: map[string]bool{}, users: map[string]bool{}}
		for _, key := range p.BindingKeys {
			principal.bindings[key] = true
		}
		for _, user := range p.UserIDs {
			principal.users[user] = true
		}
		access.principals = append(access.principals, principal)
	}
	return access, nil
}

func WithAPIAccess(access *APIAccess) Option {
	return func(h *Handler) { h.apiAccess = access }
}

// WithSynchronousChat keeps a gateway-only node from running a synchronous Agent.
func WithSynchronousChat(enabled bool) Option {
	return func(h *Handler) { h.synchronousChat = enabled }
}

func (h *Handler) authenticateAPI(w http.ResponseWriter, r *http.Request) (apiPrincipal, bool) {
	values := r.Header.Values("Authorization")
	if len(values) == 1 && h.apiAccess != nil {
		parts := strings.Fields(values[0])
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			digest := sha256.Sum256([]byte(parts[1]))
			var matched apiPrincipal
			found := false
			for _, p := range h.apiAccess.principals {
				if subtle.ConstantTimeCompare(digest[:], p.digest[:]) == 1 {
					matched, found = p, true
				}
			}
			if found {
				return matched, true
			}
		}
	}
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "unauthorized"})
	return apiPrincipal{}, false
}

func (p apiPrincipal) allows(bindingKey, userID string) bool {
	return p.bindings[bindingKey] && p.users[userID]
}

var errAPIScopeForbidden = errors.New("HTTP API scope forbidden")

func (p apiPrincipal) authorizeScope(scope runtimecontext.Scope) error {
	if scope.Validate() != nil || scope.TenantID != p.tenantID || scope.ChannelType != "http" {
		return errAPIScopeForbidden
	}
	return nil
}
