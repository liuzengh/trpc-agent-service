package admin

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Credential is one named administrator token scoped to explicit tenants. A
// tenant list containing "*" grants every tenant; otherwise the URL tenant
// must be listed. Clients can never widen scope by sending another tenant_id.
type Credential struct {
	Name    string
	Token   string
	Tenants map[string]bool
	Role    Role
}

type Role string

const (
	RoleAdmin    Role = "admin"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
)

// Allows reports whether the credential may operate on tenantID.
func (credential Credential) Allows(tenantID string) bool {
	if credential.Tenants["*"] {
		return true
	}
	return credential.Tenants[tenantID]
}

// ParseCredentials parses the TRPC_AGENT_ADMIN_TOKENS format:
// "name=token:tenant-a,tenant-b;ops=second-token:*". Entries are separated by
// ";", the tenant list is optional and defaults to no tenants (deny all).
func ParseCredentials(value string) ([]Credential, error) {
	var credentials []Credential
	for _, entry := range strings.Split(value, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, rest, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("admin: credential entry %q is missing '='", entry)
		}
		name = strings.TrimSpace(name)
		token, tenants, _ := strings.Cut(rest, ":")
		role := RoleAdmin
		if scoped, declared, ok := strings.Cut(tenants, "|"); ok {
			tenants = scoped
			role = Role(strings.TrimSpace(declared))
		}
		credential := Credential{Name: name, Token: strings.TrimSpace(token), Tenants: make(map[string]bool), Role: role}
		if credential.Name == "" || credential.Token == "" {
			return nil, fmt.Errorf("admin: credential entry %q requires a name and token", entry)
		}
		if credential.Role != RoleAdmin && credential.Role != RoleOperator && credential.Role != RoleViewer {
			return nil, fmt.Errorf("admin: credential entry %q has unsupported role", entry)
		}
		for _, tenantID := range strings.Split(tenants, ",") {
			tenantID = strings.TrimSpace(tenantID)
			if tenantID != "" {
				credential.Tenants[tenantID] = true
			}
		}
		credentials = append(credentials, credential)
	}
	return credentials, nil
}

func (credential Credential) AllowsRequest(request *http.Request) bool {
	role := credential.Role
	if role == "" {
		role = RoleAdmin
	}
	switch role {
	case RoleAdmin:
		return true
	case RoleViewer:
		return request != nil && (request.Method == http.MethodGet || request.Method == http.MethodHead)
	case RoleOperator:
		if request == nil || request.Method == http.MethodGet || request.Method == http.MethodHead {
			return true
		}
		return strings.Contains(strings.Trim(request.URL.Path, "/"), "/operations/")
	default:
		return false
	}
}

type contextKey string

const (
	actorContextKey contextKey = "admin.actor"
	traceContextKey contextKey = "admin.trace"
)

// ActorFrom returns the authenticated administrator name recorded by Wrap.
func ActorFrom(ctx context.Context) string {
	value, _ := ctx.Value(actorContextKey).(string)
	return value
}

// WithActor records a caller-owned actor name, used by non-HTTP paths such as
// the first-boot control-plane seed.
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorContextKey, actor)
}

// TraceFrom returns the request trace identifier recorded by Wrap.
func TraceFrom(ctx context.Context) string {
	value, _ := ctx.Value(traceContextKey).(string)
	return value
}

// Authenticator enforces bearer authentication and explicit tenant scope on
// the administration API. With zero credentials every request is rejected.
type Authenticator struct {
	credentials []Credential
	now         func() [16]byte
}

// NewAuthenticator creates an Authenticator. A nil or empty credential list is
// valid and rejects every request, which keeps an unconfigured production
// deployment closed instead of open.
func NewAuthenticator(credentials []Credential) (*Authenticator, error) {
	for _, credential := range credentials {
		if credential.Name == "" || credential.Token == "" {
			return nil, errors.New("admin: credentials require a name and token")
		}
	}
	return &Authenticator{credentials: credentials, now: func() [16]byte {
		var value [16]byte
		_, _ = rand.Read(value[:])
		return value
	}}, nil
}

// Wrap authenticates the bearer token, extracts the tenant from the URL path
// (never from client-supplied bodies or headers), and rejects requests whose
// credential does not cover that tenant.
func (auth *Authenticator) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		token := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
		if token == "" || token == request.Header.Get("Authorization") {
			writeError(writer, http.StatusUnauthorized, errors.New("admin bearer token is required"))
			return
		}
		var matched *Credential
		for i := range auth.credentials {
			candidate := &auth.credentials[i]
			if len(candidate.Token) == len(token) && subtle.ConstantTimeCompare([]byte(candidate.Token), []byte(token)) == 1 {
				matched = candidate
				break
			}
		}
		if matched == nil {
			writeError(writer, http.StatusUnauthorized, errors.New("invalid admin credential"))
			return
		}
		tenantID, ok := pathTenant(request.URL.Path)
		if !ok {
			http.NotFound(writer, request)
			return
		}
		if !matched.Allows(tenantID) {
			writeError(writer, http.StatusForbidden, errors.New("admin credential is not authorized for this tenant"))
			return
		}
		if !matched.AllowsRequest(request) {
			writeError(writer, http.StatusForbidden, errors.New("admin credential role does not allow this operation"))
			return
		}
		traceID := strings.TrimSpace(request.Header.Get("X-Trace-ID"))
		if traceID == "" || len(traceID) > 128 {
			random := auth.now()
			traceID = hex.EncodeToString(random[:])
		}
		ctx := context.WithValue(request.Context(), actorContextKey, matched.Name)
		ctx = context.WithValue(ctx, traceContextKey, traceID)
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

// pathTenant extracts the tenant segment from authenticated administration
// paths
// so scope checks never trust client-provided tenant fields.
func pathTenant(path string) (string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "tenants" || (parts[3] != "configs" && parts[3] != "storage" && parts[3] != "apps" && parts[3] != "operations") {
		return "", false
	}
	if parts[2] == "" {
		return "", false
	}
	return parts[2], true
}
