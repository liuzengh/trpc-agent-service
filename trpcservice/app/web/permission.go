package web

import (
	"net/http"
	"strings"
)

// Permission constants
const (
	PermTenantManage  = "tenant:manage"
	PermTenantRead    = "tenant:read"
	PermAgentCreate   = "agent:create"
	PermAgentRead     = "agent:read"
	PermAgentUpdate   = "agent:update"
	PermAgentDelete   = "agent:delete"
	PermToolManage    = "tool:manage"
	PermKbManage      = "kb:manage"
	PermSkillManage   = "skill:manage"
	PermChannelManage = "channel:manage"
	PermSecretManage  = "secret:manage"
	PermAuditRead     = "audit:read"
	PermChat          = "chat"
	PermDlqManage     = "dlq:manage"
)

// rolePermissions defines which permissions each role has.
var rolePermissions = map[string][]string{
	"owner": {
		PermTenantManage, PermTenantRead, PermAgentCreate, PermAgentRead, PermAgentUpdate, PermAgentDelete,
		PermToolManage, PermKbManage, PermSkillManage, PermChannelManage, PermSecretManage,
		PermAuditRead, PermChat, PermDlqManage,
	},
	"admin": {
		PermAgentCreate, PermAgentRead, PermAgentUpdate,
		PermToolManage, PermKbManage, PermSkillManage, PermChannelManage,
		PermAuditRead, PermChat, PermDlqManage,
	},
	"member": {
		PermTenantRead, PermAgentRead, PermChat,
	},
}

// HasPermission checks if the given role has the specified permission.
func HasPermission(role, perm string) bool {
	perms, ok := rolePermissions[role]
	if !ok {
		return false
	}
	for _, p := range perms {
		if p == perm {
			return true
		}
	}
	return false
}

// RequirePermission returns an http.HandlerFunc that checks the user has the required permission.
func RequirePermission(perm string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := GetClaims(r.Context())
		if claims == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
			return
		}
		if !HasPermission(claims.Role, perm) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient permissions"})
			return
		}
		next(w, r)
	}
}

// RequireRoutePermission applies the platform's coarse route-level RBAC.
// Handlers still validate resource ownership; this gate keeps high-privilege
// routes unreachable for ordinary members.
func RequireRoutePermission(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		perm := routePermission(r)
		if perm == "" {
			next.ServeHTTP(w, r)
			return
		}
		claims := GetClaims(r.Context())
		if claims == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
			return
		}
		if !HasPermission(claims.Role, perm) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient permissions"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func routePermission(r *http.Request) string {
	path := r.URL.Path
	switch {
	case path == "/members" || strings.HasPrefix(path, "/members/"):
		return PermTenantManage
	case path == "/tenants":
		if r.Method == http.MethodGet {
			return PermTenantRead
		}
		return PermTenantManage
	case strings.HasPrefix(path, "/tenants/"):
		if r.Method == http.MethodGet && !strings.HasSuffix(path, "/config-versions") {
			return PermTenantRead
		}
		return PermTenantManage
	case path == "/secrets" || strings.HasPrefix(path, "/secrets/"):
		return PermSecretManage
	case path == "/channels" || strings.HasPrefix(path, "/channels/"):
		return PermChannelManage
	case path == "/tools" || strings.HasPrefix(path, "/tools/"):
		return PermToolManage
	case path == "/kbs" || strings.HasPrefix(path, "/kbs/"):
		return PermKbManage
	case path == "/skills" || strings.HasPrefix(path, "/skills/"):
		return PermSkillManage
	case path == "/audit" || strings.HasPrefix(path, "/audit/"):
		return PermAuditRead
	case path == "/usage" || strings.HasPrefix(path, "/usage/"):
		return PermAuditRead
	case path == "/dlq" || strings.HasPrefix(path, "/dlq/"):
		return PermDlqManage
	case path == "/chat" || path == "/chat/stream":
		return PermChat
	case path == "/history" || strings.HasPrefix(path, "/history/") ||
		path == "/sessions" || strings.HasPrefix(path, "/sessions/"):
		return PermAgentRead
	case path == "/agents" || strings.HasPrefix(path, "/agents/"):
		if r.Method == http.MethodGet {
			return PermAgentRead
		}
		if strings.HasSuffix(path, "/publish") || strings.HasSuffix(path, "/rollback") {
			return PermAgentUpdate
		}
		switch r.Method {
		case http.MethodPost:
			return PermAgentCreate
		case http.MethodPut, http.MethodPatch:
			return PermAgentUpdate
		case http.MethodDelete:
			return PermAgentDelete
		}
	case path == "/endpoints" || strings.HasPrefix(path, "/endpoints/"):
		if r.Method == http.MethodGet {
			return PermAgentRead
		}
		switch r.Method {
		case http.MethodPost:
			return PermAgentCreate
		case http.MethodPut, http.MethodPatch:
			return PermAgentUpdate
		case http.MethodDelete:
			return PermAgentDelete
		}
	}
	return ""
}
