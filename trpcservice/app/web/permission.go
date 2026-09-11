package web

import (
	"errors"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"
)

// ErrCrossTenant is the sentinel written back whenever a caller addresses a
// resource that lives in another tenant. Handlers answer 404 (never 403): a
// foreign resource must not even be confirmed to exist.
var ErrCrossTenant = errors.New("insufficient permissions for this tenant")

// ErrAssetDenied is the sentinel for an asset that exists but is not the
// caller's to read or change (someone else's private asset). Also answered as
// 404, for the same reason.
var ErrAssetDenied = errors.New("asset not found")

// Permission constants
//
// Roles, by platform design:
//   - owner:  platform-wide. Every permission, across every tenant.
//   - admin:  manages one tenant. Everything inside that tenant.
//   - member: an employee of the tenant. Creates and shares tenant assets
//     (knowledge, skills, IM bindings, endpoints), grants tools to the agents
//     it may use, chats, and reads its own usage. Never manages tenants,
//     members, credentials or audit.
//
// Tenant assets are author-owned: a newly created asset is private to its
// author until the author shares it with the tenant. Ownership itself is not a
// permission — it is checked against the row (see OwnsAsset), because the same
// permission means "manage everything" for admin and "manage mine" for member.
const (
	PermTenantManage   = "tenant:manage"
	PermTenantRead     = "tenant:read"
	PermMemberManage   = "member:manage"
	PermAgentCreate    = "agent:create"
	PermAgentRead      = "agent:read"
	PermAgentUpdate    = "agent:update"
	PermAgentDelete    = "agent:delete"
	PermToolManage     = "tool:manage"
	PermKbManage       = "kb:manage"
	PermSkillManage    = "skill:manage"
	PermChannelManage  = "channel:manage"
	PermEndpointManage = "endpoint:manage"
	PermSecretManage   = "secret:manage"
	PermAuditRead      = "audit:read"
	PermUsageRead      = "usage:read"
	PermChat           = "chat"
	PermDlqManage      = "dlq:manage"
)

// rolePermissions defines which permissions each role has.
//
// admin and member hold the same asset-manage permissions on purpose: the
// difference between them is scope (admin: the whole tenant, member: only the
// rows it authored), which no permission string can express.
var rolePermissions = map[string][]string{
	"owner": {
		PermTenantManage, PermTenantRead, PermMemberManage, PermAgentCreate, PermAgentRead, PermAgentUpdate, PermAgentDelete,
		PermToolManage, PermKbManage, PermSkillManage, PermChannelManage, PermEndpointManage, PermSecretManage,
		PermAuditRead, PermUsageRead, PermChat, PermDlqManage,
	},
	"admin": {
		PermTenantRead, PermMemberManage,
		PermAgentCreate, PermAgentRead, PermAgentUpdate,
		PermToolManage, PermKbManage, PermSkillManage, PermChannelManage, PermEndpointManage,
		PermAuditRead, PermUsageRead, PermChat, PermDlqManage,
	},
	"member": {
		PermTenantRead, PermAgentRead,
		PermAgentCreate, PermAgentUpdate, PermAgentDelete,
		PermToolManage, PermKbManage, PermSkillManage, PermChannelManage, PermEndpointManage,
		PermUsageRead, PermChat,
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

// ScopeTenant resolves the tenant filter a read request is allowed to use.
//
// The owner is platform-wide: the requested tenant is honoured, and an empty
// filter means "every tenant". Every other role is pinned to the tenant in its
// token, so a hand-written ?tenant_id= can never read another tenant's data —
// omitting the filter does not widen the view either.
func ScopeTenant(claims *auth.Claims, requested string) string {
	if claims == nil || claims.Role == member.RoleOwner {
		return requested
	}
	return claims.TenantID
}

// ClaimTenant resolves the tenant a resource write (create) lands in. Only the
// platform owner may pick another tenant; everyone else writes into their own,
// whatever the request body claims.
func ClaimTenant(claims *auth.Claims, requested string) string {
	if claims != nil && claims.Role == member.RoleOwner {
		return requested
	}
	if claims == nil {
		return ""
	}
	return claims.TenantID
}

// TenantAccessible reports whether the caller may touch a resource that belongs
// to resourceTenant. The owner is platform-wide; everyone else is limited to
// their own tenant. Platform-shared resources carry an empty tenant and stay
// reachable for every role (they are deliberately not tenant assets).
func TenantAccessible(claims *auth.Claims, resourceTenant string) bool {
	if claims == nil {
		return false
	}
	if claims.Role == member.RoleOwner {
		return true
	}
	if resourceTenant == "" {
		return true
	}
	return resourceTenant == claims.TenantID
}

// EndpointAccessible reports whether the caller may touch a model endpoint. A
// global endpoint is a platform-wide asset; a tenant endpoint follows the usual
// tenant rule.
func EndpointAccessible(claims *auth.Claims, ep llm.Endpoint) bool {
	if claims == nil {
		return false
	}
	if ep.Scope == llm.ScopeGlobal || ep.TenantID == "" {
		return true
	}
	return TenantAccessible(claims, ep.TenantID)
}

// ---------------------------------------------------------------- assets --
//
// Tenant assets (knowledge bases, skills, channel bindings, model endpoints)
// are author-owned. Three rules drive every read and write:
//
//	visible   private -> only its author (and tenant managers) may see the row
//	          shared  -> the whole tenant may see it
//	manage    the author may change/delete its own row; a tenant manager may
//	          change/delete any row in its tenant
//	global    platform-wide assets (no tenant) belong to the owner alone
//
// "Manage everything" is a role property (admin/owner), "manage mine" is a row
// property (created_by). Keeping ownership out of the permission table is what
// lets one permission string serve both roles.

// Asset visibility values.
const (
	VisibilityPrivate = "private"
	VisibilityShared  = "shared"
)

// NormalizeVisibility maps an incoming visibility to a stored value, defaulting
// to private: an asset is not tenant-visible until its author says so.
func NormalizeVisibility(v string) string {
	if v == VisibilityShared {
		return VisibilityShared
	}
	return VisibilityPrivate
}

// ManagesTenantAssets reports whether the caller manages every asset of its
// tenant. The owner manages every tenant's assets.
func ManagesTenantAssets(claims *auth.Claims) bool {
	if claims == nil {
		return false
	}
	return claims.Role == member.RoleOwner || claims.Role == member.RoleAdmin
}

// OwnsAsset reports whether the caller authored the row. An empty createdBy
// means the row predates author tracking or was authored by the system: such
// rows are treated as tenant-managed only, never as "mine".
func OwnsAsset(claims *auth.Claims, createdBy string) bool {
	return claims != nil && createdBy != "" && createdBy == claims.UserID
}

// CanManageAsset reports whether the caller may modify or delete the row. The
// caller must already have passed the tenant check (see TenantAccessible).
func CanManageAsset(claims *auth.Claims, createdBy string) bool {
	return ManagesTenantAssets(claims) || OwnsAsset(claims, createdBy)
}

// CanReadAsset reports whether the caller may see the row: its author, any
// tenant manager, or anyone in the tenant once the author shared it.
//
// A row with no author is treated as tenant-shared. Author tracking arrived
// after those rows did, and they were tenant-visible all along; hiding them
// would silently remove every pre-existing asset from its tenant. Rows created
// from now on always carry an author, so the private default applies to them.
func CanReadAsset(claims *auth.Claims, createdBy, visibility string) bool {
	if claims == nil {
		return false
	}
	if createdBy == "" {
		return true
	}
	if CanManageAsset(claims, createdBy) {
		return true
	}
	return visibility == VisibilityShared
}

// GlobalAssetWritable reports whether the caller may create or modify a
// platform-wide asset. Global assets are not tenant assets: only the owner.
func GlobalAssetWritable(claims *auth.Claims) bool {
	return claims != nil && claims.Role == member.RoleOwner
}

// FilterReadable keeps the rows the caller may see. It is the list-side twin of
// CanReadAsset, for stores that hand back the whole tenant at once.
func FilterReadable[T any](claims *auth.Claims, rows []T, createdBy func(T) string, visibility func(T) string) []T {
	out := make([]T, 0, len(rows))
	for _, row := range rows {
		if CanReadAsset(claims, createdBy(row), visibility(row)) {
			out = append(out, row)
		}
	}
	return out
}

// WriteAssetDenied answers a read/write attempt on an asset the caller may not
// touch. The row is reported as missing on purpose, so a private asset can
// never be probed for existence. The refusal is audited (with the actor), so
// probing leaves a trace even though the response is a plain 404.
func WriteAssetDenied(claims *auth.Claims, w http.ResponseWriter, auditor assetAuditor, kind, id, tenantID, reason string) {
	recordAssetDenied(claims, auditor, kind, id, tenantID, reason)
	writeError(w, http.StatusNotFound, ErrAssetDenied)
}

// WriteCrossTenant answers a cross-tenant access attempt. The resource is
// reported as missing on purpose, so a foreign id can never be probed.
func WriteCrossTenant(w http.ResponseWriter) {
	writeError(w, http.StatusNotFound, ErrCrossTenant)
}

// ScopeMember narrows a conversation-history query to the rows the role may see:
// a plain member only ever sees the sessions they produced themselves; admin and
// owner may look across members (owner also across tenants via ScopeTenant).
func ScopeMember(claims *auth.Claims, requested string) string {
	if claims != nil && claims.Role == member.RoleMember {
		return claims.UserID
	}
	return requested
}

// CanReadSession reports whether the caller may read a conversation session.
// Sessions carry both the tenant and the producing member, so this is the single
// place the row-level history rule lives.
func CanReadSession(claims *auth.Claims, tenantID, memberID string) bool {
	if claims == nil {
		return false
	}
	if claims.Role == member.RoleOwner {
		return true
	}
	if tenantID != claims.TenantID {
		return false
	}
	if claims.Role == member.RoleMember {
		return memberID == claims.UserID
	}
	return true
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
		// Member management belongs to the tenant (admin) and to the platform
		// owner; a plain member never manages members.
		return PermMemberManage
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
		// Members bind their team's IM accounts; the handler pins the tenant and
		// the per-row ownership rule.
		return PermChannelManage
	case path == "/tools" || strings.HasPrefix(path, "/tools/"):
		// The catalog is readable and grantable by any tenant member; there is
		// no tool-definition write API.
		return PermToolManage
	case path == "/kbs" || strings.HasPrefix(path, "/kbs/"):
		return PermKbManage
	case path == "/skills" || strings.HasPrefix(path, "/skills/"):
		return PermSkillManage
	case path == "/audit" || strings.HasPrefix(path, "/audit/"):
		return PermAuditRead
	case path == "/usage" || strings.HasPrefix(path, "/usage/"):
		// Members read their own metering; the handler narrows the query to the
		// rows they triggered.
		return PermUsageRead
	case path == "/dlq" || strings.HasPrefix(path, "/dlq/"):
		return PermDlqManage
	case path == "/chat" || path == "/chat/stream":
		return PermChat
	case path == "/history" || strings.HasPrefix(path, "/history/") ||
		path == "/sessions" || strings.HasPrefix(path, "/sessions/"):
		return PermAgentRead
	case path == "/agents" || strings.HasPrefix(path, "/agents/"):
		// Members author their own agents (their private/shared scope is the
		// row-level rule in the handler), so create/update/delete are open at
		// the route level too.
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
		// Model endpoints are tenant assets like knowledge or skills: members
		// register the endpoints their team uses, and own what they register.
		return PermEndpointManage
	}
	return ""
}
