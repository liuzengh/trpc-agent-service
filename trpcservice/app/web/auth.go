package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"

	"golang.org/x/crypto/bcrypt"
)

type contextKey string

const AuthUserKey contextKey = "authenticated_user"

// LoginRequest is the JSON body for POST /auth/login.
type LoginRequest struct {
	UserID   string `json:"user_id"`
	Password string `json:"password"`
	// TenantID is accepted for old clients, but the member record is the
	// source of truth for tenant association.
	TenantID string `json:"tenant_id,omitempty"`
}

// LoginResponse is returned on successful login.
type LoginResponse struct {
	Token  string         `json:"token"`
	Member *member.Member `json:"member"`
}

// AuthMiddleware validates JWT tokens and injects claims into context.
type AuthMiddleware struct {
	memberMgr *member.Manager
	jwtSecret string
}

// NewAuthMiddleware creates an AuthMiddleware with the given member manager and JWT secret.
func NewAuthMiddleware(mgr *member.Manager, secret string) *AuthMiddleware {
	return &AuthMiddleware{
		memberMgr: mgr,
		jwtSecret: secret,
	}
}

// Wrap returns an http.Handler that enforces JWT auth on all routes except skipPaths.
//
// CORS preflight requests (OPTIONS) are always passed through: the browser never
// attaches the Authorization header to a preflight, so rejecting it makes the
// real request unreachable (the browser reports a CORS failure, not a 401). The
// handler behind this middleware is responsible for answering the preflight.
func (a *AuthMiddleware) Wrap(skipPaths []string, next http.Handler) http.Handler {
	skip := make(map[string]bool, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if r.Method == http.MethodOptions || skip[path] {
			next.ServeHTTP(w, r)
			return
		}
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or invalid Authorization header"})
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		claims, err := auth.ParseToken(a.jwtSecret, tokenStr)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or expired token"})
			return
		}
		ctx := context.WithValue(r.Context(), AuthUserKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Login handles POST /auth/login.
func (a *AuthMiddleware) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, nil)
		return
	}
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if req.UserID == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user_id and password are required"})
		return
	}
	m, err := a.memberMgr.GetByUserID(r.Context(), req.UserID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	if req.TenantID != "" && req.TenantID != m.TenantID {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	if m.Password == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(m.Password), []byte(req.Password)); err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	token, err := auth.GenerateToken(a.jwtSecret, m.TenantID, m.UserID, m.Role)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to generate token"})
		return
	}
	// Clear password before returning
	m.Password = ""
	writeJSON(w, http.StatusOK, &LoginResponse{Token: token, Member: m})
}

// Me returns the member represented by the validated bearer token.
func (a *AuthMiddleware) Me(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	if claims == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	m, err := a.memberMgr.Get(r.Context(), claims.TenantID, claims.UserID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "member no longer exists"})
		return
	}
	m.Password = ""
	writeJSON(w, http.StatusOK, m)
}

// Register handles POST /auth/register (creates a new member with password).
func (a *AuthMiddleware) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, nil)
		return
	}
	// Require auth for registration — only existing members can register new ones
	claims := GetClaims(r.Context())
	if claims == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	// Only owner/admin may register new members; the route gate (member:manage)
	// already enforces this, and the tenant is always the caller's own.
	if !HasPermission(claims.Role, PermMemberManage) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient permissions"})
		return
	}
	var req struct {
		TenantID string `json:"tenant_id"`
		UserID   string `json:"user_id"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	// Force tenant isolation: users can only create members in their own tenant.
	req.TenantID = claims.TenantID
	if req.UserID == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user_id and password are required"})
		return
	}
	if req.Role == "" {
		req.Role = member.RoleMember
	}
	switch req.Role {
	case member.RoleOwner, member.RoleAdmin, member.RoleMember:
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid role, must be owner/admin/member"})
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to hash password"})
		return
	}
	m := &member.Member{
		TenantID: req.TenantID,
		UserID:   req.UserID,
		Role:     req.Role,
		Password: string(hash),
	}
	if err := a.memberMgr.Create(r.Context(), m); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	m.Password = ""
	writeJSON(w, http.StatusCreated, m)
}

// GetClaims extracts the authenticated user claims from context.
func GetClaims(ctx context.Context) *auth.Claims {
	claims, _ := ctx.Value(AuthUserKey).(*auth.Claims)
	return claims
}
