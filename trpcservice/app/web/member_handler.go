package web

import (
	"encoding/json"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"golang.org/x/crypto/bcrypt"
)

// MemberAPI handles /members routes.
type MemberAPI struct {
	mgr *member.Manager
}

// NewMemberAPI returns a MemberAPI backed by the given manager.
func NewMemberAPI(mgr *member.Manager) *MemberAPI {
	return &MemberAPI{mgr: mgr}
}

// Register mounts member routes on the mux.
func (a *MemberAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /members", a.list)
	mux.HandleFunc("POST /members", a.create)
	mux.HandleFunc("PUT /members/{userID}", a.updateRole)
	mux.HandleFunc("DELETE /members/{userID}", a.delete)
}

// list returns all members for the authenticated user's tenant.
func (a *MemberAPI) list(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	if claims == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	if !HasPermission(claims.Role, PermTenantManage) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient permissions"})
		return
	}
	members, err := a.mgr.List(r.Context(), claims.TenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, members)
}

func (a *MemberAPI) create(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	if claims == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	if !HasPermission(claims.Role, PermTenantManage) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient permissions"})
		return
	}
	var req struct {
		UserID   string `json:"user_id"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if req.UserID == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user_id and password are required"})
		return
	}
	if req.Role == "" {
		req.Role = member.RoleMember
	}
	if req.Role == member.RoleOwner && claims.Role != member.RoleOwner {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "only owner can create an owner"})
		return
	}
	switch req.Role {
	case member.RoleOwner, member.RoleAdmin, member.RoleMember:
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid role, must be owner/admin/member"})
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	m := &member.Member{TenantID: claims.TenantID, UserID: req.UserID, Role: req.Role, Password: string(hash)}
	if err := a.mgr.Create(r.Context(), m); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	m.Password = ""
	writeJSON(w, http.StatusCreated, m)
}

// updateRole updates a member's role. Only owner/admin can do this.
func (a *MemberAPI) updateRole(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	if claims == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	// Only owner and admin can modify roles.
	if claims.Role != member.RoleOwner && claims.Role != member.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient permissions"})
		return
	}
	userID := r.PathValue("userID")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user_id is required"})
		return
	}
	target, err := a.mgr.Get(r.Context(), claims.TenantID, userID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	var req struct {
		Role string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if req.Role == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "role is required"})
		return
	}
	// Validate role.
	switch req.Role {
	case member.RoleOwner, member.RoleAdmin, member.RoleMember:
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid role, must be owner/admin/member"})
		return
	}
	if claims.Role != member.RoleOwner && (target.Role == member.RoleOwner || req.Role == member.RoleOwner) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "only owner can manage owners"})
		return
	}
	if target.UserID == claims.UserID && req.Role != claims.Role {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cannot change your own role"})
		return
	}
	if err := a.mgr.UpdateRole(r.Context(), claims.TenantID, userID, req.Role); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "role updated"})
}

// delete removes a member from the tenant.
func (a *MemberAPI) delete(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	if claims == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	if claims.Role != member.RoleOwner && claims.Role != member.RoleAdmin {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient permissions"})
		return
	}
	userID := r.PathValue("userID")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user_id is required"})
		return
	}
	target, err := a.mgr.Get(r.Context(), claims.TenantID, userID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if target.Role == member.RoleOwner {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "only owner can delete an owner"})
		return
	}
	if target.UserID == claims.UserID {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cannot delete yourself"})
		return
	}
	if err := a.mgr.Delete(r.Context(), claims.TenantID, userID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "member deleted"})
}
