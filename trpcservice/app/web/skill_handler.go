// Package web serves the admin REST API and chat pages for the Agent platform.
package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/asset"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/skill"
)

// SkillAPI exposes skill management (the four-level asset catalog) over HTTP.
type SkillAPI struct {
	mgr     *skill.Manager
	auditor assetAuditor // optional: asset changes are audited
}

// NewSkillAPI returns a skill catalog API backed by the given manager.
func NewSkillAPI(mgr *skill.Manager) *SkillAPI {
	return &SkillAPI{mgr: mgr}
}

// SetAuditor wires asset-change auditing. May be nil.
func (a *SkillAPI) SetAuditor(rec assetAuditor) { a.auditor = rec }

// Register mounts skill routes on the mux.
func (a *SkillAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /skills", a.create)
	mux.HandleFunc("GET /skills", a.list)
	mux.HandleFunc("GET /skills/{id}", a.get)
	mux.HandleFunc("PUT /skills/{id}", a.update)
	mux.HandleFunc("DELETE /skills/{id}", a.delete)
	mux.HandleFunc("POST /skills/{id}/versions", a.createVersion)
	mux.HandleFunc("POST /skills/{id}/versions/{version}/publish", a.publishVersion)
	mux.HandleFunc("GET /skills/{id}/versions", a.listVersions)
}

// skillTenant projects a skill onto the tenant that owns it. A global skill is
// platform-wide and has no owner.
func skillTenant(s *skill.Skill) string {
	if s.Scope == skill.ScopeGlobal || s.OwnerTenantID == nil {
		return ""
	}
	return *s.OwnerTenantID
}

// readable resolves the skill a caller may read. A global skill is shared
// platform-wide (readable by everyone, owned by the platform). A tenant skill
// must match the caller's tenant and be its own or shared.
func (a *SkillAPI) readable(w http.ResponseWriter, r *http.Request, id string) (*skill.Skill, bool) {
	s, err := a.mgr.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, skill.ErrSkillNotFound) {
			writeError(w, http.StatusNotFound, err)
			return nil, false
		}
		writeError(w, http.StatusInternalServerError, err)
		return nil, false
	}
	if s.Scope == skill.ScopeGlobal {
		return s, true
	}
	claims := GetClaims(r.Context())
	if !TenantAccessible(claims, skillTenant(s)) {
		WriteCrossTenant(w)
		return nil, false
	}
	if !CanReadAsset(claims, s.CreatedBy, s.Visibility) {
		WriteAssetDenied(claims, w, a.auditor, assetKindSkill, id, skillTenant(s), "not the author and not shared")
		return nil, false
	}
	return s, true
}

// writable resolves the skill a caller may change. A global skill is
// platform-owned: only the platform owner. A tenant skill follows the asset
// rule (tenant managers, or its author).
func (a *SkillAPI) writable(w http.ResponseWriter, r *http.Request, id string) (*skill.Skill, bool) {
	s, ok := a.readable(w, r, id)
	if !ok {
		return nil, false
	}
	claims := GetClaims(r.Context())
	if s.Scope == skill.ScopeGlobal {
		if !GlobalAssetWritable(claims) {
			WriteAssetDenied(claims, w, a.auditor, assetKindSkill, id, "", "global skill is platform-owned")
			return nil, false
		}
		return s, true
	}
	if !CanManageAsset(claims, s.CreatedBy) {
		WriteAssetDenied(claims, w, a.auditor, assetKindSkill, id, skillTenant(s), "shared but not authored")
		return nil, false
	}
	return s, true
}

func (a *SkillAPI) create(w http.ResponseWriter, r *http.Request) {
	var s skill.Skill
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	// A tenant skill belongs to its creator's tenant: only the platform owner
	// may mint a global skill or one owned by another tenant.
	if s.Scope == skill.ScopeGlobal {
		if !GlobalAssetWritable(claims) {
			writeError(w, http.StatusForbidden, errors.New("only the platform owner may create a global skill"))
			return
		}
		s.OwnerTenantID = nil
	} else {
		tenant := ClaimTenant(claims, "")
		if s.OwnerTenantID != nil {
			tenant = ClaimTenant(claims, *s.OwnerTenantID)
		}
		if tenant == "" {
			writeError(w, http.StatusBadRequest, errors.New("owner_tenant_id is required for a tenant skill"))
			return
		}
		s.OwnerTenantID = &tenant
	}
	s.Visibility = asset.VisibilityOrDefault(s.Visibility)
	if claims != nil {
		s.CreatedBy = claims.UserID
	}
	if err := a.mgr.Create(r.Context(), &s); err != nil {
		if errors.Is(err, skill.ErrSkillCodeExists) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	recordAssetAllowed(claims, a.auditor, assetKindSkill, s.SkillID, skillTenant(&s))
	writeJSON(w, http.StatusCreated, s)
}

func (a *SkillAPI) get(w http.ResponseWriter, r *http.Request) {
	s, ok := a.readable(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (a *SkillAPI) list(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	all, err := a.mgr.List(r.Context(), ScopeTenant(claims, r.URL.Query().Get("tenant_id")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	// Global skills are platform-shared and stay visible to everyone; a tenant
	// skill is row-level filtered (own or shared).
	visible := make([]*skill.Skill, 0, len(all))
	for _, s := range all {
		if s.Scope == skill.ScopeGlobal || CanReadAsset(claims, s.CreatedBy, s.Visibility) {
			visible = append(visible, s)
		}
	}
	writeJSON(w, http.StatusOK, visible)
}

func (a *SkillAPI) update(w http.ResponseWriter, r *http.Request) {
	var s skill.Skill
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	s.SkillID = id
	// Ownership is immutable: a write never moves a skill between tenants nor
	// promotes it to global. Visibility is author-controlled, so it is taken
	// from the payload (defaulting back to private when omitted).
	s.Scope = existing.Scope
	s.OwnerTenantID = existing.OwnerTenantID
	s.CreatedBy = existing.CreatedBy
	if err := a.mgr.Update(r.Context(), &s); err != nil {
		if errors.Is(err, skill.ErrSkillNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindSkill, id, skillTenant(existing))
	writeJSON(w, http.StatusOK, s)
}

func (a *SkillAPI) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	if err := a.mgr.Delete(r.Context(), id); err != nil {
		if errors.Is(err, skill.ErrSkillNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindSkill, id, skillTenant(existing))
	w.WriteHeader(http.StatusNoContent)
}

func (a *SkillAPI) createVersion(w http.ResponseWriter, r *http.Request) {
	skillID := r.PathValue("id")
	existing, ok := a.writable(w, r, skillID)
	if !ok {
		return
	}
	var v skill.SkillVersion
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	v.SkillID = skillID
	if err := a.mgr.CreateVersion(r.Context(), &v); err != nil {
		if errors.Is(err, skill.ErrVersionExists) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindSkill, skillID, skillTenant(existing))
	writeJSON(w, http.StatusCreated, v)
}

func (a *SkillAPI) publishVersion(w http.ResponseWriter, r *http.Request) {
	skillID := r.PathValue("id")
	existing, ok := a.writable(w, r, skillID)
	if !ok {
		return
	}
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.mgr.PublishVersion(r.Context(), skillID, version); err != nil {
		if errors.Is(err, skill.ErrSkillNotFound) || errors.Is(err, skill.ErrVersionNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindSkill, skillID, skillTenant(existing))
	writeJSON(w, http.StatusOK, map[string]int{"version": version})
}

func (a *SkillAPI) listVersions(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.readable(w, r, r.PathValue("id")); !ok {
		return
	}
	versions, err := a.mgr.ListVersions(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, versions)
}
