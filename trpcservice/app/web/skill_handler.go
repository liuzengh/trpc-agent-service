// Package web serves the admin REST API and chat pages for the Agent platform.
package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/skill"
)

// SkillAPI exposes skill management (the four-level asset catalog) over HTTP.
type SkillAPI struct {
	mgr *skill.Manager
}

// NewSkillAPI returns a skill catalog API backed by the given manager.
func NewSkillAPI(mgr *skill.Manager) *SkillAPI {
	return &SkillAPI{mgr: mgr}
}

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

func (a *SkillAPI) create(w http.ResponseWriter, r *http.Request) {
	var s skill.Skill
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.mgr.Create(r.Context(), &s); err != nil {
		if errors.Is(err, skill.ErrSkillCodeExists) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, s)
}

func (a *SkillAPI) get(w http.ResponseWriter, r *http.Request) {
	s, err := a.mgr.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, skill.ErrSkillNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (a *SkillAPI) list(w http.ResponseWriter, r *http.Request) {
	all, err := a.mgr.List(r.Context(), r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, all)
}

func (a *SkillAPI) update(w http.ResponseWriter, r *http.Request) {
	var s skill.Skill
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	s.SkillID = r.PathValue("id")
	if err := a.mgr.Update(r.Context(), &s); err != nil {
		if errors.Is(err, skill.ErrSkillNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (a *SkillAPI) delete(w http.ResponseWriter, r *http.Request) {
	if err := a.mgr.Delete(r.Context(), r.PathValue("id")); err != nil {
		if errors.Is(err, skill.ErrSkillNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *SkillAPI) createVersion(w http.ResponseWriter, r *http.Request) {
	var v skill.SkillVersion
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	v.SkillID = r.PathValue("id")
	if err := a.mgr.CreateVersion(r.Context(), &v); err != nil {
		if errors.Is(err, skill.ErrVersionExists) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (a *SkillAPI) publishVersion(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.mgr.PublishVersion(r.Context(), r.PathValue("id"), version); err != nil {
		if errors.Is(err, skill.ErrSkillNotFound) || errors.Is(err, skill.ErrVersionNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"version": version})
}

func (a *SkillAPI) listVersions(w http.ResponseWriter, r *http.Request) {
	versions, err := a.mgr.ListVersions(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, versions)
}
