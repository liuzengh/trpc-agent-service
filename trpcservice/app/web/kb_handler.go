package web

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
)

// KnowledgeAPI exposes knowledge-base management over HTTP.
type KnowledgeAPI struct {
	mgr *knowledge.Manager
}

// NewKnowledgeAPI returns a KB API backed by the given manager.
func NewKnowledgeAPI(mgr *knowledge.Manager) *KnowledgeAPI {
	return &KnowledgeAPI{mgr: mgr}
}

// Register mounts KB routes on the mux.
func (a *KnowledgeAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /kbs", a.create)
	mux.HandleFunc("GET /kbs", a.list)
	mux.HandleFunc("GET /kbs/{id}", a.get)
	mux.HandleFunc("DELETE /kbs/{id}", a.delete)
	mux.HandleFunc("POST /kbs/{id}/documents", a.addDocument)
	mux.HandleFunc("GET /kbs/{id}/documents", a.listDocuments)
	mux.HandleFunc("POST /kbs/{id}/search", a.search)
}

func (a *KnowledgeAPI) create(w http.ResponseWriter, r *http.Request) {
	var kb knowledge.KnowledgeBase
	if err := json.NewDecoder(r.Body).Decode(&kb); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if kb.ID == "" {
		kb.ID = uuid.NewString()
	}
	if err := a.mgr.Create(r.Context(), &kb); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, kb)
}

func (a *KnowledgeAPI) list(w http.ResponseWriter, r *http.Request) {
	kbs, err := a.mgr.List(r.Context(), r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, kbs)
}

func (a *KnowledgeAPI) get(w http.ResponseWriter, r *http.Request) {
	kb, err := a.mgr.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, knowledge.ErrKBNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, kb)
}

func (a *KnowledgeAPI) delete(w http.ResponseWriter, r *http.Request) {
	if err := a.mgr.Delete(r.Context(), r.PathValue("id")); err != nil {
		if errors.Is(err, knowledge.ErrKBNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *KnowledgeAPI) addDocument(w http.ResponseWriter, r *http.Request) {
	var doc knowledge.Document
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	doc.KBID = r.PathValue("id")
	if doc.ID == "" {
		doc.ID = uuid.NewString()
	}
	if err := a.mgr.AddDocument(r.Context(), &doc); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, doc)
}

func (a *KnowledgeAPI) listDocuments(w http.ResponseWriter, r *http.Request) {
	docs, err := a.mgr.ListDocuments(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, docs)
}

func (a *KnowledgeAPI) search(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
		Limit int    `json:"limit,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	hits, err := a.mgr.Search(r.Context(), r.PathValue("id"), req.Query, req.Limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, hits)
}
