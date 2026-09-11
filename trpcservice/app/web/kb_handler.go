package web

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/asset"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
)

// KnowledgeAPI exposes knowledge-base management over HTTP.
type KnowledgeAPI struct {
	mgr     *knowledge.Manager
	auditor assetAuditor // optional: asset changes are audited
}

// NewKnowledgeAPI returns a KB API backed by the given manager.
func NewKnowledgeAPI(mgr *knowledge.Manager) *KnowledgeAPI {
	return &KnowledgeAPI{mgr: mgr}
}

// SetAuditor wires asset-change auditing. May be nil (changes are then not
// recorded; the endpoints stay functional).
func (a *KnowledgeAPI) SetAuditor(rec assetAuditor) { a.auditor = rec }

// Register mounts KB routes on the mux.
func (a *KnowledgeAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /kbs", a.create)
	mux.HandleFunc("GET /kbs", a.list)
	mux.HandleFunc("GET /kbs/{id}", a.get)
	mux.HandleFunc("PUT /kbs/{id}", a.update)
	mux.HandleFunc("DELETE /kbs/{id}", a.delete)
	mux.HandleFunc("POST /kbs/{id}/documents", a.addDocument)
	mux.HandleFunc("GET /kbs/{id}/documents", a.listDocuments)
	mux.HandleFunc("POST /kbs/{id}/search", a.search)
}

// readable resolves the KB a caller may read: its tenant must match and, unless
// the caller manages the tenant, the KB must be its own or shared. A KB that
// exists but is somebody else's private one is reported as missing, so a
// private id can never be probed for existence.
func (a *KnowledgeAPI) readable(w http.ResponseWriter, r *http.Request, id string) (*knowledge.KnowledgeBase, bool) {
	kb, err := a.mgr.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, knowledge.ErrKBNotFound) {
			writeError(w, http.StatusNotFound, err)
			return nil, false
		}
		writeError(w, http.StatusInternalServerError, err)
		return nil, false
	}
	claims := GetClaims(r.Context())
	if !TenantAccessible(claims, kb.TenantID) {
		WriteCrossTenant(w)
		return nil, false
	}
	if !CanReadAsset(claims, kb.CreatedBy, kb.Visibility) {
		WriteAssetDenied(claims, w, a.auditor, assetKindKB, id, kb.TenantID, "not the author and not shared")
		return nil, false
	}
	return kb, true
}

// writable resolves the KB a caller may change or delete: tenant managers may
// change any KB of their tenant, everyone else only the ones they authored.
func (a *KnowledgeAPI) writable(w http.ResponseWriter, r *http.Request, id string) (*knowledge.KnowledgeBase, bool) {
	kb, ok := a.readable(w, r, id)
	if !ok {
		return nil, false
	}
	claims := GetClaims(r.Context())
	if !CanManageAsset(claims, kb.CreatedBy) {
		WriteAssetDenied(claims, w, a.auditor, assetKindKB, id, kb.TenantID, "shared but not authored")
		return nil, false
	}
	return kb, true
}

func (a *KnowledgeAPI) create(w http.ResponseWriter, r *http.Request) {
	var kb knowledge.KnowledgeBase
	if err := json.NewDecoder(r.Body).Decode(&kb); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	// A KB is a tenant asset: only the platform owner may place it elsewhere,
	// and the author is always the authenticated member.
	kb.TenantID = ClaimTenant(claims, kb.TenantID)
	if kb.TenantID == "" {
		writeError(w, http.StatusBadRequest, errors.New("tenant_id is required"))
		return
	}
	kb.Visibility = asset.VisibilityOrDefault(kb.Visibility)
	if claims != nil {
		kb.CreatedBy = claims.UserID
	}
	if kb.ID == "" {
		kb.ID = uuid.NewString()
	}
	if err := a.mgr.Create(r.Context(), &kb); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	recordAssetAllowed(claims, a.auditor, assetKindKB, kb.ID, kb.TenantID)
	writeJSON(w, http.StatusCreated, kb)
}

func (a *KnowledgeAPI) list(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	kbs, err := a.mgr.List(r.Context(), ScopeTenant(claims, r.URL.Query().Get("tenant_id")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	// The store filters by tenant; visibility is a row-level rule, so it is
	// applied here: private KBs stay invisible to everyone but their author and
	// the tenant's managers.
	visible := FilterReadable(claims, kbs,
		func(kb *knowledge.KnowledgeBase) string { return kb.CreatedBy },
		func(kb *knowledge.KnowledgeBase) string { return kb.Visibility })
	writeJSON(w, http.StatusOK, visible)
}

func (a *KnowledgeAPI) get(w http.ResponseWriter, r *http.Request) {
	kb, ok := a.readable(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, kb)
}

// update renames a KB and/or changes its visibility. Sharing is the author's
// call, so a shared KB stays read-only for everyone else.
func (a *KnowledgeAPI) update(w http.ResponseWriter, r *http.Request) {
	var kb knowledge.KnowledgeBase
	if err := json.NewDecoder(r.Body).Decode(&kb); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	kb.ID = id
	kb.TenantID = existing.TenantID
	kb.CreatedBy = existing.CreatedBy
	kb.EmbeddingEndpointID = existing.EmbeddingEndpointID
	kb.CollectionName = existing.CollectionName
	kb.Dimension = existing.Dimension
	if kb.Name == "" {
		kb.Name = existing.Name
	}
	if err := a.mgr.UpdateKB(r.Context(), &kb); err != nil {
		if errors.Is(err, knowledge.ErrKBNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindKB, id, existing.TenantID)
	writeJSON(w, http.StatusOK, kb)
}

func (a *KnowledgeAPI) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	kb, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	if err := a.mgr.Delete(r.Context(), id); err != nil {
		if errors.Is(err, knowledge.ErrKBNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindKB, id, kb.TenantID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *KnowledgeAPI) addDocument(w http.ResponseWriter, r *http.Request) {
	kbID := r.PathValue("id")
	kb, ok := a.writable(w, r, kbID)
	if !ok {
		return
	}
	var doc knowledge.Document
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	doc.KBID = kbID
	if doc.ID == "" {
		doc.ID = uuid.NewString()
	}
	if err := a.mgr.AddDocument(r.Context(), &doc); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindKB, kbID, kb.TenantID)
	writeJSON(w, http.StatusCreated, doc)
}

func (a *KnowledgeAPI) listDocuments(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.readable(w, r, r.PathValue("id")); !ok {
		return
	}
	docs, err := a.mgr.ListDocuments(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, docs)
}

func (a *KnowledgeAPI) search(w http.ResponseWriter, r *http.Request) {
	kbID := r.PathValue("id")
	if _, ok := a.readable(w, r, kbID); !ok {
		return
	}
	var req struct {
		Query string `json:"query"`
		Limit int    `json:"limit,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	hits, err := a.mgr.Search(r.Context(), kbID, req.Query, req.Limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, hits)
}
