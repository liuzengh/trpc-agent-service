package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// controlPlane is the optional wiring that makes /admin/v2 exist at all.
// Legacy deployments (no control_plane.mode=mysql) have no database to talk
// to, and mounting routes that would return 500 on every request is worse
// than not mounting them: an operator gets "not found" and can tell the
// feature is off, rather than chasing a half-built endpoint.
type controlPlane struct {
	db      *controlplane.DB
	resolve *auth.Resolver
	// journal and exec give the operator surfaces their authority: the
	// ledger owns the tool_calls table's read/disposition shape, and the
	// execution service owns "what does unblocking this session mean" — the
	// handler must not re-derive either from raw SQL.
	journal *tool.Journal
	exec    *execution.Service
}

// WithControlPlane enables the versioned control-plane API alongside the
// existing tenant routes. It does not replace them: the legacy /admin/tenants
// surface keeps working for a config-file deployment, and the two answer to
// different facts (the YAML file and MySQL respectively).
func (s *Service) WithControlPlane(db *controlplane.DB, resolve *auth.Resolver) *Service {
	s.cp = &controlPlane{
		db:      db,
		resolve: resolve,
		journal: tool.NewJournal(db),
		exec:    execution.NewService(db, 0),
	}
	return s
}

// handleControlPlane routes /admin/v2/... to a tenant-scoped handler after
// authenticating the bearer token against the control plane itself, not
// against the legacy single Admin token.
func (s *Service) handleControlPlane(w http.ResponseWriter, r *http.Request) {
	actor, err := s.cp.resolve.Resolve(r.Context(), r.Header.Get("Authorization"))
	if err != nil {
		switch {
		case errors.Is(err, auth.ErrUnauthenticated):
			writeErr(w, http.StatusUnauthorized, "a control-plane bearer token is required")
		default:
			writeErr(w, http.StatusInternalServerError, "authentication failed")
		}
		return
	}
	ctx, span := otel.Tracer(metrics.ServiceName).Start(r.Context(), "admin.control_plane",
		trace.WithAttributes(
			attribute.String("tenant.id", actor.TenantID),
			attribute.String("http.method", r.Method),
			attribute.String("http.target", r.URL.Path),
		))
	defer span.End()

	rest := strings.TrimPrefix(r.URL.Path, "/admin/v2/")
	segments := strings.Split(strings.Trim(rest, "/"), "/")
	// Everything from here on requires the admin role: reading is fine for
	// any authenticated member of the tenant, but a user must not be able to
	// publish a new revision or roll one back.
	//
	// The trailing-slash-stripping above means "/apps/x/revisions" is three
	// segments and "/apps/x/revisions/current" is four; both belong to the
	// revisions handler, split on whether an action segment is present.
	switch {
	case len(segments) == 2 && segments[0] == "apps":
		s.handleApps(w, r.WithContext(ctx), actor, segments[1])
	case len(segments) == 3 && segments[0] == "apps" && segments[2] == "revisions":
		s.handleRevisions(w, r.WithContext(ctx), actor, segments[1], "")
	case len(segments) == 4 && segments[0] == "apps" && segments[2] == "revisions":
		s.handleRevisions(w, r.WithContext(ctx), actor, segments[1], segments[3])
	case len(segments) == 1 && segments[0] == "tool-calls":
		s.handleToolCalls(w, r.WithContext(ctx), actor)
	case len(segments) == 3 && segments[0] == "tool-calls" && segments[2] == "resolve":
		s.handleToolCallResolve(w, r.WithContext(ctx), actor, segments[1])
	case len(segments) == 1 && segments[0] == "blocked-sessions":
		s.handleBlockedSessions(w, r.WithContext(ctx), actor)
	case len(segments) == 1 && segments[0] == "dead-letters":
		s.handleDeadLetters(w, r.WithContext(ctx), actor)
	case len(segments) == 2 && segments[0] == "dead-letters" && segments[1] == "requeue":
		s.handleDeadLetterRequeue(w, r.WithContext(ctx), actor)
	default:
		writeErr(w, http.StatusNotFound, "unknown control-plane route")
	}
}

func (s *Service) scopeFor(w http.ResponseWriter, r *http.Request, actor auth.Actor) (controlplane.Scope, bool) {
	if err := actor.RequireAdmin(); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return controlplane.Scope{}, false
	}
	scope, err := s.cp.db.Scope(actor.TenantID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "tenant scope unavailable")
		return controlplane.Scope{}, false
	}
	return scope, true
}

func (s *Service) handleApps(w http.ResponseWriter, r *http.Request, actor auth.Actor, publicID string) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "apps support GET only at this path")
		return
	}
	scope, ok := s.scopeFor(w, r, actor)
	if !ok {
		return
	}
	if publicID == "" {
		s.listApps(w, r.Context(), scope)
		return
	}
	app, err := scope.GetApp(r.Context(), publicID)
	if err != nil {
		writeControlPlaneErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, appDTO{
		PublicID:            app.PublicID,
		Name:                app.Name,
		CurrentRevisionID:   app.CurrentRevisionID.Int64,
		CurrentRevisionLive: app.CurrentRevisionID.Valid,
	})
}

func (s *Service) listApps(w http.ResponseWriter, ctx context.Context, scope controlplane.Scope) {
	apps, err := scope.ListApps(ctx)
	if err != nil {
		writeControlPlaneErr(w, err)
		return
	}
	out := make([]appDTO, 0, len(apps))
	for _, a := range apps {
		out = append(out, appDTO{
			PublicID:            a.PublicID,
			Name:                a.Name,
			CurrentRevisionID:   a.CurrentRevisionID.Int64,
			CurrentRevisionLive: a.CurrentRevisionID.Valid,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleRevisions is the write side: publishing a new revision, reading the
// current one, and rolling back. Publishing is not "save a draft" — the
// pointer moves in the same transaction (see controlplane.PublishRevision),
// so a client that gets a 200 back can rely on the next session created in
// this tenant using the revision it just sent.
func (s *Service) handleRevisions(w http.ResponseWriter, r *http.Request, actor auth.Actor, appPublicID, action string) {
	switch {
	case action == "current" && r.Method == http.MethodGet:
		s.getCurrentRevision(w, r, actor, appPublicID)
	case action == "" && r.Method == http.MethodPost:
		s.publishRevision(w, r, actor, appPublicID)
	case action == "rollback" && r.Method == http.MethodPost:
		s.rollbackRevision(w, r, actor, appPublicID)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "unsupported revisions action")
	}
}

func (s *Service) getCurrentRevision(w http.ResponseWriter, r *http.Request, actor auth.Actor, appPublicID string) {
	scope, ok := s.scopeFor(w, r, actor)
	if !ok {
		return
	}
	rev, err := scope.CurrentRevision(r.Context(), appPublicID)
	if err != nil {
		writeControlPlaneErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revisionToDTO(rev))
}

func (s *Service) publishRevision(w http.ResponseWriter, r *http.Request, actor auth.Actor, appPublicID string) {
	scope, ok := s.scopeFor(w, r, actor)
	if !ok {
		return
	}
	var in revisionDTO
	if err := decode(r, &in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	rev, err := scope.PublishRevision(r.Context(), appPublicID, in.toSpec())
	if err != nil {
		writeControlPlaneErr(w, err)
		return
	}
	s.auditAdmin(r.Context(), actor.TenantID, "publish revision "+rev.ManifestHash[:12]+" for app "+appPublicID, nil)
	writeJSON(w, http.StatusCreated, revisionToDTO(rev))
}

func (s *Service) rollbackRevision(w http.ResponseWriter, r *http.Request, actor auth.Actor, appPublicID string) {
	scope, ok := s.scopeFor(w, r, actor)
	if !ok {
		return
	}
	var in rollbackDTO
	if err := decode(r, &in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	err := scope.RollbackToRevision(r.Context(), appPublicID, in.TargetRevisionID, in.ExpectCurrentRevisionID)
	if err != nil {
		writeControlPlaneErr(w, err)
		return
	}
	s.auditAdmin(r.Context(), actor.TenantID, "rollback app "+appPublicID+" to revision", nil)
	writeJSON(w, http.StatusOK, map[string]any{"rolled_back_to": in.TargetRevisionID})
}

// Wire DTOs for the control-plane API. Kept separate from controlplane's own
// structs so the HTTP shape can evolve without dragging the storage layer
// along, the same split the existing tenant/model/channel DTOs already use.
type appDTO struct {
	PublicID            string `json:"public_id"`
	Name                string `json:"name"`
	CurrentRevisionID   int64  `json:"current_revision_id,omitempty"`
	CurrentRevisionLive bool   `json:"current_revision_live"`
}

type revisionDTO struct {
	RevisionID       int64           `json:"revision_id"`
	RevisionNo       uint32          `json:"revision_no"`
	Instruction      string          `json:"instruction"`
	ModelProfileID   int64           `json:"model_profile_id"`
	BackendProfileID int64           `json:"backend_profile_id"`
	MaxLLMCalls      int             `json:"max_llm_calls,omitempty"`
	MessageTimeoutMS int             `json:"message_timeout_ms,omitempty"`
	Guardrails       json.RawMessage `json:"guardrails,omitempty"`
	Tools            json.RawMessage `json:"tools,omitempty"`
	KnowledgeBases   json.RawMessage `json:"knowledge_bases,omitempty"`
	ManifestHash     string          `json:"manifest_hash,omitempty"`
}

type rollbackDTO struct {
	TargetRevisionID        int64 `json:"target_revision_id"`
	ExpectCurrentRevisionID int64 `json:"expect_current_revision_id"`
}

func (d revisionDTO) toSpec() controlplane.RevisionSpec {
	return controlplane.RevisionSpec{
		Instruction:      d.Instruction,
		ModelProfileID:   d.ModelProfileID,
		BackendProfileID: d.BackendProfileID,
		MaxLLMCalls:      d.MaxLLMCalls,
		MessageTimeoutMS: d.MessageTimeoutMS,
		Guardrails:       d.Guardrails,
		Tools:            d.Tools,
		KnowledgeBases:   d.KnowledgeBases,
	}
}

func revisionToDTO(r *controlplane.Revision) revisionDTO {
	return revisionDTO{
		RevisionID:       r.ID,
		RevisionNo:       r.RevisionNo,
		Instruction:      r.Spec.Instruction,
		ModelProfileID:   r.Spec.ModelProfileID,
		BackendProfileID: r.Spec.BackendProfileID,
		MaxLLMCalls:      r.Spec.MaxLLMCalls,
		MessageTimeoutMS: r.Spec.MessageTimeoutMS,
		Guardrails:       r.Spec.Guardrails,
		Tools:            r.Spec.Tools,
		KnowledgeBases:   r.Spec.KnowledgeBases,
		ManifestHash:     r.ManifestHash,
	}
}

func writeControlPlaneErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, controlplane.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not found")
	case errors.Is(err, controlplane.ErrCrossTenantReference):
		writeErr(w, http.StatusForbidden, err.Error())
	case errors.Is(err, controlplane.ErrConcurrentPublish):
		writeErr(w, http.StatusConflict, err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, "control-plane operation failed")
	}
}
