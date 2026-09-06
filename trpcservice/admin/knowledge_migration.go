package admin

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func (s *Service) SubmitKnowledgeMigrationJob(ctx context.Context, tenantID, migrationID, jobType, operationID string) (MigrationJobResult, error) {
	if s.jobs == nil || (jobType != background.JobKnowledgeBackfill && jobType != background.JobKnowledgeVerify) || !identifierPattern.MatchString(operationID) {
		return MigrationJobResult{}, invalidf("knowledge migration job configuration invalid")
	}
	m, err := s.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return MigrationJobResult{}, err
	}
	if m.ResourceType != "knowledge" || (m.State != controlplane.MigrationBackfill && m.State != controlplane.MigrationVerify && m.State != controlplane.MigrationCutover) {
		return MigrationJobResult{}, invalidf("knowledge migration must be in backfill, verify or cutover")
	}
	a, err := s.repository.GetAgentApp(ctx, tenantID, m.AppID)
	if err != nil {
		return MigrationJobResult{}, err
	}
	payload, _ := json.Marshal(background.KnowledgeMigrationPayload{MigrationID: m.ID})
	if err := s.record(ctx, tenantID, "admin_knowledge_migration_requested", map[string]any{"migration_id": m.ID, "job_type": jobType}); err != nil {
		return MigrationJobResult{}, err
	}
	queued, err := s.jobs.Enqueue(ctx, background.EnqueueRequest{TenantID: tenantID, AppID: m.AppID, RevisionID: a.StableRevisionID, Type: jobType, DedupeKey: m.ID + ":" + operationID, Payload: payload, TraceParent: background.TraceParent(ctx)})
	if err != nil {
		return MigrationJobResult{}, err
	}
	return MigrationJobResult{JobID: queued.Job.ID, MigrationID: m.ID, JobType: jobType, Duplicate: queued.Duplicate}, nil
}
func (h *Handler) handleKnowledgeMigrationJob(w http.ResponseWriter, r *http.Request, jobType string) {
	var input struct {
		TenantID    string `json:"tenant_id"`
		MigrationID string `json:"migration_id"`
		OperationID string `json:"operation_id"`
	}
	if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionWrite) {
		return
	}
	result, err := h.service.SubmitKnowledgeMigrationJob(r.Context(), input.TenantID, input.MigrationID, jobType, input.OperationID)
	h.writeResult(w, http.StatusAccepted, result, err)
}

func (h *Handler) handleKnowledgeMigrationStatus(w http.ResponseWriter, r *http.Request) {
	var input struct {
		TenantID    string `json:"tenant_id"`
		MigrationID string `json:"migration_id"`
	}
	if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionRead) {
		return
	}
	m, err := h.service.repository.GetBackendMigration(r.Context(), input.TenantID, input.MigrationID)
	if err != nil {
		h.writeResult(w, http.StatusOK, nil, err)
		return
	}
	reader, ok := h.service.repository.(interface {
		GetKnowledgeMigrationStatus(context.Context, string, string, string) (controlplane.KnowledgeMigrationStatus, error)
	})
	if !ok || m.ResourceType != "knowledge" {
		h.writeResult(w, http.StatusOK, nil, invalidf("knowledge status unavailable"))
		return
	}
	value, err := reader.GetKnowledgeMigrationStatus(r.Context(), input.TenantID, m.AppID, m.ID)
	h.writeResult(w, http.StatusOK, value, err)
}
