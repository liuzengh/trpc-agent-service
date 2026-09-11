package web

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/configcontrol"
)

type rollbackRequest struct {
	TargetRevision     string `json:"target_revision,omitempty"`
	ExpectedGeneration int64  `json:"expected_generation,omitempty"`
	Reason             string `json:"reason,omitempty"`
}

type createRevisionRequest struct {
	Revision string              `json:"revision"`
	Config   config.TenantConfig `json:"config"`
	Reason   string              `json:"reason,omitempty"`
}

type createReleaseRequest struct {
	Kind               configcontrol.ReleaseKind `json:"kind"`
	TargetRevision     string                    `json:"target_revision,omitempty"`
	RolloutPercent     int                       `json:"rollout_percent,omitempty"`
	ExpectedGeneration int64                     `json:"expected_generation,omitempty"`
	Reason             string                    `json:"reason,omitempty"`
}

func decodeRollbackRequest(w http.ResponseWriter, r *http.Request) (rollbackRequest, error) {
	var request rollbackRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxWebhookBody))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&request)
	if errors.Is(err, io.EOF) {
		return request, nil
	}
	if err != nil {
		return request, errors.New("invalid JSON body")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return request, errors.New("invalid JSON body: exactly one object is required")
	}
	return request, nil
}

func decodeOptionalJSONBody(w http.ResponseWriter, r *http.Request, target any) error {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBody))
	if err != nil {
		return errors.New("invalid JSON body")
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid JSON body")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("invalid JSON body: exactly one object is required")
	}
	return nil
}

func expectedGeneration(r *http.Request) (int64, bool) {
	raw := strings.TrimSpace(r.Header.Get("If-Match"))
	if raw == "" {
		return 0, false
	}
	raw = strings.Trim(raw, "\"")
	generation, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || generation <= 0 {
		return 0, false
	}
	return generation, true
}

func (s *Server) listTenantRevisions(w http.ResponseWriter, r *http.Request) {
	if s.control == nil {
		http.Error(w, "persistent control plane is unavailable", http.StatusNotImplemented)
		return
	}
	revisions, err := s.control.Store().ListRevisions(r.Context(), r.PathValue("tenant"))
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revisions": revisions})
}

func (s *Server) createTenantRevision(w http.ResponseWriter, r *http.Request) {
	if s.control == nil {
		http.Error(w, "persistent control plane is unavailable", http.StatusNotImplemented)
		return
	}
	var request createRevisionRequest
	if err := decodeJSONBody(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request.Config.TenantID = r.PathValue("tenant")
	if request.Revision == "" {
		request.Revision = request.Config.Version
	}
	request.Config.Version = request.Revision
	revision, err := s.control.CreateRevision(r.Context(), configcontrol.RevisionInput{
		TenantID: request.Config.TenantID, Revision: request.Revision, Tenant: request.Config,
		CreatedBy: "admin_api", ChangeReason: request.Reason,
	})
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, revisionSummaryWithConfig(revision))
}

func revisionSummaryWithConfig(revision configcontrol.Revision) map[string]any {
	return map[string]any{
		"tenant_id": revision.TenantID, "revision": revision.Revision,
		"config_sha256": revision.ConfigSHA256, "created_by": revision.CreatedBy,
		"change_reason": revision.ChangeReason, "created_at": revision.CreatedAt,
	}
}

func (s *Server) createTenantRelease(w http.ResponseWriter, r *http.Request) {
	if s.control == nil {
		http.Error(w, "persistent control plane is unavailable", http.StatusNotImplemented)
		return
	}
	var request createReleaseRequest
	if err := decodeJSONBody(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Kind == "" {
		request.Kind = configcontrol.ReleaseFull
	}
	submit, err := s.makeReleaseRequest(r, request)
	if err != nil {
		writeControlError(w, err)
		return
	}
	release, err := s.control.CreateRelease(r.Context(), submit)
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"release": release})
}

func (s *Server) promoteTenantRelease(w http.ResponseWriter, r *http.Request) {
	if s.control == nil {
		http.Error(w, "persistent control plane is unavailable", http.StatusNotImplemented)
		return
	}
	var request createReleaseRequest
	if err := decodeOptionalJSONBody(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	source, err := s.control.Store().GetRelease(r.Context(), r.PathValue("release"))
	if err != nil {
		writeControlError(w, err)
		return
	}
	if source.TenantID != r.PathValue("tenant") {
		http.Error(w, "release not found", http.StatusNotFound)
		return
	}
	if source.Kind != configcontrol.ReleaseCanary || source.Status != configcontrol.ReleaseVerified {
		writeControlError(w, configcontrol.ErrInvalidTransition)
		return
	}
	if request.TargetRevision != "" && request.TargetRevision != source.TargetRevision {
		http.Error(w, "target_revision must match the canary release", http.StatusBadRequest)
		return
	}
	request.Kind = configcontrol.ReleasePromote
	if request.TargetRevision == "" {
		request.TargetRevision = source.TargetRevision
	}
	submit, err := s.makeReleaseRequest(r, request)
	if err != nil {
		writeControlError(w, err)
		return
	}
	release, err := s.control.CreateRelease(r.Context(), submit)
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"release": release})
}

func (s *Server) makeReleaseRequest(r *http.Request, request createReleaseRequest) (configcontrol.CreateReleaseRequest, error) {
	tenantID := r.PathValue("tenant")
	state, err := s.control.Store().GetState(r.Context(), tenantID)
	if err != nil {
		return configcontrol.CreateReleaseRequest{}, err
	}
	generation := request.ExpectedGeneration
	if generation <= 0 {
		generation, _ = expectedGeneration(r)
	}
	if generation <= 0 {
		generation = state.Generation
	}
	if request.TargetRevision == "" {
		return configcontrol.CreateReleaseRequest{}, errors.New("target_revision is required")
	}
	return configcontrol.CreateReleaseRequest{
		TenantID: tenantID, Kind: request.Kind, TargetRevision: request.TargetRevision,
		TargetRolloutPercent: request.RolloutPercent, ExpectedGeneration: generation,
		ExpectedActive: state.ActiveRevision, ExpectedCanary: state.CanaryRevision,
		RequestedBy: "admin_api", ChangeReason: request.Reason,
	}, nil
}

func (s *Server) getTenantRelease(w http.ResponseWriter, r *http.Request) {
	if s.control == nil {
		http.Error(w, "persistent control plane is unavailable", http.StatusNotImplemented)
		return
	}
	release, err := s.control.Store().GetRelease(r.Context(), r.PathValue("release"))
	if err != nil {
		writeControlError(w, err)
		return
	}
	if release.TenantID != r.PathValue("tenant") {
		http.Error(w, "release not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"release": release})
}

func (s *Server) listConfigNodes(w http.ResponseWriter, r *http.Request) {
	if s.control == nil {
		http.Error(w, "persistent control plane is unavailable", http.StatusNotImplemented)
		return
	}
	nodes, err := s.control.Store().ListHeartbeats(r.Context())
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

func writeControlError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, configcontrol.ErrGenerationConflict), errors.Is(err, configcontrol.ErrReleaseConflict), errors.Is(err, configcontrol.ErrInvalidTransition), errors.Is(err, configcontrol.ErrRevisionConflict):
		status = http.StatusConflict
	case errors.Is(err, configcontrol.ErrRevisionNotFound), errors.Is(err, configcontrol.ErrTenantNotFound), errors.Is(err, configcontrol.ErrReleaseNotFound):
		status = http.StatusNotFound
	case errors.Is(err, configcontrol.ErrControlUnavailable):
		status = http.StatusServiceUnavailable
	}
	message := "configuration control-plane request failed"
	if status == http.StatusBadRequest {
		message = err.Error()
	}
	http.Error(w, message, status)
}
