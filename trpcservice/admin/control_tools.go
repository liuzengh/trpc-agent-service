package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// Operator surfaces for the tool ledger and the delivery dead letters
// (approved plan, P3: "Admin 增加 DLQ/unknown 查询处置"). Three questions an
// operator asks during an incident, and the route that answers each:
//
//	GET  /admin/v2/tool-calls?status=unknown   what did tools do, or maybe do?
//	GET  /admin/v2/blocked-sessions            what is waiting for me?
//	POST /admin/v2/tool-calls/{id}/resolve     dispose of one and unblock
//	GET  /admin/v2/dead-letters                what gave up trying?
//	POST /admin/v2/dead-letters/requeue        give one another chance
//
// Disposition is admin-only (scopeFor enforces it): a plain tenant member
// talks to the agent, they do not decide whether a side effect happened.

func (s *Service) handleToolCalls(w http.ResponseWriter, r *http.Request, actor auth.Actor) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "tool-calls support GET only at this path")
		return
	}
	if _, ok := s.scopeFor(w, r, actor); !ok {
		return
	}
	q := r.URL.Query()
	filter := tool.CallFilter{
		Status:         q.Get("status"),
		ExecutionID:    q.Get("execution_id"),
		UnresolvedOnly: q.Get("unresolved") == "1",
	}
	if v := q.Get("session_pk"); v != "" {
		pk, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "session_pk must be an integer")
			return
		}
		filter.SessionPK = pk
	}
	if v := q.Get("limit"); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		filter.Limit = limit
	}
	calls, err := s.cp.journal.List(r.Context(), actor.TenantID, filter)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "listing tool calls failed")
		return
	}
	out := make([]toolCallDTO, 0, len(calls))
	for _, c := range calls {
		out = append(out, toolCallToDTO(c))
	}
	writeJSON(w, http.StatusOK, out)
}

type resolveToolCallDTO struct {
	Resolution string `json:"resolution"`
}

func (s *Service) handleToolCallResolve(w http.ResponseWriter, r *http.Request, actor auth.Actor, callID string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "resolve tool calls with POST")
		return
	}
	if _, ok := s.scopeFor(w, r, actor); !ok {
		return
	}
	var in resolveToolCallDTO
	if err := decode(r, &in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if in.Resolution != "confirmed" && in.Resolution != "cancelled" {
		writeErr(w, http.StatusBadRequest, "resolution must be confirmed or cancelled")
		return
	}
	call, err := s.cp.journal.Resolve(r.Context(), actor.TenantID, callID, in.Resolution,
		fmt.Sprintf("principal:%d", actor.PrincipalID))
	switch {
	case errors.Is(err, controlplane.ErrNotFound):
		writeErr(w, http.StatusNotFound, "no such tool call")
		return
	case errors.Is(err, tool.ErrAlreadyResolved):
		writeErr(w, http.StatusConflict, "this tool call already has a resolution")
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "resolving the tool call failed")
		return
	}

	// With this call disposed of, the session may now be free. TryUnblock is
	// the authority on that: it re-counts under the session lock, so two
	// resolutions arriving together cannot both see "the last one".
	out, err := s.cp.exec.TryUnblock(r.Context(), actor.TenantID, call.SessionPK)
	if err != nil {
		if errors.Is(err, execution.ErrNotToolBlocked) {
			// The resolution is recorded; the session is parked for a reason
			// this path does not own. Saying so beats a misleading 500.
			writeErr(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, controlplane.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "the call's session no longer exists")
			return
		}
		writeErr(w, http.StatusInternalServerError, "unblocking the session failed")
		return
	}
	s.auditAdmin(r.Context(), actor.TenantID, "resolve tool call "+callID+" as "+in.Resolution, nil)
	writeJSON(w, http.StatusOK, map[string]any{
		"call":              toolCallToDTO(call),
		"unblocked":         out.Unblocked,
		"remaining":         out.Remaining,
		"disposition":       string(out.Disposition),
		"head_execution_id": out.HeadExecutionID,
	})
}

func (s *Service) handleBlockedSessions(w http.ResponseWriter, r *http.Request, actor auth.Actor) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "blocked-sessions support GET only")
		return
	}
	if _, ok := s.scopeFor(w, r, actor); !ok {
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	blocked, err := s.cp.journal.BlockedSessions(r.Context(), actor.TenantID, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "listing blocked sessions failed")
		return
	}
	out := make([]blockedSessionDTO, 0, len(blocked))
	for _, b := range blocked {
		out = append(out, blockedSessionDTO{
			SessionPK:     b.SessionPK,
			BlockedReason: b.BlockedReason,
			ExecutionID:   b.ExecutionID,
			InSeq:         b.InSeq,
			UpdatedAt:     b.UpdatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) handleDeadLetters(w http.ResponseWriter, r *http.Request, actor auth.Actor) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "dead-letters support GET only at this path")
		return
	}
	scope, ok := s.scopeFor(w, r, actor)
	if !ok {
		return
	}
	ctx := r.Context()
	replies, err := s.listDeadReplies(ctx, scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "listing dead replies failed")
		return
	}
	jobs, err := s.listDeadJobs(ctx, scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "listing dead jobs failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"reply_outbox":  replies,
		"outbox_events": jobs,
	})
}

type requeueDTO struct {
	Kind string `json:"kind"`
	ID   int64  `json:"id"`
}

func (s *Service) handleDeadLetterRequeue(w http.ResponseWriter, r *http.Request, actor auth.Actor) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "requeue dead letters with POST")
		return
	}
	scope, ok := s.scopeFor(w, r, actor)
	if !ok {
		return
	}
	var in requeueDTO
	if err := decode(r, &in); err != nil || in.ID <= 0 {
		writeErr(w, http.StatusBadRequest, "kind and id are required")
		return
	}
	ctx := r.Context()
	var (
		res sql.Result
		err error
	)
	switch in.Kind {
	case "reply_outbox":
		res, err = scope.Exec(ctx, `
			UPDATE reply_outbox
			SET status = 'pending', next_attempt_at = UTC_TIMESTAMP(6),
			    lease_owner = NULL, lease_until = NULL, attempts = 0
			WHERE tenant_id = ? AND outbox_id = ? AND status IN ('dead', 'unknown')`,
			actor.TenantID, in.ID)
	case "outbox_events":
		res, err = scope.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'pending', next_attempt_at = UTC_TIMESTAMP(6),
			    lease_owner = NULL, lease_until = NULL, attempts = 0
			WHERE tenant_id = ? AND job_id = ? AND status IN ('failed', 'unknown')`,
			actor.TenantID, in.ID)
	default:
		writeErr(w, http.StatusBadRequest, "kind must be reply_outbox or outbox_events")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "requeue failed")
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		writeErr(w, http.StatusNotFound, "no dead letter with that id is waiting")
		return
	}
	s.auditAdmin(ctx, actor.TenantID, fmt.Sprintf("requeue %s %d", in.Kind, in.ID), nil)
	writeJSON(w, http.StatusOK, map[string]any{"requeued": true})
}

type deadReplyDTO struct {
	OutboxID  int64  `json:"outbox_id"`
	Execution string `json:"execution_id"`
	SessionPK int64  `json:"session_pk"`
	PartSeq   int    `json:"part_seq"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	LastError string `json:"last_error"`
	CreatedAt string `json:"created_at"`
}

type deadJobDTO struct {
	JobID    int64  `json:"job_id"`
	Kind     string `json:"kind"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
	LastErr  string `json:"last_error"`
}

func (s *Service) listDeadReplies(ctx context.Context, scope controlplane.Scope) ([]deadReplyDTO, error) {
	rows, err := scope.Query(ctx, `
		SELECT outbox_id, execution_id, session_pk, part_seq, status, attempts, last_error, created_at
		FROM reply_outbox
		WHERE tenant_id = ? AND status IN ('dead', 'unknown')
		ORDER BY outbox_id DESC LIMIT 50`, scope.TenantID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]deadReplyDTO, 0)
	for rows.Next() {
		var d deadReplyDTO
		var created time.Time
		if err := rows.Scan(&d.OutboxID, &d.Execution, &d.SessionPK, &d.PartSeq, &d.Status,
			&d.Attempts, &d.LastError, &created); err != nil {
			return nil, err
		}
		d.CreatedAt = created.UTC().Format(time.RFC3339Nano)
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Service) listDeadJobs(ctx context.Context, scope controlplane.Scope) ([]deadJobDTO, error) {
	rows, err := scope.Query(ctx, `
		SELECT job_id, kind, status, attempts, last_error
		FROM outbox_events
		WHERE tenant_id = ? AND status IN ('failed', 'unknown')
		ORDER BY job_id DESC LIMIT 50`, scope.TenantID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]deadJobDTO, 0)
	for rows.Next() {
		var d deadJobDTO
		if err := rows.Scan(&d.JobID, &d.Kind, &d.Status, &d.Attempts, &d.LastErr); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Wire DTOs.
type toolCallDTO struct {
	CallID          string          `json:"call_id"`
	ExecutionID     string          `json:"execution_id"`
	SessionPK       int64           `json:"session_pk"`
	CallSeq         int             `json:"call_seq"`
	ToolName        string          `json:"tool_name"`
	ToolVersion     uint32          `json:"tool_version"`
	ToolKind        string          `json:"tool_kind"`
	SideEffect      string          `json:"side_effect"`
	Idempotent      bool            `json:"idempotent"`
	Status          string          `json:"status"`
	ErrorType       string          `json:"error_type,omitempty"`
	Detail          string          `json:"detail,omitempty"`
	LatencyMS       int             `json:"latency_ms"`
	Attempts        int             `json:"attempts"`
	ArgumentsMasked json.RawMessage `json:"arguments_masked,omitempty"`
	TraceID         string          `json:"trace_id,omitempty"`
	Resolution      string          `json:"resolution,omitempty"`
	ResolvedBy      string          `json:"resolved_by,omitempty"`
	CreatedAt       string          `json:"created_at"`
	UpdatedAt       string          `json:"updated_at"`
}

func toolCallToDTO(c tool.Call) toolCallDTO {
	return toolCallDTO{
		CallID:          c.CallID,
		ExecutionID:     c.ExecutionID,
		SessionPK:       c.SessionPK,
		CallSeq:         c.CallSeq,
		ToolName:        c.ToolName,
		ToolVersion:     c.ToolVersion,
		ToolKind:        c.ToolKind,
		SideEffect:      c.SideEffect,
		Idempotent:      c.Idempotent,
		Status:          c.Status,
		ErrorType:       c.ErrorType,
		Detail:          c.Detail,
		LatencyMS:       c.LatencyMS,
		Attempts:        c.Attempts,
		ArgumentsMasked: c.ArgumentsMasked,
		TraceID:         c.TraceID,
		Resolution:      c.Resolution,
		ResolvedBy:      c.ResolvedBy,
		CreatedAt:       c.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:       c.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

type blockedSessionDTO struct {
	SessionPK     int64  `json:"session_pk"`
	BlockedReason string `json:"blocked_reason"`
	ExecutionID   string `json:"execution_id,omitempty"`
	InSeq         uint32 `json:"in_seq,omitempty"`
	UpdatedAt     string `json:"updated_at"`
}
