package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/console"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
)

type debugInput struct {
	TenantID          string `json:"tenant_id"`
	AppID             string `json:"app_id"`
	SessionID         string `json:"session_id"`
	PreviousSessionID string `json:"previous_session_id"`
	DraftVersion      int64  `json:"draft_version"`
	Message           string `json:"message"`
	MessageID         string `json:"message_id"`
	RunID             string `json:"run_id"`
	ApprovalID        string `json:"approval_id"`
	Decision          string `json:"decision"`
	Cursor            string `json:"cursor"`
}

func (s *Service) ownedDebugSession(ctx context.Context, tenant, id, owner string) (console.Record, console.Session, error) {
	record, err := s.consoleStore.Get(ctx, "session", tenant, id)
	if err != nil {
		return record, console.Session{}, err
	}
	if record.OwnerID != owner {
		return record, console.Session{}, console.ErrNotFound
	}
	var session console.Session
	if json.Unmarshal(record.Data, &session) != nil {
		return record, session, console.ErrUnavailable
	}
	if _, err := s.repository.GetAgentApp(ctx, tenant, record.AppID); err != nil {
		return record, session, err
	}
	return record, session, nil
}
func busyDebug(status string) bool {
	return status == "queued" || status == "running" || status == "cancel_requested" || status == "awaiting_approval"
}

func (s *Service) createDebugSession(ctx context.Context, in debugInput, owner string) (map[string]any, error) {
	s.debugMu.Lock()
	defer s.debugMu.Unlock()
	draft, err := s.draft(ctx, in.TenantID, in.AppID)
	if err != nil {
		return nil, err
	}
	if draft.Version != in.DraftVersion {
		return nil, console.ErrConflict
	}
	var data DraftData
	if json.Unmarshal(draft.Data, &data) != nil {
		return nil, console.ErrUnavailable
	}
	cfg := data.Config
	if err := normalizeDraftConfig(&cfg, in.TenantID, in.AppID, owner); err != nil {
		return nil, err
	}
	if err := s.requireValidRevision(ctx, cfg); err != nil {
		return nil, err
	}
	sourceChecksum := controlplane.RevisionChecksum(cfg)
	policy, err := governance.ParseToolPolicy(cfg.ToolPolicy)
	if err != nil {
		return nil, err
	}
	if len(policy.AllowedTools) > 0 && (policy.MaxToolCalls <= 0 || policy.MaxToolCalls > 32) {
		return nil, invalidf("网页调试需要明确的工具调用上限（1～32），请先保存合适的草稿预算")
	}
	allowed := []string{}
	disabled := []string{}
	for _, name := range policy.AllowedTools {
		safe := name == "current_time" || name == "echo" || name == "read_attachment" || name == "memory_add" || name == "memory_search" || name == "memory_load" || name == "skill_load" || name == "skill_run" || name == "knowledge_search" || strings.HasPrefix(name, "mcp_")
		if safe {
			allowed = append(allowed, name)
		} else {
			disabled = append(disabled, name)
		}
	}
	policy.AllowedTools = allowed
	cfg.ToolPolicy, _ = json.Marshal(policy)
	var agentConfig map[string]any
	_ = json.Unmarshal(cfg.AgentConfig, &agentConfig)
	agentConfig["summary_every_turns"] = 0
	cfg.AgentConfig, _ = json.Marshal(agentConfig)
	var memoryConfig map[string]any
	_ = json.Unmarshal(cfg.MemoryConfig, &memoryConfig)
	memoryConfig["auto_extract"] = false
	cfg.MemoryConfig, _ = json.Marshal(memoryConfig)
	configID := newConsoleID("preview-")
	cfg.ID = configID
	cfg.Checksum = controlplane.RevisionChecksum(cfg)
	cfg.CreatedAt = time.Now().UTC()
	snapshotData, _ := json.Marshal(console.Snapshot{Config: cfg, DraftVersion: draft.Version, DisabledTools: disabled, SourceChecksum: sourceChecksum})
	var result console.Record
	err = s.consoleStore.Transaction(ctx, func(ctx context.Context) error {
		existing, err := s.consoleStore.List(ctx, console.Filter{Kind: "session", TenantID: in.TenantID, AppID: in.AppID, OwnerID: owner, Status: "active", Limit: 21})
		if err != nil {
			return err
		}
		if len(existing) >= 20 && in.PreviousSessionID == "" {
			return invalidf("活跃调试会话过多，请先结束旧会话")
		}
		if in.PreviousSessionID != "" {
			if err := s.consoleStore.LockSession(ctx, in.TenantID, in.PreviousSessionID); err != nil {
				return err
			}
			previous, state, err := s.ownedDebugSession(ctx, in.TenantID, in.PreviousSessionID, owner)
			if err != nil {
				return err
			}
			if previous.AppID != in.AppID {
				return console.ErrNotFound
			}
			if state.ActiveRunID != "" {
				run, err := s.consoleStore.Get(ctx, "run", in.TenantID, state.ActiveRunID)
				if err == nil && busyDebug(run.Status) {
					return invalidf("请先停止当前执行或处理审批，再开始新调试")
				}
			}
			previous.Status = "closed"
			if _, err := s.consoleStore.Update(ctx, previous, previous.Version); err != nil {
				return err
			}
		}
		expires := time.Now().UTC().Add(7 * 24 * time.Hour)
		if _, err := s.consoleStore.Create(ctx, console.Record{Kind: "snapshot", TenantID: in.TenantID, AppID: in.AppID, OwnerID: owner, ID: configID, Status: "frozen", Data: snapshotData, ExpiresAt: expires}); err != nil {
			return err
		}
		id := newConsoleID("session-")
		session := console.Session{SnapshotID: configID, UserID: "console_" + console.Hash(in.TenantID+"\x00"+owner+"\x00"+id)}
		raw, _ := json.Marshal(session)
		result, err = s.consoleStore.Create(ctx, console.Record{Kind: "session", TenantID: in.TenantID, AppID: in.AppID, OwnerID: owner, ID: id, Status: "active", Data: raw, ExpiresAt: expires})
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := s.record(ctx, in.TenantID, "debug_session_created", map[string]any{"app_id": in.AppID, "session_id": result.ID, "snapshot_id": configID}); err != nil {
		return nil, err
	}
	return map[string]any{"session": publicDebugSession(result), "disabled_tools": disabled}, nil
}

func publicDebugSession(record console.Record) console.Record {
	var session console.Session
	_ = json.Unmarshal(record.Data, &session)
	record.Data, _ = json.Marshal(map[string]any{"snapshot_id": session.SnapshotID, "active_run_id": session.ActiveRunID, "turns": session.Turns})
	return record
}
func publicDebugRun(record console.Record) console.Record {
	var run console.Run
	_ = json.Unmarshal(record.Data, &run)
	record.Data, _ = json.Marshal(map[string]any{"session_id": run.SessionID, "snapshot_id": run.SnapshotID, "input": run.Input, "reply": run.Reply, "error_type": run.ErrorType, "trace_id": run.TraceID, "continuation_of": run.ContinuationOf, "pending_approvals": run.PendingApprovals, "prompt_tokens": run.PromptTokens, "completion_tokens": run.CompletionTokens, "cost": run.Cost})
	return record
}

func (s *Service) sendDebug(ctx context.Context, in debugInput, owner string) (console.Record, error) {
	text := strings.TrimSpace(in.Message)
	if text == "" || len(text) > 8<<10 || !identifierPattern.MatchString(in.MessageID) {
		return console.Record{}, invalidf("消息不能为空、长度需小于 8 KiB，并携带唯一消息编号")
	}
	s.debugMu.Lock()
	defer s.debugMu.Unlock()
	var result console.Record
	err := s.consoleStore.Transaction(ctx, func(ctx context.Context) error {
		if err := s.consoleStore.LockSession(ctx, in.TenantID, in.SessionID); err != nil {
			return err
		}
		record, session, err := s.ownedDebugSession(ctx, in.TenantID, in.SessionID, owner)
		if err != nil {
			return err
		}
		if record.Status != "active" {
			return invalidf("调试会话已结束，请开始新调试")
		}
		id := "debug-" + console.Hash(in.TenantID + "\x00" + record.ID + "\x00" + in.MessageID)[:32]
		if old, err := s.consoleStore.Get(ctx, "run", in.TenantID, id); err == nil {
			var run console.Run
			_ = json.Unmarshal(old.Data, &run)
			if old.OwnerID != owner || run.InputHash != console.Hash(text) || run.SessionID != record.ID {
				return console.ErrConflict
			}
			result = old
			return nil
		} else if !errors.Is(err, console.ErrNotFound) {
			return err
		}
		if session.Turns >= 50 {
			return invalidf("本次调试已达 50 轮，请开始新调试")
		}
		if session.ActiveRunID != "" {
			active, err := s.consoleStore.Get(ctx, "run", in.TenantID, session.ActiveRunID)
			if err != nil && !errors.Is(err, console.ErrNotFound) {
				return err
			}
			if busyDebug(active.Status) {
				return invalidf("当前执行或审批尚未结束")
			}
		}
		run := console.Run{SessionID: record.ID, SnapshotID: session.SnapshotID, UserID: session.UserID, Input: text, InputHash: console.Hash(text), MessageID: in.MessageID, TraceParent: background.TraceParent(ctx)}
		raw, _ := json.Marshal(run)
		result, err = s.consoleStore.Create(ctx, console.Record{Kind: "run", TenantID: in.TenantID, AppID: record.AppID, OwnerID: owner, ID: id, Status: "queued", Data: raw, ExpiresAt: record.ExpiresAt})
		if err != nil {
			return err
		}
		session.ActiveRunID = id
		session.Turns++
		record.Data, _ = json.Marshal(session)
		_, err = s.consoleStore.Update(ctx, record, record.Version)
		return err
	})
	return result, err
}

func (s *Service) debugState(ctx context.Context, in debugInput, owner string) (map[string]any, error) {
	return s.debugStateWithHistory(ctx, in, owner, true)
}
func (s *Service) debugStateWithHistory(ctx context.Context, in debugInput, owner string, history bool) (map[string]any, error) {
	record, session, err := s.ownedDebugSession(ctx, in.TenantID, in.SessionID, owner)
	if err != nil {
		return nil, err
	}
	runs := []console.Record{}
	if history {
		runs, err = s.consoleStore.List(ctx, console.Filter{Kind: "run", TenantID: in.TenantID, AppID: record.AppID, OwnerID: owner, SessionID: record.ID, Limit: 100, Ascending: true})
		if err != nil {
			return nil, err
		}
	} else if session.ActiveRunID != "" {
		active, err := s.consoleStore.Get(ctx, "run", in.TenantID, session.ActiveRunID)
		if err != nil {
			return nil, err
		}
		if active.OwnerID != owner || active.AppID != record.AppID {
			return nil, console.ErrNotFound
		}
		runs = append(runs, active)
	}
	out := map[string]any{"session": publicDebugSession(record), "runs": []console.Record{}, "approvals": []console.ApprovalView{}, "tools": []any{}, "disabled_tools": []string{}}
	if snap, err := s.consoleStore.Get(ctx, "snapshot", in.TenantID, session.SnapshotID); err == nil {
		var snapshot console.Snapshot
		_ = json.Unmarshal(snap.Data, &snapshot)
		out["disabled_tools"] = snapshot.DisabledTools
	}
	public := []console.Record{}
	for _, run := range runs {
		public = append(public, publicDebugRun(run))
		if run.ID == session.ActiveRunID {
			out["current_run"] = publicDebugRun(run)
		}
	}
	out["runs"] = public
	if session.ActiveRunID != "" {
		journal := &console.ToolJournal{Store: s.consoleStore}
		tools, err := journal.ListByRequest(ctx, in.TenantID, session.ActiveRunID)
		if err != nil {
			return nil, err
		}
		out["tools"] = tools
		if current, ok := out["current_run"].(console.Record); ok && current.Status == "awaiting_approval" {
			pending, err := (&console.Approvals{Store: s.consoleStore}).ListPendingByRequest(ctx, in.TenantID, current.ID)
			if err != nil {
				return nil, err
			}
			views := []console.ApprovalView{}
			for _, a := range pending {
				views = append(views, console.ApprovalView{ID: a.ApprovalID, Tool: a.ToolName, Status: a.Status, ArgumentsHash: a.ArgumentsHash, ExpiresAt: a.ExpiresAt})
			}
			out["approvals"] = views
		}
	}
	return out, nil
}

func (s *Service) decideDebug(ctx context.Context, in debugInput, owner string) (map[string]any, error) {
	if in.Decision != approval.StatusApproved && in.Decision != approval.StatusDenied {
		return nil, invalidf("请选择批准或拒绝")
	}
	s.debugMu.Lock()
	defer s.debugMu.Unlock()
	var result map[string]any
	err := s.consoleStore.Transaction(ctx, func(ctx context.Context) error {
		if err := s.consoleStore.LockSession(ctx, in.TenantID, in.SessionID); err != nil {
			return err
		}
		sessionRecord, session, err := s.ownedDebugSession(ctx, in.TenantID, in.SessionID, owner)
		if err != nil {
			return err
		}
		approvals := &console.Approvals{Store: s.consoleStore}
		selected, err := approvals.Get(ctx, in.TenantID, in.ApprovalID)
		if err != nil {
			return err
		}
		if selected.UserID != session.UserID || selected.SessionID != sessionRecord.ID || selected.ChannelBindingID != console.DebugBinding {
			return console.ErrNotFound
		}
		parent, err := s.consoleStore.Get(ctx, "run", in.TenantID, selected.RequestID)
		if err != nil {
			return err
		}
		var prior console.Run
		if json.Unmarshal(parent.Data, &prior) != nil || parent.OwnerID != owner || parent.AppID != sessionRecord.AppID {
			return console.ErrNotFound
		}
		if parent.Status != "awaiting_approval" {
			if selected.Status == in.Decision {
				result = map[string]any{"duplicate": true}
				return nil
			}
			return invalidf("此轮调试已经结束，不能再创建执行任务")
		}
		if session.ActiveRunID != parent.ID {
			return console.ErrConflict
		}
		if session.Turns >= 50 && in.Decision == approval.StatusApproved {
			return invalidf("本次调试已达轮数上限，请拒绝当前请求后开始新调试")
		}
		_, err = approvals.Decide(ctx, approval.Decision{ApprovalID: selected.ApprovalID, TenantID: in.TenantID, ChannelBindingID: console.DebugBinding, UserID: session.UserID, SessionID: sessionRecord.ID, ExternalMessageID: "console-" + selected.ApprovalID, Status: in.Decision})
		if err != nil {
			return err
		}
		if in.Decision == approval.StatusDenied {
			for _, view := range prior.PendingApprovals {
				other, err := approvals.Get(ctx, in.TenantID, view.ID)
				if err == nil && other.Status == approval.StatusPending {
					if _, err := approvals.Decide(ctx, approval.Decision{ApprovalID: other.ApprovalID, TenantID: in.TenantID, ChannelBindingID: console.DebugBinding, UserID: session.UserID, SessionID: sessionRecord.ID, ExternalMessageID: "console-cancel-" + other.ApprovalID, Status: approval.StatusDenied}); err != nil {
						return err
					}
				}
			}
			parent.Status = "cancelled"
			prior.Reply = "已拒绝本轮工具请求，没有创建继续执行任务。此前已经发生的操作不会因此回滚。"
			parent.Data, _ = json.Marshal(prior)
			_, err = s.consoleStore.Update(ctx, parent, parent.Version)
			result = map[string]any{"status": "cancelled"}
			return err
		}
		calls := []governance.ApprovedToolCall{}
		for _, view := range prior.PendingApprovals {
			a, err := approvals.Get(ctx, in.TenantID, view.ID)
			if err != nil {
				return err
			}
			if a.Status == approval.StatusPending {
				result = map[string]any{"status": "awaiting_approval"}
				return nil
			}
			if a.Status != approval.StatusApproved {
				return approval.ErrConflict
			}
			calls = append(calls, governance.ApprovedToolCall{ToolName: a.ToolName, ArgumentsHash: a.ArgumentsHash})
		}
		if len(calls) == 0 {
			return approval.ErrConflict
		}
		id := "debug-" + console.Hash(parent.ID + "\x00approved")[:32]
		run := console.Run{SessionID: sessionRecord.ID, SnapshotID: session.SnapshotID, UserID: session.UserID, Input: prior.Input, InputHash: prior.InputHash, MessageID: id, ApprovedCalls: calls, ContinuationOf: parent.ID, TraceParent: prior.TraceParent}
		raw, _ := json.Marshal(run)
		queued, err := s.consoleStore.Create(ctx, console.Record{Kind: "run", TenantID: in.TenantID, AppID: sessionRecord.AppID, OwnerID: owner, ID: id, Status: "queued", Data: raw, ExpiresAt: sessionRecord.ExpiresAt})
		if err != nil {
			return err
		}
		parent.Status = "completed"
		prior.Reply = "本轮工具请求已批准，继续执行任务已提交。批准不等于执行成功，请等待下方结果。"
		parent.Data, _ = json.Marshal(prior)
		if _, err = s.consoleStore.Update(ctx, parent, parent.Version); err != nil {
			return err
		}
		session.ActiveRunID = id
		session.Turns++
		sessionRecord.Data, _ = json.Marshal(session)
		if _, err = s.consoleStore.Update(ctx, sessionRecord, sessionRecord.Version); err != nil {
			return err
		}
		result = map[string]any{"run": publicDebugRun(queued)}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := s.record(ctx, in.TenantID, "debug_approval_"+in.Decision, map[string]any{"session_id": in.SessionID, "approval_id": in.ApprovalID}); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) cancelDebug(ctx context.Context, in debugInput, owner string) (map[string]any, error) {
	s.debugMu.Lock()
	defer s.debugMu.Unlock()
	record, session, err := s.ownedDebugSession(ctx, in.TenantID, in.SessionID, owner)
	if err != nil {
		return nil, err
	}
	if session.ActiveRunID != in.RunID {
		return nil, console.ErrConflict
	}
	run, err := s.consoleStore.Get(ctx, "run", in.TenantID, in.RunID)
	if err != nil || run.OwnerID != owner || run.AppID != record.AppID {
		return nil, console.ErrNotFound
	}
	if run.Status == "awaiting_approval" {
		return nil, invalidf("请使用拒绝按钮结束待审批工具请求")
	}
	switch run.Status {
	case "queued":
		run.Status = "cancelled"
	case "running":
		run.Status = "cancel_requested"
	default:
		return map[string]any{"status": run.Status}, nil
	}
	updated, err := s.consoleStore.Update(ctx, run, run.Version)
	return map[string]any{"status": updated.Status}, err
}

func (h *Handler) handleDebug(w http.ResponseWriter, r *http.Request) {
	var in debugInput
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionDebug) {
		return
	}
	owner := PrincipalName(r.Context())
	switch r.URL.Path {
	case "/admin/debug/events":
		h.streamDebug(w, r, in, owner)
	case "/admin/debug/live":
		value, err := h.service.debugStateWithHistory(r.Context(), in, owner, false)
		h.writeResult(w, 200, value, err)
	case "/admin/debug/sessions":
		value, err := h.service.createDebugSession(r.Context(), in, owner)
		h.writeResult(w, 201, value, err)
	case "/admin/debug/latest":
		records, err := h.service.consoleStore.List(r.Context(), console.Filter{Kind: "session", TenantID: in.TenantID, AppID: in.AppID, OwnerID: owner, Status: "active", Limit: 1})
		var session any
		if len(records) > 0 {
			session = publicDebugSession(records[0])
		}
		h.writeResult(w, 200, map[string]any{"session": session}, err)
	case "/admin/debug/send":
		value, err := h.service.sendDebug(r.Context(), in, owner)
		h.writeResult(w, 202, map[string]any{"run": publicDebugRun(value)}, err)
	case "/admin/debug/get":
		value, err := h.service.debugState(r.Context(), in, owner)
		h.writeResult(w, 200, value, err)
	case "/admin/debug/decision":
		value, err := h.service.decideDebug(r.Context(), in, owner)
		h.writeResult(w, 200, value, err)
	case "/admin/debug/cancel":
		value, err := h.service.cancelDebug(r.Context(), in, owner)
		h.writeResult(w, 200, value, err)
	}
}

func (h *Handler) streamDebug(w http.ResponseWriter, r *http.Request, in debugInput, owner string) {
	state, err := h.service.debugStateWithHistory(r.Context(), in, owner, false)
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "no-store")
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
		return
	}
	if controller.Flush() != nil {
		return
	}
	deadline := time.NewTimer(25 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	last := in.Cursor
	lastPing := time.Now()
	for {
		fingerprint := map[string]any{"tools": state["tools"], "approvals": state["approvals"]}
		if current, ok := state["current_run"].(console.Record); ok {
			fingerprint["id"] = current.ID
			fingerprint["status"] = current.Status
			fingerprint["data"] = current.Data
		}
		raw, _ := json.Marshal(fingerprint)
		cursor := console.Hash(string(raw))
		if cursor != last {
			payload, _ := json.Marshal(state)
			_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := fmt.Fprintf(w, "id: %s\nevent: state\ndata: %s\n\n", cursor, payload); err != nil {
				return
			}
			if controller.Flush() != nil {
				return
			}
			last = cursor
			lastPing = time.Now()
		}
		if time.Since(lastPing) > 5*time.Second {
			_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			if controller.Flush() != nil {
				return
			}
			lastPing = time.Now()
		}
		if current, ok := state["current_run"].(console.Record); !ok || !busyDebug(current.Status) {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			return
		case <-tick.C:
		}
		state, err = h.service.debugStateWithHistory(r.Context(), in, owner, false)
		if err != nil {
			return
		}
	}
}
