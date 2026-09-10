package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/console"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type DraftData struct {
	Config               controlplane.AgentRevision `json:"config"`
	SourceRevisionID     string                     `json:"source_revision_id"`
	LastEditor           string                     `json:"last_editor"`
	PublishKey           string                     `json:"publish_key,omitempty"`
	PublishedRevisionID  string                     `json:"published_revision_id,omitempty"`
	PublishedFromVersion int64                      `json:"published_from_version,omitempty"`
	PublishedAppVersion  int64                      `json:"published_app_version,omitempty"`
}
type draftInput struct {
	TenantID           string                     `json:"tenant_id"`
	AppID              string                     `json:"app_id"`
	SourceRevisionID   string                     `json:"source_revision_id"`
	ExpectedVersion    int64                      `json:"expected_version"`
	ExpectedAppVersion int64                      `json:"expected_app_version"`
	RequestID          string                     `json:"request_id"`
	Config             controlplane.AgentRevision `json:"config"`
}

func (s *Service) draft(ctx context.Context, tenant, app string) (console.Record, error) {
	if _, err := s.repository.GetAgentApp(ctx, tenant, app); err != nil {
		return console.Record{}, err
	}
	record, err := s.consoleStore.Get(ctx, "draft", tenant, app)
	if err == nil {
		if record.AppID != app {
			return console.Record{}, console.ErrNotFound
		}
		return record, nil
	}
	if !errors.Is(err, console.ErrNotFound) {
		return console.Record{}, err
	}
	var cfg controlplane.AgentRevision
	source, err := s.repository.GetStableRevision(ctx, tenant, app)
	if err != nil && !errors.Is(err, controlplane.ErrNotFound) {
		return console.Record{}, err
	}
	if err == nil {
		cfg = source
	} else {
		cfg = controlplane.AgentRevision{AgentType: "llm", AgentConfig: json.RawMessage(`{"name":"assistant","instruction":"准确回答用户的问题，调用工具前遵守权限和审批。"}`), ModelConfig: json.RawMessage(`{"source":"startup_env"}`), ToolPolicy: json.RawMessage(`{"allowed_tools":["current_time"],"max_tool_calls":4}`), KnowledgeConfig: json.RawMessage(`{}`), MemoryConfig: json.RawMessage(`{}`), GuardrailConfig: json.RawMessage(`{}`)}
	}
	cfg.TenantID, cfg.AppID = tenant, app
	cfg.ID = ""
	cfg.RevisionNo = 0
	cfg.Checksum = ""
	cfg.CreatedAt = time.Time{}
	data, _ := json.Marshal(DraftData{Config: cfg, SourceRevisionID: source.ID})
	return console.Record{Kind: "draft", ID: app, TenantID: tenant, AppID: app, Status: "draft", Data: data}, nil
}

func normalizeDraftConfig(cfg *controlplane.AgentRevision, tenant, app, actor string) error {
	cfg.TenantID, cfg.AppID, cfg.CreatedBy = tenant, app, actor
	cfg.ID = ""
	cfg.RevisionNo = 0
	cfg.Checksum = ""
	cfg.CreatedAt = time.Time{}
	for _, raw := range []*json.RawMessage{&cfg.AgentConfig, &cfg.ModelConfig, &cfg.ToolPolicy, &cfg.KnowledgeConfig, &cfg.MemoryConfig, &cfg.GuardrailConfig} {
		if normalizeJSON(raw) != nil || len(*raw) == 0 || (*raw)[0] != '{' {
			return invalidf("配置必须是合法 JSON 对象")
		}
		var value any
		if json.Unmarshal(*raw, &value) != nil {
			return invalidf("配置无法解析")
		}
		safe, _ := json.Marshal(redactCatalog(value))
		if strings.Contains(string(safe), "[REDACTED") {
			return invalidf("配置不能包含密钥原文或脱敏占位值，请使用部署者授权的引用")
		}
	}
	if raw, _ := json.Marshal(cfg); len(raw) > 256<<10 {
		return invalidf("草稿配置超过 256 KiB 限制")
	}
	return nil
}

func (s *Service) saveDraft(ctx context.Context, in draftInput, actor string, reset bool) (console.Record, error) {
	s.draftMu.Lock()
	defer s.draftMu.Unlock()
	current, err := s.draft(ctx, in.TenantID, in.AppID)
	if err != nil {
		return console.Record{}, err
	}
	if current.Version != in.ExpectedVersion {
		return console.Record{}, console.ErrConflict
	}
	var data DraftData
	if json.Unmarshal(current.Data, &data) != nil {
		return console.Record{}, console.ErrUnavailable
	}
	if reset {
		if in.SourceRevisionID == "" {
			return console.Record{}, invalidf("请选择要编辑的版本")
		}
		revision, err := s.repository.GetRevision(ctx, in.TenantID, in.SourceRevisionID)
		if err != nil {
			return console.Record{}, err
		}
		if revision.AppID != in.AppID {
			return console.Record{}, controlplane.ErrNotFound
		}
		data.Config = revision
		data.SourceRevisionID = revision.ID
	} else {
		data.Config = in.Config
	}
	if err := normalizeDraftConfig(&data.Config, in.TenantID, in.AppID, actor); err != nil {
		return console.Record{}, err
	}
	data.LastEditor = actor
	data.PublishKey = ""
	data.PublishedRevisionID = ""
	data.PublishedFromVersion = 0
	current.Data, _ = json.Marshal(data)
	if current.Version == 0 {
		current.OwnerID = actor
		current, err = s.consoleStore.Create(ctx, current)
	} else {
		current, err = s.consoleStore.Update(ctx, current, in.ExpectedVersion)
	}
	if err != nil {
		return console.Record{}, err
	}
	if err = s.record(ctx, in.TenantID, "admin_draft_saved", map[string]any{"app_id": in.AppID, "draft_version": current.Version}); err != nil {
		return console.Record{}, err
	}
	return current, nil
}

func (s *Service) publishDraft(ctx context.Context, in draftInput, actor string) (map[string]any, error) {
	if in.ExpectedVersion < 1 || in.ExpectedAppVersion < 1 || !identifierPattern.MatchString(in.RequestID) {
		return nil, invalidf("发布需要草稿版本、应用版本和唯一请求编号")
	}
	s.draftMu.Lock()
	defer s.draftMu.Unlock()
	var output map[string]any
	err := s.consoleStore.Transaction(ctx, func(ctx context.Context) error {
		if err := s.consoleStore.LockDraft(ctx, in.TenantID, in.AppID); err != nil {
			return err
		}
		draft, err := s.consoleStore.Get(ctx, "draft", in.TenantID, in.AppID)
		if err != nil {
			return err
		}
		if draft.AppID != in.AppID {
			return console.ErrNotFound
		}
		var data DraftData
		if json.Unmarshal(draft.Data, &data) != nil {
			return console.ErrUnavailable
		}
		if data.PublishKey == in.RequestID && data.PublishedFromVersion == in.ExpectedVersion {
			output = map[string]any{"revision_id": data.PublishedRevisionID, "app_version": data.PublishedAppVersion, "draft": draft, "duplicate": true}
			return nil
		}
		if draft.Version != in.ExpectedVersion {
			return console.ErrConflict
		}
		app, err := s.repository.GetAgentApp(ctx, in.TenantID, in.AppID)
		if err != nil {
			return err
		}
		if app.Version != in.ExpectedAppVersion {
			return controlplane.ErrConflict
		}
		cfg := data.Config
		if err := normalizeDraftConfig(&cfg, in.TenantID, in.AppID, actor); err != nil {
			return err
		}
		if err := s.requireValidRevision(ctx, cfg); err != nil {
			return err
		}
		sequence, ok := s.repository.(controlplane.RevisionSequenceRepository)
		if !ok {
			return console.ErrUnavailable
		}
		cfg.ID = "rev-" + digest(in.TenantID + "\x00" + in.AppID + "\x00" + in.RequestID)[:32]
		revision, err := sequence.CreateNextRevision(ctx, cfg)
		if err != nil {
			return err
		}
		previousRevision := app.StableRevisionID
		app, err = s.repository.PublishRevision(ctx, in.TenantID, in.AppID, revision.ID, in.ExpectedAppVersion)
		if err != nil {
			return err
		}
		data.PublishKey = in.RequestID
		data.PublishedRevisionID = revision.ID
		data.PublishedFromVersion = draft.Version
		data.PublishedAppVersion = app.Version
		data.SourceRevisionID = revision.ID
		draft.Data, _ = json.Marshal(data)
		draft, err = s.consoleStore.Update(ctx, draft, draft.Version)
		if err != nil {
			return err
		}
		output = map[string]any{"revision_id": revision.ID, "app_version": app.Version, "draft": draft, "duplicate": false}
		return s.record(ctx, in.TenantID, "admin_draft_published", map[string]any{"app_id": in.AppID, "action": "publish", "revision_id": revision.ID, "previous_revision_id": previousRevision, "request_id": in.RequestID, "version": app.Version})
	})
	if err != nil {
		return nil, err
	}
	return output, nil
}

func safeConsoleValue(value any) any {
	raw, _ := json.Marshal(value)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return redactCatalog(parsed)
}

func (h *Handler) handleDrafts(w http.ResponseWriter, r *http.Request) {
	var in draftInput
	if !decodeAdmin(w, r, &in) {
		return
	}
	permission := PermissionRead
	if r.URL.Path != "/admin/drafts/get" && r.URL.Path != "/admin/drafts/readiness" {
		permission = PermissionWrite
	}
	if !h.require(w, r, in.TenantID, permission) {
		return
	}
	if !identifierPattern.MatchString(in.AppID) {
		h.writeResult(w, 0, nil, invalidf("请选择 Agent 应用"))
		return
	}
	switch r.URL.Path {
	case "/admin/drafts/readiness":
		value, err := h.service.draftReadiness(r.Context(), in.TenantID, in.AppID)
		h.writeResult(w, 200, value, err)
	case "/admin/drafts/get":
		record, err := h.service.draft(r.Context(), in.TenantID, in.AppID)
		h.writeResult(w, 200, safeConsoleValue(record), err)
	case "/admin/drafts/save", "/admin/drafts/reset":
		record, err := h.service.saveDraft(r.Context(), in, PrincipalName(r.Context()), r.URL.Path == "/admin/drafts/reset")
		h.writeResult(w, 200, safeConsoleValue(record), err)
	case "/admin/drafts/publish":
		result, err := h.service.publishDraft(r.Context(), in, PrincipalName(r.Context()))
		h.writeResult(w, 200, safeConsoleValue(result), err)
	}
}

func (s *Service) draftReadiness(ctx context.Context, tenant, app string) (map[string]any, error) {
	draft, err := s.draft(ctx, tenant, app)
	if err != nil {
		return nil, err
	}
	var data DraftData
	if json.Unmarshal(draft.Data, &data) != nil {
		return nil, console.ErrUnavailable
	}
	wanted := controlplane.RevisionChecksum(data.Config)
	sessions, err := s.consoleStore.List(ctx, console.Filter{Kind: "session", TenantID: tenant, AppID: app, Limit: 100})
	if err != nil {
		return nil, err
	}
	result := map[string]any{"state": "untested", "profile": "isolated_console", "description": "未找到当前配置的已完成调试。静态检查不能替代真实执行。"}
	for _, record := range sessions {
		var session console.Session
		if json.Unmarshal(record.Data, &session) != nil {
			continue
		}
		snapshotRecord, err := s.consoleStore.Get(ctx, "snapshot", tenant, session.SnapshotID)
		if err != nil {
			continue
		}
		var snapshot console.Snapshot
		if json.Unmarshal(snapshotRecord.Data, &snapshot) != nil || snapshot.SourceChecksum != wanted {
			continue
		}
		runs, err := s.consoleStore.List(ctx, console.Filter{Kind: "run", TenantID: tenant, AppID: app, SessionID: record.ID, Limit: 100})
		if err != nil {
			return nil, err
		}
		for _, runRecord := range runs {
			var run console.Run
			if json.Unmarshal(runRecord.Data, &run) != nil {
				continue
			}
			if runRecord.Status == "completed" && run.ErrorType == "" && len(run.PendingApprovals) == 0 {
				return map[string]any{"state": "passed", "profile": "isolated_console", "description": "此配置已完成一次隔离调试；不代表所有业务场景或 IM 用户权限已验证。", "request_id": runRecord.ID, "snapshot_id": session.SnapshotID, "checked_at": runRecord.UpdatedAt, "disabled_tools": snapshot.DisabledTools}, nil
			}
			if result["state"] == "untested" {
				result["state"] = runRecord.Status
				result["request_id"] = runRecord.ID
				result["description"] = "找到相同配置的调试记录，但尚无成功完成的结果。"
			}
		}
	}
	return result, nil
}

func (h *Handler) handleWorkspace(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID string `json:"tenant_id"`
		AppID    string `json:"app_id"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
		return
	}
	app, err := h.service.repository.GetAgentApp(r.Context(), in.TenantID, in.AppID)
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	draft, err := h.service.draft(r.Context(), in.TenantID, in.AppID)
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	var tools []map[string]any
	for _, name := range h.service.tools.Names() {
		items, err := h.service.tools.Resolve([]string{name})
		if err != nil || len(items) == 0 {
			continue
		}
		tools = append(tools, map[string]any{"name": name, "description": items[0].Declaration().Description, "requires_approval": h.service.tools.RequiresApproval(name) || h.service.tools.IsManagedSideEffect(name)})
	}
	tools = append(tools, map[string]any{"name": "skill_load", "description": "加载已授权 Skill 的说明", "requires_approval": false})
	stable, stableErr := h.service.repository.GetStableRevision(r.Context(), in.TenantID, in.AppID)
	if stableErr != nil && !errors.Is(stableErr, controlplane.ErrNotFound) {
		h.writeResult(w, 0, nil, stableErr)
		return
	}
	h.writeResult(w, 200, safeConsoleValue(map[string]any{"app": app, "draft": draft, "stable": stable, "skills": h.service.skills.List(in.TenantID), "tools": tools, "startup_model_name": h.service.startupModelName}), nil)
}

func newConsoleID(prefix string) string {
	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New()
	}
	return prefix + id.String()
}

func (h *Handler) handleAppSettings(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID        string `json:"tenant_id"`
		AppID           string `json:"app_id"`
		Name            string `json:"name"`
		Description     string `json:"description"`
		Status          string `json:"status"`
		ExpectedVersion int64  `json:"expected_version"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionWrite) {
		return
	}
	if strings.TrimSpace(in.Name) == "" || len(in.Name) > 200 || len(in.Description) > 2000 || in.ExpectedVersion < 1 || (in.Status != "active" && in.Status != "disabled") {
		h.writeResult(w, 0, nil, invalidf("应用名称、状态或版本无效"))
		return
	}
	repo, ok := h.service.repository.(controlplane.AppSettingsRepository)
	if !ok {
		h.writeResult(w, 0, nil, invalidf("应用设置不可用"))
		return
	}
	app, err := repo.UpdateAppSettings(r.Context(), in.TenantID, in.AppID, strings.TrimSpace(in.Name), in.Description, in.Status, in.ExpectedVersion)
	if err == nil {
		err = h.service.record(r.Context(), in.TenantID, "admin_app_settings_updated", map[string]any{"app_id": in.AppID, "version": app.Version, "status": app.Status})
	}
	h.writeResult(w, 200, app, err)
}

func (h *Handler) onboardApp(w http.ResponseWriter, r *http.Request) {
	var in controlplane.AgentApp
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionWrite) {
		return
	}
	if in.ID == "" {
		in.ID = newConsoleID("app-")
	}
	var result controlplane.AgentApp
	err := h.service.consoleStore.Transaction(r.Context(), func(ctx context.Context) error {
		app, err := h.service.CreateAgentApp(ctx, in)
		if err != nil {
			return err
		}
		bindings, err := h.service.repository.ListBackendBindings(ctx, app.TenantID, app.ID)
		if err != nil {
			return err
		}
		hasSession := false
		for _, b := range bindings {
			hasSession = hasSession || b.ResourceType == "session" && b.MigrationState == "active"
		}
		if !hasSession {
			_, err = h.service.CreateBackendBinding(ctx, controlplane.BackendBinding{TenantID: app.TenantID, AppID: app.ID, ResourceType: "session", BackendType: "startup_config", Config: json.RawMessage(`{}`)})
			if err != nil {
				return err
			}
		}
		result = app
		return nil
	})
	h.writeResult(w, 201, result, err)
}
