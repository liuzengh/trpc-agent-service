package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/console"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

type SystemCheck struct {
	Name           string    `json:"name"`
	Label          string    `json:"label"`
	State          string    `json:"state"`
	Description    string    `json:"description"`
	Source         string    `json:"source"`
	ObservedAt     time.Time `json:"observed_at,omitempty"`
	ProbeAvailable bool      `json:"probe_available,omitempty"`
}
type systemInfo struct {
	role, publicURL string
	checks          map[string]func(context.Context) error
	mu              sync.Mutex
	lastProbe       SystemCheck
	limiter         loginLimiter
}

func (s *Service) WithSystemInfo(role, publicURL string, checks map[string]func(context.Context) error) *Service {
	s.system = &systemInfo{role: role, publicURL: publicURL, checks: checks}
	return s
}

func (h *Handler) handleSystem(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID string `json:"tenant_id"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
		return
	}
	if _, err := h.service.repository.GetTenant(r.Context(), in.TenantID); err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	info := h.service.system
	if info == nil {
		info = &systemInfo{}
	}
	if r.URL.Path == "/admin/system/probe" {
		if !h.require(w, r, in.TenantID, PermissionOperate) {
			return
		}
		if !info.limiter.allow(PrincipalName(r.Context()) + "/" + in.TenantID) {
			w.Header().Set("Retry-After", "300")
			adminJSON(w, 429, map[string]string{"error": "探测过于频繁，请稍后重试"})
			return
		}
		u, err := url.Parse(info.publicURL)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			h.writeResult(w, 0, nil, invalidf("部署者尚未配置有效的 HTTPS 公网入口"))
			return
		}
		u.Path = strings.TrimRight(u.Path, "/") + "/healthz"
		u.RawPath = ""
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			h.writeResult(w, 0, nil, invalidf("公网入口配置无效"))
			return
		}
		client := http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		check := SystemCheck{Name: "public_entry", Label: "公网入口 / Tunnel", State: "unavailable", Source: "explicit_https_probe", ObservedAt: time.Now().UTC(), Description: "未能完成公网健康检查。请检查 Tunnel、网络与源服务。", ProbeAvailable: true}
		response, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
			check.Description = fmt.Sprintf("公网健康检查 HTTP %d；此结果不等于 IM Webhook 配置已经验收。", response.StatusCode)
			if response.StatusCode == 200 {
				check.State = "ready"
			} else if response.StatusCode < 500 {
				check.State = "unknown"
			}
		}
		info.mu.Lock()
		info.lastProbe = check
		info.mu.Unlock()
		adminJSON(w, 200, check)
		return
	}
	checks := []SystemCheck{}
	for _, entry := range []struct {
		name, label string
		check       func(context.Context) error
	}{{"control", "控制面数据库", h.service.repository.Ready}, {"console", "控制台与调试存储", h.service.consoleStore.Ready}} {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err := entry.check(ctx)
		cancel()
		state := "ready"
		description := "当前进程的基础连接检查通过。"
		if err != nil {
			state = "unavailable"
			description = "连接、schema 或角色权限检查未通过。"
		}
		checks = append(checks, SystemCheck{Name: entry.name, Label: entry.label, State: state, Description: description, Source: "current_process", ObservedAt: time.Now().UTC()})
	}
	for _, name := range []string{"session", "queue", "quota"} {
		if fn := info.checks[name]; fn != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			err := fn(ctx)
			cancel()
			state := "ready"
			description := "当前进程的基础组件检查通过，不代表全部租户自定义后端已验证。"
			if err != nil {
				state = "unavailable"
				description = "当前组件检查未通过，请检查对应依赖服务。"
			}
			checks = append(checks, SystemCheck{Name: name, Label: map[string]string{"session": "会话组件", "queue": "工作队列", "quota": "配额组件"}[name], State: state, Description: description, Source: info.role, ObservedAt: time.Now().UTC()})
		}
	}
	workers, workerErr := h.service.consoleStore.List(r.Context(), console.Filter{Kind: "worker", AllTenants: true, Limit: 100})
	live, configuredSandbox := 0, 0
	last := time.Time{}
	for _, record := range workers {
		age := time.Since(record.UpdatedAt)
		if age < 0 || age > 30*time.Second {
			continue
		}
		live++
		if record.UpdatedAt.After(last) {
			last = record.UpdatedAt
		}
		var data struct {
			Sandbox bool `json:"sandbox_enabled"`
		}
		_ = json.Unmarshal(record.Data, &data)
		if data.Sandbox {
			configuredSandbox++
		}
	}
	workerState := "unknown"
	if live > 0 {
		workerState = "ready"
	}
	description := fmt.Sprintf("最近 30 秒收到 %d 个网页调试 Worker 心跳。", live)
	if workerErr != nil {
		description = "无法读取 Worker 心跳。"
	}
	checks = append(checks, SystemCheck{Name: "workers", Label: "调试执行节点", State: workerState, Description: description, Source: "worker_heartbeat", ObservedAt: last})
	checks = append(checks, SystemCheck{Name: "sandbox", Label: "Docker 沙箱", State: "unknown", Description: fmt.Sprintf("%d 个活跃调试 Worker 配置了沙箱。未主动执行脚本；这不等于 Docker 此刻可用。", configuredSandbox), Source: "worker_configuration", ObservedAt: last})
	checks = append(checks, SystemCheck{Name: "model", Label: "模型服务", State: "unknown", Description: "配置名称：" + h.service.startupModelName + "。此页面不会调用模型；请在 Agent 工作台发起调试验证。", Source: "startup_configuration"})
	principal, _ := r.Context().Value(principalContextKey{}).(Principal)
	public := SystemCheck{Name: "public_entry", Label: "公网入口 / Tunnel", State: "unknown", Source: "not_observed", Description: "仅访问本地页面无法判断公网入口状态。配置 TRPC_AGENT_PUBLIC_BASE_URL 后可显式探测。", ProbeAvailable: info.publicURL != "" && principal.Allows(PermissionOperate, in.TenantID)}
	info.mu.Lock()
	if !info.lastProbe.ObservedAt.IsZero() {
		public = info.lastProbe
		public.ProbeAvailable = info.publicURL != "" && principal.Allows(PermissionOperate, in.TenantID)
		if time.Since(public.ObservedAt) > 30*time.Second {
			public.State = "unknown"
			public.Description = "上次公网探测已过期，请按需重新检查。"
		}
	}
	info.mu.Unlock()
	checks = append(checks, public)
	shared, workerDetails := h.service.workerDependencies(r.Context(), in.TenantID, "")
	for _, entry := range shared {
		if entry.Component == "worker" {
			continue
		}
		checks = append(checks, SystemCheck{Name: "worker_" + entry.Component, Label: map[string]string{"session": "Worker 默认 Session 后端", "queue": "Worker 工作队列", "quota": "Worker 配额后端", "sandbox": "Worker Docker 与固定镜像"}[entry.Component], State: entry.State, ObservedAt: entry.ObservedAt, Source: entry.Source, Description: "汇总活跃 Worker 的只读检查；任一不可用即标记异常。租户专用 Session 见下方节点明细，未初始化或过期的后端仍为未知。"})
	}
	adminJSON(w, 200, map[string]any{"role": info.role, "checks": checks, "workers": workerDetails})
}

type runsInput struct {
	Channel    string `json:"channel"`
	AfterTime  string `json:"after_time"`
	TenantID   string `json:"tenant_id"`
	AppID      string `json:"app_id"`
	Status     string `json:"status"`
	Limit      int    `json:"limit"`
	RequestID  string `json:"request_id"`
	Kind       string `json:"kind"`
	BeforeTime string `json:"before_time"`
	BeforeID   string `json:"before_id"`
}

func debugRunView(record console.Record) gateway.RunView {
	var run console.Run
	_ = json.Unmarshal(record.Data, &run)
	return gateway.RunView{Kind: "debug", RequestID: record.ID, TenantID: record.TenantID, AppID: record.AppID, RevisionID: run.SnapshotID, Channel: "console", Status: record.Status, ErrorType: run.ErrorType, TraceID: run.TraceID, CreatedAt: record.CreatedAt, LatencyMS: run.LatencyMS, PromptTokens: run.PromptTokens, CompletionTokens: run.CompletionTokens, Cost: run.Cost}
}

func (h *Handler) handleRuns(w http.ResponseWriter, r *http.Request) {
	var in runsInput
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
		return
	}
	if !identifierPattern.MatchString(in.TenantID) {
		h.writeResult(w, 0, nil, invalidf("请选择租户"))
		return
	}
	if r.URL.Path == "/admin/runs/list" {
		if in.Limit == 0 {
			in.Limit = 50
		}
		if in.Limit < 1 || in.Limit > 100 {
			h.writeResult(w, 0, nil, invalidf("invalid page size"))
			return
		}
		var before, after time.Time
		var err error
		if in.BeforeTime != "" {
			before, err = time.Parse(time.RFC3339Nano, in.BeforeTime)
			if err != nil || in.BeforeID != "" && !identifierPattern.MatchString(in.BeforeID) {
				h.writeResult(w, 0, nil, invalidf("invalid run cursor"))
				return
			}
		}
		if in.AfterTime != "" {
			after, err = time.Parse(time.RFC3339Nano, in.AfterTime)
			if err != nil {
				h.writeResult(w, 0, nil, invalidf("invalid start time"))
				return
			}
		}
		items := []gateway.RunView{}
		if h.service.runReader != nil && in.Channel != "console" {
			items, err = h.service.runReader.ListRuns(r.Context(), gateway.RunFilter{TenantID: in.TenantID, AppID: in.AppID, Status: in.Status, BeforeTime: before, BeforeID: in.BeforeID, Limit: in.Limit, AfterTime: after, Channel: in.Channel})
			if err != nil {
				h.writeResult(w, 0, nil, err)
				return
			}
		}
		debug := []console.Record{}
		if in.Channel == "" || in.Channel == "console" {
			debug, err = h.service.consoleStore.List(r.Context(), console.Filter{Kind: "run", TenantID: in.TenantID, AppID: in.AppID, Status: in.Status, Limit: in.Limit, Before: in.BeforeID, BeforeTime: before, AfterTime: after})
		}
		if err != nil {
			h.writeResult(w, 0, nil, err)
			return
		}
		for _, record := range debug {
			items = append(items, debugRunView(record))
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].CreatedAt.Equal(items[j].CreatedAt) {
				return items[i].RequestID > items[j].RequestID
			}
			return items[i].CreatedAt.After(items[j].CreatedAt)
		})
		var next any
		if len(items) >= in.Limit {
			items = items[:in.Limit]
			last := items[len(items)-1]
			next = map[string]string{"before_time": last.CreatedAt.Format(time.RFC3339Nano), "before_id": last.RequestID}
		}
		names := map[string]string{}
		for index := range items {
			item := &items[index]
			name, exists := names[item.AppID]
			if !exists {
				app, lookupErr := h.service.repository.GetAgentApp(r.Context(), in.TenantID, item.AppID)
				if lookupErr == nil {
					name = app.Name
				}
				names[item.AppID] = name
			}
			item.AppName = name
		}
		adminJSON(w, 200, map[string]any{"items": items, "next": next})
		return
	}
	if !identifierPattern.MatchString(in.RequestID) {
		h.writeResult(w, 0, nil, invalidf("request_id is required"))
		return
	}
	principal, _ := r.Context().Value(principalContextKey{}).(Principal)
	var view gateway.RunView
	var tools any = []any{}
	var err error
	if in.Kind == "debug" {
		record, getErr := h.service.consoleStore.Get(r.Context(), "run", in.TenantID, in.RequestID)
		if getErr != nil {
			h.writeResult(w, 0, nil, getErr)
			return
		}
		view = debugRunView(record)
		if record.OwnerID == principal.Name && principal.Allows(PermissionDebug, in.TenantID) {
			var run console.Run
			_ = json.Unmarshal(record.Data, &run)
			view.Reply = run.Reply
		}
		tools, err = (&console.ToolJournal{Store: h.service.consoleStore}).ListByRequest(r.Context(), in.TenantID, in.RequestID)
	} else {
		if h.service.runReader == nil {
			h.writeResult(w, 0, nil, gateway.ErrRunMissing)
			return
		}
		view, err = h.service.runReader.ReadRun(r.Context(), in.TenantID, in.RequestID, principal.Allows(PermissionWrite, in.TenantID))
		if err == nil && h.service.toolJournal != nil {
			tools, err = h.service.toolJournal.ListByRequest(r.Context(), in.TenantID, in.RequestID)
		}
	}
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	raw, _ := json.Marshal(view)
	var result map[string]any
	_ = json.Unmarshal(raw, &result)
	result["tools"] = tools
	result["jobs"] = []background.JobView{}
	result["jobs_state"] = "unavailable"
	if reader, ok := h.service.jobs.(background.Reader); ok {
		jobs, queryErr := reader.ListJobs(r.Context(), background.JobFilter{TenantID: in.TenantID, AppID: view.AppID, RequestID: view.RequestID, Limit: 100})
		if queryErr == nil {
			result["jobs"] = jobs
			result["jobs_state"] = "available"
		}
	}
	decisions := []audit.Event{}
	{
		events, queryErr := h.service.QueryAudit(r.Context(), audit.Query{TenantID: in.TenantID, RequestID: view.RequestID, Limit: 200})
		if queryErr == nil {
			for _, e := range events {
				if e.RequestID == view.RequestID {
					decisions = append(decisions, e)
				}
			}
		}
	}
	result["decisions"] = decisions
	adminJSON(w, 200, result)
}
