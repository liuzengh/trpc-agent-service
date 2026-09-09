package admin

import (
	"context"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// This endpoint reads existing platform records only. In particular, opening
// a channel drawer must not poll a chat, change a cursor or deliver a message.
func (h *Handler) handleChannelDiagnostics(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID  string `json:"tenant_id"`
		BindingID string `json:"binding_id"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	binding, err := h.service.repository.GetChannelBinding(ctx, in.TenantID, in.BindingID)
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	result := map[string]any{
		"binding_id": binding.ID, "status": binding.Status, "observed_at": time.Now().UTC(),
		"source": "stored_platform_records", "runs": []gateway.RunView{},
		"rejections": []wecommcp.RejectedMessage{}, "checkpoints": []wecommcp.CheckpointView{},
	}
	issues := []string{}
	switch binding.ChannelType {
	case "telegram", "wecom":
		result["callback_path"] = "/callbacks/" + binding.ChannelType + "/" + binding.CallbackKey
		result["hint"] = "此处只展示本平台接收后的记录，不能证明 IM 平台 Webhook 已正确配置。尚未到达平台的消息请结合公网入口与服务日志检查。"
	case wecommcp.ChannelType:
		result["hint"] = "企业微信消息 MCP 使用授权会话轮询，不需要 Webhook。检查点是已保存的消费进度，不等于当前外部服务在线。"
		if h.service.channelState == nil {
			issues = append(issues, "轮询状态存储未配置")
		} else {
			checkpoints, err := h.service.channelState.ListCheckpoints(ctx, in.TenantID, binding.ID)
			if err != nil {
				issues = append(issues, "暂时无法读取轮询进度")
			} else {
				if len(checkpoints) > 50 {
					checkpoints = checkpoints[:50]
					issues = append(issues, "仅显示前 50 个会话检查点")
				}
				result["checkpoints"] = checkpoints
			}
			rejections, err := h.service.channelState.ListRejections(ctx, in.TenantID, binding.ID, 20)
			if err != nil {
				issues = append(issues, "暂时无法读取消息拒绝记录")
			} else {
				result["rejections"] = rejections
			}
		}
	}
	if h.service.runReader == nil {
		issues = append(issues, "运行记录查询未配置")
	} else {
		runs, err := h.service.runReader.ListRuns(ctx, gateway.RunFilter{TenantID: in.TenantID, AppID: binding.AppID, BindingID: binding.ID, Limit: 5})
		if err != nil {
			issues = append(issues, "暂时无法读取最近请求")
		} else {
			for i := range runs {
				detail, err := h.service.runReader.ReadRun(ctx, in.TenantID, runs[i].RequestID, false)
				if err != nil {
					issues = append(issues, "部分投递状态无法读取")
					break
				}
				runs[i] = detail
			}
			result["runs"] = runs
		}
	}
	result["issues"] = issues
	h.writeResult(w, http.StatusOK, safeConsoleValue(result), nil)
}
