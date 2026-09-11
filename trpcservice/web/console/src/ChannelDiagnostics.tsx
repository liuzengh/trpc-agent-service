import { useEffect, useState } from "react";
import { Alert, Button, Descriptions, Skeleton, Table, Typography } from "antd";
import { api, errorText } from "./api";
import { Failure, Status } from "./components";
import { date, type Dict } from "./types";

export function ChannelDiagnostics({
  tenant,
  bindingID,
}: {
  tenant: string;
  bindingID: string;
}) {
  const [data, setData] = useState<Dict | null>(null);
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    let live = true;
    setData(null);
    setError("");
    api<Dict>("channel-bindings/diagnostics", {
      tenant_id: tenant,
      binding_id: bindingID,
    })
      .then((value) => {
        if (live) setData(value);
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      });
    return () => {
      live = false;
    };
  }, [tenant, bindingID, refresh]);
  return (
    <>
      <div className="list-toolbar">
        <h3>接入与投递诊断</h3>
        <Button onClick={() => setRefresh((v) => v + 1)}>刷新记录</Button>
      </div>
      {error ? (
        <Failure error={error} />
      ) : !data ? (
        <Skeleton active />
      ) : (
        <>
          <Alert
            type="info"
            showIcon
            title="只读诊断，不会读取新的外部消息或触发重发"
            description={data.hint}
          />
          <Descriptions
            column={1}
            items={[
              {
                key: "time",
                label: "查询时间",
                children: date(data.observed_at),
              },
              ...(data.callback_path
                ? [
                    {
                      key: "path",
                      label: "回调路径",
                      children: (
                        <Typography.Text copyable>
                          {data.callback_path}
                        </Typography.Text>
                      ),
                    },
                    {
                      key: "url",
                      label: "完整回调 URL",
                      children: data.callback_url ? (
                        <>
                          <Typography.Paragraph copyable>
                            {data.callback_url}
                          </Typography.Paragraph>
                          <span className="muted">
                            按部署配置生成，尚未核实此路径的公网可达性或 IM
                            注册状态。
                          </span>
                        </>
                      ) : (
                        "部署者尚未配置有效的 TRPC_AGENT_PUBLIC_BASE_URL。请用自己的公网 HTTPS 域名拼接上述路径。"
                      ),
                    },
                  ]
                : []),
            ]}
          />
          {data.message_policy && (
            <Alert
              type={
                data.message_policy.mode === "realtime" ? "info" : "warning"
              }
              showIcon
              title={
                data.message_policy.mode === "realtime"
                  ? `近期优先 · 接收窗口 ${data.message_policy.max_age_seconds} 秒`
                  : "完整补读 · 离线历史可能延迟新消息"
              }
              description="新消息优先接收，历史缺口在独立后台补读；已接收请求会持久保存。模型暂时不可用时等待恢复，不因消息年龄直接丢弃。"
            />
          )}
          {!!data.gaps?.length && (
            <>
              <h4>历史补读进度（旧版本跳过记录不会自动重放）</h4>
              <Table
                size="small"
                pagination={false}
                rowKey="recorded_at"
                dataSource={data.gaps}
                columns={[
                  {
                    title: "原进度",
                    render: (_, r: Dict) => date(r.previous_through),
                  },
                  {
                    title: "近期窗口起点",
                    render: (_, r: Dict) => date(r.recent_from),
                  },
                  {
                    title: "补读状态",
                    render: (_, r: Dict) =>
                      ({
                        pending: "补读中",
                        completed: "已补读",
                        blocked: `需处理：${r.last_error}`,
                        skipped: "旧版跳过 · 未重放",
                      })[String(r.status)] || r.status,
                  },
                  {
                    title: "补读进度",
                    render: (_, r: Dict) => date(r.cursor_at),
                  },
                  {
                    title: "记录时间",
                    render: (_, r: Dict) => date(r.recorded_at),
                  },
                ]}
              />
            </>
          )}
          {!!data.dispositions?.length && (
            <>
              <h4>历史忽略 / 无效消息记录</h4>
              <Table
                size="small"
                pagination={false}
                rowKey="message_id"
                dataSource={data.dispositions}
                columns={[
                  {
                    title: "原消息时间",
                    render: (_, r: Dict) => date(r.occurred_at),
                  },
                  {
                    title: "处理原因",
                    render: (_, r: Dict) =>
                      r.reason === "message_expired"
                        ? "聊天消息已过期"
                        : "消息时间无效",
                  },
                ]}
              />
            </>
          )}
          {data.issues?.map((issue: string, index: number) => (
            <Alert key={index} type="warning" showIcon title={issue} />
          ))}
          <h4>最近接收的请求</h4>
          <Table
            size="small"
            pagination={false}
            rowKey="request_id"
            dataSource={data.runs || []}
            locale={{ emptyText: "尚无已接收的请求；这不代表通道已经可用。" }}
            columns={[
              {
                title: "接收时间 / 请求",
                render: (_, row: Dict) => (
                  <div className="table-name">
                    <strong>{date(row.created_at)}</strong>
                    <small>{row.request_id}</small>
                  </div>
                ),
              },
              {
                title: "执行",
                render: (_, row: Dict) => (
                  <>
                    <Status
                      value={
                        row.status === "completed" && row.error_type
                          ? "completed_with_issues"
                          : row.status
                      }
                    />
                    {row.error_type && <small>{row.error_type}</small>}
                  </>
                ),
              },
              {
                title: "投递",
                render: (_, row: Dict) =>
                  row.delivery ? (
                    <>
                      <Status value={row.delivery.status} />
                      <small>
                        {row.delivery.error_type ||
                          `尝试 ${row.delivery.attempts} 次`}
                      </small>
                    </>
                  ) : (
                    "尚无投递记录"
                  ),
              },
            ]}
          />
          {data.checkpoints?.length > 0 && (
            <>
              <h4>企业微信轮询进度</h4>
              <Table
                size="small"
                pagination={false}
                rowKey="chat_hash"
                dataSource={data.checkpoints}
                columns={[
                  { title: "会话指纹", dataIndex: "chat_hash", ellipsis: true },
                  {
                    title: "已处理至",
                    render: (_, row: Dict) => date(row.through_at),
                  },
                  { title: "版本", dataIndex: "version" },
                ]}
              />
            </>
          )}
          {data.rejections?.length > 0 && (
            <>
              <h4>最近拒绝原因</h4>
              <Table
                size="small"
                pagination={false}
                rowKey="fingerprint"
                dataSource={data.rejections}
                columns={[
                  {
                    title: "时间",
                    render: (_, row: Dict) => date(row.observed_at),
                  },
                  { title: "原因", dataIndex: "reason" },
                  { title: "Trace", dataIndex: "trace_id", ellipsis: true },
                ]}
              />
            </>
          )}
        </>
      )}
    </>
  );
}
