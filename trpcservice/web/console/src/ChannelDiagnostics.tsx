import { useEffect, useState } from "react";
import { Alert, Button, Descriptions, Skeleton, Table } from "antd";
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
                      children: <code>{data.callback_path}</code>,
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
                  ? `近期优先 · 有效期 ${data.message_policy.max_age_seconds} 秒`
                  : "完整补读 · 离线历史可能延迟新消息"
              }
              description="过期聊天不触发 Agent 或工具，不补发旧回复。已经开始执行的任务保留原有执行与恢复记录。"
            />
          )}
          {!!data.gaps?.length && (
            <>
              <h4>按近期策略跳过的历史区间</h4>
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
                    title: "记录时间",
                    render: (_, r: Dict) => date(r.recorded_at),
                  },
                ]}
              />
            </>
          )}
          {!!data.dispositions?.length && (
            <>
              <h4>未执行的过期消息</h4>
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
