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
