import { useEffect, useState } from "react";
import { Alert, Button, Space, Table, Tag } from "antd";
import { api, errorText } from "./api";
import { Failure, Status } from "./components";
import { date, type Dict } from "./types";

const names: Record<string, string> = {
  session_summary: "会话摘要",
  memory_extract: "记忆提取",
  knowledge_upsert: "知识导入",
  knowledge_delete: "知识删除",
  memory_backfill: "记忆回填",
  memory_verify: "记忆校验",
  session_backfill: "会话回填",
  session_verify: "会话校验",
  knowledge_backfill: "知识回填",
  knowledge_verify: "知识校验",
};
export function JobTable({ items }: { items: Dict[] }) {
  return (
    <Table
      size="small"
      rowKey="job_id"
      pagination={false}
      dataSource={items}
      locale={{ emptyText: "没有记录到关联任务" }}
      columns={[
        {
          title: "任务",
          render: (_, r: Dict) => (
            <div className="table-name">
              <strong>{names[r.type] || r.type}</strong>
              <small>{r.job_id}</small>
            </div>
          ),
        },
        { title: "状态", render: (_, r: Dict) => <Status value={r.status} /> },
        {
          title: "尝试",
          render: (_, r: Dict) => `${r.attempt_count}/${r.max_attempts}`,
        },
        {
          title: "创建 / 完成",
          render: (_, r: Dict) => (
            <div className="table-name">
              <span>{date(r.created_at)}</span>
              <small>{date(r.completed_at)}</small>
            </div>
          ),
        },
        {
          title: "诊断",
          render: (_, r: Dict) =>
            r.has_error ? (
              <Tag color="warning">有失败记录，请按任务编号核对</Tag>
            ) : (
              "—"
            ),
        },
      ]}
    />
  );
}
export function ActivityPanel({
  kind,
  tenant,
  appID,
}: {
  kind: "jobs" | "releases";
  tenant: string;
  appID: string;
}) {
  const [items, setItems] = useState<Dict[]>([]),
    [next, setNext] = useState<Dict | null>(null),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [refresh, setRefresh] = useState(0);
  useEffect(() => {
    let live = true;
    setBusy(true);
    setError("");
    setItems([]);
    setNext(null);
    api<{ items: Dict[]; next: Dict | null }>(kind + "/list", {
      tenant_id: tenant,
      app_id: appID,
      limit: 30,
    })
      .then((data) => {
        if (live) {
          setItems(data.items || []);
          setNext(data.next);
        }
      })
      .catch((e) => {
        if (live) setError(errorText(e));
      })
      .finally(() => {
        if (live) setBusy(false);
      });
    return () => {
      live = false;
    };
  }, [tenant, appID, kind, refresh]);
  return (
    <>
      <div className="list-toolbar">
        <h3>{kind === "jobs" ? "后台任务" : "发布与回滚历史"}</h3>
        <Button onClick={() => setRefresh((v) => v + 1)}>刷新记录</Button>
      </div>
      {error && <Failure error={error} />}
      {kind === "jobs" ? (
        <>
          <Alert
            type="info"
            title="包含当前应用的摘要、记忆、知识同步与迁移任务"
            description="只展示持久化事实，不自动重试。旧任务没有源请求编号时，不会推测它属于哪次对话。"
          />
          <JobTable items={items} />
        </>
      ) : (
        <Table
          size="small"
          rowKey="audit_id"
          loading={busy}
          pagination={false}
          dataSource={items}
          locale={{ emptyText: "暂无保留的发布操作记录；版本创建不等于发布" }}
          columns={[
            { title: "操作时间", render: (_, r: Dict) => date(r.occurred_at) },
            { title: "操作人", dataIndex: "user_id" },
            {
              title: "操作",
              render: (_, r: Dict) => (
                <Tag>
                  {(
                    {
                      publish: "发布",
                      rollback: "回滚",
                      republish: "重新发布",
                      rollout: "灰度调整",
                    } as Dict
                  )[r.details?.action] ||
                    (r.decision === "admin_rollout_policy_updated"
                      ? "灰度调整"
                      : "发布 / 切换")}
                </Tag>
              ),
            },
            {
              title: "版本变化",
              render: (_, r: Dict) => (
                <div className="table-name">
                  <small>
                    {r.details?.previous_revision_id || "此前版本未记录"}
                  </small>
                  <strong>
                    {r.details?.revision_id ||
                      r.details?.canary_revision_id ||
                      "关闭灰度"}
                  </strong>
                  {r.details?.canary_percent !== undefined && (
                    <span>灰度 {r.details.canary_percent}%</span>
                  )}
                </div>
              ),
            },
          ]}
        />
      )}
      <Space>
        {busy && <span className="muted">正在读取…</span>}
        {next && (
          <Button
            loading={busy}
            onClick={async () => {
              setBusy(true);
              try {
                const data = await api<{ items: Dict[]; next: Dict | null }>(
                  kind + "/list",
                  { tenant_id: tenant, app_id: appID, limit: 30, ...next },
                );
                setItems((old) => [
                  ...new Map(
                    [...old, ...data.items].map((item) => [
                      item.audit_id || item.job_id,
                      item,
                    ]),
                  ).values(),
                ]);
                setNext(data.next);
              } catch (e) {
                setError(errorText(e));
              } finally {
                setBusy(false);
              }
            }}
          >
            加载更多
          </Button>
        )}
      </Space>
    </>
  );
}
