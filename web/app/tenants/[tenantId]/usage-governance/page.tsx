"use client";

import { useParams, useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import {
  ApiNotice,
  Button,
  PageHeader,
  StatCard,
  StatusBadge,
} from "../../../../components/ui";
import {
  controlApi,
  type Tenant,
  type UsagePolicy,
  type UsageSummary,
} from "../../../../lib/control-api";

const empty = (tenantId: string): UsagePolicy => ({
  schema_version: 1,
  tenant_id: tenantId,
  revision: 0,
  enabled: false,
  im: { allow_all: false, rules: [] },
  requests: { tenant_per_minute: 100, user_per_minute: 10 },
  execution: { max_concurrent_runs: 4 },
  tokens: {
    period_seconds: 2592000,
    limit: 1_000_000,
    reservation_per_run: 4096,
    input_micros_per_million_tokens: 0,
    output_micros_per_million_tokens: 0,
  },
});
const lines = (value: string) =>
  Array.from(
    new Set(
      value
        .split(/\r?\n/)
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  ).sort();

export default function UsageGovernancePage() {
  const { tenantId } = useParams<{ tenantId: string }>();
  const router = useRouter();
  const [tenant, setTenant] = useState<Tenant>();
  const [policy, setPolicy] = useState<UsagePolicy>(() => empty(tenantId));
  const [summary, setSummary] = useState<UsageSummary>();
  const [summaryUnavailable, setSummaryUnavailable] = useState(false);
  const [rules, setRules] = useState<UsagePolicy["im"]["rules"]>([]);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>();
  const [saved, setSaved] = useState(false);
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    void Promise.all([
      controlApi.getTenant(tenantId),
      controlApi.getUsagePolicy(tenantId),
      controlApi.getUsageSummary(tenantId).catch(() => {
        setSummaryUnavailable(true);
        return undefined;
      }),
    ])
      .then(([t, p, u]) => {
        if (cancelled) return;
        if (t.role !== "OWNER") {
          router.replace(
            `/tenants/${encodeURIComponent(tenantId)}/agents?notice=owner-only`,
          );
          return;
        }
        setTenant(t);
        setPolicy(p.enabled ? p : { ...empty(tenantId), revision: p.revision });
        setSummary(u);
        setRules(p.im.rules);
      })
      .catch(setError)
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [router, tenantId]);
  const consumed =
    (summary?.used_tokens ?? 0) + (summary?.reserved_tokens ?? 0);
  const remaining = Math.max(0, policy.tokens.limit - consumed);
  const estimated = useMemo(
    () =>
      ((summary?.estimated_cost_micros ?? 0) / 1_000_000).toLocaleString(
        "zh-CN",
        { style: "currency", currency: "USD", maximumFractionDigits: 4 },
      ),
    [summary],
  );
  async function save() {
    setBusy(true);
    setSaved(false);
    setError(undefined);
    try {
      const enabled = policy.enabled;
      const next: UsagePolicy = {
        ...policy,
        tenant_id: tenantId,
        revision: 0,
        im: enabled
          ? policy.im.allow_all
            ? { allow_all: true, rules: [] }
            : {
                allow_all: false,
                rules: rules.map((rule) => ({
                  account_id: rule.account_id.trim(),
                  ...(rule.binding_id?.trim()
                    ? { binding_id: rule.binding_id.trim() }
                    : {}),
                  user_ids: lines(rule.user_ids.join("\n")),
                  group_ids: lines(rule.group_ids.join("\n")),
                })),
              }
          : { allow_all: false, rules: [] },
        requests: enabled
          ? policy.requests
          : { tenant_per_minute: 0, user_per_minute: 0 },
        execution: enabled ? policy.execution : { max_concurrent_runs: 0 },
        tokens: enabled
          ? policy.tokens
          : {
              period_seconds: 0,
              limit: 0,
              reservation_per_run: 0,
              input_micros_per_million_tokens: 0,
              output_micros_per_million_tokens: 0,
            },
      };
      const result = await controlApi.replaceUsagePolicy(
        tenantId,
        summaryPolicyRevision(policy),
        next,
        crypto.randomUUID(),
      );
      setPolicy(
        result.enabled
          ? result
          : { ...empty(tenantId), revision: result.revision },
      );
      setSaved(true);
    } catch (caught) {
      setError(caught);
    } finally {
      setBusy(false);
    }
  }
  if (loading)
    return (
      <div className="panel-loading">
        <div className="loading-mark" />
        <span>正在读取租户使用策略…</span>
      </div>
    );
  return (
    <>
      <PageHeader
        eyebrow="TENANT GOVERNANCE"
        title={`${tenant?.name ?? "租户"} · 使用治理`}
        description="后台成员权限与 IM 使用权限分开管理；Gateway 执行入口授权和限流，Worker 共享并发及 Token 额度。"
      />
      <ApiNotice error={error} />
      {summaryUnavailable && (
        <div className="warning-notice">
          Worker 用量投影当前不可用；页面不会把未知用量显示为零。
        </div>
      )}
      {saved && (
        <div className="success-notice">
          使用策略已保存并立即用于新的消息和 Run。
        </div>
      )}
      <div className="stats-grid">
        <StatCard
          label="已用 Token"
          value={(summary?.used_tokens ?? 0).toLocaleString("zh-CN")}
          detail="仅统计 Provider 明确返回的真实 usage"
        />
        <StatCard
          label="预留 / 未知"
          value={(summary?.reserved_tokens ?? 0).toLocaleString("zh-CN")}
          detail={`未知 ${summary?.unknown_usage_count ?? 0}，待结算 ${summary?.pending_usage_count ?? 0}`}
        />
        <StatCard
          label="剩余额度"
          value={remaining.toLocaleString("zh-CN")}
          detail={`估算成本 ${estimated}，不是供应商账单`}
        />
      </div>
      <section className="panel governance-form">
        <div className="toolbar">
          <div>
            <strong>集中使用策略</strong>
            <p className="governance-hint">
              Revision {policy.revision} · 修改使用并发控制，不保存策略历史。
            </p>
          </div>
          <StatusBadge tone={policy.enabled ? "green" : "gray"}>
            {policy.enabled ? "已启用" : "未启用"}
          </StatusBadge>
        </div>
        <div className="governance-grid">
          <label className="field">
            <span>启用治理</span>
            <select
              value={policy.enabled ? "true" : "false"}
              onChange={(e) =>
                setPolicy({ ...policy, enabled: e.target.value === "true" })
              }
            >
              <option value="false">关闭</option>
              <option value="true">启用</option>
            </select>
          </label>
          <label className="field">
            <span>IM 授权方式</span>
            <select
              disabled={!policy.enabled}
              value={policy.im.allow_all ? "all" : "list"}
              onChange={(e) =>
                setPolicy({
                  ...policy,
                  im: { ...policy.im, allow_all: e.target.value === "all" },
                })
              }
            >
              <option value="list">仅允许清单</option>
              <option value="all">允许所有身份</option>
            </select>
          </label>
          {!policy.im.allow_all && policy.enabled && (
            <div className="governance-rule-list">
              <div className="toolbar">
                <strong>Bot / Binding 授权规则</strong>
                <Button
                  onClick={() =>
                    setRules([
                      ...rules,
                      { account_id: "", user_ids: [], group_ids: [] },
                    ])
                  }
                >
                  新增规则
                </Button>
              </div>
              {rules.map((rule, index) => (
                <div className="governance-rule" key={index}>
                  <label className="field">
                    <span>Bot / Channel Account ID</span>
                    <input
                      value={rule.account_id}
                      onChange={(e) => updateRule(index, { account_id: e.target.value })}
                    />
                  </label>
                  <label className="field">
                    <span>Binding ID（可选）</span>
                    <input
                      value={rule.binding_id ?? ""}
                      onChange={(e) => updateRule(index, { binding_id: e.target.value || undefined })}
                    />
                  </label>
                  <label className="field">
                    <span>允许的用户 ID（每行一个）</span>
                    <textarea
                      value={rule.user_ids.join("\n")}
                      onChange={(e) => updateRule(index, { user_ids: e.target.value.split(/\r?\n/) })}
                    />
                  </label>
                  <label className="field">
                    <span>允许的群 / 会话 ID（每行一个）</span>
                    <textarea
                      value={rule.group_ids.join("\n")}
                      onChange={(e) => updateRule(index, { group_ids: e.target.value.split(/\r?\n/) })}
                    />
                  </label>
                  <Button onClick={() => setRules(rules.filter((_, i) => i !== index))}>
                    删除规则
                  </Button>
                </div>
              ))}
            </div>
          )}
          <NumberField
            label="租户每分钟请求"
            disabled={!policy.enabled}
            value={policy.requests.tenant_per_minute}
            onChange={(v) =>
              setPolicy({
                ...policy,
                requests: { ...policy.requests, tenant_per_minute: v },
              })
            }
          />
          <NumberField
            label="单用户每分钟请求"
            disabled={!policy.enabled}
            value={policy.requests.user_per_minute}
            onChange={(v) =>
              setPolicy({
                ...policy,
                requests: { ...policy.requests, user_per_minute: v },
              })
            }
          />
          <NumberField
            label="租户活跃 Run 上限"
            disabled={!policy.enabled}
            value={policy.execution.max_concurrent_runs}
            onChange={(v) =>
              setPolicy({ ...policy, execution: { max_concurrent_runs: v } })
            }
          />
          <NumberField
            label="Token 周期（秒）"
            disabled={!policy.enabled}
            value={policy.tokens.period_seconds}
            onChange={(v) =>
              setPolicy({
                ...policy,
                tokens: { ...policy.tokens, period_seconds: v },
              })
            }
          />
          <NumberField
            label="周期 Token 配额"
            disabled={!policy.enabled}
            value={policy.tokens.limit}
            onChange={(v) =>
              setPolicy({ ...policy, tokens: { ...policy.tokens, limit: v } })
            }
          />
          <NumberField
            label="每 Run 预留 Token"
            disabled={!policy.enabled}
            value={policy.tokens.reservation_per_run}
            onChange={(v) =>
              setPolicy({
                ...policy,
                tokens: { ...policy.tokens, reservation_per_run: v },
              })
            }
          />
          <NumberField
            label="输入单价（微美元/百万 Token）"
            disabled={!policy.enabled}
            value={policy.tokens.input_micros_per_million_tokens}
            onChange={(v) =>
              setPolicy({
                ...policy,
                tokens: {
                  ...policy.tokens,
                  input_micros_per_million_tokens: v,
                },
              })
            }
          />
          <NumberField
            label="输出单价（微美元/百万 Token）"
            disabled={!policy.enabled}
            value={policy.tokens.output_micros_per_million_tokens}
            onChange={(v) =>
              setPolicy({
                ...policy,
                tokens: {
                  ...policy.tokens,
                  output_micros_per_million_tokens: v,
                },
              })
            }
          />
        </div>
        <div className="governance-actions">
          <p>
            Provider 未返回 usage 时显示“未知”并继续占用预留量，不按 0 计算。
          </p>
          <Button disabled={busy} onClick={() => void save()}>
            {busy ? "正在保存…" : "保存使用策略"}
          </Button>
        </div>
      </section>
    </>
  );

  function updateRule(index: number, change: Partial<UsagePolicy["im"]["rules"][number]>) {
    setRules(rules.map((rule, i) => (i === index ? { ...rule, ...change } : rule)));
  }
}
function NumberField({
  label,
  value,
  disabled,
  onChange,
}: {
  label: string;
  value: number;
  disabled: boolean;
  onChange: (value: number) => void;
}) {
  return (
    <label className="field">
      <span>{label}</span>
      <input
        type="number"
        min={0}
        disabled={disabled}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
      />
    </label>
  );
}
function summaryPolicyRevision(policy: UsagePolicy) {
  return policy.revision;
}
