"use client";

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { channelApi, channelError, receiveModeLabel, requiredCredentialPurposes, type ChannelAccount } from "../../lib/channel-api";
import { channelHref, readChannelTarget, channelTargetQuery } from "../../lib/channel-editor-state";
import { Button, EmptyState, PageHeader, StatusBadge } from "../ui";
import { useChannelAccess } from "./use-channel-access";
import styles from "./create.module.css";

export function AccountList({ tenantId, query = "" }: { tenantId: string; query?: string }) {
  const access = useChannelAccess(tenantId);
  const target = readChannelTarget(query);
  const targetRequested = new URLSearchParams(query).has("deployment_id") || new URLSearchParams(query).has("revision_number");
  const invalidTarget = targetRequested && !target;
  const suffix = target ? `?${channelTargetQuery(target)}` : "";
  const [accounts, setAccounts] = useState<ChannelAccount[]>([]);
  const [cursor, setCursor] = useState<string | undefined>();
  const [failedCursor, setFailedCursor] = useState<string | undefined>();
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState("");
  const sequence = useRef(0);
  const inFlight = useRef(false);

  const load = useCallback(async (after?: string) => {
    if (inFlight.current) return;
    inFlight.current = true;
    const request = ++sequence.current;
    setLoading(true); setError(""); setFailedCursor(after);
    try {
      const page = await channelApi.listAccounts(tenantId, after, 50);
      if (sequence.current !== request) return;
      setAccounts((previous) => after ? [...previous, ...page.accounts.filter((a) => !previous.some((p) => p.account_id === a.account_id))] : page.accounts);
      setCursor(page.next_cursor); setLoaded(true);
    } catch (cause) {
      if (sequence.current === request) setError(channelError(cause));
    } finally {
      if (sequence.current === request) { inFlight.current = false; setLoading(false); }
    }
  }, [tenantId]);

  useEffect(() => {
    sequence.current += 1; inFlight.current = false;
    setAccounts([]); setCursor(undefined); setLoaded(false); setError("");
    if (!access.loading && !access.error && access.userId) void load();
    return () => { sequence.current += 1; inFlight.current = false; };
  }, [access.userId, access.loading, access.error, load]);

  const createLink = `${channelHref(tenantId)}/new${suffix}`;
  return <div className={styles.page}>
    <PageHeader eyebrow="CHANNELS" title="渠道接入" description="管理机器人的接入配置，再将新消息交给固定的部署版本。" action={!access.loading && access.canWrite ? <Link className="button primary" href={createLink}>＋ 添加渠道账户</Link> : undefined} />
    {invalidTarget && <div className={styles.error} role="alert">入口中的固定部署参数无效或重复，请返回部署版本详情重新选择。下面仅展示账户，不会预选或修改运行目标。</div>}
    {target ? <div className={styles.banner}>正在为部署 <strong>{target.deployment_id} · r{target.revision_number}</strong> 选择渠道账户。选择只打开工作台；已有目标需要另行确认切换，不会自动改绑。</div> : <div className={styles.banner}>先保存渠道账户与固定目标，再分别启用接入和消息路由。配置启用不等于连接或 Agent 执行成功。</div>}
    {!access.loading && !access.canWrite && !access.error && <p className={styles.small}>当前为只读访问。添加账户、修改凭据和启停操作由租户 OWNER 完成。</p>}
    {access.error && <div className={styles.error} role="alert">{access.error} <Button variant="secondary" onClick={access.reload}>重新读取访问权限</Button></div>}
    {error && <div className={styles.error} role="alert">{error} <Button variant="secondary" disabled={loading} onClick={() => void load(failedCursor)}>重新读取渠道账户</Button></div>}
    <section className={styles.card} aria-label="渠道账户列表">
      {(access.loading || (loading && !loaded)) && <p role="status">正在读取渠道账户…</p>}
      {loaded && !accounts.length && !error && <EmptyState title="还没有渠道账户" detail={access.canWrite ? "添加 Telegram 或企业微信机器人，保存后默认保持停用。" : "当前租户还没有渠道账户，请由 OWNER 添加。"} />}
      {!!accounts.length && <div className={styles.scroll}><table className={styles.table}>
        <thead><tr><th>账户 / 说明</th><th>渠道身份</th><th>接入配置</th><th>凭据配置</th><th>更新时间</th><th>操作</th></tr></thead>
        <tbody>{accounts.map((account) => <tr key={account.account_id}>
          <td><Link className={styles.link} href={channelHref(tenantId, account.account_id) + suffix}>{account.name}</Link><p className={styles.small}>{account.description || "暂无说明"}</p></td>
          <td>{account.provider === "telegram" ? "Telegram" : "企业微信"}<p><code>{account.provider_account_id}</code></p></td>
          <td><StatusBadge tone={account.enabled ? "blue" : "gray"}>{account.enabled ? "配置已启用" : "配置已停用"}</StatusBadge>{account.provider === "telegram" && <p className={styles.small}>{receiveModeLabel(account.config.receive_mode)}</p>}</td>
          <td>必需 {requiredCredentialPurposes(account.provider, account.config.receive_mode).filter((purpose) => account.credentials.find((credential) => credential.purpose === purpose)?.configured).length} / {requiredCredentialPurposes(account.provider, account.config.receive_mode).length} 项已配置</td>
          <td>{new Date(account.updated_at).toLocaleString("zh-CN")}</td>
          <td><Link className={styles.link} href={channelHref(tenantId, account.account_id) + suffix}>{target ? "选择此账户 →" : "打开工作台 →"}</Link></td>
        </tr>)}</tbody>
      </table></div>}
      {loaded && !!accounts.length && <div className={styles.pagination}><span className={styles.small}>已加载 {accounts.length} 个账户</span>{cursor && <Button variant="secondary" disabled={loading || !!access.error} onClick={() => void load(cursor)}>{loading ? "正在加载…" : "加载更多"}</Button>}</div>}
      <p className={styles.small}>列表展示已保存的接入配置；连接观测、运行目标和路由分发请在账户工作台查看。</p>
    </section>
  </div>;
}
