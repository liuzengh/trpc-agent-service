"use client";
import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  channelApi, channelError, ChannelApiError, credentialPurposes, requiredCredentialPurposes, receiveModeLabel, isTelegramReceiveMode, isChannelUncertain, validateCredential,
  type ChannelAccountDetails, type TelegramReceiveMode, type TelegramEndpointProfile, type ChannelTarget, type CredentialPurpose, type ChannelCommandResult,
} from "../../lib/channel-api";
import { channelHref, channelPendingKey, clearChannelPending, loadChannelPending, readChannelTarget, saveChannelPending, type ChannelPending } from "../../lib/channel-editor-state";
import { deploymentApi, type Deployment, type DeploymentRevision } from "../../lib/deployment-api";
import { Button, PageHeader, StatusBadge } from "../ui";
import { useChannelAccess } from "./use-channel-access";
import { ChannelDialog } from "./confirmation-dialog";
import { ChannelTargetSelector } from "./target-selector";
import { ChannelPreflightPanel } from "./preflight-panel";
import styles from "./channel.module.css";
import { ReceiveModeSelector } from "./receive-mode-selector";

type Action = { kind: "mode"; mode: TelegramReceiveMode; endpoint?: TelegramEndpointProfile } | { kind: "account"; enabled: boolean } | { kind: "binding"; enabled: boolean } | { kind: "target"; target: ChannelTarget } | { kind: "credential"; purpose: CredentialPurpose; action: "replace" | "clear"; value?: string };
type Prepared = { action: Action; snapshot: ChannelAccountDetails; readAt: number };
const sameRevision = (a: ChannelAccountDetails, b: ChannelAccountDetails) => a.account.account_revision === b.account.account_revision && a.binding?.binding_revision === b.binding?.binding_revision && a.binding?.binding_id === b.binding?.binding_id;
const targetName = (d: ChannelAccountDetails, deployment?: Deployment | null) => d.binding ? `${deployment?.id === d.binding.target.deployment_id && deployment.tenant_id === d.account.tenant_id && deployment.name.trim() ? deployment.name : "已保存部署"} · r${d.binding.target.revision_number}` : "未配置";
const time = (value: string | number) => new Date(value).toLocaleString("zh-CN");
const purposeLabel: Record<CredentialPurpose, string> = { "telegram.bot_token": "Bot Token", "telegram.webhook_secret": "Webhook Secret", "wecom.bot_secret": "Bot Secret" };
const distributionLabel: Record<string, string> = { NOT_EMITTED: "尚未产生路由事件", PENDING: "等待发布", IN_FLIGHT: "发布中", PUBLISHED: "事件已发布到消息流", FAILED: "发布失败" };
const observationReason: Record<string, string> = { WAITING_FOR_OWNER: "等待接收控制权", RECEIVER_DRAINING: "正在等待旧接收退出", WEBHOOK_CONFLICT: "Webhook 配置冲突，未接管外部入口", POLLING_CONFLICT: "长轮询存在竞争接收者", OWNERSHIP_LOST: "接收控制权已失效", REGISTRATION_PENDING: "正在协调远端接收方式", REGISTRATION_UNKNOWN: "远端协调结果待核对" };
const observationLabel: Record<string, string> = { CONFIG_APPLIED: "已应用连接配置", CONNECTING: "连接中", READY: "报告就绪（连接）", DISABLED: "报告已停用", ERROR: "连接错误", STALE: "观测已过期" };

export function ChannelWorkspace({ tenantId, accountId, query = "", initialPreflightId, invalidPreflightQuery = false }: { tenantId: string; accountId: string; query?: string; initialPreflightId?: string; invalidPreflightQuery?: boolean }) {
  const access = useChannelAccess(tenantId);
  const [data, setData] = useState<ChannelAccountDetails | null>(null); const [readAt, setReadAt] = useState(0); const [error, setError] = useState(""); const [notice, setNotice] = useState("");
  const [tab, setTab] = useState(initialPreflightId || invalidPreflightQuery ? "diagnostics" : query ? "target" : "overview"); const [refreshing, setRefreshing] = useState(false); const [busy, setBusy] = useState(false); const [preparing, setPreparing] = useState(false);
  const [prepared, setPrepared] = useState<Prepared | null>(null); const [pending, setPending] = useState<ChannelPending | null>(null); const [pendingBlocked, setPendingBlocked] = useState(false); const [recoveryReady, setRecoveryReady] = useState(false); const [storageError, setStorageError] = useState(""); const [accepted, setAccepted] = useState(false);
  const [target, setTarget] = useState<ChannelTarget | null>(null); const [selectionRevision, setSelectionRevision] = useState<DeploymentRevision | null>(null); const [currentRevision, setCurrentRevision] = useState<DeploymentRevision | null>(null); const [targetReadError, setTargetReadError] = useState(""); const [targetEpoch, setTargetEpoch] = useState(0);
  const [currentDeployment, setCurrentDeployment] = useState<Deployment | null>(null); const [deploymentReadError, setDeploymentReadError] = useState("");
  const [nextEndpoint, setNextEndpoint] = useState<TelegramEndpointProfile>("official");
  const [modeEditing, setModeEditing] = useState(false); const [nextMode, setNextMode] = useState<TelegramReceiveMode>("long_polling");
  const [metaEditing, setMetaEditing] = useState(false); const [metadataRevision, setMetadataRevision] = useState<number | null>(null); const [name, setName] = useState(""); const [description, setDescription] = useState("");
  const [credentialPurpose, setCredentialPurpose] = useState<CredentialPurpose | null>(null); const [credentialValue, setCredentialValue] = useState(""); const [retryValue, setRetryValue] = useState("");
  const mounted = useRef(true); const readSequence = useRef(0); const lock = useRef(false); const failures = useRef(0); const secretValue = useRef(""); const currentData = useRef(data); currentData.current = data;
  const pendingKey = access.userId ? channelPendingKey(access.userId, tenantId, accountId) : "";
  const initialTarget = readChannelTarget(query);
  const invalidTarget = !!query && !initialTarget;
  const onTargetChange = useCallback((next: ChannelTarget | null, revision: DeploymentRevision | null) => { setTarget(next); setSelectionRevision(revision); }, []);
  const refresh = useCallback(async () => {
    const sequence = ++readSequence.current; if (mounted.current) setRefreshing(true);
    try {
      const next = await channelApi.getAccount(tenantId, accountId);
      if (!mounted.current || sequence !== readSequence.current) return null;
      setData(next); currentData.current = next; setReadAt(Date.now()); setError(""); setAccepted(false); failures.current = 0;
      return next;
    } catch (e) {
      if (mounted.current && sequence === readSequence.current) { setError(channelError(e)); failures.current += 1; if (e instanceof ChannelApiError && [401, 403].includes(e.status)) access.denyWrites(); }
      return null;
    } finally { if (mounted.current && sequence === readSequence.current) setRefreshing(false); }
  }, [tenantId, accountId, access.denyWrites]);
  useEffect(() => {
    mounted.current = true; let timer: ReturnType<typeof setTimeout> | undefined; let stopped = false; let ticking = false;
    const schedule = () => { clearTimeout(timer); if (!stopped) timer = setTimeout(tick, Math.min(60_000, 15_000 * 2 ** Math.min(failures.current, 2))); };
    const tick = async () => { if (stopped || ticking) return; ticking = true; try { if (document.visibilityState !== "hidden" && !lock.current) await refresh(); } finally { ticking = false; schedule(); } };
    const focus = () => { clearTimeout(timer); if (document.visibilityState !== "hidden" && !lock.current) void tick(); else schedule(); };
    void tick(); window.addEventListener("focus", focus); document.addEventListener("visibilitychange", focus);
    return () => { stopped = true; mounted.current = false; ++readSequence.current; clearTimeout(timer); secretValue.current = ""; window.removeEventListener("focus", focus); document.removeEventListener("visibilitychange", focus); };
  }, [refresh]);
  const readRecovery = useCallback(() => {
    setRecoveryReady(false);
    if (!pendingKey) return;
    try {
      sessionStorage.getItem(pendingKey); // Explicit preflight: the tolerant loader hides storage errors.
      const old = loadChannelPending(sessionStorage, pendingKey); setPending(old); setRecoveryReady(true); setStorageError("");
      if (old) setNotice("发现上次尚未确认的操作；请先核对当前状态，不要重新创建或轮换。");
    } catch { setStorageError("恢复记录暂时读取失败。新写操作已暂停，请重试读取恢复记录，避免把未确认请求误当作新操作。"); }
  }, [pendingKey]);
  useEffect(() => { readRecovery(); }, [readRecovery]);
  useEffect(() => {
    secretValue.current = ""; setCredentialValue(""); setRetryValue(""); setCredentialPurpose(null); setPrepared(null); setModeEditing(false);
  }, [access.userId, access.canWrite]);
  const binding = data?.binding;
  useEffect(() => { let active = true; setCurrentRevision(null); setTargetReadError(""); if (!binding) return;
    void deploymentApi.getRevision(tenantId, binding.target.deployment_id, binding.target.revision_number).then((revision) => { if (active) setCurrentRevision(revision); }).catch(() => { if (active) setTargetReadError("固定目标已保存，但来源快照暂时读取失败。"); });
    return () => { active = false; };
  }, [tenantId, binding?.target.deployment_id, binding?.target.revision_number, targetEpoch]);
  // One metadata read for this detail's saved target, not one per account in the Channel list.
  useEffect(() => {
    let active = true; setCurrentDeployment(null); setDeploymentReadError("");
    if (!binding) return;
    void deploymentApi.get(tenantId, binding.target.deployment_id).then((deployment) => {
      if (!active) return;
      if (deployment.id !== binding.target.deployment_id || deployment.tenant_id !== tenantId || !deployment.name.trim()) { setDeploymentReadError("固定目标已保存，部署名称暂时读取失败；固定版本和技术标识仍保留。"); return; }
      setCurrentDeployment(deployment);
    }).catch(() => { if (active) setDeploymentReadError("固定目标已保存，部署名称暂时读取失败；固定版本和技术标识仍保留。"); });
    return () => { active = false; };
  }, [tenantId, binding?.target.deployment_id, targetEpoch]);
  const blocked = busy || preparing || !!pending || accepted || !access.canWrite || !recoveryReady;
  const staleConfirmation = !!prepared && !!data && !sameRevision(prepared.snapshot, data);

  async function openAction(action: Action) {
    if (lock.current || !access.canWrite || pending || accepted || !recoveryReady) return;
    lock.current = true; setPreparing(true); setNotice("");
    const snapshot = await refresh();
    if (mounted.current && snapshot) {
      if (action.kind === "binding" && action.enabled && (!snapshot.account.enabled || requiredCredentialPurposes(snapshot.account.provider, snapshot.account.config.receive_mode).some((p) => !snapshot.account.credentials.find((c) => c.purpose === p)?.configured))) setError("请先启用本平台接入并配置全部必需凭据，再开启消息路由。");
      else if (action.kind === "account" && action.enabled && requiredCredentialPurposes(snapshot.account.provider, snapshot.account.config.receive_mode).some((p) => !snapshot.account.credentials.find((c) => c.purpose === p)?.configured)) setError("当前接收方式缺少必需凭据，请先在凭据与设置中补齐，再明确启用。");
      else if (action.kind === "mode" && (snapshot.account.provider !== "telegram" || snapshot.account.enabled || !isTelegramReceiveMode(snapshot.account.config.receive_mode))) setError("请先停用接入并读取明确的接收方式，再保存模式。");
      else if (action.kind === "mode" && action.mode === snapshot.account.config.receive_mode && (action.endpoint ?? "official") === (snapshot.account.config.endpoint_profile ?? "official")) { setModeEditing(false); setNotice("接收方式未变化，本次未提交。"); }
      else if (action.kind === "credential" && !snapshot.account.credentials.find((c) => c.purpose === action.purpose)) setError("此项凭据元数据尚未返回，请重新读取；不使用臆造版本提交。");
      else if (action.kind === "binding" && !snapshot.binding) setError("请先保存固定部署目标。");
      else if (action.kind === "credential" && action.action === "clear" && snapshot.account.enabled) setError("清除凭据前，请先停用本平台接入。");
      else setPrepared({ action, snapshot, readAt: Date.now() });
    }
    lock.current = false; if (mounted.current) setPreparing(false);
  }
  function makePending(p: Prepared): ChannelPending {
    const a = p.action, d = p.snapshot; let operation: string; let input: Record<string, unknown>; let secret = false;
    switch (a.kind) {
      case "mode": operation = "updateAccount"; input = { expected_account_revision: d.account.account_revision, config: { receive_mode: a.mode, ...((a.endpoint === "test" || d.account.config.endpoint_profile === "test") ? {endpoint_profile:a.endpoint} : {}) } }; break;
      case "account": operation = "setAccountEnabled"; input = { expected_account_revision: d.account.account_revision, enabled: a.enabled }; break;
      case "binding": operation = "setBindingEnabled"; input = { expected_binding_revision: d.binding!.binding_revision, enabled: a.enabled }; break;
      case "target": operation = d.binding ? "setBindingTarget" : "createBinding"; input = d.binding ? { expected_binding_revision: d.binding.binding_revision, target: a.target } : { account_id: accountId, target: a.target }; break;
      case "credential": operation = "updateCredential"; input = { purpose: a.purpose, expected_account_revision: d.account.account_revision, expected_credential_version: d.account.credentials.find((c) => c.purpose === a.purpose)?.credential_version ?? 0, action: a.action }; secret = a.action === "replace"; secretValue.current = secret ? a.value ?? "" : ""; break;
    }
    return { operation, key: crypto.randomUUID(), input, secret, createdAt: new Date().toISOString() };
  }
  function persist(p: ChannelPending): boolean {
    try { if (pendingKey && saveChannelPending(sessionStorage, pendingKey, p)) return true; } catch { /* Report a failed preparation, never submit without its recovery marker. */ }
    setStorageError("待确认标记保存失败，本次尚未提交。请恢复浏览器存储后以原请求重试确认。"); return false;
  }
  async function execute(p: ChannelPending, value = secretValue.current): Promise<ChannelCommandResult> {
    const i = p.input, key = p.key; const b = currentData.current?.binding;
    if (p.operation === "setAccountEnabled") return channelApi.setAccountEnabled(tenantId, accountId, { expected_account_revision: Number(i.expected_account_revision), enabled: i.enabled === true }, key);
    if (p.operation === "updateAccount") return channelApi.updateAccount(tenantId, accountId, { expected_account_revision: Number(i.expected_account_revision), ...(typeof i.name === "string" ? { name: i.name } : {}), ...(typeof i.description === "string" ? { description: i.description } : {}), ...(i.config ? { config: i.config as { receive_mode: TelegramReceiveMode; endpoint_profile?: TelegramEndpointProfile } } : {}) }, key);
    if (p.operation === "createBinding") return channelApi.createBinding(tenantId, { account_id: String(i.account_id), target: i.target as ChannelTarget }, key);
    if (p.operation === "setBindingTarget" && b) return channelApi.setBindingTarget(tenantId, b.binding_id, { expected_binding_revision: Number(i.expected_binding_revision), target: i.target as ChannelTarget }, key);
    if (p.operation === "setBindingEnabled" && b) return channelApi.setBindingEnabled(tenantId, b.binding_id, { expected_binding_revision: Number(i.expected_binding_revision), enabled: i.enabled === true }, key);
    if (p.operation === "updateCredential") return channelApi.updateCredential(tenantId, accountId, i.purpose as CredentialPurpose, { expected_account_revision: Number(i.expected_account_revision), expected_credential_version: Number(i.expected_credential_version), ...(i.action === "replace" ? { action: "replace" as const, value } : { action: i.action as "keep" | "clear" }) }, key);
    throw new ChannelApiError(409, "CHANNEL_BINDING_NOT_FOUND", "原操作对象不可见，请重新核对账户。");
  }
  async function submit(p: ChannelPending, value?: string) {
    if (lock.current || !access.canWrite) return;
    if (p.secret && !value && !secretValue.current) { setError("请重新输入原凭据值，以原请求标识确认；不要输入新值。"); return; }
    if (!persist(p)) { setPending(p); setPrepared(null); setCredentialValue(""); return; }
    lock.current = true; setBusy(true); setError(""); setStorageError(""); setPending(p); setPrepared(null); setCredentialValue("");
    if (value !== undefined) secretValue.current = value;
    try {
      await execute(p);
      if (!mounted.current) return;
      secretValue.current = ""; setRetryValue(""); setCredentialPurpose(null); setMetaEditing(false); setModeEditing(false); setPending(null); setPendingBlocked(false);
      try { clearChannelPending(sessionStorage, pendingKey); } catch { /* No write is automatically replayed on load. */ }
      setAccepted(true); setNotice("操作已接受，正在读取最新事实…");
      const fresh = await refresh();
      if (mounted.current) { if (fresh) setNotice(p.operation === "updateAccount" ? p.input.config ? "接收方式已保存，账户仍保持停用。凭据与路由意图保留；请检查接入条件，再单独确认启用。" : "名称与描述已保存。" : "配置已保存。接入意图、路由分发和连接观测分别更新；这不表示 Agent 已运行或已回复。"); else { setAccepted(true); setNotice("操作已接受，最新状态读取失败。请仅重试读取，不要再次提交。"); } }
    } catch (e) {
      if (!mounted.current) return;
      const reenterOriginal = p.secret && e instanceof ChannelApiError && ["CHANNEL_IDEMPOTENCY_CONFLICT", "CHANNEL_INPUT_INVALID"].includes(e.code);
      if (reenterOriginal) { secretValue.current = ""; setRetryValue(""); }
      setPendingBlocked(!isChannelUncertain(e) && !reenterOriginal); setError(channelError(e));
      if (e instanceof ChannelApiError && [401, 403].includes(e.status)) access.denyWrites();
      setNotice(reenterOriginal ? "本次原请求重放被拒绝。请重新输入最初的凭据值，继续使用原请求标识；原请求结果仍待确认。" : isChannelUncertain(e) ? "操作结果待确认。重试会使用原请求标识、原始版本和原始内容。" : "请求被拒绝或发生冲突。保留你的输入，请先读取当前事实，再明确结束原请求并重新准备。");
    } finally { lock.current = false; if (mounted.current) setBusy(false); }
  }
  async function finishPending() {
    if (lock.current) return; lock.current = true; setPreparing(true);
    const fresh = await refresh();
    if (fresh && mounted.current) { try { clearChannelPending(sessionStorage, pendingKey); } catch { /* Explicit ending is kept in this page too. */ } setPending(null); setPendingBlocked(false); secretValue.current = ""; setRetryValue(""); if (pending?.operation === "updateAccount" && pending.input.config) {
        setNextMode((pending.input.config as { receive_mode: TelegramReceiveMode; endpoint_profile?: TelegramEndpointProfile }).receive_mode); setNextEndpoint((pending.input.config as { endpoint_profile?: TelegramEndpointProfile }).endpoint_profile ?? fresh.account.config.endpoint_profile ?? "official"); setModeEditing(true);
        setNotice("原模式选择已保留。请核对最新状态，再单独确认保存；本次未提交，也未启用。");
      } else if (pending?.operation === "updateAccount") {
        setMetadataRevision(fresh.account.account_revision); setName(typeof pending.input.name === "string" ? pending.input.name : fresh.account.name); setDescription(typeof pending.input.description === "string" ? pending.input.description : fresh.account.description); setMetaEditing(true);
        setNotice("已按最新账户版本重新准备。你的名称与描述已保留，请核对后点击保存；本次尚未提交。");
      } else setNotice("已结束原请求。当前状态不证明原凭据值已提交；如需修改，请重新准备并确认一次新操作。"); }
    lock.current = false; if (mounted.current) setPreparing(false);
  }
  function editMetadata() { if (!data) return; setName(data.account.name); setDescription(data.account.description); setMetadataRevision(data.account.account_revision); setMetaEditing(true); }
  function saveMetadata() {
    if (blocked || lock.current || !metaEditing || metadataRevision === null) return;
    if (!name.trim() || Array.from(name.trim()).length > 128 || Array.from(description).length > 4096) { setError("名称须为 1–128 个字符，描述最多 4096 个字符。"); return; }
    // Keep the edit baseline: a background refresh must not silently authorize overwriting another owner's changes.
    void submit({ operation: "updateAccount", key: crypto.randomUUID(), input: { expected_account_revision: metadataRevision, name: name.trim(), description }, secret: false, createdAt: new Date().toISOString() });
  }
  const root = `/tenants/${encodeURIComponent(tenantId)}`;
  return <div className={styles.page}>
    <PageHeader eyebrow="CHANNEL WORKSPACE" title={data?.account.name ?? "渠道账户"} description="固定接入身份与部署目标；接入和新消息路由分别管理。" action={<Link className="button secondary" href={channelHref(tenantId)}>返回渠道列表</Link>} />
    {invalidPreflightQuery && <div className={styles.warning} role="alert">预检分享链接无效：请使用单个任务标识。此链接未发起检查，也未改变运行目标。</div>}
    {(error || access.error) && <div className={styles.error} role="alert">{error || access.error}<div className={styles.actions}><Button variant="secondary" disabled={busy || preparing} onClick={() => { void refresh(); if (access.error) access.reload(); }}>重新读取账户状态</Button>{!access.canWrite && <Link className={styles.link} href="/login">重新登录</Link>}</div></div>}
    {notice && <div className={accepted || pending ? styles.warning : styles.notice} role="status">{notice}</div>}
    {storageError && <div className={styles.warning} role="alert">{storageError}{!recoveryReady && <Button variant="secondary" onClick={readRecovery}>重试读取恢复记录</Button>}</div>}
    {access.loading && <p role="status">正在读取渠道权限…</p>}
    {!access.loading && !access.canWrite && !access.error && <div className={styles.notice}>当前为只读视图。渠道写操作由租户 OWNER 管理。</div>}
    {pending && <section className={styles.warning} aria-label="待确认渠道操作"><h2>上次操作待确认</h2><p>{pending.operation} · {time(pending.createdAt)}</p><p>最新 GET 只能显示当前事实，不证明原请求是否成功，尤其不能证明某个凭据值已被接受。</p>{pending.secret && !secretValue.current && <label className={styles.field}><span>重新输入原凭据值</span><input aria-label="重新输入原凭据值" type="password" autoComplete="off" value={retryValue} onChange={(e) => setRetryValue(e.target.value)} /><small>只用于原 key、原版本、原内容重放，不自动改用新 key。</small></label>}<div className={styles.actions}><Button disabled={busy || preparing || pendingBlocked || !access.canWrite || !data || (pending.secret && !secretValue.current && !retryValue)} onClick={() => void submit(pending, pending.secret && !secretValue.current ? retryValue : undefined)}>以原请求重试确认</Button><Button variant="secondary" disabled={busy || preparing} onClick={() => void refresh()}>只读取当前事实</Button><Button variant="secondary" disabled={busy || preparing} onClick={() => void finishPending()}>已核实状态，结束原请求并重新准备</Button></div></section>}
    {!data ? !error && <p role="status">正在读取渠道账户…</p> : <>
      <ol className={styles.steps} aria-label="渠道接入步骤">{[[true, "保存渠道账户"], [!!binding, "固定部署目标"], [data.account.enabled, "启用本平台接入"], [!!binding?.enabled, "开启消息路由"]].map(([done, label], i) => <li key={String(label)} className={done ? styles.done : ""}><span>{done ? "✓" : i + 1}</span>{label}</li>)}</ol>
      <div className={styles.grid}><section className={styles.card}><div className={styles.row}><h2>账户接入</h2><StatusBadge tone={data.account.enabled ? "blue" : "gray"}>{data.account.enabled ? "接入配置已启用" : "接入配置已停用"}</StatusBadge></div><p className={styles.hint}>{data.account.provider === "telegram" ? "Telegram" : "企业微信"} · {data.account.provider_account_id}</p>{access.canWrite && <Button variant={data.account.enabled ? "secondary" : "primary"} disabled={blocked} onClick={() => void openAction({ kind: "account", enabled: !data.account.enabled })}>{data.account.enabled ? "停用本平台接入" : "启用本平台接入"}</Button>}</section>
      <section className={styles.card}><div className={styles.row}><h2>新消息路由</h2><StatusBadge tone={binding?.enabled ? "blue" : "gray"}>{binding?.enabled ? "路由开启意图已保存" : binding ? "路由已暂停" : "尚未配置目标"}</StatusBadge></div><p className={styles.hint}>{targetName(data, currentDeployment)}</p>{access.canWrite && (binding ? <Button variant={binding.enabled ? "secondary" : "primary"} disabled={blocked || (!binding.enabled && !data.account.enabled)} onClick={() => void openAction({ kind: "binding", enabled: !binding.enabled })}>{binding.enabled ? "暂停消息路由" : "开启消息路由"}</Button> : <Button variant="secondary" onClick={() => setTab("target")}>配置固定目标</Button>)}{binding && !binding.enabled && !data.account.enabled && <p className={styles.hint}>开启消息路由前，请先启用本平台接入。</p>}{!data.account.enabled && binding?.enabled && <p className={styles.warning}>接入已停用，但路由开启意图保留。重新启用账户可能同时恢复服务端当时保存的目标。</p>}</section></div>
      {tab !== "diagnostics" && <div className={styles.notice}><div className={styles.row}><div><strong>{data.account.provider === "wecom" ? "启用前先诊断连接认证" : "启用前先检查接入条件"}</strong><p className={styles.hint}>{data.account.provider === "wecom" ? "企微诊断会短时连接并认证，可能替换同一 Bot 的其他客户端；需 OWNER 在账户停用后明确确认影响。" : "预检只读取 Telegram 信息，不修改 Webhook，也不要求先配置运行目标。"}</p></div><Button variant="secondary" onClick={() => setTab("diagnostics")}>{data.account.provider === "wecom" ? "查看企微接入诊断" : "查看接入预检"}</Button></div></div>}
      <div className={styles.row}><div className={styles.tabs} role="tablist" aria-label="渠道账户详情">{[["overview", "概览"], ["target", "运行目标"], ["settings", "凭据与设置"], ["diagnostics", "连接诊断"]].map(([id, label]) => <button role="tab" type="button" aria-selected={tab === id} aria-controls={`channel-${id}`} key={id} onClick={() => setTab(id)}>{label}</button>)}</div><span className={styles.hint}>{refreshing ? "正在刷新…" : `最近读取 ${time(readAt)}`}</span></div>
      {tab === "overview" && <section className={styles.card} role="tabpanel" id="channel-overview" aria-label="概览"><h2>账户身份与配置</h2><AccountFacts data={data} /><p className={styles.hint}>{data.account.description || "暂无说明"}</p><div className={styles.notice}>保存或启用配置不等于机器人在线。事件分发、Gateway 路由应用与连接观测请分别查看「连接诊断」。</div><div className={styles.actions}><Button variant="secondary" onClick={() => setTab("target")}>查看运行目标</Button><Button variant="secondary" onClick={() => setTab("diagnostics")}>查看连接诊断</Button></div></section>}
      {tab === "target" && <section className={styles.card} role="tabpanel" id="channel-target" aria-label="运行目标"><h2>固定部署目标</h2>{binding ? <div className={styles.notice}><strong>{targetName(data, currentDeployment)}</strong><details><summary>部署技术标识</summary><dl className={styles.facts}><dt>部署 ID</dt><dd className={styles.mono}>{binding.target.deployment_id}</dd><dt>部署版本 ID</dt><dd className={styles.mono}>{binding.target.deployment_revision_id}</dd><dt>Binding ID</dt><dd className={styles.mono}>{binding.binding_id}</dd></dl></details><Link className={styles.link} href={`${root}/deployments/${encodeURIComponent(binding.target.deployment_id)}/revisions/${binding.target.revision_number}`}>查看当前部署快照 →</Link>{currentRevision && <p>Agent v{currentRevision.agent_version_number} / Profile r{currentRevision.profile_revision_number}</p>}<p className={styles.hint}>切换目标影响传播后的新消息接纳，已有 Run 保留原快照。回退旧部署也是一次新版本更新。</p></div> : <p className={styles.hint}>此账户还没有 Binding。可以在账户停用时先固定部署目标；保存后消息路由仍为暂停。</p>}{(targetReadError || deploymentReadError) && <p className={styles.warning}>{[deploymentReadError, targetReadError].filter(Boolean).join(" ")} <Button variant="secondary" onClick={() => setTargetEpoch((n) => n + 1)}>重试读取当前来源</Button></p>}
        {access.canWrite && <div className={styles.stack}>{invalidTarget && <p className={styles.warning}>接入链接的部署或版本参数无效／重复，已忽略预选。请手动选择固定目标。</p>}<ChannelTargetSelector tenantId={tenantId} initialTarget={initialTarget} disabled={blocked} onChange={onTargetChange} /><div className={styles.actions}><Button disabled={blocked || !target || !selectionRevision || (binding?.target.deployment_id === target.deployment_id && binding?.target.revision_number === target.revision_number)} onClick={() => target && void openAction({ kind: "target", target })}>{binding ? "切换固定目标" : "保存固定目标"}</Button></div></div>}
      </section>}
      {tab === "settings" && <div role="tabpanel" id="channel-settings" aria-label="凭据与设置" className={styles.stack}><section className={styles.card}><div className={styles.row}><h2>基本信息</h2>{access.canWrite && !metaEditing && <Button variant="secondary" disabled={blocked} onClick={editMetadata}>编辑名称与描述</Button>}</div>{metaEditing ? <div className={styles.stack}><label className={styles.field}><span>渠道名称</span><input value={name} onChange={(e) => setName(e.target.value)} disabled={blocked} /></label><label className={styles.field}><span>渠道描述</span><textarea value={description} onChange={(e) => setDescription(e.target.value)} disabled={blocked} /></label><div className={styles.actions}><Button disabled={blocked} onClick={saveMetadata}>保存基本信息</Button><Button variant="secondary" disabled={busy} onClick={() => setMetaEditing(false)}>取消编辑</Button></div></div> : <p>{data.account.name} · {data.account.description || "暂无描述"}</p>}<h3>只读接入配置</h3><AccountFacts data={data} /><p className={styles.hint}>Provider、物理身份和生成字段创建后固定。基本信息仅发送名称、描述和账户版本；接收方式通过下方独立操作修改。</p></section>
      {data.account.provider === "telegram" && <section className={styles.card}><div className={styles.row}><h2>接收方式</h2><StatusBadge tone="gray">{receiveModeLabel(data.account.config.receive_mode)}</StatusBadge></div><p className={styles.hint}>只更改期望配置；保存不会访问 Telegram、启动接收或清空积压。旧接收停止由 Gateway 在下一次启用时协调，不由页面推定完成。</p>{data.account.config.receive_mode === "webhook" && !data.account.credentials.find((c) => c.purpose === "telegram.webhook_secret")?.configured && <p className={styles.warning}>Webhook Secret 尚未配置。模式可保存在停用状态，启用前请补齐。</p>}{access.canWrite && (data.account.enabled ? <><p className={styles.hint}>更改前请先停用接入。路由开启意图保留；不会自动重新启用。</p><Button variant="secondary" disabled={blocked} onClick={() => void openAction({ kind: "account", enabled: false })}>先停用接入</Button></> : modeEditing ? <div className={styles.stack}><label>接入环境<select value={nextEndpoint} onChange={(e) => setNextEndpoint(e.target.value === "test" ? "test" : "official")}><option value="official">官方 Telegram</option><option value="test">测试 Telegram（Channel Lab）</option></select></label><ReceiveModeSelector value={nextMode} onChange={setNextMode} disabled={blocked} /><div className={styles.actions}><Button disabled={blocked || (nextMode === data.account.config.receive_mode && nextEndpoint === (data.account.config.endpoint_profile ?? "official"))} onClick={() => void openAction({ kind: "mode", mode: nextMode, endpoint:nextEndpoint })}>检查并保存接收方式</Button><Button variant="secondary" disabled={busy} onClick={() => setModeEditing(false)}>取消更改接收方式</Button></div></div> : <Button variant="secondary" disabled={blocked || !isTelegramReceiveMode(data.account.config.receive_mode)} onClick={() => { if (isTelegramReceiveMode(data.account.config.receive_mode)) { setNextMode(data.account.config.receive_mode); setNextEndpoint(data.account.config.endpoint_profile ?? "official"); setModeEditing(true); } }}>更改接收方式</Button>)}</section>}
      <section className={styles.card}><h2>凭据</h2><p className={styles.hint}>只显示配置状态与版本，不读取秘密值。逐项更新不是原子批量；在线替换可能触发重连，清除前须先停用接入。</p><div className={styles.stack}>{credentialPurposes(data.account.provider).map((purpose) => { const c = data.account.credentials.find((c) => c.purpose === purpose); return <div className={styles.credential} key={purpose}><div className={styles.row}><div><strong>{purposeLabel[purpose]}</strong>{purpose === "telegram.webhook_secret" && data.account.config.receive_mode === "long_polling" && <p className={styles.hint}>可选；已保存的值保留，当前模式不需要。</p>}<p className={styles.mono}>{purpose} · v{c?.credential_version ?? "未返回"}</p></div><StatusBadge tone={c?.configured ? "blue" : "gray"}>{c?.configured ? "已配置" : "未配置"}</StatusBadge></div>{access.canWrite && <><div className={styles.actions}><Button variant="secondary" disabled={blocked || !c} onClick={() => { setCredentialPurpose(purpose); setCredentialValue(""); }}>替换 {purposeLabel[purpose]}</Button><Button variant="ghost" disabled={blocked || data.account.enabled || !c?.configured} onClick={() => void openAction({ kind: "credential", purpose, action: "clear" })}>清除 {purposeLabel[purpose]}</Button></div>{credentialPurpose === purpose && <div className={styles.stack}><label className={styles.field}><span>新的 {purposeLabel[purpose]}</span><input aria-label={`新的 ${purposeLabel[purpose]}`} type="password" autoComplete="off" value={credentialValue} disabled={blocked} onChange={(e) => setCredentialValue(e.target.value)} /><small>秘密仅保留本页内存，不保存到浏览器恢复记录。</small></label><div className={styles.actions}><Button disabled={blocked} onClick={() => { const invalid = validateCredential(purpose, credentialValue); if (invalid) setError(invalid); else void openAction({ kind: "credential", purpose, action: "replace", value: credentialValue }); }}>确认替换 {purposeLabel[purpose]}</Button><Button variant="secondary" disabled={busy} onClick={() => { setCredentialPurpose(null); setCredentialValue(""); }}>取消凭据修改</Button></div></div>}</>}</div>; })}</div></section></div>}
      {tab === "diagnostics" && <ChannelPreflightPanel key={JSON.stringify([access.userId, tenantId, accountId, initialPreflightId])} account={data.account} userId={access.userId} canWrite={access.canWrite} denyWrites={access.denyWrites} initialPreflightId={initialPreflightId} blocked={busy || preparing || !!pending || accepted || (access.canWrite && !recoveryReady)} />}
      {tab === "diagnostics" && <section className={styles.card} role="tabpanel" id="channel-diagnostics" aria-label="连接诊断"><h2>状态与连接观测</h2><div className={styles.grid}><div className={styles.notice}><h3>路由事件分发</h3><strong>{distributionLabel[data.distribution] ?? `未知状态：${data.distribution}`}</strong><p className={styles.hint}>PUBLISHED 仅确认消息流发布，不代表 Gateway 已应用。</p></div><div className={styles.warning}><h3>Gateway 路由应用</h3><strong>尚无路由应用确认</strong><p className={styles.hint}>当前接口返回 {data.gateway_application}；不根据连接 READY 或事件发布推断路由已应用。</p></div></div><h3>逐实例连接观测</h3><p className={styles.hint}>接收时间属于连接观测，不是最近用户消息时间。使用服务端 effective_state 判断过期，不按最大 owner_epoch 自行选主。</p>{!data.observations.length ? <p className={styles.notice}>尚未收到连接观测</p> : data.observations.map((o) => <div className={styles.observation} key={`${o.instance_id}:${o.instance_epoch}`}><div className={styles.row}><strong>{o.instance_id}</strong><StatusBadge tone={o.effective_state === "READY" && o.connection_revision === data.account.connection_revision && (data.account.provider !== "telegram" || o.receive_mode === data.account.config.receive_mode) ? "blue" : ["ERROR", "STALE"].includes(o.effective_state) ? "amber" : "gray"}>{observationLabel[o.effective_state] ?? `未知观测：${o.effective_state}`}</StatusBadge></div>{(o.connection_revision !== data.account.connection_revision || data.account.provider === "telegram" && o.receive_mode !== undefined && o.receive_mode !== data.account.config.receive_mode) && <p className={styles.warning}>历史连接观测（非当前配置），不代表当前接收已就绪。</p>}<p className={styles.hint}>最近观测接收时间：{time(o.received_at)}<br />报告接收方式：{receiveModeLabel(o.receive_mode)}<br />原因：{observationReason[o.reason_code] ?? (o.reason_code || "未提供")} · connection_revision={o.connection_revision}</p><details><summary>实例技术字段</summary><dl className={styles.facts}><dt>实例 epoch</dt><dd>{o.instance_epoch}</dd><dt>序列</dt><dd>{o.report_sequence}</dd><dt>原始观测</dt><dd>{o.state} / {time(o.observed_at)}</dd>{o.owner_epoch !== undefined && <><dt>Owner epoch</dt><dd>{o.owner_epoch}</dd></>}</dl></details></div>)}<details><summary>路由与版本技术信息</summary><dl className={styles.facts}><dt>账户版本</dt><dd>{data.account.account_revision}</dd><dt>连接版本</dt><dd>{data.account.connection_revision}</dd><dt>部署 ID</dt><dd>{binding?.target.deployment_id ?? "无"}</dd><dt>固定部署版本</dt><dd>{binding ? `r${binding.target.revision_number}` : "无"}</dd><dt>Binding 版本</dt><dd>{binding?.binding_revision ?? "无"}</dd><dt>Route generation</dt><dd>{data.route_generation}</dd><dt>Route event ID</dt><dd>{data.route_event_id ?? "无"}</dd><dt>Manifest ref</dt><dd>{binding?.target.manifest_ref ?? "无"}</dd><dt>Manifest digest</dt><dd>{binding?.target.manifest_digest ?? "无"}</dd></dl></details></section>}
    </>}
    {prepared && <ChannelDialog title={actionTitle(prepared.action)} busy={busy} disabled={staleConfirmation || !access.canWrite} confirmLabel="确认此操作" onClose={() => setPrepared(null)} onConfirm={() => { if (!lock.current && !staleConfirmation) void submit(makePending(prepared)); }}><ActionImpact prepared={prepared} deployment={currentDeployment} />{staleConfirmation && <div className={styles.warning}>读取到了较新的账户或 Binding 版本，请重新核对后确认。<Button variant="secondary" onClick={() => void openAction(prepared.action)}>重新读取并核对影响</Button></div>}</ChannelDialog>}
  </div>;
}
function AccountFacts({ data }: { data: ChannelAccountDetails }) { const a = data.account; return <dl className={styles.facts}><dt>Provider / 物理身份</dt><dd>{a.provider} / {a.provider_account_id}</dd><dt>账户 ID</dt><dd className={styles.mono}>{a.account_id}</dd>{a.provider === "telegram" && <><dt>接入环境</dt><dd>{a.config.endpoint_profile === "test" ? "测试 Telegram（Channel Lab）" : "官方 Telegram"}</dd><dt>已保存接收方式</dt><dd>{receiveModeLabel(a.config.receive_mode)} · 连接版本 {a.connection_revision}</dd></>}<dt>{a.provider === "telegram" ? a.config.receive_mode === "long_polling" ? "Webhook 路径（预留，当前不适用）" : "Webhook 路径（只读）" : "Bot ID（只读）"}</dt><dd className={styles.mono}>{a.provider === "telegram" ? a.config.webhook_path ?? "未返回" : a.config.bot_id ?? "未返回"}</dd></dl>; }
function actionTitle(a: Action) { return a.kind === "mode" ? "保存接收方式？" : a.kind === "account" ? a.enabled ? "启用本平台接入？" : "停用本平台接入？" : a.kind === "binding" ? a.enabled ? "开启消息路由？" : "暂停消息路由？" : a.kind === "target" ? "保存固定部署目标？" : a.action === "replace" ? "替换此项凭据？" : "清除此项凭据？"; }
function ActionImpact({ prepared: p, deployment }: { prepared: Prepared; deployment: Deployment | null }) {
  const a = p.action, d = p.snapshot; return <div className={styles.stack}><dl className={styles.facts}><dt>账户</dt><dd>{d.account.name} · {d.account.provider} / {d.account.provider_account_id}</dd><dt>账户 / Binding 版本</dt><dd>{d.account.account_revision} / {d.binding?.binding_revision ?? "无"}</dd><dt>最近读取目标</dt><dd>{targetName(d, deployment)}</dd>{d.binding && <><dt>部署 ID</dt><dd className={styles.mono}>{d.binding.target.deployment_id}</dd></>}<dt>最近读取路由意图</dt><dd>{d.binding ? d.binding.enabled ? "开启" : "暂停" : "未配置"}</dd><dt>读取时间</dt><dd>{time(p.readAt)}</dd></dl>
    {a.kind === "mode" && <><p>接入环境：{a.endpoint === "test" ? "测试 Telegram（Channel Lab）" : "官方 Telegram"}。切换环境后请配置对应 Token；现有凭据不会自动替换。</p><div className={styles.notice}>{receiveModeLabel(d.account.config.receive_mode)} → {receiveModeLabel(a.mode)} · 连接版本 {d.account.connection_revision}</div><p>本次只保存期望接收方式，账户仍停用；保留 Bot Token、Webhook Secret、积压与路由意图。不会注册或删除远端 Webhook，也不会自动启用。</p>{a.mode === "webhook" && !d.account.credentials.find((c) => c.purpose === "telegram.webhook_secret")?.configured && <p className={styles.warning}>保存后需补齐 Webhook Secret，才可单独确认启用。</p>}<p>之后启用会由 Gateway 等待旧接收结束，再协调新模式。未知外部 Webhook 保持冲突，不提供盲目接管。</p></>}
    {a.kind === "account" && <><p>{a.enabled ? "将启用账户接入配置，等待 Gateway 同步与认证。" : "将保存账户停用配置；Gateway 应用后停止此账户接入与新消息接纳。账户、凭据和固定目标保留。"}</p><div className={styles.warning}>{d.binding?.enabled ? "消息路由的开启意图将保留。重新启用接入时，若意图仍开启，会恢复服务端当时保存的目标。" : d.binding ? "路由当前已暂停；账户命令本身不会替你开启路由。" : "当前未配置消息路由。"}<p>最近读取快照不是目标锁。其他 OWNER 修改 Binding 不推进账户版本，提交时以服务端保存的 Binding 状态为准，不保证恢复本框显示的旧目标。</p></div>{d.account.provider === "telegram" && <p>{a.enabled ? (d.account.config.receive_mode === "long_polling" ? "启用长轮询会由 Gateway 主动接收更新；仅协调平台已管理或空 Webhook，必要时移除平台管理的 Webhook。未知外部 Webhook 保持冲突，不盲目接管。" : "Telegram 启用会触发身份核对和 setWebhook 注册；仅协调平台已管理或空 Webhook，未知外部 Webhook 保持冲突，不盲目接管。") : "仅停用本平台接入，不承诺删除 Telegram 远端 Webhook。后续回复仍受账户发送资格约束。"}</p>}</>}
    {a.kind === "account" && a.enabled && d.account.provider === "telegram" && <p>接收方式：{receiveModeLabel(d.account.config.receive_mode)} · 连接版本 {d.account.connection_revision}。Gateway 先确认旧接收路径停止，再启动新路径；平台不主动丢弃积压（Telegram 保留期最长 24 小时）。启用成功仅表示意图已保存，不证明收到消息或 Agent 已回复。</p>}
    {a.kind === "binding" && <p>{a.enabled ? `将开启新消息路由到 ${targetName(d, deployment)}；以 Binding revision ${d.binding?.binding_revision} 校验，目标变化会返回冲突。此操作不自动运行一条消息，也不把 READY 作为额外启用门槛。` : "将把此 Binding 的路由意图设为暂停。账户接入配置、凭据和固定目标保持不变。保留目标不等于保留开启意图，恢复需再次显式开启消息路由。"}</p>}
    {a.kind === "target" && <><div className={styles.notice}>当前：{targetName(d, deployment)}<br />待保存：{a.target.deployment_id} · r{a.target.revision_number}</div><p>{d.binding ? "切换或回退目标都会提交新的 Binding 版本；保留当前路由启停意图。" : "首次保存将创建暂停状态的 Binding；不会自动启用账户或消息路由。"}</p></>}
    {a.kind === "credential" && <p>{purposeLabel[a.purpose]} · 原凭据版本 {d.account.credentials.find((c) => c.purpose === a.purpose)?.credential_version}<br />{a.action === "replace" ? "只替换此项凭据；即使新值相同也可能推进版本，在线更新可能触发重连。多项凭据需逐项确认，不是原子批量。" : "账户已停用后才允许清除此项凭据；重新启用前需补齐必需凭据。"}<br />凭据值不在确认框或恢复存储中展示。</p>}
    <p className={styles.hint}>配置异步传播，不承诺各 Gateway 同时生效；不取消或重定向已经接纳的 Run。</p>
  </div>;
}
