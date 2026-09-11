"use client";
import { useEffect, useRef, useState } from "react";
import { receiveModeLabel, type ChannelAccount } from "../../lib/channel-api";
import { channelPreflightApi, ChannelPreflightApiError, isPreflightUncertain, preflightError, preflightResultHref, PREFLIGHT_CHECK_IDS, WECOM_PREFLIGHT_CHECK_IDS, PREFLIGHT_STORAGE_PREFIX, validPreflightId, validPreflightInput, type ChannelPreflightInput, type ChannelPreflightResult, type PreflightCheckId } from "../../lib/channel-preflight-api";
import { ChannelDialog } from "./confirmation-dialog";
import styles from "./preflight-panel.module.css";

type Props = { account: ChannelAccount; userId: string; canWrite: boolean; denyWrites: () => void; initialPreflightId?: string; blocked?: boolean };
type Pending = { kind: "pending"; key: string; input: ChannelPreflightInput; createdAt: string };
type Task = { kind: "task"; preflightId: string; deadlineAt: string; createdAt: string };
type Recovery = Pending | Task;
const labels: Record<PreflightCheckId, string> = { credential_configuration: "凭据配置", bot_identity: "机器人身份", connection_authentication: "企微连接认证", public_origin: "公开入口", webhook_registration: "现有 Webhook", pending_updates: "积压消息", delivery_errors: "历史投递错误", recovery_materials: "原 Webhook 恢复资料", delivery_verification: "Telegram 实际投递" };
const statusLabels = { PASS: "通过", WARN: "留意", FAIL: "失败", UNKNOWN: "未知 / 未验证", SKIPPED: "未执行", NOT_APPLICABLE: "不适用" };
const codeMessages: Record<string, string> = {
  PUBLIC_ORIGIN_NOT_APPLICABLE: "长轮询无需公网入站回调；此项不适用，不代表已验证公网可达。",
  DELIVERY_ERRORS_NOT_APPLICABLE: "Webhook 历史投递错误不用于判断当前长轮询健康；此项不适用。",
  RECOVERY_MATERIALS_NOT_APPLICABLE: "长轮询不使用 Webhook 恢复资料；这不表示可以还原未知外部 Webhook。",
  WEBHOOK_BLOCKS_LONG_POLLING: "现有 Webhook 阻挡长轮询。预检没有删除或接管它；启用仅协调平台已管理的入口，未知外部配置保持冲突。",
  CREDENTIALS_CONFIGURED: "已保存所需凭据；实际可用性由身份检查确认。", BOT_TOKEN_MISSING: "尚未配置 Bot Token，请先更新该凭据。", WEBHOOK_SECRET_MISSING: "尚未配置 Webhook Secret，请先更新该凭据。",
  BOT_IDENTITY_MATCH: "Token 对应的机器人与保存的物理身份一致。", BOT_IDENTITY_MISMATCH: "Token 对应另一机器人，请核对保存的 Bot ID 和 Token。", TOKEN_REJECTED: "Telegram 拒绝了当前 Token，请核对或更新凭据。",
  PROVIDER_NETWORK: "连接 Telegram 失败，请检查 Gateway 出网网络。", PROVIDER_TIMEOUT: "Telegram 请求超时，尚未获得确定结果。", PROVIDER_RATE_LIMITED: "Telegram 限流，请稍后重新检查。", PROVIDER_UNAVAILABLE: "Telegram 服务暂时异常。", PROVIDER_RESPONSE_INVALID: "Telegram 响应格式异常，未采信为成功。", NOT_EXECUTED: "前置检查未通过，此项没有执行。",
  PUBLIC_ORIGIN_STATIC_VALID: "公开入口通过静态格式检查；不代表公网可达。", PUBLIC_ORIGIN_INVALID: "公开入口格式不符合要求，请检查 Gateway 平台配置。", PUBLIC_ORIGIN_NOT_PUBLIC: "公开入口是本地或非公开地址，请配置公开 HTTPS 入口。",
  WEBHOOK_MATCH: "当前 Webhook 与本账户预期入口一致；本次未修改注册。", WEBHOOK_DIFFERENT: "已有 Webhook 指向其他入口，请核实后再决定是否接入；本次未接管。", WEBHOOK_NONE: "当前没有注册 Webhook；预检不会自动注册。", WEBHOOK_COMPARISON_UNAVAILABLE: "已存在 Webhook，但预期入口无效，未作相同或不同的推断。",
  PENDING_UPDATES_ZERO: "没有报告积压消息，不代表已经收到测试消息。", PENDING_UPDATES_PRESENT: "Telegram 报告有待投递更新；这不是 Poller 内部队列或游标落后量，也不会主动清空积压。",
  DELIVERY_ERROR_NOT_REPORTED: "没有报告近期投递错误，不代表实际投递已验证。", DELIVERY_ERROR_REPORTED: "Telegram 报告历史投递错误；不据此推断当前服务持续故障。",
  RECOVERY_MATERIALS_UNAVAILABLE: "旧 secret_token 不可回读，本结果不是完整恢复资料。", DELIVERY_NOT_TESTED: "本次没有发送测试消息，也没有验证 Telegram 实际投递。",
};
const wecomMessages: Record<string, string> = {
  CREDENTIALS_CONFIGURED: "Bot Secret 已保存；是否被企微接受由连接认证单独判断。",
  BOT_SECRET_MISSING: "尚未配置 Bot Secret，本次没有执行连接认证。",
  WECOM_AUTHENTICATED: "企微接受了本次短连接的认证；不代表持续在线、收到真实消息或 Agent 已回复。",
  WECOM_AUTH_REJECTED: "企微拒绝了本次连接认证，请核对 Bot ID 与 Bot Secret。",
  WECOM_CONNECTION_REPLACED: "诊断连接被同一 Bot 的其他连接替换，认证结果未确认；请先协调其他客户端。",
  PROVIDER_NETWORK: "连接企微失败，请检查 Gateway 出网网络。",
  PROVIDER_TIMEOUT: "企微连接认证超时，尚未获得确定结果。",
  PROVIDER_RESPONSE_INVALID: "企微响应格式异常，没有采信为认证成功。",
  PROVIDER_UNAVAILABLE: "企微连接服务暂时异常，认证结果未确认。",
  DELIVERY_NOT_TESTED: "本次不验证真实消息到达或 Agent 回复。诊断期间收到的业务消息会被丢弃，不产生 Run。",
};
const terminal = (r: ChannelPreflightResult) => r.state !== "QUEUED" && r.state !== "RUNNING";
const validDate = (v: unknown): v is string => typeof v === "string" && v.length <= 40 && Number.isFinite(Date.parse(v));
function recovery(raw: string): Recovery | null {
  if (raw.length > 2048) return null;
  let v: unknown; try { v = JSON.parse(raw); } catch { return null; }
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const r = v as Record<string, unknown>; const keys = Object.keys(r).sort().join(",");
  if (r.kind === "pending" && keys === "createdAt,input,key,kind" && typeof r.key === "string" && /^[\x21-\x7e]{1,128}$/.test(r.key) && validPreflightInput(r.input) && validDate(r.createdAt)) return r as Pending;
  if (r.kind === "task" && keys === "createdAt,deadlineAt,kind,preflightId" && validPreflightId(r.preflightId) && validDate(r.deadlineAt) && validDate(r.createdAt)) return r as Task;
  return null;
}
export function ChannelPreflightPanel(props: Props) {
  return <PreflightPanel key={[props.userId, props.account.tenant_id, props.account.account_id, props.account.provider, props.initialPreflightId ?? ""].join("|")} {...props} />;
}
function PreflightPanel({ account, userId, canWrite, denyWrites, initialPreflightId, blocked = false }: Props) {
  const isWeCom = account.provider === "wecom";
  const [confirmation, setConfirmation] = useState<{ input: ChannelPreflightInput; currentInput: string; original?: Pending } | null>(null);
  const [consent, setConsent] = useState(false);
  const storageKey = PREFLIGHT_STORAGE_PREFIX + [userId, account.tenant_id, account.account_id].map(encodeURIComponent).join(":");
  const [ready, setReady] = useState(false); const [marker, setMarker] = useState<Recovery | null>(null);
  const [taskId, setTaskId] = useState(""); const [data, setData] = useState<ChannelPreflightResult | null>(null);
  const [error, setError] = useState(""); const [storageError, setStorageError] = useState(""); const [postError, setPostError] = useState("");
  const [readEpoch, setReadEpoch] = useState(0); const [busy, setBusy] = useState(false); const [reading, setReading] = useState(false); const [stopped, setStopped] = useState(false);
  const [writeDenied, setWriteDenied] = useState(false); const [readDenied, setReadDenied] = useState(false); const [sessionExpired, setSessionExpired] = useState(false);
  const [pendingRejected, setPendingRejected] = useState(false); const [now, setNow] = useState(Date.now());
  const mounted = useRef(true); const writeLock = useRef(false); const postController = useRef<AbortController | null>(null);
  const markerRef = useRef<Recovery | null>(null); const denial = useRef(denyWrites); denial.current = denyWrites;
  const pollBudget = useRef<{ id: string; until: number; deadline: number }>({ id: "", until: 0, deadline: Infinity });
  const invalidLink = initialPreflightId !== undefined && !validPreflightId(initialPreflightId);
  const remember = (value: Recovery) => {
    markerRef.current = value; setMarker(value);
    try { sessionStorage.setItem(storageKey, JSON.stringify(value)); return true; }
    catch { setStorageError("浏览器恢复存储不可用。已受理任务请保留固定链接；存储恢复前不会创建新任务。"); return false; }
  };
  useEffect(() => {
    mounted.current = true;
    if (invalidLink) { setError("预检链接参数无效，请返回当前账户后重新打开固定结果链接。"); setReady(true); return () => { mounted.current = false; }; }
    try {
      const raw = sessionStorage.getItem(storageKey); const restored = raw === null ? null : recovery(raw);
      if (raw !== null && (restored === null || restored.kind === "pending" && Object.hasOwn(restored.input, "expected_bot_secret_version") !== isWeCom)) setStorageError("恢复记录损坏。请先核实已有任务，再明确清除本地记录；不会自动创建新任务。");
      else { markerRef.current = restored; setMarker(restored); if (!initialPreflightId && restored?.kind === "task") setTaskId(restored.preflightId); }
    } catch { setStorageError("浏览器恢复存储不可用，当前不会创建新预检任务。请恢复存储后重试。"); }
    if (initialPreflightId) setTaskId(initialPreflightId);
    setReady(true);
    return () => { mounted.current = false; postController.current?.abort(); };
  }, [storageKey, initialPreflightId, invalidLink, isWeCom]);
  useEffect(() => {
    if (!ready || !taskId || !userId || readDenied) return;
    let active = true; let timer: ReturnType<typeof setTimeout> | undefined; let deadlineTimer: ReturnType<typeof setTimeout> | undefined; let inFlight = false; let finished = false;
    let controller: AbortController | undefined;
    if (pollBudget.current.id !== taskId) {
      const saved = markerRef.current;
      pollBudget.current = { id: taskId, until: performance.now() + 120_000, deadline: saved?.kind === "task" && saved.preflightId === taskId ? Date.parse(saved.deadlineAt) : Infinity };
    }
    setError(""); setStopped(false);
    const exhausted = () => performance.now() >= pollBudget.current.until || Date.now() >= pollBudget.current.deadline;
    const boundPolling = () => {
      clearTimeout(deadlineTimer);
      const remaining = Math.min(pollBudget.current.until - performance.now(), pollBudget.current.deadline - Date.now());
      if (remaining <= 0) return; // A manual final GET remains available after automatic polling stops.
      deadlineTimer = setTimeout(() => { if (active && !finished) { finished = true; clearTimeout(timer); controller?.abort(); setStopped(true); setReading(false); } }, remaining);
    };
    boundPolling();
    const tick = async (force = false) => {
      clearTimeout(timer);
      if (!active || finished || inFlight || document.visibilityState === "hidden") return;
      if (!force && exhausted()) { setStopped(true); return; }
      inFlight = true; controller?.abort(); controller = new AbortController(); setReading(true);
      try {
        const value = await channelPreflightApi.get(account.tenant_id, account.account_id, taskId, controller.signal);
        if (!active || finished) return;
        if (value.provider !== account.provider) throw new ChannelPreflightApiError(200, "INVALID_RESPONSE");
        setData(value); setError(""); setNow(Date.now());
        pollBudget.current.deadline = Math.min(pollBudget.current.deadline, Date.parse(value.job_deadline_at));
        finished = terminal(value);
        if (finished) { clearTimeout(deadlineTimer); setStopped(false); }
        else if (exhausted()) setStopped(true);
        else { boundPolling(); timer = setTimeout(() => void tick(), Math.min(2000, Math.max(1, pollBudget.current.deadline - Date.now()))); }
      } catch (failure) {
        if (!active || finished) return;
        setError(failure instanceof ChannelPreflightApiError && failure.status === 403 ? "当前账户没有读取此预检结果的权限，请联系租户 OWNER 核实访问资格。" : preflightError(failure)); finished = true; clearTimeout(deadlineTimer);
        if (failure instanceof ChannelPreflightApiError && (failure.status === 401 || failure.status === 403)) {
          setWriteDenied(true); setReadDenied(true); setSessionExpired(failure.status === 401); denial.current();
        }
      } finally { inFlight = false; if (active) setReading(false); }
    };
    const visibility = () => { clearTimeout(timer); if (document.visibilityState !== "hidden") void tick(); };
    document.addEventListener("visibilitychange", visibility); void tick(true);
    return () => { active = false; clearTimeout(timer); clearTimeout(deadlineTimer); controller?.abort(); document.removeEventListener("visibilitychange", visibility); };
  }, [ready, taskId, userId, account.tenant_id, account.account_id, account.provider, readEpoch, readDenied]);
  useEffect(() => {
    if (!data?.expires_at) return;
    const delay = Date.parse(data.expires_at) - Date.now(); if (delay <= 0) return;
    const timer = setTimeout(() => setNow(Date.now()), Math.min(delay + 1, 2_147_483_647)); return () => clearTimeout(timer);
  }, [data]);
  const token = account.credentials.find((c) => c.purpose === (isWeCom ? "wecom.bot_secret" : "telegram.bot_token"));
  const versions = { expected_account_revision: account.account_revision, expected_connection_revision: account.connection_revision };
  const input: ChannelPreflightInput = isWeCom ? { ...versions, expected_bot_secret_version: token?.credential_version ?? 0, allow_connection_probe: true } : { ...versions, expected_bot_token_version: token?.credential_version ?? 0 };
  const pending = marker?.kind === "pending" ? marker : null;
  const completedCurrent = !!data && data.preflight_id === taskId && terminal(data);
  const canStart = ready && !!userId && canWrite && !writeDenied && !blocked && !busy && !reading && !account.enabled && !storageError && !invalidLink && !pending && validPreflightInput(input) && (!taskId || completedCurrent);
  const canRetry = !!pending && !busy && canWrite && !writeDenied && !blocked && !pendingRejected && Date.now() - Date.parse(pending.createdAt) < 86_400_000 && (!isWeCom || !account.enabled);
  const confirmationCurrent = !!confirmation && confirmation.currentInput === JSON.stringify(input) && !account.enabled && (confirmation.original ? canRetry : canStart);
  async function submit(original: Pending) {
    if (writeLock.current || !canWrite || writeDenied || blocked || isWeCom && account.enabled) return;
    writeLock.current = true; setBusy(true); setPostError(""); setPendingRejected(false);
    postController.current = new AbortController();
    try {
      const accepted = await channelPreflightApi.create(account.tenant_id, account.account_id, original.input, original.key, postController.current.signal);
      if (!mounted.current) return;
      remember({ kind: "task", preflightId: accepted.preflight_id, deadlineAt: accepted.job_deadline_at, createdAt: accepted.requested_at });
      setData(null); setError(""); setTaskId(accepted.preflight_id); setReadEpoch((n) => n + 1);
    } catch (failure) {
      if (!mounted.current) return;
      setPostError(preflightError(failure)); setPendingRejected(!isPreflightUncertain(failure) && !(failure instanceof ChannelPreflightApiError && failure.status === 429));
      if (failure instanceof ChannelPreflightApiError && (failure.status === 401 || failure.status === 403)) {
        setWriteDenied(true); denial.current();
        // A MEMBER can still read shared results after an OWNER-only POST is denied.
        if (failure.status === 401) { setReadDenied(true); setSessionExpired(true); }
      }
    } finally { writeLock.current = false; if (mounted.current) setBusy(false); }
  }
  function start() {
    if (!canStart || writeLock.current) return;
    if (isWeCom) { setConsent(false); setConfirmation({ input, currentInput: JSON.stringify(input) }); return; }
    begin(input);
  }
  function begin(originalInput: ChannelPreflightInput) {
    const next: Pending = { kind: "pending", key: crypto.randomUUID(), input: originalInput, createdAt: new Date().toISOString() };
    if (!remember(next)) return;
    void submit(next);
  }
  function retry() {
    if (!pending || !canRetry || writeLock.current) return;
    if (isWeCom) { setConsent(false); setConfirmation({ input: pending.input, currentInput: JSON.stringify(input), original: pending }); return; }
    void submit(pending);
  }
  function confirmProbe() {
    if (!confirmation || !consent || !confirmationCurrent || writeLock.current) return;
    setConfirmation(null);
    if (confirmation.original) void submit(confirmation.original);
    else begin(confirmation.input);
  }
  function clearRecovery() {
    if (busy) return;
    try { sessionStorage.removeItem(storageKey); markerRef.current = null; setMarker(null); setStorageError(""); setPostError(""); setPendingRejected(false); }
    catch { setStorageError("浏览器恢复存储仍不可用，尚未清除记录；不会开始新任务。"); }
  }
  const checkedMode = data?.provider === "telegram" ? data.receive_mode ?? "webhook" : "long_connection";
  const modeChanged = data?.provider === "telegram" && account.config.receive_mode !== undefined && checkedMode !== account.config.receive_mode;
  const checkedCredentialVersion = data?.provider === "wecom" ? data.bot_secret_version : data?.bot_token_version;
  const stale = !!data && (modeChanged || data.freshness === "STALE" || data.connection_revision !== account.connection_revision || checkedCredentialVersion !== token?.credential_version);
  const expired = !!data && !stale && (data.freshness === "EXPIRED" || data.expires_at !== null && Date.parse(data.expires_at) <= now);
  const metadataChanged = !!data && (data.metadata_changed || data.account_revision !== account.account_revision);
  const stateLabel = data?.state === "QUEUED" ? "正在排队，等待 Gateway" : data?.state === "RUNNING" ? "正在检查接入条件" : data?.state === "TIMED_OUT" ? "预检超时，未获得检查结果" : data?.state === "STALE" ? "检查期间配置或授权变化，任务已失效" : data?.outcome === "PASS" ? isWeCom ? "诊断完成 · 连接认证通过" : "检查完成 · 配置检查通过" : data?.outcome === "WARN" ? "检查完成 · 需要留意" : data?.outcome === "FAIL" ? "检查完成 · 存在问题" : "检查完成 · 存在未知项";
  const reasonMessages: Record<string, string> = { CHANNEL_PREFLIGHT_NO_EXECUTOR: "期限内没有 Gateway 领取任务，请检查 Gateway 预检执行器。", CHANNEL_PREFLIGHT_EXECUTION_TIMEOUT: "Gateway 未在期限内完成检查，请核对服务后重新检查。", CHANNEL_PREFLIGHT_REQUESTER_REVOKED: "发起者的 OWNER 授权已撤销，本次任务不再执行。", CHANNEL_PREFLIGHT_TENANT_INACTIVE: "租户已停用，本次任务不再执行。", CHANNEL_PREFLIGHT_GATEWAY_CONFIG_CHANGED: "Gateway 配置发生变化，请重新检查。", CHANNEL_PREFLIGHT_ACCOUNT_CHANGED: "账户连接配置已变化，请重新检查。", CHANNEL_SOURCE_EPOCH_MISMATCH: "预检服务来源已变化，请重新检查。" };
  return <section className={styles.panel} aria-labelledby="channel-preflight-title">
    {account.config.endpoint_profile === "test" && <p>测试 Telegram（Channel Lab）：本次只验证模拟平台，不代表官方 Telegram 连通。</p>}
    <div className={styles.heading}><div><h2 id="channel-preflight-title">{isWeCom ? "企微接入诊断" : "Telegram 接入预检"}</h2><p>{isWeCom ? "使用已保存的 Bot ID 与 Bot Secret 进行短时连接认证；不要求配置运行目标。" : "检查已保存的账户、凭据和现有 Webhook；不要求配置运行目标。"}</p></div>{canWrite && <button className="button primary" type="button" disabled={!canStart} onClick={start}>{busy ? "正在提交检查…" : isWeCom ? completedCurrent ? "重新诊断连接" : "开始连接诊断" : completedCurrent ? "重新检查接入条件" : "检查接入条件"}</button>}</div>
    <p className={styles.boundary}>{isWeCom ? "这不是只读查询：诊断会短时建立企微长连接并认证，可能替换同一 Bot 的其他客户端，期间可能收到消息。诊断会丢弃业务消息，不创建 Run、不发送回复；结束后断开，不自动恢复被替换的外部客户端。" : "检查不会启用账户、切换路由、注册或删除 Webhook，也不会发送消息或调用 getUpdates 消费更新。配置检查通过不等于上线成功。"}</p>
    {isWeCom && <dl className={styles.facts} aria-label="当前企微诊断配置"><dt>Bot ID</dt><dd>{account.provider_account_id}</dd><dt>当前 Bot Secret</dt><dd>{token ? `v${token.credential_version} · ${token.configured ? "已配置" : "未配置"}` : "版本尚未读取"}</dd></dl>}
    {account.enabled && <p className={styles.warning}>此账户当前已启用。请先在账户接入操作中单独确认停用，再检查；本面板不会自动停用。</p>}
    {!canWrite && <p className={styles.note}>当前为只读访问，可查看已知任务结果；发起预检需要租户 OWNER。</p>}
    {!validPreflightInput(input) && <p className={styles.warning}>{isWeCom ? "账户版本或 Bot Secret 版本尚不完整，请先重新读取账户。" : "账户版本或 Bot Token 版本尚不完整，请先重新读取账户。"}</p>}
    {blocked && <p className={styles.note}>请先确认当前账户的其他写入结果，再发起新检查。</p>}
    {storageError && <div className={styles.error} role="alert">{storageError}<button type="button" className="button" disabled={busy} onClick={clearRecovery}>已核实状态，清除本地恢复记录</button></div>}
    {pending && <div className={styles.warning}><strong>原预检请求待确认</strong><p>{isWeCom ? "保留原请求标识、三个原始版本和连接探测确认，不会改用最新版本或自动重复提交。重试仍需确认连接影响。" : "保留原请求标识与三个原始版本，不会改用最新版本或自动重复提交。"}</p>{postError && <p role="alert">{postError}</p>}<div className={styles.actions}><button type="button" className="button" disabled={!canRetry} onClick={retry}>使用原请求重试确认</button><button type="button" className="button" disabled={busy} onClick={clearRecovery}>已核实状态，清除本地恢复记录</button></div><small>清除只影响本地恢复记录，不会取消服务端任务。超过 24 小时的原请求不再自动重放。</small></div>}
    {error && <div className={styles.error} role="alert">{error}</div>}
    {sessionExpired && <a className={styles.link} href="/login">重新登录后继续核实</a>}
    {taskId && <div className={styles.actions}><a className={styles.link} href={preflightResultHref(account.tenant_id, account.account_id, taskId)}>此预检的固定链接</a><button className="button" type="button" disabled={reading || readDenied} onClick={() => setReadEpoch((n) => n + 1)}>重新读取预检结果</button></div>}
    {reading && <p role="status">正在读取预检结果…</p>}
    {stopped && <p className={styles.warning}>已到任务期限，自动轮询已停止；请手动读取服务端终态，不会自动新建任务。</p>}
    {data && <div className={styles.result}><div className={styles.resultHeading}><h3>{stateLabel}</h3>{data.state === "COMPLETED" && <span className={stale || expired ? styles.warning : styles.note}>{stale ? "结果已失效" : expired ? "结果已过期" : "针对检查时配置"}</span>}</div>
      {modeChanged && <p className={styles.warning}>接收方式已变更；下列结果仍按检查时的模式解释，没有重算历史检查项。</p>}
      {stale && <p className={styles.warning}>配置已变化，此结果已失效。它仅保留检查时的历史事实，请重新检查。</p>}
      {expired && <p className={styles.warning}>检查结果已超过有效期，请重新检查；历史结果不再代表当前条件。</p>}
      {metadataChanged && !stale && <p className={styles.note}>名称或说明已变更，连接检查仍适用；任务保留原账户版本作为审计快照。</p>}
      {reasonMessages[data.reason_code] && <p className={styles.warning}>{reasonMessages[data.reason_code]}</p>}
      {data.state === "COMPLETED" && <ol className={`${styles.checks} ${isWeCom ? styles.wecomChecks : ""}`}>{(isWeCom ? WECOM_PREFLIGHT_CHECK_IDS : PREFLIGHT_CHECK_IDS).map((id) => { const item = data.checks.find((c) => c.id === id); return <li key={id} data-testid="preflight-check"><div><strong>{isWeCom && id === "delivery_verification" ? "真实消息与 Agent 回复" : labels[id]}</strong><span className={item?.status === "PASS" ? styles.pass : item?.status === "FAIL" ? styles.fail : styles.neutral}>{item ? statusLabels[item.status] : "未知 / 未验证"}</span></div><p>{item ? (isWeCom ? wecomMessages[item.code] ?? codeMessages[item.code] : codeMessages[item.code]) ?? "未识别的检查结论，不作为成功依据。" : "未获得此项检查事实。"}</p>{id === "pending_updates" && typeof item?.details.pending_update_count === "number" && <small>积压数量：{item.details.pending_update_count}</small>}{id === "delivery_errors" && typeof item?.details.last_error_at === "string" && <small>最近报告错误时间：{item.details.last_error_at}</small>}</li>; })}</ol>}
      <dl className={styles.facts}><dt>{isWeCom ? "诊断连接方式" : "检查时接收方式"}</dt><dd>{isWeCom ? "企微长连接 · 短时认证后断开" : receiveModeLabel(checkedMode)}{!data.diagnostic_policy ? "（旧版预检）" : ""}</dd>{!isWeCom && <><dt>当前保存接收方式</dt><dd>{receiveModeLabel(account.config.receive_mode)}</dd></>}<dt>检查时间</dt><dd>{data.checked_at ?? "尚未完成"}</dd><dt>结果有效至</dt><dd>{data.expires_at ?? "尚无检查结果"}</dd><dt>检查时公开入口</dt><dd>{isWeCom ? "不适用（企微出站长连接）" : checkedMode === "long_polling" ? "不适用（长轮询）" : data.expected_public_origin ?? "未确认合法入口"}</dd><dt>固定连接版本</dt><dd>r{data.connection_revision} · {isWeCom ? "Bot Secret" : "Token"} v{checkedCredentialVersion}</dd></dl>
      {data.gateway_config_digest && <details><summary>检查技术信息</summary><p className={styles.mono}>共享 Gateway 配置：{data.gateway_config_digest}</p>{data.diagnostic_policy && <p className={styles.mono}>诊断策略：{data.diagnostic_policy}</p>}{data.effective_config_digest && <p className={styles.mono}>本任务有效配置：{data.effective_config_digest}</p>}<p className={styles.mono}>{data.preflight_id}</p></details>}
    </div>}
    {!taskId && !pending && ready && !invalidLink && <p className={styles.note}>尚未选择预检任务。新检查只使用当前已保存的配置与凭据版本。</p>}
    <div className={styles.limits}><strong>结果边界</strong>{isWeCom ? <ul><li>凭据配置、连接认证、真实消息与 Agent 回复是三层不同的事实。</li><li>认证通过只代表本次短连接的认证结果；COMPLETED 只代表诊断任务结束，不代表持续在线。</li><li>真实消息与 Agent 回复：未验证（NOT_TESTED）；诊断期间业务消息不会进入 Run。</li><li>诊断不启用账户、不修改路由；不会替你恢复被替换的其他客户端连接。</li><li>Gateway 当前配置：尚未确认（UNCONFIRMED）；结果只针对检查时配置。</li></ul> : <ul><li>公开入口：Webhook 仅静态格式检查，未探测 DNS、TLS 或公网可达性；长轮询不适用。</li><li>Gateway 当前配置：尚未确认（UNCONFIRMED）；结果只针对检查时配置。</li><li>Telegram 实际投递：未验证（NOT_TESTED），不以 READY 或 PUBLISHED 代替。</li><li>旧 Webhook 的 secret_token 不可回读，预检结果不是完整恢复资料。</li></ul>}</div>
    {confirmation && <ChannelDialog title="确认企微短时连接诊断" busy={busy} disabled={!consent || !confirmationCurrent} confirmLabel={confirmation.original ? "同意影响，以原请求重试" : "同意影响并开始诊断"} onClose={() => setConfirmation(null)} onConfirm={confirmProbe}>
      <p>本操作会使用已保存的 Bot Secret 向企微发起真实连接认证，可能替换同一 Bot 在其他客户端的连接；账户在本平台停用不代表其他客户端已断开。</p>
      <p>诊断期间可能短暂接收消息，但业务消息会被丢弃，不产生 Run、不发送 Agent 回复。探测结束后断开，不自动恢复其他客户端连接。</p>
      <dl className={styles.facts}><dt>Bot ID</dt><dd>{account.provider_account_id}</dd><dt>账户 / 连接版本</dt><dd>r{confirmation.input.expected_account_revision} / r{confirmation.input.expected_connection_revision}</dd><dt>Bot Secret 版本</dt><dd>v{confirmation.input.expected_bot_secret_version}</dd></dl>
      {confirmation.original && <p className={styles.note}>只重放已确认过的原请求标识、原版本与确认位；不会按当前版本创建新诊断。</p>}
      {!confirmationCurrent && <p className={styles.warning}>账户版本、启用状态或操作资格已变化，请取消后重新核对；本次没有发起诊断。</p>}
      <label className={styles.consent}><input type="checkbox" checked={consent} onChange={(event) => setConsent(event.target.checked)} />我已了解连接替换和消息接收影响，同意进行短时认证诊断</label>
    </ChannelDialog>}
  </section>;
}
