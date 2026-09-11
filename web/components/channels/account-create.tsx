"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useRef, useState, type FormEvent } from "react";
import { channelApi, ChannelApiError, channelError, isChannelUncertain, validateChannelAccount, isTelegramReceiveMode, receiveModeLabel, type CredentialPurpose, type TelegramReceiveMode, type TelegramEndpointProfile, type ChannelProvider, type CreateAccountInput } from "../../lib/channel-api";
import { channelHref, channelPendingKey, loadChannelPending, readChannelTarget, saveChannelPending, channelTargetQuery } from "../../lib/channel-editor-state";
import { Button, PageHeader, StatusBadge } from "../ui";
import { ChannelDialog } from "./confirmation-dialog";
import { useChannelAccess } from "./use-channel-access";
import styles from "./create.module.css";
import { ReceiveModeSelector } from "./receive-mode-selector";

type CreateMarker = {
  operation: "createAccount"; key: string; secret: true; createdAt: string;
  input: Omit<CreateAccountInput, "credentials"> & { supplied_purposes?: CredentialPurpose[] };
};

export function AccountCreate({ tenantId, query = "" }: { tenantId: string; query?: string }) {
  const access = useChannelAccess(tenantId);
  return <AccountCreateForm key={`${tenantId}:${access.userId}`} tenantId={tenantId} query={query} access={access} />;
}

function AccountCreateForm({ tenantId, query, access }: { tenantId: string; query: string; access: ReturnType<typeof useChannelAccess> }) {
  const router = useRouter();
  const target = readChannelTarget(query);
  const targetRequested = new URLSearchParams(query).has("deployment_id") || new URLSearchParams(query).has("revision_number");
  const invalidTarget = targetRequested && !target;
  const suffix = target ? `?${channelTargetQuery(target)}` : "";
  const rootHref = channelHref(tenantId) + suffix;
  const [provider, setProvider] = useState<ChannelProvider>("telegram");
  const [endpointProfile, setEndpointProfile] = useState<TelegramEndpointProfile>("official");
  const [receiveMode, setReceiveMode] = useState<TelegramReceiveMode>("long_polling");
  const [physicalID, setPhysicalID] = useState("");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [botToken, setBotToken] = useState("");
  const [webhookSecret, setWebhookSecret] = useState("");
  const [botSecret, setBotSecret] = useState("");
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [error, setError] = useState("");
  const [pending, setPending] = useState<CreateMarker | null>(null);
  const [restored, setRestored] = useState(false);
  const [initialized, setInitialized] = useState(false);
  const [recoveryError, setRecoveryError] = useState("");
  const [recoveryRead, setRecoveryRead] = useState(0);
  const [busy, setBusy] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [acknowledged, setAcknowledged] = useState(false);
  const [createdID, setCreatedID] = useState("");
  const originalBody = useRef<CreateAccountInput | null>(null);
  const reviewedBody = useRef<CreateAccountInput | null>(null);
  const locked = useRef(false);
  const mounted = useRef(false);
  const storageKey = access.userId ? channelPendingKey(access.userId, tenantId, "new") : "";

  function clearSecrets() {
    originalBody.current = null; reviewedBody.current = null;
    setBotToken(""); setWebhookSecret(""); setBotSecret("");
  }

  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; originalBody.current = null; reviewedBody.current = null; };
  }, []);

  useEffect(() => {
    if (!storageKey) return;
    try {
      const storage = window.sessionStorage;
      // The shared parser tolerates blocked storage; creating must distinguish it from no prior command.
      storage.getItem(storageKey);
      const marker = loadChannelPending(storage, storageKey);
      if (marker?.operation === "createAccount" && marker.secret && (marker.input.provider === "telegram" || marker.input.provider === "wecom") && typeof marker.input.provider_account_id === "string" && typeof marker.input.name === "string") {
        const restoredMarker: CreateMarker = { operation: "createAccount", secret: true, key: marker.key, createdAt: marker.createdAt, input: { provider: marker.input.provider, provider_account_id: marker.input.provider_account_id, name: marker.input.name, ...(typeof marker.input.description === "string" ? { description: marker.input.description } : {}), ...(marker.input.config ? { config: marker.input.config as { receive_mode: TelegramReceiveMode; endpoint_profile?: TelegramEndpointProfile }, supplied_purposes: marker.input.supplied_purposes as CredentialPurpose[] } : {}) } };
        setPending(restoredMarker); setProvider(restoredMarker.input.provider); setPhysicalID(restoredMarker.input.provider_account_id);
        if (restoredMarker.input.config) { setReceiveMode(restoredMarker.input.config.receive_mode); setEndpointProfile(restoredMarker.input.config.endpoint_profile ?? "official"); }
        setName(restoredMarker.input.name); setDescription(restoredMarker.input.description ?? ""); setRestored(true);
      }
      setRecoveryError(""); setInitialized(true);
    } catch { setRecoveryError("浏览器恢复记录读取失败。请先恢复记录读取或核对渠道账户列表，避免以新标识重复提交原操作。"); setInitialized(false); }
  }, [storageKey, recoveryRead]);

  useEffect(() => {
    if (!access.loading && !access.canWrite) { clearSecrets(); setConfirming(false); setRestored(true); }
  }, [access.loading, access.canWrite]);

  function bodyFromForm(): CreateAccountInput {
    const saved = pending?.input;
    const identity: Omit<CreateAccountInput, "credentials"> = saved ? {
      provider: saved.provider, provider_account_id: saved.provider_account_id, name: saved.name,
      ...(saved.description !== undefined ? { description: saved.description } : {}), ...(saved.config ? { config: saved.config } : {}),
    } : { provider, provider_account_id: provider === "telegram" ? physicalID.replace(/^0+/, "") : physicalID, name: name.trim(), description, ...(provider === "telegram" ? { config: { receive_mode: receiveMode, ...(endpointProfile === "test" ? {endpoint_profile: endpointProfile} : {}) } } : {}) };
    // A legacy marker always used both replace purposes; a modern one preserves exact presence.
    const includeSecret = saved ? saved.supplied_purposes ? saved.supplied_purposes.includes("telegram.webhook_secret") : true : receiveMode === "webhook" || webhookSecret.length > 0;
    return { ...identity, credentials: identity.provider === "telegram" ? {
      "telegram.bot_token": { action: "replace", value: botToken },
      ...(includeSecret ? { "telegram.webhook_secret": { action: "replace" as const, value: webhookSecret } } : {}),
    } : { "wecom.bot_secret": { action: "replace", value: botSecret } } };
  }

  function review(event?: FormEvent) {
    event?.preventDefault();
    if (!access.canWrite || busy || createdID || !initialized) return;
    const body = originalBody.current ?? bodyFromForm();
    const legacy = pending?.input.provider === "telegram" && !pending.input.config;
    const validation = validateChannelAccount(legacy ? { ...body, config: { receive_mode: "webhook" } } : body);
    setErrors(validation);
    if (Object.keys(validation).length) { setError("请补齐或修正下方标出的字段，本次尚未提交。"); return; }
    setError(""); reviewedBody.current = body; setAcknowledged(false); setConfirming(true);
  }

  async function create() {
    if (locked.current || !access.canWrite || !acknowledged || !reviewedBody.current) return;
    locked.current = true; setBusy(true); setError("");
    const body = originalBody.current ?? reviewedBody.current;
    const recovering = pending !== null;
    let marker = pending;
    try {
      if (!marker) {
        marker = { operation: "createAccount", secret: true, key: crypto.randomUUID(), createdAt: new Date().toISOString(), input: {
          provider: body.provider, provider_account_id: body.provider_account_id, name: body.name,
          ...(body.description !== undefined ? { description: body.description } : {}),
          ...(body.config ? { config: body.config, supplied_purposes: Object.keys(body.credentials) as CredentialPurpose[] } : {}),
        } };
      }
      if (!saveChannelPending(window.sessionStorage, storageKey, marker)) {
        setError("浏览器恢复记录写入失败，本次尚未提交。请检查存储设置后重试。");
        return;
      }
      originalBody.current = body; setPending(marker); setRestored(false); setConfirming(false);
      const result = marker.input.provider === "telegram" && !marker.input.config
        ? await channelApi.createAccount(tenantId, body, marker.key, "webhook-v1")
        : await channelApi.createAccount(tenantId, body, marker.key);
      if (!mounted.current) return;
      const id = result.account?.account_id;
      if (!id) throw new ChannelApiError(0, "INVALID_RESPONSE", "创建响应缺少账户标识，结果待确认，请保留原请求。");
      setCreatedID(id); clearSecrets(); setPending(null); setRestored(false);
      try { window.sessionStorage.removeItem(storageKey); } catch { /* A stale non-secret marker can only replay this same command. */ }
      // A confirmed create is never retried because its following read fails.
      try { await channelApi.getAccount(tenantId, id); }
      catch { if (mounted.current) setError("账户已创建，最新状态读取失败。正在打开已创建账户的工作台，可在那里重新读取。"); }
      if (mounted.current) router.replace(channelHref(tenantId, id) + suffix);
    } catch (cause) {
      if (!mounted.current) return;
      setError(channelError(cause));
      if (cause instanceof ChannelApiError && cause.code === "CHANNEL_IDEMPOTENCY_CONFLICT") {
        clearSecrets(); setRestored(true);
        setError("本次请求标识已对应不同内容。账户身份和原标识继续保留，请核对账户列表；如需精确重放，请重新输入上一次的原凭据，不要以新标识重复创建。");
      }
      if (cause instanceof ChannelApiError && (cause.status === 401 || cause.status === 403)) {
        clearSecrets(); setRestored(true); access.denyWrites();
      }
      if (!isChannelUncertain(cause) && !recovering && !(cause instanceof ChannelApiError && cause.code === "CHANNEL_IDEMPOTENCY_CONFLICT")) {
        setPending(null); originalBody.current = null;
        try { window.sessionStorage.removeItem(storageKey); } catch { /* Recovery remains explicit if browser storage is unavailable. */ }
      }
    } finally {
      locked.current = false;
      if (mounted.current) setBusy(false);
    }
  }

  function changeProvider(value: ChannelProvider) {
    setProvider(value); setReceiveMode("long_polling"); setPhysicalID(""); clearSecrets(); setErrors({}); setError("");
  }

  function generateWebhookSecret() {
    try { setWebhookSecret(Array.from(crypto.getRandomValues(new Uint8Array(32)), (b) => b.toString(16).padStart(2, "0")).join("")); }
    catch { setError("随机值生成失败，请手动输入符合要求的 Webhook Secret。"); }
  }

  const legacyRecovery = pending?.input.provider === "telegram" && !pending.input.config;
  const optionalOmitted = !!pending?.input.config && !pending.input.supplied_purposes?.includes("telegram.webhook_secret");
  const metadataLocked = busy || !!pending || !!createdID;
  const secretLocked = busy || !!createdID || (!!pending && !restored);
  const fieldError = (key: string) => errors[key] ? <small className={styles.fieldError}>{errors[key]}</small> : null;
  const fieldInvalid = (key: string) => errors[key] ? true : undefined;

  return <div className={styles.page}>
    <PageHeader eyebrow="CHANNELS / NEW" title="添加渠道账户" description="先保存机器人身份和接入凭据，随后选择固定部署。账户创建后保持停用。" action={<Link className="button secondary" href={rootHref}>返回渠道接入</Link>} />
    {invalidTarget && <div className={styles.error} role="alert">入口中的固定部署参数无效或重复，请返回部署版本详情重新选择。本页不会绑定这个无效目标；创建账户后仍需明确选择有效版本。</div>}
    {target && <div className={styles.banner}>后续准备绑定部署 <strong>{target.deployment_id} · r{target.revision_number}</strong>。本页只创建渠道账户，不会自动创建 Binding 或启用接入。</div>}
    {(access.loading || (!initialized && access.userId && !recoveryError)) && <p role="status">正在读取访问权限与恢复记录…</p>}
    {recoveryError && <div className={styles.error} role="alert">{recoveryError} <Button variant="secondary" onClick={() => setRecoveryRead((value) => value + 1)}>重新读取恢复记录</Button></div>}
    {access.error && <div className={styles.error} role="alert">{access.error} <Button variant="secondary" onClick={access.reload}>重新读取访问权限</Button></div>}
    {!access.loading && !access.canWrite && !access.error && <section className={styles.card}><h2>当前为只读访问</h2><p>添加渠道账户需要租户 OWNER。现有账户和脱敏诊断仍可在渠道列表查看。</p><Link className={styles.link} href={rootHref}>查看渠道账户 →</Link></section>}
    {error && <div className={styles.error} role="alert">{error}</div>}
    {createdID && <div className={styles.success} role="status">账户已经创建，保持停用。<Link className={styles.link} href={channelHref(tenantId, createdID) + suffix}>打开已创建账户 →</Link></div>}
    {access.canWrite && initialized && !createdID && <>
      {pending && <div className={styles.warning} role="status"><strong>上一次创建结果待确认</strong><p>{restored ? "页面刷新后只恢复了非秘密字段和原请求标识，凭据输入已清空。可以先在本租户列表核对 Provider 与机器人 ID；如需精确重放，请重新输入上一次的全部原凭据，确认后使用同一请求标识重试。" : "原请求内容仅保留在当前页面内存。请重试确认同一次创建，不要再次新建相同机器人。"}</p><Link className={styles.link} href={rootHref}>核对已存在账户 →</Link><p className={styles.small}>列表中的状态变化本身不证明某个凭据值已被接受。退出本页不会撤销可能已经保存的账户。</p></div>}
      <form className={styles.form} onSubmit={review} noValidate>
        <section className={styles.card} aria-labelledby="channel-identity-title"><h2 id="channel-identity-title">1. 机器人身份</h2><p className={styles.small}>渠道类型和稳定机器人 ID 创建后固定，停用不会释放身份占用。</p>
          <div className={styles.grid}>
            <label className={styles.field}><span>渠道类型</span><select value={provider} disabled={metadataLocked} onChange={(e) => changeProvider(e.target.value as ChannelProvider)}><option value="telegram">Telegram</option><option value="wecom">企业微信</option></select></label>
            <label className={styles.field}><span>{provider === "telegram" ? "Telegram 数字 Bot ID" : "企业微信 Bot ID"}</span><input value={physicalID} inputMode={provider === "telegram" ? "numeric" : "text"} autoComplete="off" disabled={metadataLocked} aria-invalid={fieldInvalid("provider_account_id")} onChange={(e) => setPhysicalID(e.target.value)} /><small>{provider === "telegram" ? "填写稳定数字 ID，不是 @用户名。ID 始终按字符串处理，前导零会规范化。" : "填写智能机器人长连接 SDK 使用的 Bot ID，不是 CorpID 或应用 AgentID。"}</small>{fieldError("provider_account_id")}</label>
            <label className={styles.field}><span>账户名称</span><input value={name} disabled={metadataLocked} aria-invalid={fieldInvalid("name")} onChange={(e) => setName(e.target.value)} /><small>1～128 个字符，便于租户成员识别。</small>{fieldError("name")}</label>
            <label className={`${styles.field} ${styles.full}`}><span>说明（可选）</span><textarea value={description} disabled={metadataLocked} aria-invalid={fieldInvalid("description")} onChange={(e) => setDescription(e.target.value)} /><small>最多 4096 个字符；不在说明中填写凭据。</small>{fieldError("description")}</label>
          </div>
        </section>
        {provider === "telegram" && <section className={styles.card}><h2>2. 接收方式</h2><label>接入环境<select value={endpointProfile} disabled={metadataLocked} onChange={(e) => setEndpointProfile(e.target.value === "test" ? "test" : "official")}><option value="official">官方 Telegram</option><option value="test">测试 Telegram（Channel Lab）</option></select></label><p>测试环境使用 Channel Lab 生成的 Token，不使用官方 Bot Token。</p>{legacyRecovery ? <p className={styles.warning}>升级前的 Webhook 创建请求。身份与原请求标识保持锁定；仅在明确确认后，以 webhook-v1 兼容契约和原始两项凭据恢复，不套用长轮询默认。</p> : <ReceiveModeSelector value={receiveMode} onChange={(value) => { if (isTelegramReceiveMode(value)) { setReceiveMode(value); setErrors({}); } }} disabled={metadataLocked} />}<p className={styles.small}>这里只保存接收方式；不会启动接收、切换 Telegram 远端配置或清空积压。</p></section>}
        <section className={styles.card} aria-labelledby="channel-credential-title"><h2 id="channel-credential-title">{provider === "telegram" ? "3" : "2"}. 接入凭据</h2><p className={styles.small}>凭据仅用于本账户接入，不放入 Runtime Profile。保存后只显示配置状态，不回显原值。</p>
          <div className={styles.grid}>{provider === "telegram" ? <>
            <label className={`${styles.field} ${styles.full}`}><span>Bot Token</span><input type="password" autoComplete="new-password" value={botToken} disabled={secretLocked} aria-invalid={fieldInvalid("credentials.telegram.bot_token")} onChange={(e) => setBotToken(e.target.value)} />{fieldError("credentials.telegram.bot_token")}</label>
            <div className={`${styles.secretRow} ${styles.full}`}><label className={styles.field}><span>Webhook Secret{!legacyRecovery && receiveMode === "long_polling" ? "（可选）" : "（必需）"}</span><input type="password" autoComplete="new-password" value={webhookSecret} disabled={secretLocked || optionalOmitted} aria-invalid={fieldInvalid("credentials.telegram.webhook_secret")} onChange={(e) => setWebhookSecret(e.target.value)} /><small>{optionalOmitted ? "原请求未包含此可选项，恢复时继续省略；创建成功后可单独补充。" : "1～256 位 ASCII 字母、数字、下划线或连字符；与 Bot Token 是两项独立凭据。"}</small>{fieldError("credentials.telegram.webhook_secret")}</label><Button type="button" variant="secondary" disabled={secretLocked || !!pending} onClick={generateWebhookSecret}>生成随机值</Button></div>
          </> : <label className={`${styles.field} ${styles.full}`}><span>Bot Secret</span><input type="password" autoComplete="new-password" value={botSecret} disabled={secretLocked} aria-invalid={fieldInvalid("credentials.wecom.bot_secret")} onChange={(e) => setBotSecret(e.target.value)} />{fieldError("credentials.wecom.bot_secret")}</label>}</div>
        </section>
        <div className={styles.banner}><strong>创建不会启用机器人。</strong>保存后还需选择已发布的 Deployment rN，再分别启用接入与消息路由。Provider 与 Bot ID 创建后固定；Telegram 接收方式可以在停用后单独更改，生成的连接字段保持只读。</div>
        <div className={styles.actions}><Button type="submit" disabled={busy || !access.canWrite}>{busy ? "正在确认创建结果…" : pending ? "重试确认原创建请求" : "检查并创建账户"}</Button><Link className="button secondary" href={rootHref} onClick={() => clearSecrets()}>{pending ? "稍后核对" : "取消"}</Link></div>
      </form>
    </>}
    {confirming && reviewedBody.current && <ChannelDialog title={pending ? "确认重放原创建请求" : "确认创建渠道账户"} busy={busy} disabled={!acknowledged} confirmLabel={pending ? "使用原请求标识确认" : "确认创建并保持停用"} onConfirm={() => void create()} onClose={() => { setConfirming(false); reviewedBody.current = null; }}>
      <dl className={styles.summary}><dt>渠道类型</dt><dd>{reviewedBody.current.provider === "telegram" ? "Telegram" : "企业微信"}</dd><dt>稳定机器人 ID</dt><dd>{reviewedBody.current.provider_account_id}</dd><dt>账户名称</dt><dd>{reviewedBody.current.name}</dd>{reviewedBody.current.provider === "telegram" && <><dt>接收方式</dt><dd>{legacyRecovery ? "Webhook（原请求兼容恢复）" : receiveModeLabel(reviewedBody.current.config?.receive_mode)}</dd></>}<dt>创建后的接入配置</dt><dd><StatusBadge tone="gray">停用</StatusBadge></dd><dt>消息运行目标</dt><dd>尚未创建，不自动启用</dd></dl>
      <p>Provider 和机器人身份创建后固定；停用不删除记录或释放身份。这里只保存凭据，不做 Provider 在线认证或 Webhook 注册。</p>
      <label className={styles.check}><input type="checkbox" checked={acknowledged} onChange={(e) => setAcknowledged(e.target.checked)} /><span>{pending && restored ? "我重新输入了上一次请求的原凭据，并确认使用相同请求标识核对结果。" : "我已核对机器人身份，确认创建后保持停用。"}</span></label>
    </ChannelDialog>}
  </div>;
}
