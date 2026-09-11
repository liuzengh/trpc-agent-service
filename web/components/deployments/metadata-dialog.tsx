"use client";
import { useRef, useState } from "react";
import { deploymentApi, DeploymentApiError, deploymentError, type Deployment } from "../../lib/deployment-api";
import { Button } from "../ui";
import { ProfileDialog } from "../runtime-profiles/profile-dialog";
import styles from "./deployment.module.css";

export function DeploymentMetadataDialog({ tenantId, deployment, onClose, onSaved }: {
  tenantId: string; deployment: Deployment; onClose(): void; onSaved(value: Deployment): void;
}) {
  const [name, setName] = useState(deployment.name);
  const [description, setDescription] = useState(deployment.description);
  const [baseline, setBaseline] = useState(deployment);
  const [latest, setLatest] = useState<Deployment | null>(null);
  const [conflict, setConflict] = useState(false);
  const [busy, setBusy] = useState(false); const lock = useRef(false);
  const [error, setError] = useState("");
  async function act(refresh: boolean) {
    if (lock.current) return; lock.current = true; setBusy(true); setError("");
    try {
      if (refresh) setLatest(await deploymentApi.get(tenantId, deployment.id));
      else onSaved(await deploymentApi.update(tenantId, deployment.id, { expected_metadata_revision: baseline.metadata_revision, name: name.trim(), description }));
    } catch (e) {
      setError(deploymentError(e));
      if (e instanceof DeploymentApiError && e.status === 409) setConflict(true);
    } finally { lock.current = false; setBusy(false); }
  }
  return <ProfileDialog title="编辑部署信息" description="只修改名称和描述，不改变已发布版本。" onClose={onClose} busy={busy} footer={<>
    <Button variant="secondary" disabled={busy} onClick={onClose}>取消</Button>
    <Button disabled={busy || conflict || !name.trim() || [...name.trim()].length > 128 || [...description].length > 4096} onClick={() => void act(false)}>保存信息</Button>
  </>}>
    <label className={styles.field}>部署名称<input value={name} onChange={(e) => setName(e.target.value)} disabled={busy} /></label>
    <label className={styles.field}>描述<textarea value={description} onChange={(e) => setDescription(e.target.value)} disabled={busy} /></label>
    {error && <div role="alert" className={styles.error}>{error}</div>}
    {conflict && <div className={styles.warning}><p>本地文字已保留。请先读取当前信息，再确认覆盖基线。</p><Button variant="secondary" disabled={busy} onClick={() => void act(true)}>读取当前部署信息</Button>
      {latest && <><p>服务器当前名称：{latest.name}</p><p>服务器当前描述：{latest.description || "无"}</p><p>元数据 r{baseline.metadata_revision} → r{latest.metadata_revision}</p><Button disabled={busy} onClick={() => { setBaseline(latest); setConflict(false); setLatest(null); setError(""); }}>确认以当前信息为基线</Button></>}
    </div>}
  </ProfileDialog>;
}
