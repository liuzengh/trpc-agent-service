import { useId } from "react";
import type { TelegramReceiveMode } from "../../lib/channel-api";
import styles from "./receive-mode.module.css";

export function ReceiveModeSelector({ value, onChange, disabled = false }: { value: TelegramReceiveMode; onChange: (value: TelegramReceiveMode) => void; disabled?: boolean }) {
  const id = useId();
  return <fieldset className={styles.group} disabled={disabled}>
    <legend>Telegram 接收方式</legend>
    <div className={styles.options}>
      <label className={styles.option}>
        <input type="radio" name={id} value="long_polling" checked={value === "long_polling"} onChange={() => onChange("long_polling")} />
        <span><strong>长轮询</strong><small>新账户默认。Gateway 主动接收更新，无需公网入站回调；仍需出站网络。</small></span>
      </label>
      <label className={styles.option}>
        <input type="radio" name={id} value="webhook" checked={value === "webhook"} onChange={() => onChange("webhook")} />
        <span><strong>Webhook</strong><small>Telegram 推送到平台公开 HTTPS 入口，需要独立的入站校验凭据。</small></span>
      </label>
    </div>
  </fieldset>;
}
