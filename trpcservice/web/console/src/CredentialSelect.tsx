import { useEffect, useState } from "react";
import { Alert, Button, Select } from "antd";
import { api, errorText } from "./api";
import type { Page } from "./types";

// The browser receives authorized references only, not credential values.
export function CredentialSelect({
  tenant,
  purpose,
  value,
  onChange,
  disabled,
}: {
  tenant: string;
  purpose: string;
  value?: string;
  onChange?: (value: string) => void;
  disabled?: boolean;
}) {
  const [refs, setRefs] = useState<string[]>([]);
  const [error, setError] = useState("");
  const [next, setNext] = useState("");
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    let live = true;
    setRefs([]);
    setError("");
    setBusy(true);
    setNext("");
    api<Page<{ reference: string }>>("resources/list", {
      tenant_id: tenant,
      kind: "credentials",
      purpose,
    })
      .then((data) => {
        if (live) {
          setRefs(data.items.map((item) => item.reference));
          setNext(data.next || "");
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
  }, [tenant, purpose, refresh]);
  return (
    <>
      <Select
        style={{ width: "100%" }}
        showSearch
        allowClear
        disabled={disabled || busy || !!error}
        loading={busy}
        value={value || undefined}
        onChange={(v) => onChange?.(v || "")}
        options={refs.map((ref) => ({ value: ref, label: ref }))}
        placeholder="选择部署者已授权的凭据引用"
        notFoundContent="暂无授权；由部署者配置 grant 后刷新"
      />
      {!busy && value && !refs.includes(value) && !next && (
        <Alert
          type="warning"
          title="当前引用不在此用途的授权清单内，请重新选择；原配置尚未修改。"
        />
      )}
      {error && <Alert type="warning" title={error} />}
      <Button type="link" size="small" onClick={() => setRefresh((v) => v + 1)}>
        刷新授权清单
      </Button>
      {next && (
        <Button
          type="link"
          size="small"
          loading={busy}
          onClick={async () => {
            setBusy(true);
            try {
              const data = await api<Page<{ reference: string }>>(
                "resources/list",
                {
                  tenant_id: tenant,
                  kind: "credentials",
                  purpose,
                  after: next,
                },
              );
              setRefs((old) => [
                ...new Set([...old, ...data.items.map((i) => i.reference)]),
              ]);
              setNext(data.next || "");
            } catch (e) {
              setError(errorText(e));
            } finally {
              setBusy(false);
            }
          }}
        >
          更多引用
        </Button>
      )}
    </>
  );
}
