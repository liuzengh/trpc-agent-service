"use client";
import { useCallback, useEffect, useRef, useState } from "react";
import { controlApi } from "../../lib/control-api";
import { channelError, channelRead } from "../../lib/channel-api";
import { establishChannelIdentity } from "../../lib/channel-editor-state";

/** The API remains the authority; a denied write also immediately removes local write controls. */
export function useChannelAccess(tenantId: string) {
  const [state, setState] = useState({ userId: "", canWrite: false, loading: true, error: "" });
  const denialGeneration = useRef(0);
  const [epoch, setEpoch] = useState(0);
  const reload = useCallback(() => setEpoch((n) => n + 1), []);
  const denyWrites = useCallback(() => { ++denialGeneration.current; setState((s) => ({ ...s, canWrite: false })); }, []);
  useEffect(() => {
    let active = true; const generation = denialGeneration.current;
    setState({ userId: "", canWrite: false, loading: true, error: "" });
    void channelRead(Promise.all([controlApi.getMe(), controlApi.getTenant(tenantId)])).then(([session, tenant]) => {
      if (!active) return;
      try { establishChannelIdentity(sessionStorage, session.user.id); } catch { /* Form surfaces report unavailable recovery storage. */ }
      // Successful /me authenticates the account; its public user projection omits status.
      const accountActive = session.user.status === undefined || session.user.status === "ACTIVE";
      setState({ userId: session.user.id, canWrite: generation === denialGeneration.current && session.password_change_required === false && accountActive && tenant.id === tenantId && tenant.role === "OWNER" && tenant.status === "ACTIVE", loading: false, error: "" });
    }).catch((error: unknown) => { if (active) setState({ userId: "", canWrite: false, loading: false, error: channelError(error) }); });
    return () => { active = false; };
  }, [tenantId, epoch]);
  return { ...state, reload, denyWrites };
}
