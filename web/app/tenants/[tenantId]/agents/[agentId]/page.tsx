"use client";

import { useParams } from "next/navigation";

import { AgentWorkspace } from "../../../../../components/agents/agent-workspace";

export default function AgentWorkspacePage() {
  const { tenantId, agentId } = useParams<{ tenantId: string; agentId: string }>();
  return <AgentWorkspace agentId={agentId} tenantId={tenantId} />;
}
