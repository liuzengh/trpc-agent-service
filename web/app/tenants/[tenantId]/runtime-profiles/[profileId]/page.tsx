"use client";
import { useParams } from "next/navigation";
import { ProfileWorkspace } from "../../../../../components/runtime-profiles/profile-workspace";
export default function RuntimeProfileWorkspacePage() {
  const { tenantId, profileId } = useParams<{ tenantId: string; profileId: string }>();
  return <ProfileWorkspace key={`${tenantId}/${profileId}`} tenantId={tenantId} profileId={profileId} />;
}
