"use client";
import { useParams } from "next/navigation";
import { ProfileList } from "../../../../components/runtime-profiles/profile-list";
export default function RuntimeProfilesPage() {
  const { tenantId } = useParams<{ tenantId: string }>();
  return <ProfileList key={tenantId} tenantId={tenantId} />;
}
