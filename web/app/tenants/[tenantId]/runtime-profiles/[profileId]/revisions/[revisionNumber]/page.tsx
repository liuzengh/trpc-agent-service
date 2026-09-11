"use client";

import { useParams } from "next/navigation";

import { ProfileRevisionDetail } from "../../../../../../../components/runtime-profiles/profile-revision";

export default function ProfileRevisionPage() {
  const { tenantId, profileId, revisionNumber } = useParams<{ tenantId: string; profileId: string; revisionNumber: string }>();
  const parsedRevision = /^[1-9]\d*$/.test(revisionNumber) ? Number(revisionNumber) : Number.NaN;
  return <ProfileRevisionDetail key={`${tenantId}/${profileId}/${revisionNumber}`} tenantId={tenantId} profileId={profileId} revisionNumber={parsedRevision} />;
}
