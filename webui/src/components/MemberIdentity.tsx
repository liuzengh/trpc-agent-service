import type { MemberSummary } from '../api'

export function memberDisplayName(member: MemberSummary): string {
  const displayName = member.display_name?.trim()
  if (displayName && displayName !== member.platform_user_id && !looksLikeOpaqueID(displayName)) return displayName
  return member.email?.trim() || '未命名用户'
}

function looksLikeOpaqueID(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
}

function compactID(value: string): string {
  if (value.length <= 18) return value
  return `${value.slice(0, 8)}…${value.slice(-6)}`
}

export function memberProviderText(member: MemberSummary): string {
  return member.providers?.length ? member.providers.join(' · ') : '未关联'
}

export function formatMemberLastLogin(value: string): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

export function MemberIdentity({ member }: { member: MemberSummary }) {
  const name = memberDisplayName(member)
  const secondary = member.email?.trim() || `用户 ID · ${compactID(member.platform_user_id)}`
  return (
    <div className="member-person">
      <span className="avatar avatar-sm">{name.slice(0, 1).toUpperCase()}</span>
      <span>
        <strong>{name}</strong>
        <small title={member.email || member.platform_user_id}>{secondary}</small>
      </span>
    </div>
  )
}
