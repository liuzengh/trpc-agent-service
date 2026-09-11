/*
 * 资产归属与可见性
 *
 * 租户资产（知识库 / Skill / IM 绑定 / 模型端点）是「作者所有」的：
 * - private：仅作者与租户管理员（admin/owner）可见；
 * - shared：作者主动共享后，租户内所有人可读，但仍只有作者与管理员可改/删。
 *
 * 后端按 JWT claims + 行内 created_by/visibility 双重校验，前端只负责把
 * 对应按钮显隐到一致，不作为安全边界。
 */

export type AssetVisibility = 'private' | 'shared'

/** 作者才可改/删，管理员可管全部；旧数据 created_by 为空时仅管理员可管。 */
export function canManageAsset(
  row: { created_by?: string } | null | undefined,
  userId: string | undefined,
  managesTenant: boolean,
): boolean {
  if (managesTenant) return true
  return !!row?.created_by && row.created_by === userId
}

/** 共享后的资产对租户只读可见。 */
export function isShared(row: { visibility?: string } | null | undefined): boolean {
  return row?.visibility === 'shared'
}

export function visibilityLabel(v: string | undefined): string {
  return v === 'shared' ? '共享' : '私有'
}
