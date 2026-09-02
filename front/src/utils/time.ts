// 统一北京时间格式化：数据库存储 UTC，浏览器本地时区可能不同，这里强制
// 以 Asia/Shanghai 展示，保证前端所有时间列一致。
export function formatBeijingTime(iso?: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false })
}
