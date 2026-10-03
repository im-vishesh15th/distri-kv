// Decimal (SI) units, so "1 GB" is exactly 1,000,000,000 bytes.
const UNITS = ['B', 'KB', 'MB', 'GB', 'TB'] as const

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B'
  let i = 0
  let v = bytes
  while (v >= 1000 && i < UNITS.length - 1) {
    v /= 1000
    i++
  }
  const digits = i === 0 ? 0 : v >= 100 ? 0 : v >= 10 ? 1 : 2
  return `${v.toFixed(digits)} ${UNITS[i]}`
}

// Exact gigabytes with enough precision to see small tenants move.
export function bytesToGB(bytes: number): string {
  return (bytes / 1e9).toFixed(4)
}
