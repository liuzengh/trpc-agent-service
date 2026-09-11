// Metrics API: fetches the Prometheus text exposition from /metrics and
// parses it into rows. No auth: the endpoint is public by design.
import type { MetricRow } from '../types';

export async function fetchMetrics(): Promise<MetricRow[]> {
  const res = await fetch('/metrics');
  if (!res.ok) throw new Error(`/metrics ${res.status}`);
  return parsePromText(await res.text());
}

export function parsePromText(text: string): MetricRow[] {
  const rows: MetricRow[] = [];
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const m = trimmed.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+(-?[\d.eE+]+)$/);
    if (!m) continue;
    const labels: Record<string, string> = {};
    if (m[3]) {
      for (const pair of m[3].split(',')) {
        const idx = pair.indexOf('=');
        if (idx < 0) continue;
        const key = pair.slice(0, idx).trim();
        const value = pair.slice(idx + 1).trim().replace(/^"|"$/g, '');
        if (key) labels[key] = value;
      }
    }
    rows.push({ name: m[1], labels, value: Number(m[4]) });
  }
  return rows;
}

/** Sum of every row matching name (and, when given, exact label equality). */
export function sumMetric(rows: MetricRow[], name: string, labels?: Record<string, string>): number {
  return rows
    .filter(
      (row) =>
        row.name === name &&
        (!labels || Object.entries(labels).every(([k, v]) => row.labels[k] === v)),
    )
    .reduce((sum, row) => sum + row.value, 0);
}
