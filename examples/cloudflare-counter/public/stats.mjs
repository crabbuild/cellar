// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
export const percentile = (values, p) => {
  const sorted = values.filter(Number.isFinite).toSorted((a,b)=>a-b);
  return sorted.length ? sorted[Math.max(0, Math.ceil(p*sorted.length)-1)] : null;
};
export function parseTimings(header) {
  const result = {};
  for (const entry of (header || "").split(",")) {
    const match = entry.trim().match(/^([\w-]+);\s*dur=([\d.]+)/);
    if (match) { const n = Number(match[2]); if (Number.isFinite(n) && n>=0) result[match[1]]=n; }
  }
  return result;
}
export function resourceTimings(entry) {
  if (!entry) return {};
  const duration=(start,end)=> Number.isFinite(start) && Number.isFinite(end) && end>=start ? end-start : null;
  return {
    dns:duration(entry.domainLookupStart,entry.domainLookupEnd),
    tcp:duration(entry.connectStart,entry.secureConnectionStart>0 ? entry.secureConnectionStart : entry.connectEnd),
    tls:entry.secureConnectionStart>0 ? duration(entry.secureConnectionStart,entry.connectEnd) : null,
    resource_ttfb:duration(entry.requestStart,entry.responseStart),
    download:duration(entry.responseStart,entry.responseEnd),
    transfer_bytes:entry.transferSize
  };
}
export function summary(samples, field) {
  const values=samples.filter(s=>s.ok).map(s=>s.timings[field]).filter(Number.isFinite);
  return {count:values.length,p50:percentile(values,.5),p95:percentile(values,.95),p99:percentile(values,.99)};
}
