export function combinedRunNameFromSourceId(sourceId) {
  const normalized = String(sourceId || '')
    .trim()
    .replace(/[^A-Za-z0-9._-]+/gu, '-')
    .replace(/-+/gu, '-')
    .replace(/^-|-$/gu, '');
  return `${normalized || 'source'}-combined`;
}
