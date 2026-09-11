/** Normalize one build-time project path; a custom domain uses an empty path. */
export function normalizeBasePath(value = "") {
  const base = value.trim().replace(/\/+$/, "");
  if (!base) return "";
  if (!/^\/(?:[A-Za-z0-9_-][A-Za-z0-9_.-]*)(?:\/[A-Za-z0-9_-][A-Za-z0-9_.-]*)*$/.test(base)) throw new Error("Invalid SITE_BASE_PATH");
  return base;
}
export const basePath = normalizeBasePath(process.env.NEXT_PUBLIC_SITE_BASE_PATH ?? process.env.SITE_BASE_PATH ?? "");
/** @param {string} href */
export function siteUrl(href) {
  if (!href.startsWith("/") || href.startsWith("//")) return href;
  const normalized = href === "/docs" ? "/docs/" : href;
  return `${basePath}${normalized}`;
}
/** Prefix only site-root HTML links; fragment and relative links stay local. */
export function prefixLinks(html) {
  return html.replace(/href="(\/(?!\/)[^"]*)"/g, (_, href) => `href="${siteUrl(href)}"`);
}
