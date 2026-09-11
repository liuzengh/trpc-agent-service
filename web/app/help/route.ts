import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

export function GET() {
  const configured = process.env.DOCS_SITE_URL?.trim();
  if (configured) {
    try {
      const target = new URL(configured);
      if (["https:", "http:"].includes(target.protocol) && !target.username && !target.password && !target.search && !target.hash) {
        const response = NextResponse.redirect(target, 307);
        response.headers.set("Cache-Control", "no-store");
        response.headers.set("Referrer-Policy", "no-referrer");
        return response;
      }
    } catch { /* Render the same actionable setup page for a missing or invalid URL. */ }
  }
  return new Response(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>帮助文档 · Agent tRPC</title><body style="margin:0;background:#f7f9fc;color:#172b4d;font:16px/1.7 system-ui,sans-serif"><main style="max-width:600px;margin:12vh auto;padding:32px"><h1>帮助文档尚未配置</h1><p>请联系平台管理员设置 Web 服务的 <code>DOCS_SITE_URL</code>，填写已部署文档中心的完整 HTTP(S) 地址，然后重启 Web 服务。</p><p>配置后，“帮助文档”将在新标签页打开，当前工作区保持不变。</p></main></body></html>`, { status: 503, headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store", "Referrer-Policy": "no-referrer" } });
}
