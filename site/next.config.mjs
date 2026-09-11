import { normalizeBasePath } from "./lib/urls.mjs";
const basePath = normalizeBasePath(process.env.SITE_BASE_PATH ?? "");
export default {
  output: "export",
  trailingSlash: true,
  basePath,
  env: { NEXT_PUBLIC_SITE_BASE_PATH: basePath },
};
