import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";

export default defineConfig({
  plugins: [
    react(),
    {
      name: "console-csp-styles",
      enforce: "pre",
      resolveId(source, importer) {
        const from = importer?.replaceAll("\\", "/") || "";
        if (from.endsWith("/src/cspStyles.ts")) return;
        const direct =
          /^@rc-component\/util\/es\/Dom\/dynamicCSS(?:\.js)?$/.test(source);
        const relative =
          from.includes("/@rc-component/util/es/") &&
          (source === "./Dom/dynamicCSS" || source === "./Dom/dynamicCSS.js");
        if (direct || relative)
          return fileURLToPath(new URL("./src/cspStyles.ts", import.meta.url));
      },
    },
  ],
  base: "/admin/ui/",
  build: {
    outDir: "../../admin/ui/dist",
    emptyOutDir: true,
    manifest: true,
    sourcemap: false,
  },
  server: {
    proxy: { "^/admin/(?!ui(?:/|$))": { target: "http://127.0.0.1:8080" } },
  },
});
