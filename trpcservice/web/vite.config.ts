import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "node:path";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: "/",
  resolve: { alias: { "@": path.resolve(process.cwd(), "./src") } },
  server: { proxy: { "/admin": "http://localhost:8080" } },
});
