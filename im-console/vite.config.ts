import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// base 必须是 /static/：构建产物嵌入 Go 服务的 web/static，
// 由 GET /static/{asset...} 路由提供（见 trpcservice/web/server.go）。
export default defineConfig({
  base: '/static/',
  plugins: [react()],
  server: {
    port: 5173,
    // 开发模式下把后端 API 代理到本地 Go 服务（ADMIN_TOKEN 模式）
    proxy: {
      '/admin': 'http://localhost:8080',
      '/v1': 'http://localhost:8080',
      '/metrics': 'http://localhost:8080',
      '/healthz': 'http://localhost:8080',
      '/readyz': 'http://localhost:8080',
    },
  },
  build: {
    outDir: 'dist',
    chunkSizeWarningLimit: 700,
  },
});
