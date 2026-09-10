/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The production bundle is served by the Go binary under /console/.
// During development `npm run dev` proxies API calls to :8080.
export default defineConfig({
  base: '/console/',
  resolve: {
    dedupe: ['react', 'react-dom'],
  },
  plugins: [
    react(),
    {
      name: 'redirect-console-slash',
      configureServer(server) {
        server.middlewares.use((req, res, next) => {
          const rawUrl = req.url ?? ''
          const [pathname, search] = rawUrl.split('?')
          if (pathname === '/console') {
            const query = search ? `?${search}` : ''
            res.writeHead(302, { Location: `/console/${query}` })
            res.end()
            return
          }
          next()
        })
      },
    },
  ],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: true,
      },
    },
  },
  test: {
    coverage: {
      provider: 'v8',
      all: true,
      // Pure domain/state modules are owned by Vitest. React pages and their
      // integrations are owned by the Playwright quality gate in tests/browser.
      include: [
        'src/access.ts',
        'src/botDraft.ts',
        'src/execution.ts',
        'src/components/Markdown.tsx',
        'src/components/modelCatalog.ts',
      ],
      reporter: ['text', 'json-summary', 'html'],
      reportsDirectory: 'coverage',
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: 'assets/app.js',
        chunkFileNames: 'assets/[name].js',
        assetFileNames: 'assets/[name][extname]',
      },
    },
  },
})
