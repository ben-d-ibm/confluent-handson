import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// La UI es un bundle estático (web/dist) servido por Express. Solo usa rutas relativas /api/*.
export default defineConfig({
  root: 'web',
  plugins: [react()],
  build: { outDir: 'dist', emptyOutDir: true },
  test: {
    environment: 'jsdom',
    globals: false,
    css: false,
    setupFiles: ['./src/test-setup.js'],
    include: ['src/**/*.test.jsx'],
  },
});
