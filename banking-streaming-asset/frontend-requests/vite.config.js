import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// La UI es un bundle estático (web/dist) pensado para servirse desde Express.
// Las llamadas a la API usan siempre la ruta relativa /api/loan-requests (sin hosts ni puertos).
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
