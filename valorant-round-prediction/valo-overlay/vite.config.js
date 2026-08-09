import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  base: './',
  plugins: [react()],
  server: {
    port: 5173,
    open: true,
    // Tell Vite's HMR to use a separate port so it never touches 8765
    hmr: {
      port: 5174,
    },
    // Reject any proxy attempts to the backend port from Vite itself
    proxy: {},
  },
  build: {
    outDir: 'dist',
  }
})
