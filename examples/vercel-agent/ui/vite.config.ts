import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath } from "node:url";
const gateway = "http://127.0.0.1:4318";
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  server: {
    proxy: {
      "/api": {
        target: gateway,
        changeOrigin: true,
        configure(proxy) {
          proxy.on("proxyReq", (outgoing, incoming) => {
            // Preserve foreign origins so the gateway still refuses cross-site writes.
            if (incoming.headers.origin === `http://${incoming.headers.host}`)
              outgoing.setHeader("origin", gateway);
          });
        },
      },
    },
  },
  build: { outDir: "dist", sourcemap: false },
});
