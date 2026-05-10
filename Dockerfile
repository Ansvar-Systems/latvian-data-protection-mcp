# ─────────────────────────────────────────────────────────────────────────────
# Latvian Data Protection MCP — multi-stage Dockerfile
# ─────────────────────────────────────────────────────────────────────────────
# Build:  docker build -t latvian-data-protection-mcp .
# Run:    docker run --rm -p 3000:3000 latvian-data-protection-mcp
#
# The image bakes a pre-built database at /app/data/dvi.db.
# Override with DVI_DB_PATH for a custom location.
# ─────────────────────────────────────────────────────────────────────────────

# --- Stage 1: Build TypeScript + native deps ---
FROM node:20-slim AS builder

WORKDIR /app

# Build deps for better-sqlite3 native binding
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
# Full install (with dev + scripts) so better-sqlite3 postinstall builds the .node binding
RUN npm ci

COPY tsconfig.json ./
COPY src/ src/
RUN npm run build

# Prune to production deps but keep the already-built native binding
RUN npm prune --omit=dev

# --- Stage 2: Production ---
FROM node:20-slim AS production

WORKDIR /app
ENV NODE_ENV=production
ENV DVI_DB_PATH=/app/data/dvi.db

# Bring across the built node_modules (with better-sqlite3 .node binding intact)
COPY --from=builder /app/node_modules/ node_modules/
COPY --from=builder /app/dist/ dist/
COPY package.json package-lock.json* ./

# Bake the database into the image
COPY data/database.db data/dvi.db

# Non-root user for security
RUN addgroup --system --gid 1001 mcp && \
    adduser --system --uid 1001 --ingroup mcp mcp && \
    chown -R mcp:mcp /app
USER mcp

# Health check: verify HTTP server responds
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

CMD ["node", "dist/src/http-server.js"]
