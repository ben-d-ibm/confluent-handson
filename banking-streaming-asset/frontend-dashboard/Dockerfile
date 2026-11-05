# Build context = banking-streaming-asset/ (root of the asset), same as frontend-requests.
#   podman build -f frontend-dashboard/Dockerfile -t loan-decision-dashboard:0.1.0 .
# Ignore rules: frontend-dashboard/Dockerfile.dockerignore (allowlist; no .env, .env.runtime, node_modules,
# web/dist, tfstate or secrets).
#
# One container, one process:  Express  ->  /  (React + Carbon static build)  +  /api/*  <-  Kafka consumer
# Node 24 is the current LTS line (the project requires Node >= 22; @platformatic/kafka needs >= 22.22).
# registry.access.redhat.com images need no Red Hat authentication.

# ---- deps stage: production dependencies only (what the runtime needs) ----
FROM registry.access.redhat.com/ubi9/nodejs-24-minimal:latest AS deps
USER 0
RUN mkdir -p /app/frontend-dashboard && chown -R 1001:0 /app
USER 1001
WORKDIR /app/frontend-dashboard
COPY --chown=1001:0 frontend-dashboard/package.json frontend-dashboard/package-lock.json ./
# --ignore-scripts: no install-time scripts (e.g. vendor telemetry) run inside the build.
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund

# ---- build stage: offline tests + React production build ----
FROM registry.access.redhat.com/ubi9/nodejs-24-minimal:latest AS build
USER 0
RUN mkdir -p /app/frontend-dashboard && chown -R 1001:0 /app
USER 1001
WORKDIR /app/frontend-dashboard
COPY --chown=1001:0 frontend-dashboard/package.json frontend-dashboard/package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY --chown=1001:0 frontend-dashboard/vite.config.js ./
COPY --chown=1001:0 frontend-dashboard/src ./src
COPY --chown=1001:0 frontend-dashboard/test ./test
COPY --chown=1001:0 frontend-dashboard/web ./web
# The image is only produced if API + UI tests pass (all offline), then the static bundle is built.
RUN npm test && npm run build

# ---- runtime stage: Express serving web/dist + API ----
FROM registry.access.redhat.com/ubi9/nodejs-24-minimal:latest
ENV NODE_ENV=production
WORKDIR /app/frontend-dashboard
COPY --from=deps  --chown=1001:0 /app/frontend-dashboard/node_modules ./node_modules
COPY --from=build --chown=1001:0 /app/frontend-dashboard/package.json ./package.json
COPY --from=build --chown=1001:0 /app/frontend-dashboard/src ./src
COPY --from=build --chown=1001:0 /app/frontend-dashboard/web/dist ./web/dist

# Non-root (UID 1001 = default user of the Red Hat Node.js images; GID 0, no extra privileges).
USER 1001

# Configuration is injected at runtime only (never baked into the image):
#   KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, DASHBOARD_API_KAFKA_API_KEY/SECRET,
#   DASHBOARD_API_SR_API_KEY/SECRET, DASHBOARD_API_GROUP_PREFIX, TOPIC_LOAN_DECISIONS,
#   HOST (127.0.0.1 | 0.0.0.0, default 127.0.0.1), PORT (default 3001)
# `node` as PID 1 receives SIGTERM directly, which triggers the clean shutdown in src/server.js.
CMD ["node", "src/server.js"]
