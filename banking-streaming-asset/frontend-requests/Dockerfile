# Build context = banking-streaming-asset/ (root of the asset), because the Avro schema is reused from
# terraform/schemas/loan-requests.avsc and not copied into this module.
#   podman build -f frontend-requests/Dockerfile -t loan-request-frontend:0.1.0 .
# Ignore rules: frontend-requests/Dockerfile.dockerignore (allowlist; no .env, .env.runtime, node_modules,
# web/dist, tfstate or secrets).
#
# One container, one process:  Express  ->  /  (React + Carbon static build)  +  /api/loan-requests  ->  Kafka
# Node 24 is the current LTS line (the project requires Node >= 22; @platformatic/kafka needs >= 22.22).
# registry.access.redhat.com images need no Red Hat authentication.

# ---- deps stage: production dependencies only (what the runtime needs) ----
FROM registry.access.redhat.com/ubi9/nodejs-24-minimal:latest AS deps
USER 0
RUN mkdir -p /app/frontend-requests && chown -R 1001:0 /app
USER 1001
WORKDIR /app/frontend-requests
COPY --chown=1001:0 frontend-requests/package.json frontend-requests/package-lock.json ./
# --ignore-scripts: no install-time scripts (e.g. vendor telemetry) run inside the build.
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund

# ---- build stage: offline tests + React production build ----
FROM registry.access.redhat.com/ubi9/nodejs-24-minimal:latest AS build
USER 0
RUN mkdir -p /app/frontend-requests /app/terraform/schemas && chown -R 1001:0 /app
USER 1001
WORKDIR /app/frontend-requests
COPY --chown=1001:0 frontend-requests/package.json frontend-requests/package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY --chown=1001:0 terraform/schemas/loan-requests.avsc /app/terraform/schemas/loan-requests.avsc
COPY --chown=1001:0 frontend-requests/vite.config.js ./
COPY --chown=1001:0 frontend-requests/src ./src
COPY --chown=1001:0 frontend-requests/test ./test
COPY --chown=1001:0 frontend-requests/web ./web
# The image is only produced if API + UI tests pass (all offline), then the static bundle is built.
RUN npm test && npm run build:web

# ---- runtime stage: Express serving web/dist + API ----
FROM registry.access.redhat.com/ubi9/nodejs-24-minimal:latest
ENV NODE_ENV=production
# Same layout as the repo, so the code finds its files with its default relative paths:
#   /app/frontend-requests/{src,web/dist,node_modules}  and  /app/terraform/schemas/loan-requests.avsc
WORKDIR /app/frontend-requests
COPY --from=deps  --chown=1001:0 /app/frontend-requests/node_modules ./node_modules
COPY --from=build --chown=1001:0 /app/frontend-requests/package.json ./package.json
COPY --from=build --chown=1001:0 /app/frontend-requests/src ./src
COPY --from=build --chown=1001:0 /app/frontend-requests/web/dist ./web/dist
COPY --from=build --chown=1001:0 /app/terraform/schemas/loan-requests.avsc /app/terraform/schemas/loan-requests.avsc

# Non-root (UID 1001 = default user of the Red Hat Node.js images; GID 0, no extra privileges).
USER 1001

# Configuration is injected at runtime only (never baked into the image):
#   KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, REQUESTS_API_KAFKA_API_KEY/SECRET,
#   REQUESTS_API_SR_API_KEY/SECRET, TOPIC_LOAN_REQUESTS, PORT (optional, default 3000)
# Express binds to 127.0.0.1 (fixed in src/server.js); the container networking model is decided later.
# `node` as PID 1 receives SIGTERM directly, which triggers the clean shutdown in src/server.js.
CMD ["node", "src/server.js"]
