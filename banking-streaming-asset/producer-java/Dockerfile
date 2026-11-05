# Build context = banking-streaming-asset/ (repo root of the asset), because the Avro schema is reused from
# terraform/schemas/loan-requests.avsc and not copied into this module.
#   podman build -f producer-java/Dockerfile -t loan-request-producer:0.1.0 .
# Ignore rules: producer-java/Dockerfile.dockerignore (allowlist; no .env, .env.runtime, target/ or secrets).

# ---- build stage: UBI 9 + OpenJDK 17 + Maven ----
FROM registry.access.redhat.com/ubi9/openjdk-17:latest AS build
WORKDIR /build
COPY --chown=185:0 terraform/schemas/loan-requests.avsc terraform/schemas/loan-requests.avsc
COPY --chown=185:0 producer-java/pom.xml producer-java/pom.xml
COPY --chown=185:0 producer-java/src producer-java/src
# Unit tests are run separately (mvn test); the image build only packages the shaded JAR.
RUN mvn -B -f producer-java/pom.xml -DskipTests package

# ---- runtime stage: UBI 9 OpenJDK 17 runtime (JRE only) ----
FROM registry.access.redhat.com/ubi9/openjdk-17-runtime:latest
WORKDIR /app
COPY --from=build --chown=185:0 /build/producer-java/target/loan-request-producer-0.1.0.jar /app/loan-request-producer.jar

# Non-root (UID 185 = default user of the Red Hat OpenJDK images).
USER 185

# Configuration is injected at runtime only (never baked into the image):
#   KAFKA_BOOTSTRAP_SERVERS, PRODUCER_KAFKA_API_KEY, PRODUCER_KAFKA_API_SECRET,
#   SCHEMA_REGISTRY_URL, PRODUCER_SR_API_KEY, PRODUCER_SR_API_SECRET, TOPIC_LOAN_REQUESTS (optional)
# Optional args: [count=10] [intervalMs=500]
ENTRYPOINT ["java", "-jar", "/app/loan-request-producer.jar"]
