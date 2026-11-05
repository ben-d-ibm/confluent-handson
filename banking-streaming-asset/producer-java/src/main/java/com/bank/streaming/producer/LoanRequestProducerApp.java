package com.bank.streaming.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Clock;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes synthetic loan requests (Avro + Schema Registry) to the existing loan-requests topic using the
 * producer-specific credentials. Does not score, decide, or touch infrastructure.
 *
 * Usage: java -jar loan-request-producer.jar [count] [intervalMs]
 *
 * Optional continuous demo mode: DEMO_TRAFFIC=true publishes one random request every
 * DEMO_TRAFFIC_INTERVAL_SECONDS (default 10) until the process is stopped (SIGTERM). Arguments are ignored.
 */
public final class LoanRequestProducerApp {

    private static final Logger LOG = LoggerFactory.getLogger(LoanRequestProducerApp.class);
    /** Seeded customers are 1001-1050 (postgres/init/02-seed.sql). */
    private static final int CUSTOMER_POOL = 50;

    private LoanRequestProducerApp() {}

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 10;
        long intervalMs = args.length > 1 ? Long.parseLong(args[1]) : 500L;
        String topic = optional(System::getenv, "TOPIC_LOAN_REQUESTS", "loan-requests");
        boolean demo = isDemoTraffic(System::getenv);
        long demoIntervalSeconds = demoIntervalSeconds(System::getenv);

        LoanRequestFactory factory = new LoanRequestFactory(new Random(), Clock.systemUTC(), CUSTOMER_POOL);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(buildConfig(System::getenv))) {
            if (demo) {
                runDemoTraffic(producer, topic, factory, demoIntervalSeconds);
                return;
            }
            for (int i = 0; i < count; i++) {
                LoanRequest fixed = fixedRequest(System::getenv, Clock.systemUTC());
                send(producer, topic, fixed != null ? fixed : factory.next());
                if (intervalMs > 0) {
                    Thread.sleep(intervalMs);
                }
            }
            producer.flush();
        }
    }

    /** Continuous mode: one request per interval until SIGTERM/SIGINT; the hook waits for a clean close. */
    private static void runDemoTraffic(KafkaProducer<String, GenericRecord> producer, String topic,
            LoanRequestFactory factory, long intervalSeconds) throws InterruptedException {
        CountDownLatch stop = new CountDownLatch(1);
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop.countDown();
            try {
                mainThread.join(5000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));
        LOG.info("Demo traffic started: 1 request every {}s", intervalSeconds);
        do {
            send(producer, topic, factory.next());
            producer.flush();
        } while (!stop.await(intervalSeconds, TimeUnit.SECONDS));
        LOG.info("Demo traffic stopped");
    }

    private static void send(KafkaProducer<String, GenericRecord> producer, String topic, LoanRequest req) {
        // Key by customer_id so a customer's requests stay ordered in one partition.
        String key = String.valueOf(req.customerId());
        producer.send(new ProducerRecord<>(topic, key, LoanRequestAvro.toRecord(req)), (md, ex) -> {
            if (ex != null) {
                LOG.error("Send failed for request_id={}", req.requestId(), ex);
            } else {
                LOG.info("Sent request_id={} partition={} offset={}",
                        req.requestId(), md.partition(), md.offset());
            }
        });
    }

    /** DEMO_TRAFFIC must be exactly "true" or "false" (default false); anything else is rejected. */
    static boolean isDemoTraffic(Function<String, String> env) {
        String v = optional(env, "DEMO_TRAFFIC", "false");
        if (v.equals("true")) {
            return true;
        }
        if (v.equals("false")) {
            return false;
        }
        throw new IllegalStateException("DEMO_TRAFFIC must be 'true' or 'false'");
    }

    /** DEMO_TRAFFIC_INTERVAL_SECONDS: positive integer, default 10. */
    static long demoIntervalSeconds(Function<String, String> env) {
        String v = optional(env, "DEMO_TRAFFIC_INTERVAL_SECONDS", "10");
        try {
            long seconds = Long.parseLong(v);
            if (seconds > 0) {
                return seconds;
            }
        } catch (NumberFormatException ignored) {
            // fall through
        }
        throw new IllegalStateException("DEMO_TRAFFIC_INTERVAL_SECONDS must be a positive integer");
    }

    /**
     * Optional deterministic request (smoke checks): if LOAN_CUSTOMER_ID is set, every request uses
     * LOAN_CUSTOMER_ID / LOAN_AMOUNT / LOAN_TERM_MONTHS / LOAN_PURPOSE (defaults 10000.00 / 24 / PERSONAL).
     * request_id and requested_at are still generated per request. Returns null when not set.
     */
    static LoanRequest fixedRequest(Function<String, String> env, Clock clock) {
        String customer = optional(env, "LOAN_CUSTOMER_ID", "");
        if (customer.isEmpty()) {
            return null;
        }
        return new LoanRequest(
                java.util.UUID.randomUUID().toString(),
                Integer.parseInt(customer),
                new java.math.BigDecimal(optional(env, "LOAN_AMOUNT", "10000.00")).setScale(2),
                Integer.parseInt(optional(env, "LOAN_TERM_MONTHS", "24")),
                optional(env, "LOAN_PURPOSE", "PERSONAL"),
                java.time.Instant.now(clock).truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
    }

    /** Variable names are the ones generated by scripts/gen-env.sh into .env.runtime. */
    static Properties buildConfig(Function<String, String> env) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, required(env, "KAFKA_BOOTSTRAP_SERVERS"));
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", "PLAIN");
        p.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + required(env, "PRODUCER_KAFKA_API_KEY") + "\" password=\""
                        + required(env, "PRODUCER_KAFKA_API_SECRET") + "\";");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // Schema Registry: use the schema already registered by Terraform (subject loan-requests-value).
        p.put("schema.registry.url", required(env, "SCHEMA_REGISTRY_URL"));
        p.put("basic.auth.credentials.source", "USER_INFO");
        p.put("basic.auth.user.info",
                required(env, "PRODUCER_SR_API_KEY") + ":" + required(env, "PRODUCER_SR_API_SECRET"));
        p.put("auto.register.schemas", "false");
        p.put("use.latest.version", "true");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "loan-request-producer");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        return p;
    }

    private static String required(Function<String, String> env, String name) {
        String v = env.apply(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return v;
    }

    private static String optional(Function<String, String> env, String name, String def) {
        String v = env.apply(name);
        return v == null || v.isBlank() ? def : v;
    }
}
